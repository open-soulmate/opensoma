use axum::{
    extract::{Path, Query, State},
    http::{HeaderValue, Method, StatusCode},
    middleware::{self, Next},
    response::Response,
    routing::{get, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::info;

/// CORS middleware — allows cross-origin requests from OpenMate and other clients.
async fn cors_layer(req: axum::extract::Request, next: Next) -> Response {
    let is_options = req.method() == Method::OPTIONS;
    let mut resp = if is_options {
        // Respond to preflight immediately
        Response::new(axum::body::Body::empty())
    } else {
        next.run(req).await
    };
    let headers = resp.headers_mut();
    headers.insert(
        "access-control-allow-origin",
        HeaderValue::from_static("*"),
    );
    headers.insert(
        "access-control-allow-methods",
        HeaderValue::from_static("GET, POST, PUT, DELETE, OPTIONS"),
    );
    headers.insert(
        "access-control-allow-headers",
        HeaderValue::from_static("content-type, authorization"),
    );
    headers.insert(
        "access-control-max-age",
        HeaderValue::from_static("86400"),
    );
    if is_options {
        *resp.status_mut() = StatusCode::NO_CONTENT;
    }
    resp
}

/// Shared state for the status server.
#[derive(Clone)]
pub struct StatusServerState {
    pub node_id: String,
    pub start_time: std::time::Instant,
    pub events_collected: Arc<RwLock<u64>>,
    pub events_synced: Arc<RwLock<u64>>,
    pub connectors_active: Arc<RwLock<Vec<String>>>,
    pub last_error: Arc<RwLock<Option<String>>>,
    /// Connector enable/disable toggle state.
    pub connector_enabled: Arc<RwLock<HashMap<String, bool>>>,
    /// Per-connector event counts for monitoring.
    pub connector_event_counts: Arc<RwLock<HashMap<String, u64>>>,
    /// Cache statistics snapshot (updated periodically by sync engine).
    pub cache_stats: Arc<RwLock<CacheStatsSnapshot>>,
    /// Direct cache handle for event search queries (read-only usage).
    pub cache: Option<crate::sync::cache::Cache>,
    /// Pipeline metrics collector for internal pipeline tracking.
    pub pipeline_metrics: Option<crate::metrics::PipelineMetrics>,
    /// Connector health checker.
    pub health_checker: Option<crate::health::HealthChecker>,
    /// Plugin registry for dynamic plugin management.
    pub plugin_registry: Option<std::sync::Arc<crate::plugins::PluginRegistry>>,
    /// Sanitized config snapshot for the /api/config endpoint (secrets redacted).
    pub config_snapshot: Option<ConfigSnapshot>,
    /// Circuit breaker registry for connector resilience monitoring.
    pub circuit_breakers: Option<crate::connector::circuit_breaker::CircuitBreakerRegistry>,
}

/// Snapshot of cache statistics for the status API.
#[derive(Clone, Serialize, Deserialize, Default)]
pub struct CacheStatsSnapshot {
    pub total: usize,
    pub uploaded: usize,
    pub pending: usize,
    pub cache_size_bytes: u64,
}

#[derive(Serialize)]
struct HealthResponse {
    status: String,
    component: String,
    node_id: String,
    uptime_seconds: u64,
}

#[derive(Serialize)]
struct StatusResponse {
    component: String,
    node_id: String,
    uptime_seconds: u64,
    events_collected: u64,
    events_synced: u64,
    connectors_active: Vec<String>,
    last_error: Option<String>,
    hostname: String,
    ip: String,
    cpu_percent: f32,
    memory_used_mb: u64,
    memory_total_mb: u64,
}

#[derive(Serialize)]
struct ConnectorInfo {
    id: String,
    name: String,
    enabled: bool,
    status: String,
    event_count: u64,
}

#[derive(Deserialize)]
struct ToggleRequest {
    enabled: bool,
}

/// Sanitized config snapshot for the /api/config endpoint.
/// All secrets and tokens are redacted for safety.
#[derive(Clone, Serialize, Deserialize)]
pub struct ConfigSnapshot {
    pub node_id: String,
    pub log_level: String,
    pub data_dir: String,
    pub status_port: u16,
    pub soul_endpoint: String,
    pub heartbeat_interval: u64,
    pub connect_timeout: u64,
    pub watch_dirs: Vec<String>,
    pub include_patterns: Vec<String>,
    pub exclude_patterns: Vec<String>,
    pub debounce_ms: u64,
    pub process_interval_ms: u64,
    pub network_interval_ms: u64,
    pub clipboard_interval_ms: u64,
    pub sync_batch_size: usize,
    pub sync_upload_interval: u64,
    pub sync_max_retries: u32,
    pub sync_retry_backoff_ms: u64,
    pub sync_cache_size_mb: u64,
    pub connectors: Vec<ConnectorConfigSummary>,
}

/// Per-connector config summary (secrets redacted).
#[derive(Clone, Serialize, Deserialize)]
pub struct ConnectorConfigSummary {
    pub name: String,
    pub enabled: bool,
    #[serde(default)]
    pub extra: HashMap<String, String>,
}

impl ConfigSnapshot {
    /// Build a sanitized snapshot from the app config.
    pub fn from_config(config: &crate::config::AppConfig) -> Self {
        let mut connectors = Vec::new();

        macro_rules! summarize_connector {
            ($name:expr, $cfg:expr) => {
                if let Some(ref c) = $cfg {
                    connectors.push(ConnectorConfigSummary {
                        name: $name.to_string(),
                        enabled: c.enabled,
                        extra: HashMap::new(),
                    });
                }
            };
        }

        summarize_connector!("feishu", config.connector.feishu);
        summarize_connector!("dingtalk", config.connector.dingtalk);
        summarize_connector!("wecom", config.connector.wecom);
        summarize_connector!("rss", config.connector.rss);
        summarize_connector!("email", config.connector.email);
        summarize_connector!("notion", config.connector.notion);
        summarize_connector!("git", config.connector.git);
        summarize_connector!("obsidian", config.connector.obsidian);
        summarize_connector!("webhook", config.connector.webhook);
        summarize_connector!("github", config.connector.github);
        summarize_connector!("slack", config.connector.slack);
        summarize_connector!("telegram", config.connector.telegram);
        summarize_connector!("discord", config.connector.discord);

        Self {
            node_id: config.daemon.node_id.clone(),
            log_level: config.daemon.log_level.clone(),
            data_dir: config.daemon.data_dir.clone(),
            status_port: config.daemon.status_port,
            soul_endpoint: config.soul.endpoint.clone(),
            heartbeat_interval: config.soul.heartbeat_interval,
            connect_timeout: config.soul.connect_timeout,
            watch_dirs: config.collector.watch_dirs.clone(),
            include_patterns: config.collector.include.clone(),
            exclude_patterns: config.collector.exclude.clone(),
            debounce_ms: config.collector.debounce_ms,
            process_interval_ms: config.collector.process_interval_ms,
            network_interval_ms: config.collector.network_interval_ms,
            clipboard_interval_ms: config.collector.clipboard_interval_ms,
            sync_batch_size: config.sync.batch_size,
            sync_upload_interval: config.sync.upload_interval,
            sync_max_retries: config.sync.max_retries,
            sync_retry_backoff_ms: config.sync.retry_backoff_ms,
            sync_cache_size_mb: config.sync.cache_size_mb,
            connectors,
        }
    }
}

/// Re-export web assets from the centralized web module.
use crate::web;

/// Start the HTTP status server on the given port.
/// Exposes /health, /status, /api/* endpoints and the web UI.
/// Build the axum router for testing or embedding.
pub fn build_router(state: StatusServerState) -> Router {
    Router::new()
        .route("/", get(index_handler))
        .route("/shared-sidebar.css", get(css_handler))
        .route("/admin-framework.css", get(css_handler))
        .route("/admin-framework.js", get(js_handler))
        .route("/sidebar.css", get(sidebar_css_handler))
        .route("/sidebar.js", get(sidebar_js_handler))
        .route("/health", get(health_handler))
        .route("/status", get(status_handler))
        .route("/metrics", get(metrics_handler))
        .route("/api/status", get(api_status_handler))
        .route("/api/connectors", get(api_connectors_handler))
        .route("/api/collectors", get(api_collectors_handler))
        .route("/api/connectors/:name/toggle", post(api_connector_toggle))
        .route("/api/connectors/:name/events", get(api_connector_events))
        .route("/api/cache/stats", get(api_cache_stats_handler))
        .route("/api/cache/evict", post(api_cache_evict_handler))
        .route("/api/page/:page", get(api_page_handler))
        .route("/api/events/recent", get(api_events_recent_handler))
        .route("/api/events/search", get(api_events_search_handler))
        .route("/api/system/info", get(api_system_info_handler))
        .route("/api/connectors/health", get(api_connectors_health_handler))
        .route("/api/pipeline/metrics", get(api_pipeline_metrics_handler))
        .route("/api/plugins", get(api_plugins_handler))
        .route("/api/circuit-breakers", get(api_circuit_breakers_handler))
        .route("/api/config", get(api_config_handler))
        .layer(middleware::from_fn(cors_layer))
        .with_state(state)
}

pub async fn start_status_server(
    port: u16,
    state: StatusServerState,
) -> tokio::task::JoinHandle<()> {
    let app = build_router(state);

    let addr = format!("0.0.0.0:{}", port);
    info!("OpenSoma status server listening on {}", addr);

    tokio::spawn(async move {
        let listener = tokio::net::TcpListener::bind(&addr)
            .await
            .expect("Failed to bind status server");
        axum::serve(listener, app)
            .await
            .expect("Status server failed");
    })
}

/// Serve the embedded index.html
async fn index_handler() -> axum::response::Response {
    axum::response::Response::builder()
        .header("content-type", "text/html; charset=utf-8")
        .body(axum::body::Body::from(web::INDEX_HTML))
        .unwrap()
}

/// Serve the shared sidebar CSS
async fn css_handler() -> axum::response::Response {
    axum::response::Response::builder()
        .header("content-type", "text/css; charset=utf-8")
        .body(axum::body::Body::from(web::ADMIN_CSS))
        .unwrap()
}

/// Serve the admin framework JS
async fn js_handler() -> axum::response::Response {
    axum::response::Response::builder()
        .header("content-type", "application/javascript; charset=utf-8")
        .body(axum::body::Body::from(web::ADMIN_JS))
        .unwrap()
}

/// Serve the shared sidebar CSS
async fn sidebar_css_handler() -> axum::response::Response {
    axum::response::Response::builder()
        .header("content-type", "text/css; charset=utf-8")
        .body(axum::body::Body::from(web::SIDEBAR_CSS))
        .unwrap()
}

/// Serve the shared sidebar JS
async fn sidebar_js_handler() -> axum::response::Response {
    axum::response::Response::builder()
        .header("content-type", "application/javascript; charset=utf-8")
        .body(axum::body::Body::from(web::SIDEBAR_JS))
        .unwrap()
}

async fn health_handler(State(state): State<StatusServerState>) -> Json<HealthResponse> {
    Json(HealthResponse {
        status: "ok".to_string(),
        component: "OpenSoma".to_string(),
        node_id: state.node_id.clone(),
        uptime_seconds: state.start_time.elapsed().as_secs(),
    })
}

async fn status_handler(State(state): State<StatusServerState>) -> Json<StatusResponse> {
    build_status_response(&state).await
}

/// /api/status — same as /status but under /api/ namespace for the web UI
async fn api_status_handler(State(state): State<StatusServerState>) -> Json<StatusResponse> {
    build_status_response(&state).await
}

/// /api/connectors — list all known connectors with their active state and event counts
async fn api_connectors_handler(
    State(state): State<StatusServerState>,
) -> Json<Vec<ConnectorInfo>> {
    let active = state.connectors_active.read().await.clone();
    let toggle_state = state.connector_enabled.read().await.clone();
    let event_counts = state.connector_event_counts.read().await.clone();
    let all_connectors = [
        ("feishu", "飞书"),
        ("dingtalk", "钉钉"),
        ("wecom", "企业微信"),
        ("rss", "RSS"),
        ("email", "邮件"),
        ("webhook", "Webhook"),
        ("github", "GitHub"),
        ("notion", "Notion"),
        ("git", "Git"),
        ("obsidian", "Obsidian"),
        ("slack", "Slack"),
        ("telegram", "Telegram"),
        ("discord", "Discord"),
    ];

    let list: Vec<ConnectorInfo> = all_connectors
        .iter()
        .map(|(id, name)| {
            let is_active = active.contains(&id.to_string());
            let is_enabled = toggle_state.get(*id).copied().unwrap_or(true);
            ConnectorInfo {
                id: id.to_string(),
                name: name.to_string(),
                enabled: is_enabled,
                status: if is_active && is_enabled {
                    "running".to_string()
                } else if !is_enabled {
                    "disabled".to_string()
                } else {
                    "stopped".to_string()
                },
                event_count: event_counts.get(*id).copied().unwrap_or(0),
            }
        })
        .collect();

    Json(list)
}

/// /api/collectors — list collector status
async fn api_collectors_handler(
    State(_state): State<StatusServerState>,
) -> Json<Vec<ConnectorInfo>> {
    let collectors = [
        ("file", "文件采集器"),
        ("process", "进程采集器"),
        ("network", "网络采集器"),
        ("clipboard", "剪贴板采集器"),
    ];

    let list: Vec<ConnectorInfo> = collectors
        .iter()
        .map(|(id, name)| ConnectorInfo {
            id: id.to_string(),
            name: name.to_string(),
            enabled: true,
            status: "running".to_string(),
            event_count: 0,
        })
        .collect();

    Json(list)
}

/// /api/connectors/{name}/toggle — toggle a connector on/off.
/// Updates shared state so the connector can be dynamically enabled/disabled.
async fn api_connector_toggle(
    Path(name): Path<String>,
    State(state): State<StatusServerState>,
    Json(payload): Json<ToggleRequest>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    let valid_connectors = [
        "feishu", "dingtalk", "wecom", "rss", "email", "webhook", "github", "notion", "git",
        "obsidian", "slack", "telegram", "discord",
    ];

    if !valid_connectors.contains(&name.as_str()) {
        return Err(StatusCode::NOT_FOUND);
    }

    info!("Connector '{}' toggle: enabled={}", name, payload.enabled);

    // Update the shared toggle state
    let mut toggle = state.connector_enabled.write().await;
    toggle.insert(name.clone(), payload.enabled);

    // Update active list
    let mut active = state.connectors_active.write().await;
    if payload.enabled {
        if !active.contains(&name) {
            active.push(name.clone());
        }
    } else {
        active.retain(|n| n != &name);
    }

    Ok(Json(serde_json::json!({
        "connector": name,
        "enabled": payload.enabled,
        "status": "ok"
    })))
}

/// /api/connectors/{name}/events — get event count for a specific connector
async fn api_connector_events(
    Path(name): Path<String>,
    State(state): State<StatusServerState>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    let counts = state.connector_event_counts.read().await;
    let count = counts.get(&name).copied().unwrap_or(0);

    Ok(Json(serde_json::json!({
        "connector": name,
        "event_count": count,
    })))
}

/// Build the shared status response
async fn build_status_response(state: &StatusServerState) -> Json<StatusResponse> {
    let events_collected = *state.events_collected.read().await;
    let events_synced = *state.events_synced.read().await;
    let connectors = state.connectors_active.read().await.clone();
    let last_error = state.last_error.read().await.clone();

    // Collect system metrics
    let mut sys = sysinfo::System::new_all();
    sys.refresh_all();
    let cpu_percent = sys.global_cpu_usage();
    let memory_total_mb = sys.total_memory() / 1024 / 1024;
    let memory_used_mb = sys.used_memory() / 1024 / 1024;
    let hostname = sysinfo::System::host_name().unwrap_or_else(|| "unknown".to_string());
    let ip = local_ip_address::local_ip()
        .map(|ip| ip.to_string())
        .unwrap_or_else(|_| "127.0.0.1".to_string());

    Json(StatusResponse {
        component: "OpenSoma".to_string(),
        node_id: state.node_id.clone(),
        uptime_seconds: state.start_time.elapsed().as_secs(),
        events_collected,
        events_synced,
        connectors_active: connectors,
        last_error,
        hostname,
        ip,
        cpu_percent,
        memory_used_mb,
        memory_total_mb,
    })
}

/// /metrics — Prometheus-compatible metrics endpoint.
/// Returns metrics in Prometheus text exposition format without requiring
/// an external prometheus crate dependency.
async fn metrics_handler(State(state): State<StatusServerState>) -> axum::response::Response {
    let events_collected = *state.events_collected.read().await;
    let events_synced = *state.events_synced.read().await;
    let connectors = state.connectors_active.read().await.clone();
    let uptime = state.start_time.elapsed().as_secs_f64();
    let event_counts = state.connector_event_counts.read().await.clone();

    // System metrics
    let mut sys = sysinfo::System::new_all();
    sys.refresh_all();
    let cpu_percent = sys.global_cpu_usage() as f64;
    let memory_total_bytes = sys.total_memory();
    let memory_used_bytes = sys.used_memory();

    let mut lines: Vec<String> = Vec::new();

    // Metadata
    lines.push("# HELP opensoma_info OpenSoma daemon information.".to_string());
    lines.push("# TYPE opensoma_info gauge".to_string());
    lines.push(format!(
        "opensoma_info{{node_id=\"{}\",version=\"{}\",git_hash=\"{}\",branch=\"{}\"}} 1",
        state.node_id,
        crate::build_info::VERSION,
        crate::build_info::GIT_HASH,
        crate::build_info::GIT_BRANCH
    ));

    // Uptime
    lines.push("# HELP opensoma_uptime_seconds Daemon uptime in seconds.".to_string());
    lines.push("# TYPE opensoma_uptime_seconds gauge".to_string());
    lines.push(format!("opensoma_uptime_seconds {}", uptime));

    // Events
    lines.push("# HELP opensoma_events_collected_total Total events collected.".to_string());
    lines.push("# TYPE opensoma_events_collected_total counter".to_string());
    lines.push(format!(
        "opensoma_events_collected_total {}",
        events_collected
    ));

    lines.push("# HELP opensoma_events_synced_total Total events synced to Soul.".to_string());
    lines.push("# TYPE opensoma_events_synced_total counter".to_string());
    lines.push(format!("opensoma_events_synced_total {}", events_synced));

    // Pending events (difference)
    let pending = events_collected.saturating_sub(events_synced);
    lines.push("# HELP opensoma_events_pending Events awaiting sync.".to_string());
    lines.push("# TYPE opensoma_events_pending gauge".to_string());
    lines.push(format!("opensoma_events_pending {}", pending));

    // Connectors
    lines.push("# HELP opensoma_connectors_active Number of active connectors.".to_string());
    lines.push("# TYPE opensoma_connectors_active gauge".to_string());
    lines.push(format!("opensoma_connectors_active {}", connectors.len()));

    // Per-connector event counts
    if !event_counts.is_empty() {
        lines.push("# HELP opensoma_connector_events_total Events per connector.".to_string());
        lines.push("# TYPE opensoma_connector_events_total counter".to_string());
        for (name, count) in &event_counts {
            lines.push(format!(
                "opensoma_connector_events_total{{connector=\"{}\"}} {}",
                name, count
            ));
        }
    }

    // System metrics
    lines.push("# HELP opensoma_cpu_usage_percent CPU usage percentage.".to_string());
    lines.push("# TYPE opensoma_cpu_usage_percent gauge".to_string());
    lines.push(format!("opensoma_cpu_usage_percent {}", cpu_percent));

    lines.push("# HELP opensoma_memory_total_bytes Total system memory in bytes.".to_string());
    lines.push("# TYPE opensoma_memory_total_bytes gauge".to_string());
    lines.push(format!(
        "opensoma_memory_total_bytes {}",
        memory_total_bytes
    ));

    lines.push("# HELP opensoma_memory_used_bytes Used system memory in bytes.".to_string());
    lines.push("# TYPE opensoma_memory_used_bytes gauge".to_string());
    lines.push(format!("opensoma_memory_used_bytes {}", memory_used_bytes));

    lines.push("".to_string()); // trailing newline required by Prometheus

    // Append pipeline-internal metrics (latency, cache, conflicts, etc.)
    if let Some(ref pm) = state.pipeline_metrics {
        let pipeline_prom = pm.to_prometheus();
        lines.push(pipeline_prom);
    }

    axum::response::Response::builder()
        .header("content-type", "text/plain; version=0.0.4; charset=utf-8")
        .body(axum::body::Body::from(lines.join("\n")))
        .unwrap()
}

/// /api/system/info — detailed system diagnostics for monitoring and troubleshooting.
/// /api/connectors/health — summary of all connector health statuses.
/// Returns a JSON array of connector health objects with name, enabled, and status.
async fn api_connectors_health_handler(
    State(state): State<StatusServerState>,
) -> Json<serde_json::Value> {
    let enabled_map = state.connector_enabled.read().await;
    let event_counts = state.connector_event_counts.read().await;

    let connectors = vec![
        "feishu", "dingtalk", "wecom", "rss", "email", "notion", "git", "obsidian", "webhook",
        "github", "slack", "telegram", "discord",
    ];

    let health: Vec<serde_json::Value> = connectors
        .iter()
        .map(|name| {
            let enabled = enabled_map.get(*name).copied().unwrap_or(false);
            let events = event_counts.get(*name).copied().unwrap_or(0);
            serde_json::json!({
                "name": name,
                "enabled": enabled,
                "status": if enabled { "active" } else { "disabled" },
                "events_collected": events,
            })
        })
        .collect();

    let enabled_count = health
        .iter()
        .filter(|c| c["enabled"].as_bool().unwrap_or(false))
        .count();
    let total_events: u64 = event_counts.values().sum();

    Json(serde_json::json!({
        "connectors": health,
        "summary": {
            "total": connectors.len(),
            "enabled": enabled_count,
            "disabled": connectors.len() - enabled_count,
            "total_events": total_events,
        }
    }))
}
/// /api/pipeline/metrics — internal pipeline metrics (counters, latency, cache stats).
async fn api_pipeline_metrics_handler(
    State(state): State<StatusServerState>,
) -> Json<serde_json::Value> {
    if let Some(ref metrics) = state.pipeline_metrics {
        let snapshot = metrics.snapshot();
        Json(serde_json::json!({
            "pipeline_metrics": snapshot,
        }))
    } else {
        Json(serde_json::json!({
            "pipeline_metrics": null,
            "message": "Pipeline metrics not initialized",
        }))
    }
}

/// Returns plugin registry status — registered plugins, their state, and health.
async fn api_plugins_handler(State(state): State<StatusServerState>) -> Json<serde_json::Value> {
    if let Some(ref registry) = state.plugin_registry {
        let plugins = registry.list().await;
        let health = registry.health_all().await;
        Json(serde_json::json!({
            "total": plugins.len(),
            "active": health.iter().filter(|h| h.state == crate::plugins::PluginState::Active).count(),
            "plugins": plugins,
            "health": health,
        }))
    } else {
        Json(serde_json::json!({
            "total": 0,
            "active": 0,
            "plugins": [],
            "health": [],
            "message": "Plugin registry not initialized",
        }))
    }
}

/// Circuit breaker status API endpoint.
async fn api_circuit_breakers_handler(
    State(state): State<StatusServerState>,
) -> Json<serde_json::Value> {
    if let Some(ref registry) = state.circuit_breakers {
        let snapshots = registry.snapshot_all().await;
        Json(serde_json::json!({
            "breakers": snapshots,
            "total": snapshots.len(),
            "tripped": snapshots.iter().filter(|s| s.state == crate::connector::circuit_breaker::CircuitState::Open).count(),
        }))
    } else {
        Json(serde_json::json!({ "breakers": [], "total": 0, "tripped": 0 }))
    }
}

/// /api/config — returns the sanitized running configuration (secrets redacted).
async fn api_config_handler(State(state): State<StatusServerState>) -> Json<serde_json::Value> {
    if let Some(ref config) = state.config_snapshot {
        Json(
            serde_json::to_value(config)
                .unwrap_or_else(|_| serde_json::json!({"error": "Failed to serialize config"})),
        )
    } else {
        Json(serde_json::json!({
            "error": "Config snapshot not available",
            "message": "Run with --config to enable config API"
        }))
    }
}

/// Returns OS, kernel, CPU cores, disk usage, network interfaces, and process info.
async fn api_system_info_handler(
    State(state): State<StatusServerState>,
) -> Json<serde_json::Value> {
    let mut sys = sysinfo::System::new_all();
    sys.refresh_all();

    let hostname = sysinfo::System::host_name().unwrap_or_else(|| "unknown".to_string());
    let os_name = sysinfo::System::name().unwrap_or_else(|| "unknown".to_string());
    let os_version = sysinfo::System::os_version().unwrap_or_else(|| "unknown".to_string());
    let kernel = sysinfo::System::kernel_version().unwrap_or_else(|| "unknown".to_string());
    let cpu_cores = sys.cpus().len();
    let cpu_brand = sys
        .cpus()
        .first()
        .map(|c| c.brand().to_string())
        .unwrap_or_default();
    let cpu_percent = sys.global_cpu_usage();
    let memory_total_mb = sys.total_memory() / 1024 / 1024;
    let memory_used_mb = sys.used_memory() / 1024 / 1024;
    let memory_available_mb = sys.available_memory() / 1024 / 1024;
    let uptime = state.start_time.elapsed().as_secs();

    // Disk usage
    let disks: Vec<serde_json::Value> = sysinfo::Disks::new_with_refreshed_list()
        .iter()
        .map(|d| {
            serde_json::json!({
                "mount": d.mount_point().to_string_lossy(),
                "total_gb": d.total_space() / 1024 / 1024 / 1024,
                "available_gb": d.available_space() / 1024 / 1024 / 1024,
                "filesystem": d.file_system().to_string_lossy(),
            })
        })
        .collect();

    // Network interfaces
    let networks: Vec<serde_json::Value> = sysinfo::Networks::new_with_refreshed_list()
        .iter()
        .map(|(name, data)| {
            serde_json::json!({
                "name": name,
                "rx_bytes": data.total_received(),
                "tx_bytes": data.total_transmitted(),
            })
        })
        .collect();

    let build = crate::build_info::version_json();

    Json(serde_json::json!({
        "node_id": state.node_id,
        "version": env!("CARGO_PKG_VERSION"),
        "build": build,
        "uptime_seconds": uptime,
        "hostname": hostname,
        "os": os_name,
        "os_version": os_version,
        "kernel": kernel,
        "cpu": {
            "cores": cpu_cores,
            "brand": cpu_brand,
            "usage_percent": cpu_percent,
        },
        "memory": {
            "total_mb": memory_total_mb,
            "used_mb": memory_used_mb,
            "available_mb": memory_available_mb,
            "usage_percent": if memory_total_mb > 0 { (memory_used_mb as f64 / memory_total_mb as f64 * 100.0).round() } else { 0.0 },
        },
        "disks": disks,
        "networks": networks,
        "collectors": ["file", "process", "network", "clipboard"],
        "connectors_count": 13,
        "start_time": chrono::Utc::now().checked_sub_signed(chrono::Duration::seconds(uptime as i64)).map(|t| t.to_rfc3339()),
    }))
}

/// /api/cache/stats — return current cache statistics
async fn api_cache_stats_handler(
    State(state): State<StatusServerState>,
) -> Json<CacheStatsSnapshot> {
    let stats = state.cache_stats.read().await.clone();
    Json(stats)
}

/// /api/cache/evict — trigger cache eviction (remove uploaded entries older than cutoff).
/// Expects JSON body: {"cutoff_hours": 24}
async fn api_cache_evict_handler(
    State(_state): State<StatusServerState>,
    Json(payload): Json<serde_json::Value>,
) -> Json<serde_json::Value> {
    let cutoff_hours = payload
        .get("cutoff_hours")
        .and_then(|v| v.as_u64())
        .unwrap_or(24);
    // Eviction is handled by the sync engine; this just logs the request
    tracing::info!(
        "Cache eviction requested (cutoff={}h ago) — will be processed by sync engine",
        cutoff_hours
    );
    Json(serde_json::json!({
        "status": "ok",
        "cutoff_hours": cutoff_hours,
        "message": "Eviction request accepted. Sync engine will process on next tick."
    }))
}

/// /api/page/{page} — return HTML content for each page section.
/// Used by the hash-based routing in the web UI.
async fn api_page_handler(
    Path(page): Path<String>,
    State(state): State<StatusServerState>,
) -> Json<serde_json::Value> {
    let html = match page.as_str() {
        "dashboard" => build_dashboard_page(&state).await,
        "connectors" => build_connectors_page(&state).await,
        "collectors" => build_collectors_page().await,
        "sync" => build_sync_page(&state).await,
        "monitor" => build_monitor_page(&state).await,
        "config" => build_config_page().await,
        "plugins" => build_plugins_page(&state).await,
        _ => format!(
            "<div class=\"text-center text-muted-foreground py-12\">页面未找到: {}</div>",
            page
        ),
    };
    Json(serde_json::json!({ "html": html }))
}

/// Query parameters for event search.
#[derive(Deserialize)]
struct EventSearchQuery {
    /// Filter by source prefix (e.g. "file:", "connector:github")
    source: Option<String>,
    /// Filter by event type (exact or prefix match)
    event_type: Option<String>,
    /// Full-text search on payload
    q: Option<String>,
    /// Start timestamp (ms since epoch)
    after: Option<i64>,
    /// End timestamp (ms since epoch)
    before: Option<i64>,
    /// Max results to return (default 50, max 200)
    limit: Option<usize>,
}

/// /api/events/recent — return the N most recent cached events.
async fn api_events_recent_handler(
    Query(params): Query<std::collections::HashMap<String, String>>,
    State(state): State<StatusServerState>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    let limit = params
        .get("limit")
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(50)
        .min(200);

    match &state.cache {
        Some(cache) => match cache.get_recent(limit) {
            Ok(events) => {
                let summaries: Vec<serde_json::Value> = events
                    .iter()
                    .map(|e| {
                        serde_json::json!({
                            "id": e.id,
                            "source": e.source,
                            "event_type": e.event_type,
                            "timestamp_ms": e.timestamp_ms,
                            "payload_size": e.payload.len(),
                            "tags": e.tags,
                        })
                    })
                    .collect();
                Ok(Json(serde_json::json!({
                    "count": summaries.len(),
                    "events": summaries,
                })))
            }
            Err(e) => {
                tracing::error!("Event recent query failed: {}", e);
                Err(StatusCode::INTERNAL_SERVER_ERROR)
            }
        },
        None => Ok(Json(serde_json::json!({
            "count": 0,
            "events": [],
            "error": "Cache not available for queries",
        }))),
    }
}

/// /api/events/search — search cached events by source, type, query, or time range.
async fn api_events_search_handler(
    Query(query): Query<EventSearchQuery>,
    State(state): State<StatusServerState>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    let limit = query.limit.unwrap_or(50).min(200);

    let cache = match &state.cache {
        Some(c) => c,
        None => {
            return Ok(Json(serde_json::json!({
                "count": 0,
                "events": [],
                "error": "Cache not available for queries",
            })));
        }
    };

    let events = if let Some(ref q) = query.q {
        // Full-text search has highest priority
        cache.search_by_payload(q, limit)
    } else if let Some(ref after) = query.after {
        // Time range search
        let before = query.before.unwrap_or(i64::MAX);
        cache.search_by_time_range(*after, before, limit)
    } else if let Some(ref source) = query.source {
        // Source prefix search
        cache.search_by_source(source, limit)
    } else if let Some(ref et) = query.event_type {
        // Event type search
        cache.search_by_type(et, limit)
    } else {
        // No filter — return recent
        cache.get_recent(limit)
    };

    match events {
        Ok(events) => {
            let summaries: Vec<serde_json::Value> = events
                .iter()
                .map(|e| {
                    serde_json::json!({
                        "id": e.id,
                        "source": e.source,
                        "event_type": e.event_type,
                        "timestamp_ms": e.timestamp_ms,
                        "payload_size": e.payload.len(),
                        "tags": e.tags,
                    })
                })
                .collect();
            Ok(Json(serde_json::json!({
                "count": summaries.len(),
                "events": summaries,
                "query": {
                    "source": query.source,
                    "event_type": query.event_type,
                    "q": query.q,
                    "after": query.after,
                    "before": query.before,
                    "limit": limit,
                },
            })))
        }
        Err(e) => {
            tracing::error!("Event search failed: {}", e);
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

async fn build_dashboard_page(state: &StatusServerState) -> String {
    let events_collected = *state.events_collected.read().await;
    let events_synced = *state.events_synced.read().await;
    let connectors = state.connectors_active.read().await.clone();
    let uptime_secs = state.start_time.elapsed().as_secs();
    let h = uptime_secs / 3600;
    let m = (uptime_secs % 3600) / 60;
    let uptime_str = if h > 0 {
        format!("{}h {}m", h, m)
    } else {
        format!("{}m", m)
    };
    let cache = state.cache_stats.read().await.clone();

    format!(
        r#"
<div class="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4 mb-6">
  <div class="bg-card border border-border rounded-lg p-4">
    <div class="text-muted-foreground text-xs mb-1">运行时间</div>
    <div class="text-2xl font-bold text-foreground">{uptime}</div>
  </div>
  <div class="bg-card border border-border rounded-lg p-4">
    <div class="text-muted-foreground text-xs mb-1">已采集事件</div>
    <div class="text-2xl font-bold text-primary">{collected}</div>
  </div>
  <div class="bg-card border border-border rounded-lg p-4">
    <div class="text-muted-foreground text-xs mb-1">已同步事件</div>
    <div class="text-2xl font-bold text-green-500">{synced}</div>
  </div>
  <div class="bg-card border border-border rounded-lg p-4">
    <div class="text-muted-foreground text-xs mb-1">活跃连接器</div>
    <div class="text-2xl font-bold text-foreground">{active_count}</div>
  </div>
</div>
<div class="grid grid-cols-1 md:grid-cols-2 gap-4">
  <div class="bg-card border border-border rounded-lg p-4">
    <h3 class="text-sm font-semibold text-foreground mb-3">缓存状态</h3>
    <div class="space-y-2 text-sm">
      <div class="flex justify-between"><span class="text-muted-foreground">总计</span><span>{cache_total}</span></div>
      <div class="flex justify-between"><span class="text-muted-foreground">已上传</span><span class="text-green-500">{cache_uploaded}</span></div>
      <div class="flex justify-between"><span class="text-muted-foreground">待处理</span><span class="text-yellow-500">{cache_pending}</span></div>
      <div class="flex justify-between"><span class="text-muted-foreground">缓存大小</span><span>{cache_size}</span></div>
    </div>
  </div>
  <div class="bg-card border border-border rounded-lg p-4">
    <h3 class="text-sm font-semibold text-foreground mb-3">活跃连接器</h3>
    <div class="space-y-1 text-sm">
      {connector_list}
    </div>
  </div>
</div>"#,
        uptime = uptime_str,
        collected = events_collected,
        synced = events_synced,
        active_count = connectors.len(),
        cache_total = cache.total,
        cache_uploaded = cache.uploaded,
        cache_pending = cache.pending,
        cache_size = format_bytes(cache.cache_size_bytes),
        connector_list = if connectors.is_empty() {
            "<span class=\"text-muted-foreground\">无活跃连接器</span>".to_string()
        } else {
            connectors.iter().map(|c| format!(
                "<div class=\"flex items-center gap-2\"><span class=\"w-2 h-2 rounded-full bg-green-500\"></span>{}</div>",
                c
            )).collect::<Vec<_>>().join("")
        },
    )
}

async fn build_connectors_page(state: &StatusServerState) -> String {
    let active = state.connectors_active.read().await.clone();
    let event_counts = state.connector_event_counts.read().await.clone();
    let all_connectors = [
        ("feishu", "飞书", "接收飞书机器人消息和文档变更"),
        ("dingtalk", "钉钉", "钉钉审批、考勤、群消息采集"),
        ("wecom", "企业微信", "企业微信应用消息和通讯录"),
        ("rss", "RSS", "RSS/Atom Feed 定时拉取"),
        ("email", "邮件", "IMAP 邮件轮询采集"),
        ("webhook", "Webhook", "通用 Webhook HTTP 接收"),
        ("github", "GitHub", "Issues/PR/Release 采集"),
        ("notion", "Notion", "Notion 数据库同步"),
        ("git", "Git", "Git 仓库轮询采集"),
        ("obsidian", "Obsidian", "Obsidian Vault 文件监控"),
        ("slack", "Slack", "Slack 频道消息和线程采集"),
        ("telegram", "Telegram", "Telegram Bot 消息采集"),
        ("discord", "Discord", "Discord 服务器消息和频道采集"),
    ];

    let cards: String = all_connectors.iter().map(|(id, name, desc)| {
        let is_active = active.contains(&id.to_string());
        let count = event_counts.get(*id).copied().unwrap_or(0);
        let status_color = if is_active { "bg-green-500" } else { "bg-gray-500" };
        let status_text = if is_active { "运行中" } else { "未启用" };
        format!(r#"
<div class="bg-card border border-border rounded-lg p-4 flex items-start gap-4">
  <div class="w-10 h-10 rounded-lg bg-primary/10 flex items-center justify-center text-primary text-lg">🔌</div>
  <div class="flex-1">
    <div class="flex items-center gap-2 mb-1">
      <span class="font-semibold text-foreground">{name}</span>
      <span class="w-2 h-2 rounded-full {status_color}"></span>
      <span class="text-xs text-muted-foreground">{status_text}</span>
    </div>
    <div class="text-sm text-muted-foreground mb-2">{desc}</div>
    <div class="text-xs text-muted-foreground">事件数: {count}</div>
  </div>
</div>"#, name = name, status_color = status_color, status_text = status_text, desc = desc, count = count)
    }).collect::<Vec<_>>().join("");

    format!(
        r#"
<div class="mb-4">
  <h2 class="text-lg font-semibold text-foreground">连接器管理</h2>
  <p class="text-sm text-muted-foreground">管理与外部服务的数据连接</p>
</div>
<div class="grid grid-cols-1 md:grid-cols-2 gap-4">{cards}</div>"#,
        cards = cards
    )
}

async fn build_collectors_page() -> String {
    let collectors = [
        (
            "file",
            "文件采集器",
            "监控文件系统变更，采集文件创建/修改/删除事件",
        ),
        (
            "process",
            "进程采集器",
            "监控系统进程，采集进程启动/退出事件",
        ),
        (
            "network",
            "网络采集器",
            "监控网络连接，采集 TCP/UDP 连接状态变更",
        ),
        (
            "clipboard",
            "剪贴板采集器",
            "监控剪贴板内容变更，采集复制事件",
        ),
    ];

    let cards: String = collectors.iter().map(|(_id, name, desc)| {
        format!(r#"
<div class="bg-card border border-border rounded-lg p-4 flex items-start gap-4">
  <div class="w-10 h-10 rounded-lg bg-blue-500/10 flex items-center justify-center text-blue-500 text-lg">📡</div>
  <div class="flex-1">
    <div class="flex items-center gap-2 mb-1">
      <span class="font-semibold text-foreground">{name}</span>
      <span class="w-2 h-2 rounded-full bg-green-500"></span>
      <span class="text-xs text-muted-foreground">运行中</span>
    </div>
    <div class="text-sm text-muted-foreground">{desc}</div>
  </div>
</div>"#, name = name, desc = desc)
    }).collect::<Vec<_>>().join("");

    format!(
        r#"
<div class="mb-4">
  <h2 class="text-lg font-semibold text-foreground">采集器</h2>
  <p class="text-sm text-muted-foreground">本地数据采集模块</p>
</div>
<div class="grid grid-cols-1 md:grid-cols-2 gap-4">{cards}</div>"#,
        cards = cards
    )
}

async fn build_sync_page(state: &StatusServerState) -> String {
    let cache = state.cache_stats.read().await.clone();
    let events_collected = *state.events_collected.read().await;
    let events_synced = *state.events_synced.read().await;
    let pending = events_collected.saturating_sub(events_synced);
    let sync_pct = if events_collected > 0 {
        (events_synced as f64 / events_collected as f64 * 100.0) as u64
    } else {
        100
    };

    format!(
        r#"
<div class="mb-4">
  <h2 class="text-lg font-semibold text-foreground">同步状态</h2>
  <p class="text-sm text-muted-foreground">数据同步到 OpenSoul 的状态</p>
</div>
<div class="grid grid-cols-1 md:grid-cols-3 gap-4 mb-6">
  <div class="bg-card border border-border rounded-lg p-4">
    <div class="text-muted-foreground text-xs mb-1">同步进度</div>
    <div class="text-2xl font-bold text-primary">{sync_pct}%</div>
    <div class="w-full bg-background rounded-full h-2 mt-2">
      <div class="bg-primary rounded-full h-2" style="width: {sync_pct}%"></div>
    </div>
  </div>
  <div class="bg-card border border-border rounded-lg p-4">
    <div class="text-muted-foreground text-xs mb-1">待同步</div>
    <div class="text-2xl font-bold text-yellow-500">{pending}</div>
  </div>
  <div class="bg-card border border-border rounded-lg p-4">
    <div class="text-muted-foreground text-xs mb-1">缓存大小</div>
    <div class="text-2xl font-bold text-foreground">{cache_size}</div>
  </div>
</div>
<div class="bg-card border border-border rounded-lg p-4">
  <h3 class="text-sm font-semibold text-foreground mb-3">缓存详情</h3>
  <div class="space-y-2 text-sm">
    <div class="flex justify-between"><span class="text-muted-foreground">总条目</span><span>{total}</span></div>
    <div class="flex justify-between"><span class="text-muted-foreground">已上传</span><span class="text-green-500">{uploaded}</span></div>
    <div class="flex justify-between"><span class="text-muted-foreground">待处理</span><span class="text-yellow-500">{cache_pending}</span></div>
  </div>
</div>"#,
        sync_pct = sync_pct,
        pending = pending,
        cache_size = format_bytes(cache.cache_size_bytes),
        total = cache.total,
        uploaded = cache.uploaded,
        cache_pending = cache.pending,
    )
}

async fn build_monitor_page(state: &StatusServerState) -> String {
    let mut sys = sysinfo::System::new_all();
    sys.refresh_all();
    let cpu_percent = sys.global_cpu_usage();
    let memory_total_mb = sys.total_memory() / 1024 / 1024;
    let memory_used_mb = sys.used_memory() / 1024 / 1024;
    let memory_pct = if memory_total_mb > 0 {
        (memory_used_mb as f64 / memory_total_mb as f64 * 100.0) as u64
    } else {
        0
    };
    let hostname = sysinfo::System::host_name().unwrap_or_else(|| "unknown".to_string());
    let ip = local_ip_address::local_ip()
        .map(|ip| ip.to_string())
        .unwrap_or_else(|_| "127.0.0.1".to_string());
    let last_error = state.last_error.read().await.clone();

    format!(
        r#"
<div class="mb-4">
  <h2 class="text-lg font-semibold text-foreground">系统监控</h2>
  <p class="text-sm text-muted-foreground">系统资源和运行状态</p>
</div>
<div class="grid grid-cols-1 md:grid-cols-2 gap-4 mb-6">
  <div class="bg-card border border-border rounded-lg p-4">
    <div class="text-muted-foreground text-xs mb-1">CPU 使用率</div>
    <div class="text-2xl font-bold text-foreground">{cpu:.1}%</div>
    <div class="w-full bg-background rounded-full h-2 mt-2">
      <div class="bg-primary rounded-full h-2" style="width: {cpu:.0}%"></div>
    </div>
  </div>
  <div class="bg-card border border-border rounded-lg p-4">
    <div class="text-muted-foreground text-xs mb-1">内存使用</div>
    <div class="text-2xl font-bold text-foreground">{mem_used}MB / {mem_total}MB</div>
    <div class="w-full bg-background rounded-full h-2 mt-2">
      <div class="bg-green-500 rounded-full h-2" style="width: {mem_pct}%"></div>
    </div>
  </div>
</div>
<div class="bg-card border border-border rounded-lg p-4">
  <h3 class="text-sm font-semibold text-foreground mb-3">系统信息</h3>
  <div class="space-y-2 text-sm">
    <div class="flex justify-between"><span class="text-muted-foreground">主机名</span><span>{hostname}</span></div>
    <div class="flex justify-between"><span class="text-muted-foreground">IP 地址</span><span>{ip}</span></div>
    <div class="flex justify-between"><span class="text-muted-foreground">最后错误</span><span class="text-red-400">{last_error}</span></div>
  </div>
</div>"#,
        cpu = cpu_percent,
        mem_used = memory_used_mb,
        mem_total = memory_total_mb,
        mem_pct = memory_pct,
        hostname = hostname,
        ip = ip,
        last_error = last_error.unwrap_or_else(|| "无".to_string()),
    )
}

async fn build_config_page() -> String {
    r#"
<div class="mb-4">
  <h2 class="text-lg font-semibold text-foreground">配置</h2>
  <p class="text-sm text-muted-foreground">OpenSoma 配置文件说明</p>
</div>
<div class="bg-card border border-border rounded-lg p-4">
  <h3 class="text-sm font-semibold text-foreground mb-3">配置文件</h3>
  <div class="text-sm text-muted-foreground space-y-2">
    <p>配置文件路径: <code class="bg-background px-1.5 py-0.5 rounded text-primary font-mono">config.toml</code></p>
    <p>启动时使用 <code class="bg-background px-1.5 py-0.5 rounded text-primary font-mono">opensoma -c /path/to/config.toml</code> 指定配置文件</p>
    <p>使用 <code class="bg-background px-1.5 py-0.5 rounded text-primary font-mono">opensoma --init</code> 生成默认配置</p>
    <p>使用 <code class="bg-background px-1.5 py-0.5 rounded text-primary font-mono">opensoma --validate</code> 验证配置</p>
  </div>
</div>
<div class="bg-card border border-border rounded-lg p-4 mt-4">
  <h3 class="text-sm font-semibold text-foreground mb-3">环境变量覆盖</h3>
  <div class="text-sm text-muted-foreground space-y-1">
    <p><code class="bg-background px-1.5 py-0.5 rounded text-primary font-mono text-xs">OPENSOMA_DAEMON_NODE_ID</code></p>
    <p><code class="bg-background px-1.5 py-0.5 rounded text-primary font-mono text-xs">OPENSOMA_SOUL_ENDPOINT</code></p>
    <p><code class="bg-background px-1.5 py-0.5 rounded text-primary font-mono text-xs">OPENSOMA_CONNECTOR_GITHUB_TOKEN</code></p>
    <p><code class="bg-background px-1.5 py-0.5 rounded text-primary font-mono text-xs">OPENSOMA_CONNECTOR_DINGTALK_APP_KEY</code></p>
    <p class="text-xs mt-2">格式: <code>OPENSOMA_&lt;SECTION&gt;_&lt;FIELD&gt;</code> (大写)</p>
  </div>
</div>"#.to_string()
}

async fn build_plugins_page(state: &StatusServerState) -> String {
    let (total, active, rows) = if let Some(ref registry) = state.plugin_registry {
        let plugins = registry.list().await;
        let health = registry.health_all().await;
        let active_count = health
            .iter()
            .filter(|h| h.state == crate::plugins::PluginState::Active)
            .count();

        let mut table_rows = String::new();
        for p in &plugins {
            let h = health.iter().find(|h| h.plugin_id == p.id);
            let state_str = h
                .map(|h| match &h.state {
                    crate::plugins::PluginState::Active => "active",
                    crate::plugins::PluginState::Error => "error",
                    _ => "inactive",
                })
                .unwrap_or("unknown");
            let state_class = match state_str {
                "active" => "badge-ok",
                "error" => "badge-error",
                _ => "badge-disabled",
            };
            let requests = h
                .map(|h| h.requests_handled.to_string())
                .unwrap_or_else(|| "0".to_string());
            let errors = h
                .map(|h| h.errors.to_string())
                .unwrap_or_else(|| "0".to_string());

            use std::fmt::Write;
            let _ = write!(
                table_rows,
                "<tr><td style=\"font-weight:500;\">{name}</td><td class=\"monospace\">{id}</td><td><span class=\"badge {sc}\">{state}</span></td><td class=\"monospace\">{ver}</td><td class=\"monospace\">{req}</td><td class=\"monospace\">{err}</td></tr>",
                name = p.name,
                id = p.id,
                sc = state_class,
                state = state_str,
                ver = p.version,
                req = requests,
                err = errors,
            );
        }

        (plugins.len(), active_count, table_rows)
    } else {
        (0, 0, String::new())
    };

    if total == 0 {
        return String::from(
            "<div style=\"text-align:center;padding:48px 20px;color:#71717a;\">\
             <div style=\"font-size:40px;margin-bottom:12px;\">🧩</div>\
             <p>暂无已注册插件</p></div>",
        );
    }

    format!(
        "<h2 style=\"font-size:14px;font-weight:600;margin:0 0 16px;\">插件管理</h2>\
         <div class=\"stats-grid\">\
           <div class=\"stat-card\"><div class=\"stat-value\">{total}</div><div class=\"stat-label\">已注册插件</div></div>\
           <div class=\"stat-card\"><div class=\"stat-value\" style=\"color:#22c55e;\">{active}</div><div class=\"stat-label\">活跃插件</div></div>\
         </div>\
         <div class=\"card\"><table>\
           <thead><tr><th>名称</th><th>ID</th><th>状态</th><th>版本</th><th>请求数</th><th>错误数</th></tr></thead>\
           <tbody>{rows}</tbody></table></div>",
        total = total,
        active = active,
        rows = rows,
    )
}

fn format_bytes(bytes: u64) -> String {
    if bytes < 1024 {
        format!("{} B", bytes)
    } else if bytes < 1024 * 1024 {
        format!("{:.1} KB", bytes as f64 / 1024.0)
    } else if bytes < 1024 * 1024 * 1024 {
        format!("{:.1} MB", bytes as f64 / (1024.0 * 1024.0))
    } else {
        format!("{:.2} GB", bytes as f64 / (1024.0 * 1024.0 * 1024.0))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_health_response_serialization() {
        let resp = HealthResponse {
            status: "ok".to_string(),
            component: "OpenSoma".to_string(),
            node_id: "test-node".to_string(),
            uptime_seconds: 3600,
        };
        let json = serde_json::to_string(&resp).unwrap();
        assert!(json.contains("\"status\":\"ok\""));
        assert!(json.contains("\"uptime_seconds\":3600"));
    }

    #[test]
    fn test_connector_info_serialization() {
        let info = ConnectorInfo {
            id: "feishu".to_string(),
            name: "飞书".to_string(),
            enabled: true,
            status: "running".to_string(),
            event_count: 100,
        };
        let json = serde_json::to_string(&info).unwrap();
        assert!(json.contains("\"event_count\":100"));
    }

    #[test]
    fn test_status_response_serialization() {
        let resp = StatusResponse {
            component: "OpenSoma".to_string(),
            node_id: "node-1".to_string(),
            uptime_seconds: 100,
            events_collected: 50,
            events_synced: 45,
            connectors_active: vec!["feishu".into()],
            last_error: None,
            hostname: "test-host".to_string(),
            ip: "127.0.0.1".to_string(),
            cpu_percent: 25.5,
            memory_used_mb: 1024,
            memory_total_mb: 4096,
        };
        let json = serde_json::to_string(&resp).unwrap();
        assert!(json.contains("\"events_collected\":50"));
        assert!(json.contains("\"events_synced\":45"));
    }

    #[test]
    fn test_format_bytes() {
        assert_eq!(format_bytes(0), "0 B");
        assert_eq!(format_bytes(512), "512 B");
        assert_eq!(format_bytes(1024), "1.0 KB");
        assert_eq!(format_bytes(1536), "1.5 KB");
        assert_eq!(format_bytes(1048576), "1.0 MB");
        assert_eq!(format_bytes(1073741824), "1.00 GB");
    }

    #[test]
    fn test_cache_stats_snapshot_default() {
        let stats = CacheStatsSnapshot::default();
        assert_eq!(stats.total, 0);
        assert_eq!(stats.uploaded, 0);
        assert_eq!(stats.pending, 0);
        assert_eq!(stats.cache_size_bytes, 0);
    }

    #[test]
    fn test_cache_stats_snapshot_serialization() {
        let stats = CacheStatsSnapshot {
            total: 100,
            uploaded: 80,
            pending: 20,
            cache_size_bytes: 1024 * 512,
        };
        let json = serde_json::to_string(&stats).unwrap();
        assert!(json.contains("\"total\":100"));
        assert!(json.contains("\"uploaded\":80"));
        assert!(json.contains("\"pending\":20"));
    }

    #[test]
    fn test_toggle_request_deserialization() {
        let json = r#"{"enabled":true}"#;
        let req: ToggleRequest = serde_json::from_str(json).unwrap();
        assert!(req.enabled);

        let json2 = r#"{"enabled":false}"#;
        let req2: ToggleRequest = serde_json::from_str(json2).unwrap();
        assert!(!req2.enabled);
    }

    #[test]
    fn test_format_bytes_edge_cases() {
        assert_eq!(format_bytes(1), "1 B");
        assert_eq!(format_bytes(1023), "1023 B");
        assert_eq!(format_bytes(1024 * 1024 - 1), "1024.0 KB");
        assert_eq!(format_bytes(1024 * 1024), "1.0 MB");
    }

    // ── HTTP integration tests ──────────────────────────────────────────

    /// Build an axum test app with a default StatusServerState.
    fn build_test_app() -> Router {
        let state = StatusServerState {
            node_id: "test-node".to_string(),
            start_time: std::time::Instant::now(),
            events_collected: Arc::new(RwLock::new(42)),
            events_synced: Arc::new(RwLock::new(38)),
            connectors_active: Arc::new(RwLock::new(vec!["feishu".into(), "github".into()])),
            last_error: Arc::new(RwLock::new(None)),
            connector_enabled: Arc::new(RwLock::new(HashMap::new())),
            connector_event_counts: Arc::new(RwLock::new({
                let mut m = HashMap::new();
                m.insert("feishu".to_string(), 150u64);
                m.insert("github".to_string(), 200u64);
                m
            })),
            cache_stats: Arc::new(RwLock::new(CacheStatsSnapshot {
                total: 500,
                uploaded: 450,
                pending: 50,
                cache_size_bytes: 1024 * 256,
            })),
            cache: None,
            pipeline_metrics: None,
            health_checker: None,
            plugin_registry: None,
            config_snapshot: None,
            circuit_breakers: None,
        };

        Router::new()
            .route("/health", get(health_handler))
            .route("/status", get(status_handler))
            .route("/api/status", get(api_status_handler))
            .route("/api/connectors", get(api_connectors_handler))
            .route("/api/collectors", get(api_collectors_handler))
            .route("/api/connectors/:name/toggle", post(api_connector_toggle))
            .route("/api/connectors/:name/events", get(api_connector_events))
            .route("/api/cache/stats", get(api_cache_stats_handler))
            .route("/api/cache/evict", post(api_cache_evict_handler))
            .route("/metrics", get(metrics_handler))
            .route("/api/system/info", get(api_system_info_handler))
            .route("/api/connectors/health", get(api_connectors_health_handler))
            .route("/api/pipeline/metrics", get(api_pipeline_metrics_handler))
            .with_state(state)
    }

    fn build_test_app_with_state(state: StatusServerState) -> Router {
        Router::new()
            .route("/health", get(health_handler))
            .route("/api/connectors", get(api_connectors_handler))
            .route("/api/connectors/:name/toggle", post(api_connector_toggle))
            .route("/api/connectors/:name/events", get(api_connector_events))
            .route("/api/cache/stats", get(api_cache_stats_handler))
            .with_state(state)
    }

    #[tokio::test]
    async fn test_health_endpoint_returns_ok() {
        use tower::ServiceExt;
        let app = build_test_app();

        let req = axum::http::Request::builder()
            .uri("/health")
            .body(axum::body::Body::empty())
            .unwrap();

        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), axum::http::StatusCode::OK);

        let body = http_body_util::BodyExt::collect(resp.into_body())
            .await
            .unwrap()
            .to_bytes();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

        assert_eq!(json["status"], "ok");
        assert_eq!(json["component"], "OpenSoma");
        assert_eq!(json["node_id"], "test-node");
    }

    #[tokio::test]
    async fn test_connectors_endpoint_lists_all_13() {
        use tower::ServiceExt;
        let app = build_test_app();

        let req = axum::http::Request::builder()
            .uri("/api/connectors")
            .body(axum::body::Body::empty())
            .unwrap();

        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), axum::http::StatusCode::OK);

        let body = http_body_util::BodyExt::collect(resp.into_body())
            .await
            .unwrap()
            .to_bytes();
        let connectors: Vec<serde_json::Value> = serde_json::from_slice(&body).unwrap();

        assert_eq!(connectors.len(), 13);

        // Verify all connector IDs are present
        let ids: Vec<&str> = connectors
            .iter()
            .map(|c| c["id"].as_str().unwrap())
            .collect();
        for expected in &[
            "feishu", "dingtalk", "wecom", "rss", "email", "webhook", "github", "notion", "git",
            "obsidian", "slack", "telegram", "discord",
        ] {
            assert!(ids.contains(expected), "Missing connector: {}", expected);
        }

        // Verify feishu shows as running (we added it to connectors_active)
        let feishu = connectors.iter().find(|c| c["id"] == "feishu").unwrap();
        assert_eq!(feishu["status"], "running");
        assert_eq!(feishu["event_count"], 150);

        // Verify dingtalk shows as stopped (not in active list)
        let dingtalk = connectors.iter().find(|c| c["id"] == "dingtalk").unwrap();
        assert_eq!(dingtalk["status"], "stopped");
    }

    #[tokio::test]
    async fn test_connector_toggle_enable_disable() {
        use tower::ServiceExt;
        let app = build_test_app();

        // Enable a connector
        let req = axum::http::Request::builder()
            .method("POST")
            .uri("/api/connectors/feishu/toggle")
            .header("content-type", "application/json")
            .body(axum::body::Body::from(r#"{"enabled":true}"#))
            .unwrap();

        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), axum::http::StatusCode::OK);

        let body = http_body_util::BodyExt::collect(resp.into_body())
            .await
            .unwrap()
            .to_bytes();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

        assert_eq!(json["connector"], "feishu");
        assert_eq!(json["enabled"], true);
        assert_eq!(json["status"], "ok");
    }

    #[tokio::test]
    async fn test_connector_toggle_invalid_name_returns_404() {
        use tower::ServiceExt;
        let app = build_test_app();

        let req = axum::http::Request::builder()
            .method("POST")
            .uri("/api/connectors/nonexistent/toggle")
            .header("content-type", "application/json")
            .body(axum::body::Body::from(r#"{"enabled":true}"#))
            .unwrap();

        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), axum::http::StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_connector_events_endpoint() {
        use tower::ServiceExt;
        let app = build_test_app();

        let req = axum::http::Request::builder()
            .uri("/api/connectors/github/events")
            .body(axum::body::Body::empty())
            .unwrap();

        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), axum::http::StatusCode::OK);

        let body = http_body_util::BodyExt::collect(resp.into_body())
            .await
            .unwrap()
            .to_bytes();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

        assert_eq!(json["connector"], "github");
        assert_eq!(json["event_count"], 200);
    }

    #[tokio::test]
    async fn test_connector_events_unknown_connector_returns_zero() {
        use tower::ServiceExt;
        let app = build_test_app();

        let req = axum::http::Request::builder()
            .uri("/api/connectors/unknown/events")
            .body(axum::body::Body::empty())
            .unwrap();

        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), axum::http::StatusCode::OK);

        let body = http_body_util::BodyExt::collect(resp.into_body())
            .await
            .unwrap()
            .to_bytes();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

        assert_eq!(json["connector"], "unknown");
        assert_eq!(json["event_count"], 0);
    }

    #[tokio::test]
    async fn test_cache_stats_endpoint() {
        use tower::ServiceExt;
        let app = build_test_app();

        let req = axum::http::Request::builder()
            .uri("/api/cache/stats")
            .body(axum::body::Body::empty())
            .unwrap();

        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), axum::http::StatusCode::OK);

        let body = http_body_util::BodyExt::collect(resp.into_body())
            .await
            .unwrap()
            .to_bytes();
        let stats: CacheStatsSnapshot = serde_json::from_slice(&body).unwrap();

        assert_eq!(stats.total, 500);
        assert_eq!(stats.uploaded, 450);
        assert_eq!(stats.pending, 50);
    }

    #[tokio::test]
    async fn test_cache_evict_endpoint() {
        use tower::ServiceExt;
        let app = build_test_app();

        let req = axum::http::Request::builder()
            .method("POST")
            .uri("/api/cache/evict")
            .header("content-type", "application/json")
            .body(axum::body::Body::from(r#"{"cutoff_hours":48}"#))
            .unwrap();

        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), axum::http::StatusCode::OK);

        let body = http_body_util::BodyExt::collect(resp.into_body())
            .await
            .unwrap()
            .to_bytes();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

        assert_eq!(json["status"], "ok");
        assert_eq!(json["cutoff_hours"], 48);
    }

    #[tokio::test]
    async fn test_cache_evict_default_cutoff() {
        use tower::ServiceExt;
        let app = build_test_app();

        // Empty body — should default to 24 hours
        let req = axum::http::Request::builder()
            .method("POST")
            .uri("/api/cache/evict")
            .header("content-type", "application/json")
            .body(axum::body::Body::from(r#"{}"#))
            .unwrap();

        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), axum::http::StatusCode::OK);

        let body = http_body_util::BodyExt::collect(resp.into_body())
            .await
            .unwrap()
            .to_bytes();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

        assert_eq!(json["cutoff_hours"], 24);
    }

    #[tokio::test]
    async fn test_metrics_endpoint_returns_prometheus_format() {
        use tower::ServiceExt;
        let app = build_test_app();

        let req = axum::http::Request::builder()
            .uri("/metrics")
            .body(axum::body::Body::empty())
            .unwrap();

        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), axum::http::StatusCode::OK);

        let body = http_body_util::BodyExt::collect(resp.into_body())
            .await
            .unwrap()
            .to_bytes();
        let text = String::from_utf8(body.to_vec()).unwrap();

        // Verify Prometheus text format
        assert!(text.contains("# HELP opensoma_info"));
        assert!(text.contains("# TYPE opensoma_info gauge"));
        assert!(text.contains("opensoma_info{node_id=\"test-node\""));
        assert!(text.contains("# HELP opensoma_uptime_seconds"));
        assert!(text.contains("opensoma_events_collected_total 42"));
        assert!(text.contains("opensoma_events_synced_total 38"));
        assert!(text.contains("opensoma_events_pending 4"));
        assert!(text.contains("opensoma_connectors_active 2"));
        assert!(text.contains("# HELP opensoma_connector_events_total"));
        assert!(text.contains("opensoma_connector_events_total{connector=\"feishu\"} 150"));
        assert!(text.contains("opensoma_connector_events_total{connector=\"github\"} 200"));
        assert!(text.contains("# HELP opensoma_cpu_usage_percent"));
        assert!(text.contains("# HELP opensoma_memory_total_bytes"));
        assert!(text.contains("# HELP opensoma_memory_used_bytes"));
    }

    #[tokio::test]
    async fn test_metrics_content_type() {
        use tower::ServiceExt;
        let app = build_test_app();

        let req = axum::http::Request::builder()
            .uri("/metrics")
            .body(axum::body::Body::empty())
            .unwrap();

        let resp = app.oneshot(req).await.unwrap();
        let content_type = resp
            .headers()
            .get("content-type")
            .unwrap()
            .to_str()
            .unwrap();
        assert!(content_type.contains("text/plain"));
        assert!(content_type.contains("version=0.0.4"));
    }

    #[tokio::test]
    async fn test_status_endpoint_returns_system_info() {
        use tower::ServiceExt;
        let app = build_test_app();

        let req = axum::http::Request::builder()
            .uri("/status")
            .body(axum::body::Body::empty())
            .unwrap();

        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), axum::http::StatusCode::OK);

        let body = http_body_util::BodyExt::collect(resp.into_body())
            .await
            .unwrap()
            .to_bytes();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

        assert_eq!(json["component"], "OpenSoma");
        assert_eq!(json["node_id"], "test-node");
        assert_eq!(json["events_collected"], 42);
        assert_eq!(json["events_synced"], 38);
        // System info should be present
        assert!(json["hostname"].is_string());
        assert!(json["ip"].is_string());
        assert!(json["cpu_percent"].is_number());
        assert!(json["memory_used_mb"].is_number());
        assert!(json["memory_total_mb"].is_number());
    }

    #[tokio::test]
    async fn test_collectors_endpoint_lists_four() {
        use tower::ServiceExt;
        let app = build_test_app();

        let req = axum::http::Request::builder()
            .uri("/api/collectors")
            .body(axum::body::Body::empty())
            .unwrap();

        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), axum::http::StatusCode::OK);

        let body = http_body_util::BodyExt::collect(resp.into_body())
            .await
            .unwrap()
            .to_bytes();
        let collectors: Vec<serde_json::Value> = serde_json::from_slice(&body).unwrap();

        assert_eq!(collectors.len(), 4);
        let ids: Vec<&str> = collectors
            .iter()
            .map(|c| c["id"].as_str().unwrap())
            .collect();
        assert!(ids.contains(&"file"));
        assert!(ids.contains(&"process"));
        assert!(ids.contains(&"network"));
        assert!(ids.contains(&"clipboard"));
    }

    #[tokio::test]
    async fn test_connector_toggle_disable_removes_from_active() {
        use tower::ServiceExt;

        let state = StatusServerState {
            node_id: "toggle-test".to_string(),
            start_time: std::time::Instant::now(),
            events_collected: Arc::new(RwLock::new(0)),
            events_synced: Arc::new(RwLock::new(0)),
            connectors_active: Arc::new(RwLock::new(vec!["github".into()])),
            last_error: Arc::new(RwLock::new(None)),
            connector_enabled: Arc::new(RwLock::new(HashMap::new())),
            connector_event_counts: Arc::new(RwLock::new(HashMap::new())),
            cache_stats: Arc::new(RwLock::new(CacheStatsSnapshot::default())),
            cache: None,
            pipeline_metrics: None,
            health_checker: None,
            plugin_registry: None,
            config_snapshot: None,
            circuit_breakers: None,
        };

        // Verify github is active before toggle
        {
            let active = state.connectors_active.read().await;
            assert!(active.contains(&"github".to_string()));
        }

        let app = build_test_app_with_state(state.clone());

        // Disable github
        let req = axum::http::Request::builder()
            .method("POST")
            .uri("/api/connectors/github/toggle")
            .header("content-type", "application/json")
            .body(axum::body::Body::from(r#"{"enabled":false}"#))
            .unwrap();

        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), axum::http::StatusCode::OK);

        // Verify github was removed from active list
        let active = state.connectors_active.read().await;
        assert!(!active.contains(&"github".to_string()));
    }

    #[tokio::test]
    async fn test_health_with_last_error() {
        use tower::ServiceExt;

        let state = StatusServerState {
            node_id: "error-node".to_string(),
            start_time: std::time::Instant::now(),
            events_collected: Arc::new(RwLock::new(0)),
            events_synced: Arc::new(RwLock::new(0)),
            connectors_active: Arc::new(RwLock::new(vec![])),
            last_error: Arc::new(RwLock::new(Some("connection refused".to_string()))),
            connector_enabled: Arc::new(RwLock::new(HashMap::new())),
            connector_event_counts: Arc::new(RwLock::new(HashMap::new())),
            cache_stats: Arc::new(RwLock::new(CacheStatsSnapshot::default())),
            cache: None,
            pipeline_metrics: None,
            health_checker: None,
            plugin_registry: None,
            config_snapshot: None,
            circuit_breakers: None,
        };

        let app = build_test_app_with_state(state);

        let req = axum::http::Request::builder()
            .uri("/health")
            .body(axum::body::Body::empty())
            .unwrap();

        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), axum::http::StatusCode::OK);

        let body = http_body_util::BodyExt::collect(resp.into_body())
            .await
            .unwrap()
            .to_bytes();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

        // Health endpoint always returns "ok" regardless of last_error
        assert_eq!(json["status"], "ok");
        assert_eq!(json["node_id"], "error-node");
    }

    #[tokio::test]
    async fn test_system_info_endpoint() {
        use tower::ServiceExt;

        let app = build_test_app();

        let req = axum::http::Request::builder()
            .uri("/api/system/info")
            .body(axum::body::Body::empty())
            .unwrap();

        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), axum::http::StatusCode::OK);

        let body = http_body_util::BodyExt::collect(resp.into_body())
            .await
            .unwrap()
            .to_bytes();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

        // Verify required fields exist
        assert!(json["node_id"].is_string());
        assert!(json["version"].is_string());
        assert!(json["hostname"].is_string());
        assert!(json["os"].is_string());
        assert!(json["kernel"].is_string());
        assert!(json["cpu"]["cores"].is_number());
        assert!(json["memory"]["total_mb"].is_number());
        assert!(json["memory"]["usage_percent"].is_number());
        assert!(json["disks"].is_array());
        assert!(json["networks"].is_array());
        assert!(json["collectors"].is_array());
        assert_eq!(json["node_id"], "test-node");
        assert_eq!(json["connectors_count"], 13);
    }

    #[tokio::test]
    async fn test_connectors_health_endpoint() {
        use tower::ServiceExt;
        let app = build_test_app();

        let req = axum::http::Request::builder()
            .uri("/api/connectors/health")
            .body(axum::body::Body::empty())
            .unwrap();

        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), axum::http::StatusCode::OK);

        let body = http_body_util::BodyExt::collect(resp.into_body())
            .await
            .unwrap()
            .to_bytes();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

        // Verify structure
        assert!(json["connectors"].is_array());
        assert!(json["summary"].is_object());

        let connectors = json["connectors"].as_array().unwrap();
        assert_eq!(connectors.len(), 13);

        // Verify summary
        let summary = &json["summary"];
        assert_eq!(summary["total"].as_u64().unwrap(), 13);
        assert!(summary["enabled"].as_u64().is_some());
        assert!(summary["disabled"].as_u64().is_some());
        assert!(summary["total_events"].as_u64().is_some());

        // Verify each connector has required fields
        for conn in connectors {
            assert!(conn["name"].is_string());
            assert!(conn["enabled"].as_bool().is_some());
            assert!(conn["status"].is_string());
            assert!(conn["events_collected"].is_number());
        }
    }

    #[tokio::test]
    async fn test_cors_headers_on_get_request() {
        use tower::ServiceExt;
        let state = StatusServerState {
            node_id: "test-cors".to_string(),
            start_time: std::time::Instant::now(),
            events_collected: Arc::new(RwLock::new(0)),
            events_synced: Arc::new(RwLock::new(0)),
            connectors_active: Arc::new(RwLock::new(vec![])),
            last_error: Arc::new(RwLock::new(None)),
            connector_enabled: Arc::new(RwLock::new(HashMap::new())),
            connector_event_counts: Arc::new(RwLock::new(HashMap::new())),
            cache_stats: Arc::new(RwLock::new(CacheStatsSnapshot::default())),
            cache: None,
            pipeline_metrics: None,
            health_checker: None,
            plugin_registry: None,
            config_snapshot: None,
            circuit_breakers: None,
        };
        let app = build_router(state);

        let req = axum::http::Request::builder()
            .uri("/health")
            .body(axum::body::Body::empty())
            .unwrap();

        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), axum::http::StatusCode::OK);

        let headers = resp.headers();
        assert_eq!(
            headers.get("access-control-allow-origin").unwrap(),
            "*"
        );
        assert!(headers.get("access-control-allow-methods").is_some());
        assert!(headers.get("access-control-allow-headers").is_some());
    }

    #[tokio::test]
    async fn test_cors_preflight_options_returns_204() {
        use tower::ServiceExt;
        let state = StatusServerState {
            node_id: "test-cors".to_string(),
            start_time: std::time::Instant::now(),
            events_collected: Arc::new(RwLock::new(0)),
            events_synced: Arc::new(RwLock::new(0)),
            connectors_active: Arc::new(RwLock::new(vec![])),
            last_error: Arc::new(RwLock::new(None)),
            connector_enabled: Arc::new(RwLock::new(HashMap::new())),
            connector_event_counts: Arc::new(RwLock::new(HashMap::new())),
            cache_stats: Arc::new(RwLock::new(CacheStatsSnapshot::default())),
            cache: None,
            pipeline_metrics: None,
            health_checker: None,
            plugin_registry: None,
            config_snapshot: None,
            circuit_breakers: None,
        };
        let app = build_router(state);

        let req = axum::http::Request::builder()
            .method(axum::http::Method::OPTIONS)
            .uri("/api/status")
            .header("origin", "http://localhost:3002")
            .header("access-control-request-method", "GET")
            .body(axum::body::Body::empty())
            .unwrap();

        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), axum::http::StatusCode::NO_CONTENT);

        let headers = resp.headers();
        assert_eq!(
            headers.get("access-control-allow-origin").unwrap(),
            "*"
        );
        assert_eq!(
            headers.get("access-control-max-age").unwrap(),
            "86400"
        );
    }
}
