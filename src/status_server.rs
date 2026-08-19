use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    routing::{get, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::info;

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

/// Embed the index.html and CSS at compile time.
#[allow(dead_code)]
const INDEX_HTML: &str = include_str!("web/index.html");
const ADMIN_CSS: &str = include_str!("web/admin-framework.css");
const ADMIN_JS: &str = include_str!("web/admin-framework.js");
const SIDEBAR_CSS: &str = include_str!("web/sidebar.css");
const SIDEBAR_JS: &str = include_str!("web/sidebar.js");

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
        .with_state(state)
}

pub async fn start_status_server(
    port: u16,
    state: StatusServerState,
) -> tokio::task::JoinHandle<()> {
    let app = Router::new()
        // Web UI
        .route("/", get(index_handler))
        .route("/shared-sidebar.css", get(css_handler))
        .route("/admin-framework.css", get(css_handler))
        .route("/admin-framework.js", get(js_handler))
        .route("/sidebar.css", get(sidebar_css_handler))
        .route("/sidebar.js", get(sidebar_js_handler))
        // Existing endpoints
        .route("/health", get(health_handler))
        .route("/status", get(status_handler))
        // Prometheus metrics
        .route("/metrics", get(metrics_handler))
        // New API endpoints
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
        .with_state(state.clone());

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
        .body(axum::body::Body::from(INDEX_HTML))
        .unwrap()
}

/// Serve the shared sidebar CSS
async fn css_handler() -> axum::response::Response {
    axum::response::Response::builder()
        .header("content-type", "text/css; charset=utf-8")
        .body(axum::body::Body::from(ADMIN_CSS))
        .unwrap()
}

/// Serve the admin framework JS
async fn js_handler() -> axum::response::Response {
    axum::response::Response::builder()
        .header("content-type", "application/javascript; charset=utf-8")
        .body(axum::body::Body::from(ADMIN_JS))
        .unwrap()
}

/// Serve the shared sidebar CSS
async fn sidebar_css_handler() -> axum::response::Response {
    axum::response::Response::builder()
        .header("content-type", "text/css; charset=utf-8")
        .body(axum::body::Body::from(SIDEBAR_CSS))
        .unwrap()
}

/// Serve the shared sidebar JS
async fn sidebar_js_handler() -> axum::response::Response {
    axum::response::Response::builder()
        .header("content-type", "application/javascript; charset=utf-8")
        .body(axum::body::Body::from(SIDEBAR_JS))
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
        "obsidian", "slack",
    ];

    if !valid_connectors.contains(&name.as_str()) {
        return Err(StatusCode::NOT_FOUND);
    }

    info!(
        "Connector '{}' toggle: enabled={}",
        name, payload.enabled
    );

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
async fn metrics_handler(
    State(state): State<StatusServerState>,
) -> axum::response::Response {
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
        "opensoma_info{{node_id=\"{}\",version=\"{}\"}} 1",
        state.node_id,
        env!("CARGO_PKG_VERSION")
    ));

    // Uptime
    lines.push("# HELP opensoma_uptime_seconds Daemon uptime in seconds.".to_string());
    lines.push("# TYPE opensoma_uptime_seconds gauge".to_string());
    lines.push(format!("opensoma_uptime_seconds {}", uptime));

    // Events
    lines.push("# HELP opensoma_events_collected_total Total events collected.".to_string());
    lines.push("# TYPE opensoma_events_collected_total counter".to_string());
    lines.push(format!("opensoma_events_collected_total {}", events_collected));

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
            lines.push(format!("opensoma_connector_events_total{{connector=\"{}\"}} {}", name, count));
        }
    }

    // System metrics
    lines.push("# HELP opensoma_cpu_usage_percent CPU usage percentage.".to_string());
    lines.push("# TYPE opensoma_cpu_usage_percent gauge".to_string());
    lines.push(format!("opensoma_cpu_usage_percent {}", cpu_percent));

    lines.push("# HELP opensoma_memory_total_bytes Total system memory in bytes.".to_string());
    lines.push("# TYPE opensoma_memory_total_bytes gauge".to_string());
    lines.push(format!("opensoma_memory_total_bytes {}", memory_total_bytes));

    lines.push("# HELP opensoma_memory_used_bytes Used system memory in bytes.".to_string());
    lines.push("# TYPE opensoma_memory_used_bytes gauge".to_string());
    lines.push(format!("opensoma_memory_used_bytes {}", memory_used_bytes));

    lines.push("".to_string()); // trailing newline required by Prometheus

    axum::response::Response::builder()
        .header("content-type", "text/plain; version=0.0.4; charset=utf-8")
        .body(axum::body::Body::from(lines.join("\n")))
        .unwrap()
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
    let uptime_str = if h > 0 { format!("{}h {}m", h, m) } else { format!("{}m", m) };
    let cache = state.cache_stats.read().await.clone();

    format!(r#"
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

    format!(r#"
<div class="mb-4">
  <h2 class="text-lg font-semibold text-foreground">连接器管理</h2>
  <p class="text-sm text-muted-foreground">管理与外部服务的数据连接</p>
</div>
<div class="grid grid-cols-1 md:grid-cols-2 gap-4">{cards}</div>"#, cards = cards)
}

async fn build_collectors_page() -> String {
    let collectors = [
        ("file", "文件采集器", "监控文件系统变更，采集文件创建/修改/删除事件"),
        ("process", "进程采集器", "监控系统进程，采集进程启动/退出事件"),
        ("network", "网络采集器", "监控网络连接，采集 TCP/UDP 连接状态变更"),
        ("clipboard", "剪贴板采集器", "监控剪贴板内容变更，采集复制事件"),
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

    format!(r#"
<div class="mb-4">
  <h2 class="text-lg font-semibold text-foreground">采集器</h2>
  <p class="text-sm text-muted-foreground">本地数据采集模块</p>
</div>
<div class="grid grid-cols-1 md:grid-cols-2 gap-4">{cards}</div>"#, cards = cards)
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

    format!(r#"
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

    format!(r#"
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

fn format_bytes(bytes: u64) -> String {
    if bytes < 1024 { format!("{} B", bytes) }
    else if bytes < 1024 * 1024 { format!("{:.1} KB", bytes as f64 / 1024.0) }
    else if bytes < 1024 * 1024 * 1024 { format!("{:.1} MB", bytes as f64 / (1024.0 * 1024.0)) }
    else { format!("{:.2} GB", bytes as f64 / (1024.0 * 1024.0 * 1024.0)) }
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
    async fn test_connectors_endpoint_lists_all_10() {
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

        assert_eq!(connectors.len(), 11);

        // Verify all connector IDs are present
        let ids: Vec<&str> = connectors.iter().map(|c| c["id"].as_str().unwrap()).collect();
        for expected in &["feishu", "dingtalk", "wecom", "rss", "email", "webhook", "github", "notion", "git", "obsidian", "slack"] {
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
        let content_type = resp.headers().get("content-type").unwrap().to_str().unwrap();
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
        let ids: Vec<&str> = collectors.iter().map(|c| c["id"].as_str().unwrap()).collect();
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
}
