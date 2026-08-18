use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::Html,
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
const INDEX_HTML: &str = include_str!("web/index.html");
const ADMIN_CSS: &str = include_str!("web/admin-framework.css");
const ADMIN_JS: &str = include_str!("web/admin-framework.js");

/// Start the HTTP status server on the given port.
/// Exposes /health, /status, /api/* endpoints and the web UI.
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
        // Existing endpoints
        .route("/health", get(health_handler))
        .route("/status", get(status_handler))
        // Prometheus metrics
        .route("/metrics", get(metrics_handler))
        // New API endpoints
        .route("/api/status", get(api_status_handler))
        .route("/api/connectors", get(api_connectors_handler))
        .route("/api/collectors", get(api_collectors_handler))
        .route("/api/connectors/{name}/toggle", post(api_connector_toggle))
        .route("/api/connectors/{name}/events", get(api_connector_events))
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
async fn index_handler() -> Html<&'static str> {
    Html(INDEX_HTML)
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
        "obsidian",
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
}
