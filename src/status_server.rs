use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::Html,
    routing::{get, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};
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
        // New API endpoints
        .route("/api/status", get(api_status_handler))
        .route("/api/connectors", get(api_connectors_handler))
        .route("/api/collectors", get(api_collectors_handler))
        .route("/api/connectors/{name}/toggle", post(api_connector_toggle))
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

/// /api/connectors — list all known connectors with their active state
async fn api_connectors_handler(
    State(state): State<StatusServerState>,
) -> Json<Vec<ConnectorInfo>> {
    let active = state.connectors_active.read().await.clone();
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
        .map(|(id, name)| ConnectorInfo {
            id: id.to_string(),
            name: name.to_string(),
            enabled: active.contains(&id.to_string()),
            status: if active.contains(&id.to_string()) {
                "enabled".to_string()
            } else {
                "disabled".to_string()
            },
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
        })
        .collect();

    Json(list)
}

/// /api/connectors/{name}/toggle — toggle a connector on/off
async fn api_connector_toggle(
    Path(name): Path<String>,
    Json(payload): Json<ToggleRequest>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    info!(
        "Connector '{}' toggle requested: enabled={}",
        name, payload.enabled
    );
    // In a real implementation this would update the connector state.
    // For now, acknowledge the request.
    Ok(Json(serde_json::json!({
        "connector": name,
        "enabled": payload.enabled,
        "status": "ok"
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
