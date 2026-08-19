use anyhow::{Context, Result};
use axum::{
    http::StatusCode,
    routing::post,
    Router,
};
use tokio::task::JoinHandle;
use tracing::{debug, error, info, warn};

use crate::collector::{EventTx, RawEvent};
use crate::config::WebhookConfig;
use crate::connector::Connector;

/// Webhook connector implementing the unified Connector trait.
pub struct WebhookConnector {
    config: WebhookConfig,
}

impl WebhookConnector {
    pub fn new(config: WebhookConfig) -> Self {
        Self { config }
    }
}

#[async_trait::async_trait]
impl Connector for WebhookConnector {
    fn name(&self) -> &str {
        "webhook"
    }

    async fn ping(&self) -> Result<()> {
        // Try to bind to the listen address to verify it's available
        let listener = tokio::net::TcpListener::bind(&self.config.listen)
            .await
            .with_context(|| format!("Cannot bind to {}", self.config.listen))?;
        // Drop the listener immediately (we just wanted to test the bind)
        drop(listener);
        Ok(())
    }
}

/// Shared state for the webhook server.
#[derive(Clone)]
struct WebhookState {
    tx: EventTx,
    secret: Option<String>,
    allowed_origins: Vec<String>,
}

/// Start the Webhook connector. Runs an HTTP server that receives webhook
/// payloads and forwards them into the collector pipeline.
pub async fn start(config: WebhookConfig, tx: EventTx) -> Result<JoinHandle<()>> {
    let listen = config.listen.clone();
    let state = WebhookState {
        tx,
        secret: config.secret.clone(),
        allowed_origins: config.allowed_origins.clone(),
    };

    info!("Webhook connector starting — listening on {}", listen);

    let handle = tokio::spawn(async move {
        if let Err(e) = run_axum_server(&listen, state).await {
            error!("Webhook server failed: {}", e);
        }
    });

    Ok(handle)
}

/// Run the webhook HTTP server using axum.
async fn run_axum_server(listen: &str, state: WebhookState) -> Result<()> {
    let app = Router::new()
        .route("/{*path}", post(webhook_handler))
        .route("/", post(webhook_handler))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(listen)
        .await
        .with_context(|| format!("Failed to bind webhook server to {}", listen))?;

    info!("Webhook server listening on {}", listen);

    axum::serve(listener, app)
        .await
        .context("Webhook server error")?;

    Ok(())
}

/// Axum handler for incoming webhook POST requests.
async fn webhook_handler(
    axum::extract::State(state): axum::extract::State<WebhookState>,
    axum::extract::Path(path): axum::extract::Path<String>,
    headers: axum::http::HeaderMap,
    body: String,
) -> Result<axum::response::Response, StatusCode> {
    // Check allowed origins
    if let Some(origin) = headers.get("origin").and_then(|v| v.to_str().ok()) {
        if !state.allowed_origins.is_empty()
            && !state.allowed_origins.iter().any(|o| origin.starts_with(o))
        {
            warn!("Webhook rejected: origin '{}' not in allowed list", origin);
            return Err(StatusCode::FORBIDDEN);
        }
    }

    // Verify HMAC signature if secret is configured
    if let Some(ref secret) = state.secret {
        let signature = headers
            .get("x-signature")
            .or_else(|| headers.get("x-hub-signature-256"))
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");

        if !verify_hmac_signature(secret, body.as_bytes(), signature) {
            warn!("Webhook signature mismatch");
            return Err(StatusCode::UNAUTHORIZED);
        }
    }

    // Build the event
    let source_id = path.replace('/', "_");
    let payload_json = serde_json::json!({
        "path": format!("/{}", path),
        "body": body.chars().take(10_000).collect::<String>(),
    });

    let event = RawEvent {
        id: uuid::Uuid::new_v4().to_string(),
        source: format!("connector:webhook:{}", source_id),
        event_type: "webhook_received".to_string(),
        timestamp_ms: chrono::Utc::now().timestamp_millis(),
        payload: payload_json.to_string().into_bytes(),
        tags: {
            let mut tags = std::collections::HashMap::new();
            tags.insert("connector".to_string(), "webhook".to_string());
            tags.insert("path".to_string(), format!("/{}", path));
            tags
        },
    };

    if state.tx.send(event).await.is_err() {
        error!("Webhook collector channel closed");
        return Err(StatusCode::SERVICE_UNAVAILABLE);
    }

    debug!("Webhook received on path: /{}", path);

    Ok(axum::response::Response::builder()
        .status(200)
        .header("content-type", "application/json")
        .body(axum::body::Body::from(r#"{"ok":true}"#))
        .unwrap())
}

/// Verify HMAC-SHA256 signature using constant-time comparison.
fn verify_hmac_signature(secret: &str, body: &[u8], signature: &str) -> bool {
    use hmac::{Hmac, Mac};
    use sha2::Sha256;
    type HmacSha256 = Hmac<Sha256>;

    let mut mac = match HmacSha256::new_from_slice(secret.as_bytes()) {
        Ok(m) => m,
        Err(_) => return false,
    };
    mac.update(body);
    let expected = format!("sha256={}", hex::encode(mac.finalize().into_bytes()));

    constant_time_eq(signature.as_bytes(), expected.as_bytes())
}

/// Constant-time byte comparison.
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_constant_time_eq_same() {
        assert!(constant_time_eq(b"hello", b"hello"));
    }

    #[test]
    fn test_constant_time_eq_different() {
        assert!(!constant_time_eq(b"hello", b"world"));
    }

    #[test]
    fn test_constant_time_eq_different_length() {
        assert!(!constant_time_eq(b"hi", b"hello"));
    }

    #[test]
    fn test_constant_time_eq_empty() {
        assert!(constant_time_eq(b"", b""));
    }

    #[test]
    fn test_verify_hmac_signature_valid() {
        use hmac::{Hmac, Mac};
        use sha2::Sha256;
        type HmacSha256 = Hmac<Sha256>;

        let secret = "test-secret";
        let body = b"test body";
        let mut mac = HmacSha256::new_from_slice(secret.as_bytes()).unwrap();
        mac.update(body);
        let sig = format!("sha256={}", hex::encode(mac.finalize().into_bytes()));

        assert!(verify_hmac_signature(secret, body, &sig));
    }

    #[test]
    fn test_verify_hmac_signature_invalid() {
        assert!(!verify_hmac_signature("secret", b"body", "sha256=invalid"));
    }

    #[test]
    fn test_verify_hmac_signature_wrong_secret() {
        use hmac::{Hmac, Mac};
        use sha2::Sha256;
        type HmacSha256 = Hmac<Sha256>;

        let mut mac = HmacSha256::new_from_slice(b"correct-secret").unwrap();
        mac.update(b"body");
        let sig = format!("sha256={}", hex::encode(mac.finalize().into_bytes()));

        assert!(!verify_hmac_signature("wrong-secret", b"body", &sig));
    }
}
