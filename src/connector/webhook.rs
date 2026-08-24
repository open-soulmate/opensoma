use anyhow::{Context, Result};
use axum::{http::StatusCode, routing::get, Router};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio::task::JoinHandle;
use tracing::{debug, error, info, warn};

use crate::collector::{EventTx, RawEvent};
use crate::config::WebhookConfig;
use crate::connector::circuit_breaker::CircuitBreaker;
use crate::connector::Connector;

/// Maximum requests per IP per minute before rate limiting kicks in.
const RATE_LIMIT_PER_MINUTE: u32 = 60;
/// Rate limit window duration.
const RATE_LIMIT_WINDOW_SECS: u64 = 60;

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
    rate_limiter: Arc<RwLock<IpRateLimiter>>,
}

/// Per-IP rate limiter using sliding window counters.
struct IpRateLimiter {
    /// IP address -> (request_count, window_start_timestamp_secs)
    counters: HashMap<String, (u32, u64)>,
}

impl IpRateLimiter {
    fn new() -> Self {
        Self {
            counters: HashMap::new(),
        }
    }

    /// Check if the IP is within rate limits. Returns true if allowed, false if throttled.
    fn check_and_record(&mut self, ip: &str) -> bool {
        let now = chrono::Utc::now().timestamp() as u64;
        let window_start = now.saturating_sub(RATE_LIMIT_WINDOW_SECS);

        let entry = self
            .counters
            .entry(ip.to_string())
            .or_insert((0, now));

        // Reset if window expired
        if entry.1 < window_start {
            *entry = (1, now);
            return true;
        }

        entry.0 += 1;
        entry.0 <= RATE_LIMIT_PER_MINUTE
    }

    /// Remove stale entries older than 2 windows.
    fn cleanup(&mut self) {
        let now = chrono::Utc::now().timestamp() as u64;
        let cutoff = now.saturating_sub(RATE_LIMIT_WINDOW_SECS * 2);
        self.counters.retain(|_, (_, start)| *start >= cutoff);
    }
}

/// Start the Webhook connector. Runs an HTTP server that receives webhook
/// payloads and forwards them into the collector pipeline.
pub async fn start(config: WebhookConfig, tx: EventTx, _circuit_breaker: Option<CircuitBreaker>) -> Result<JoinHandle<()>> {
    let listen = config.listen.clone();
    let state = WebhookState {
        tx,
        secret: config.secret.clone(),
        allowed_origins: config.allowed_origins.clone(),
        rate_limiter: Arc::new(RwLock::new(IpRateLimiter::new())),
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
        .route("/", get(webhook_health).post(webhook_handler))
        .route("/{*path}", get(webhook_health).post(webhook_handler))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(listen)
        .await
        .with_context(|| format!("Failed to bind webhook server to {}", listen))?;

    info!("Webhook server listening on {} (rate limit: {} req/min/IP)", listen, RATE_LIMIT_PER_MINUTE);

    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<std::net::SocketAddr>(),
    )
    .await
    .context("Webhook server error")?;

    Ok(())
}

/// Health check endpoint for the webhook server (GET requests).
async fn webhook_health() -> axum::response::Response {
    axum::response::Response::builder()
        .status(200)
        .header("content-type", "application/json")
        .body(axum::body::Body::from(
            r#"{"status":"ok","component":"webhook"}"#,
        ))
        .unwrap()
}

/// Axum handler for incoming webhook POST requests.
async fn webhook_handler(
    axum::extract::State(state): axum::extract::State<WebhookState>,
    axum::extract::Path(path): axum::extract::Path<String>,
    headers: axum::http::HeaderMap,
    axum::extract::ConnectInfo(addr): axum::extract::ConnectInfo<std::net::SocketAddr>,
    body: String,
) -> Result<axum::response::Response, StatusCode> {
    // Extract client IP (check X-Forwarded-For, then X-Real-IP, then socket addr)
    let client_ip = headers
        .get("x-forwarded-for")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.split(',').next())
        .map(|s| s.trim().to_string())
        .or_else(|| {
            headers
                .get("x-real-ip")
                .and_then(|v| v.to_str().ok())
                .map(|s| s.to_string())
        })
        .unwrap_or_else(|| addr.ip().to_string());

    // Rate limit check
    {
        let mut limiter = state.rate_limiter.write().await;
        limiter.cleanup();
        if !limiter.check_and_record(&client_ip) {
            warn!("Webhook rate limited: IP {}", client_ip);
            return Err(StatusCode::TOO_MANY_REQUESTS);
        }
    }

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

    #[test]
    fn test_verify_hmac_signature_empty_body() {
        use hmac::{Hmac, Mac};
        use sha2::Sha256;
        type HmacSha256 = Hmac<Sha256>;

        let secret = "test-secret";
        let body = b"";
        let mut mac = HmacSha256::new_from_slice(secret.as_bytes()).unwrap();
        mac.update(body);
        let sig = format!("sha256={}", hex::encode(mac.finalize().into_bytes()));

        assert!(verify_hmac_signature(secret, body, &sig));
    }

    #[test]
    fn test_verify_hmac_signature_large_body() {
        use hmac::{Hmac, Mac};
        use sha2::Sha256;
        type HmacSha256 = Hmac<Sha256>;

        let secret = "test-secret";
        let body = vec![0xABu8; 1024 * 1024]; // 1MB body
        let mut mac = HmacSha256::new_from_slice(secret.as_bytes()).unwrap();
        mac.update(&body);
        let sig = format!("sha256={}", hex::encode(mac.finalize().into_bytes()));

        assert!(verify_hmac_signature(secret, &body, &sig));
    }

    #[test]
    fn test_verify_hmac_signature_empty_prefix() {
        // Signature without "sha256=" prefix should fail
        assert!(!verify_hmac_signature("secret", b"body", "abcdef123456"));
    }

    #[test]
    fn test_verify_hmac_signature_github_format() {
        // GitHub uses x-hub-signature-256 with sha256= prefix
        use hmac::{Hmac, Mac};
        use sha2::Sha256;
        type HmacSha256 = Hmac<Sha256>;

        let secret = "webhook-secret";
        let body = b"{\"action\":\"opened\"}";
        let mut mac = HmacSha256::new_from_slice(secret.as_bytes()).unwrap();
        mac.update(body);
        let sig = format!("sha256={}", hex::encode(mac.finalize().into_bytes()));

        assert!(verify_hmac_signature(secret, body, &sig));
    }

    #[test]
    fn test_constant_time_eq_single_byte_diff() {
        // Differ by only one byte
        assert!(!constant_time_eq(b"aaa", b"aab"));
    }

    #[test]
    fn test_constant_time_eq_long_strings() {
        let a = vec![0x42u8; 10000];
        let b = vec![0x42u8; 10000];
        assert!(constant_time_eq(&a, &b));

        let mut c = vec![0x42u8; 10000];
        c[9999] = 0x43;
        assert!(!constant_time_eq(&a, &c));
    }

    #[test]
    fn test_webhook_state_clone() {
        let state = WebhookState {
            tx: tokio::sync::mpsc::channel(1).0,
            secret: Some("test".to_string()),
            allowed_origins: vec!["https://example.com".to_string()],
            rate_limiter: Arc::new(RwLock::new(IpRateLimiter::new())),
        };
        let cloned = state.clone();
        assert_eq!(cloned.secret, Some("test".to_string()));
        assert_eq!(cloned.allowed_origins.len(), 1);
    }

    #[test]
    fn test_allowed_origins_matching() {
        // Simulate the origin check logic from webhook_handler
        let allowed_origins = vec![
            "https://example.com".to_string(),
            "https://trusted.org".to_string(),
        ];

        // Matching origin
        let origin = "https://example.com";
        assert!(
            allowed_origins.is_empty() || allowed_origins.iter().any(|o| origin.starts_with(o))
        );

        // Non-matching origin
        let origin = "https://evil.com";
        assert!(!allowed_origins.iter().any(|o| origin.starts_with(o)));

        // Empty allowed list = allow all
        let empty_origins: Vec<String> = vec![];
        let origin = "https://anything.com";
        assert!(empty_origins.is_empty() || empty_origins.iter().any(|o| origin.starts_with(o)));
    }

    #[test]
    fn test_rate_limiter_allows_within_limit() {
        let mut limiter = IpRateLimiter::new();
        for _ in 0..RATE_LIMIT_PER_MINUTE {
            assert!(limiter.check_and_record("192.168.1.1"));
        }
    }

    #[test]
    fn test_rate_limiter_blocks_over_limit() {
        let mut limiter = IpRateLimiter::new();
        for _ in 0..RATE_LIMIT_PER_MINUTE {
            assert!(limiter.check_and_record("10.0.0.1"));
        }
        // Next request should be blocked
        assert!(!limiter.check_and_record("10.0.0.1"));
    }

    #[test]
    fn test_rate_limiter_per_ip_isolation() {
        let mut limiter = IpRateLimiter::new();
        // Exhaust limit for IP A
        for _ in 0..RATE_LIMIT_PER_MINUTE {
            assert!(limiter.check_and_record("10.0.0.1"));
        }
        assert!(!limiter.check_and_record("10.0.0.1"));
        // IP B should still be allowed
        assert!(limiter.check_and_record("10.0.0.2"));
    }

    #[test]
    fn test_rate_limiter_cleanup() {
        let mut limiter = IpRateLimiter::new();
        limiter.check_and_record("192.168.1.1");
        assert_eq!(limiter.counters.len(), 1);
        limiter.cleanup();
        // Entry is recent, should survive cleanup
        assert_eq!(limiter.counters.len(), 1);
    }

    #[test]
    fn test_rate_limiter_new_is_empty() {
        let limiter = IpRateLimiter::new();
        assert!(limiter.counters.is_empty());
    }
}
