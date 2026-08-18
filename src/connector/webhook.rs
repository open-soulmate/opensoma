use anyhow::Result;
use tokio::task::JoinHandle;
use tracing::{debug, error, info, warn};

use crate::collector::{EventTx, RawEvent};
use crate::config::WebhookConfig;

/// Start the Webhook connector. Runs an HTTP server that receives webhook
/// payloads and forwards them into the collector pipeline.
pub async fn start(config: WebhookConfig, tx: EventTx) -> Result<JoinHandle<()>> {
    let listen = config.listen.clone();
    let secret = config.secret.clone();
    let allowed_origins = config.allowed_origins.clone();

    info!("Webhook connector starting — listening on {}", listen);

    let handle = tokio::spawn(async move {
        if let Err(e) = run_server(&listen, secret, allowed_origins, tx).await {
            error!("Webhook server failed: {}", e);
        }
    });

    Ok(handle)
}

/// Run the webhook HTTP server using a minimal hyper-based approach.
/// For simplicity we use a raw TCP listener + manual HTTP parsing to avoid
/// pulling in the full axum/actix stack (OpenSoma is a lightweight daemon).
async fn run_server(
    listen: &str,
    secret: Option<String>,
    allowed_origins: Vec<String>,
    tx: EventTx,
) -> Result<()> {
    let listener = tokio::net::TcpListener::bind(listen).await?;
    info!("Webhook server listening on {}", listen);

    loop {
        let (stream, addr) = listener.accept().await?;
        debug!("Webhook connection from {}", addr);

        let tx = tx.clone();
        let secret = secret.clone();
        let origins = allowed_origins.clone();

        tokio::spawn(async move {
            if let Err(e) = handle_connection(stream, secret, origins, tx).await {
                warn!("Webhook request error from {}: {}", addr, e);
            }
        });
    }
}

/// Handle a single HTTP connection.
async fn handle_connection(
    mut stream: tokio::net::TcpStream,
    secret: Option<String>,
    allowed_origins: Vec<String>,
    tx: EventTx,
) -> Result<()> {
    use tokio::io::{AsyncBufReadExt, AsyncReadExt};

    let mut reader = tokio::io::BufReader::new(&mut stream);
    let mut request_line = String::new();
    reader.read_line(&mut request_line).await?;

    let parts: Vec<&str> = request_line.trim().split_whitespace().collect();
    if parts.len() < 2 {
        send_response(&mut stream, 400, "Bad Request").await?;
        return Ok(());
    }

    let method = parts[0];
    let path = parts[1];

    // Read headers
    let mut content_length: usize = 0;
    let mut origin = String::new();
    let mut signature = String::new();
    loop {
        let mut header_line = String::new();
        reader.read_line(&mut header_line).await?;
        if header_line.trim().is_empty() {
            break;
        }
        let lower = header_line.to_lowercase();
        if lower.starts_with("content-length:") {
            content_length = lower["content-length:".len()..].trim().parse().unwrap_or(0);
        }
        if lower.starts_with("origin:") {
            origin = header_line["Origin:".len()..].trim().to_string();
        }
        if lower.starts_with("x-signature:") || lower.starts_with("x-hub-signature-256:") {
            signature = header_line
                .split_once(':')
                .map(|(_, v)| v.trim().to_string())
                .unwrap_or_default();
        }
    }

    // Only accept POST
    if method != "POST" {
        send_response(&mut stream, 405, "Method Not Allowed").await?;
        return Ok(());
    }

    // Check allowed origins
    if !allowed_origins.is_empty() && !origin.is_empty() {
        if !allowed_origins.iter().any(|o| origin.starts_with(o)) {
            send_response(&mut stream, 403, "Origin not allowed").await?;
            return Ok(());
        }
    }

    // Read body
    let mut body = vec![0u8; content_length.min(1_048_576)]; // max 1MB
    if content_length > 0 {
        reader.read_exact(&mut body).await?;
    }

    // Verify HMAC signature if secret is configured
    if let Some(ref secret) = secret {
        use hmac::{Hmac, Mac};
        use sha2::Sha256;
        type HmacSha256 = Hmac<Sha256>;

        let mut mac =
            HmacSha256::new_from_slice(secret.as_bytes()).map_err(|e| anyhow::anyhow!("{}", e))?;
        mac.update(&body);
        let expected = format!("sha256={}", hex::encode(mac.finalize().into_bytes()));

        if !constant_time_eq(signature.as_bytes(), expected.as_bytes()) {
            warn!("Webhook signature mismatch");
            send_response(&mut stream, 401, "Invalid signature").await?;
            return Ok(());
        }
    }

    // Parse body as JSON (or store as raw text)
    let body_str = String::from_utf8_lossy(&body);

    // Extract source info from path
    let source_id = path.trim_start_matches('/').replace('/', "_");

    let payload_json = serde_json::json!({
        "path": path,
        "method": method,
        "origin": origin,
        "content_length": content_length,
        "body": body_str.chars().take(10_000).collect::<String>(),
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
            tags.insert("path".to_string(), path.to_string());
            tags
        },
    };

    if tx.send(event).await.is_err() {
        error!("Webhook collector channel closed");
        send_response(&mut stream, 503, "Service Unavailable").await?;
        return Ok(());
    }

    debug!("Webhook received on path: {}", path);
    send_response(&mut stream, 200, r#"{"ok":true}"#).await?;
    Ok(())
}

/// Send a simple HTTP response.
async fn send_response(stream: &mut tokio::net::TcpStream, status: u16, body: &str) -> Result<()> {
    use tokio::io::AsyncWriteExt;
    let reason = match status {
        200 => "OK",
        400 => "Bad Request",
        401 => "Unauthorized",
        403 => "Forbidden",
        405 => "Method Not Allowed",
        503 => "Service Unavailable",
        _ => "Unknown",
    };
    let response = format!(
        "HTTP/1.1 {} {}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        status,
        reason,
        body.len(),
        body
    );
    stream.write_all(response.as_bytes()).await?;
    stream.flush().await?;
    Ok(())
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
