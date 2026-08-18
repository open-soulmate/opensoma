use anyhow::{Context, Result};
use std::time::Duration;
use tracing::{debug, info, warn};

use super::soul;
use crate::config::SoulConfig;

/// HTTP client for communicating with OpenSoul's Nerve API.
/// Replaces the stub gRPC client with real HTTP calls.
#[derive(Clone)]
pub struct SoulClient {
    /// Base URL of the OpenSoul server (e.g. "http://localhost:8090")
    base_url: String,
    /// HTTP client with connection pooling
    http: reqwest::Client,
    /// Our node_id for registration/heartbeat
    node_id: String,
}

impl SoulClient {
    /// Create a new SoulClient connected to the configured endpoint.
    pub async fn new(config: &SoulConfig) -> Result<Self> {
        let base_url = config.endpoint.trim_end_matches('/').to_string();
        let timeout = Duration::from_secs(config.connect_timeout);

        info!("Connecting to Soul at: {}", base_url);

        let http = reqwest::Client::builder()
            .timeout(timeout)
            .build()
            .context("Failed to build HTTP client")?;

        // Verify connectivity
        let health_url = format!("{}/api/health", base_url);
        match http.get(&health_url).send().await {
            Ok(resp) if resp.status().is_success() => {
                info!("Soul health check OK at {}", base_url);
            }
            Ok(resp) => {
                warn!(
                    "Soul health check returned {} — will retry on next operation",
                    resp.status()
                );
            }
            Err(e) => {
                warn!(
                    "Soul not reachable at {}: {} — will retry on next operation",
                    base_url, e
                );
            }
        }

        Ok(Self {
            base_url,
            http,
            node_id: String::new(),
        })
    }

    /// Register our node with Soul's Nerve bus.
    pub async fn register_node(&self, node_id: &str, node_type: &str) -> Result<()> {
        let url = format!("{}/api/nerve/nodes/register", self.base_url);
        let body = serde_json::json!({
            "node_id": node_id,
            "node_type": node_type,
            "metadata": {
                "version": env!("CARGO_PKG_VERSION"),
                "runtime": "opensoma-rust"
            }
        });

        let resp = self
            .http
            .post(&url)
            .json(&body)
            .send()
            .await
            .context("Failed to register node")?;

        if resp.status().is_success() {
            info!("Node '{}' registered with Soul Nerve bus", node_id);
        } else {
            warn!(
                "Node registration returned {}: {}",
                resp.status(),
                resp.text().await.unwrap_or_default()
            );
        }

        Ok(())
    }

    /// Send a heartbeat to Soul via the Nerve API.
    pub async fn heartbeat(&self, node_id: &str) -> Result<soul::HeartbeatResponse> {
        debug!("Sending heartbeat for node: {}", node_id);

        let url = format!("{}/api/nerve/nodes/heartbeat", self.base_url);
        let body = serde_json::json!({
            "node_id": node_id
        });

        match self.http.post(&url).json(&body).send().await {
            Ok(resp) if resp.status().is_success() => {
                debug!("Heartbeat OK for node: {}", node_id);
                Ok(soul::HeartbeatResponse {
                    server_timestamp_ms: chrono::Utc::now().timestamp_millis(),
                    ok: true,
                    message: "pong".to_string(),
                })
            }
            Ok(resp) => {
                let status = resp.status();
                let text = resp.text().await.unwrap_or_default();
                warn!("Heartbeat returned {}: {}", status, text);
                Ok(soul::HeartbeatResponse {
                    server_timestamp_ms: chrono::Utc::now().timestamp_millis(),
                    ok: false,
                    message: format!("HTTP {}: {}", status, text),
                })
            }
            Err(e) => {
                warn!("Heartbeat failed: {}. Will retry next cycle.", e);
                Ok(soul::HeartbeatResponse {
                    server_timestamp_ms: chrono::Utc::now().timestamp_millis(),
                    ok: false,
                    message: e.to_string(),
                })
            }
        }
    }

    /// Upload a batch of collected events to Soul via the Nerve publish API.
    /// Events are published to topic "soma.events" with the event data.
    pub async fn upload_events(
        &self,
        events: &[soul::CollectedEvent],
    ) -> Result<soul::UploadEventsResponse> {
        debug!("Uploading {} events to Soul", events.len());

        let mut accepted: i64 = 0;
        let mut rejected: i64 = 0;
        let mut reject_reasons: Vec<String> = Vec::new();

        // Publish each event to the Nerve bus
        for event in events {
            let url = format!("{}/api/nerve/publish", self.base_url);

            // Convert payload bytes to string (best-effort)
            let payload_str = String::from_utf8_lossy(&event.payload).to_string();

            let body = serde_json::json!({
                "topic": format!("soma.{}", event.event_type),
                "data": {
                    "id": event.id,
                    "source": event.source,
                    "event_type": event.event_type,
                    "timestamp_ms": event.timestamp_ms,
                    "payload": payload_str,
                    "tags": event.tags,
                },
                "source": format!("opensoma:{}", event.source),
            });

            match self.http.post(&url).json(&body).send().await {
                Ok(resp) if resp.status().is_success() => {
                    accepted += 1;
                }
                Ok(resp) => {
                    rejected += 1;
                    reject_reasons.push(format!("HTTP {} for event {}", resp.status(), event.id));
                }
                Err(e) => {
                    rejected += 1;
                    reject_reasons.push(format!("Network error for event {}: {}", event.id, e));
                }
            }
        }

        if accepted > 0 {
            info!("Uploaded {}/{} events to Soul", accepted, events.len());
        }
        if rejected > 0 {
            warn!(
                "Rejected {}/{} events: {:?}",
                rejected,
                events.len(),
                reject_reasons
            );
        }

        Ok(soul::UploadEventsResponse {
            accepted,
            rejected,
            new_cursor: String::new(),
            reject_reasons,
        })
    }

    /// Get the connected endpoint URL.
    pub fn endpoint(&self) -> &str {
        &self.base_url
    }

    /// Check if the server is reachable.
    pub async fn is_ready(&self) -> bool {
        let url = format!("{}/api/health", self.base_url);
        self.http
            .get(&url)
            .send()
            .await
            .map(|r| r.status().is_success())
            .unwrap_or(false)
    }
    /// Stream a single event in real-time (StreamEvents HTTP equivalent).
    /// Unlike batch upload, this sends events immediately for low-latency use cases.
    /// Falls back gracefully if the endpoint is unavailable.
    pub async fn stream_event(&self, event: &soul::CollectedEvent) -> Result<bool> {
        let url = format!("{}/api/nerve/stream/upload", self.base_url);
        let payload_str = String::from_utf8_lossy(&event.payload).to_string();
        let body = serde_json::json!({
            "id": event.id,
            "source": event.source,
            "event_type": event.event_type,
            "timestamp_ms": event.timestamp_ms,
            "payload": payload_str,
            "tags": event.tags,
        });

        match self.http.post(&url).json(&body).send().await {
            Ok(resp) if resp.status().is_success() => Ok(true),
            Ok(resp) => {
                debug!("Stream upload returned {}: {}", resp.status(), resp.text().await.unwrap_or_default());
                Ok(false)
            }
            Err(e) => {
                debug!("Stream upload failed (will batch): {}", e);
                Ok(false)
            }
        }
    }
}
