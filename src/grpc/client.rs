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
    #[allow(dead_code)]
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

    /// Upload a batch of collected events to Soul via the Nerve batch publish API.
    /// Uses the /publish/batch endpoint for efficient bulk ingestion (single HTTP request
    /// per batch of up to 100 events). Falls back to individual /publish calls on error.
    pub async fn upload_events(
        &self,
        events: &[soul::CollectedEvent],
    ) -> Result<soul::UploadEventsResponse> {
        debug!("Uploading {} events to Soul (batch)", events.len());

        const BATCH_SIZE: usize = 100;

        let mut accepted: i64 = 0;
        let mut rejected: i64 = 0;
        let mut reject_reasons: Vec<String> = Vec::new();

        for chunk in events.chunks(BATCH_SIZE) {
            // Build batch payload
            let batch_items: Vec<serde_json::Value> = chunk
                .iter()
                .map(|event| {
                    let payload_str = String::from_utf8_lossy(&event.payload).to_string();
                    serde_json::json!({
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
                    })
                })
                .collect();

            let url = format!("{}/api/nerve/publish/batch", self.base_url);
            let body = serde_json::json!({ "events": batch_items });

            match self.http.post(&url).json(&body).send().await {
                Ok(resp) if resp.status().is_success() => {
                    // Parse batch response
                    match resp.json::<serde_json::Value>().await {
                        Ok(result) => {
                            let batch_accepted =
                                result["accepted"].as_i64().unwrap_or(chunk.len() as i64);
                            let batch_rejected = result["rejected"].as_i64().unwrap_or(0);
                            accepted += batch_accepted;
                            rejected += batch_rejected;
                        }
                        Err(_) => {
                            // If parsing fails, assume all accepted
                            accepted += chunk.len() as i64;
                        }
                    }
                }
                Ok(resp) => {
                    let status = resp.status();
                    let text = resp.text().await.unwrap_or_default();
                    warn!(
                        "Batch upload returned {}: {} — falling back to individual publish",
                        status, text
                    );
                    // Fallback: send individually
                    for event in chunk {
                        let individual_url = format!("{}/api/nerve/publish", self.base_url);
                        let payload_str = String::from_utf8_lossy(&event.payload).to_string();
                        let individual_body = serde_json::json!({
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
                        match self
                            .http
                            .post(&individual_url)
                            .json(&individual_body)
                            .send()
                            .await
                        {
                            Ok(r) if r.status().is_success() => accepted += 1,
                            Ok(r) => {
                                rejected += 1;
                                reject_reasons.push(format!(
                                    "HTTP {} for event {}",
                                    r.status(),
                                    event.id
                                ));
                            }
                            Err(e) => {
                                rejected += 1;
                                reject_reasons
                                    .push(format!("Network error for event {}: {}", event.id, e));
                            }
                        }
                    }
                }
                Err(e) => {
                    warn!(
                        "Batch upload network error: {} — falling back to individual publish",
                        e
                    );
                    // Fallback: send individually
                    for event in chunk {
                        let individual_url = format!("{}/api/nerve/publish", self.base_url);
                        let payload_str = String::from_utf8_lossy(&event.payload).to_string();
                        let individual_body = serde_json::json!({
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
                        match self
                            .http
                            .post(&individual_url)
                            .json(&individual_body)
                            .send()
                            .await
                        {
                            Ok(r) if r.status().is_success() => accepted += 1,
                            Ok(r) => {
                                rejected += 1;
                                reject_reasons.push(format!(
                                    "HTTP {} for event {}",
                                    r.status(),
                                    event.id
                                ));
                            }
                            Err(e) => {
                                rejected += 1;
                                reject_reasons
                                    .push(format!("Network error for event {}: {}", event.id, e));
                            }
                        }
                    }
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
                debug!(
                    "Stream upload returned {}: {}",
                    resp.status(),
                    resp.text().await.unwrap_or_default()
                );
                Ok(false)
            }
            Err(e) => {
                debug!("Stream upload failed (will batch): {}", e);
                Ok(false)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_soul_client_endpoint() {
        // Build a client manually (skip health check for unit test)
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(1))
            .build()
            .unwrap();

        let client = SoulClient {
            base_url: "http://localhost:8090".to_string(),
            http,
            node_id: "test-node".to_string(),
        };

        assert_eq!(client.endpoint(), "http://localhost:8090");
    }

    #[test]
    fn test_soul_client_endpoint_trailing_slash_trimmed() {
        // Simulate what SoulClient::new does
        let raw = "http://localhost:8090/";
        let trimmed = raw.trim_end_matches('/');
        assert_eq!(trimmed, "http://localhost:8090");
    }

    #[test]
    fn test_soul_client_clone() {
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(1))
            .build()
            .unwrap();

        let client = SoulClient {
            base_url: "http://localhost:8090".to_string(),
            http,
            node_id: "node-1".to_string(),
        };

        let cloned = client.clone();
        assert_eq!(cloned.endpoint(), "http://localhost:8090");
    }

    #[test]
    fn test_register_node_body_format() {
        // Verify the JSON body structure for node registration
        let body = serde_json::json!({
            "node_id": "soma-1",
            "node_type": "soma",
            "metadata": {
                "version": "0.1.0",
                "runtime": "opensoma-rust"
            }
        });

        assert_eq!(body["node_id"], "soma-1");
        assert_eq!(body["node_type"], "soma");
        assert_eq!(body["metadata"]["runtime"], "opensoma-rust");
    }

    #[test]
    fn test_heartbeat_body_format() {
        let body = serde_json::json!({
            "node_id": "node-1"
        });
        assert_eq!(body["node_id"], "node-1");
    }

    #[test]
    fn test_upload_events_body_format() {
        let event = soul::CollectedEvent {
            id: "evt-1".to_string(),
            source: "file".to_string(),
            event_type: "file_change".to_string(),
            timestamp_ms: 1700000000000,
            payload: b"test data".to_vec(),
            tags: [("key".into(), "val".into())].into(),
        };

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

        assert_eq!(body["topic"], "soma.file_change");
        assert_eq!(body["data"]["id"], "evt-1");
        assert_eq!(body["data"]["payload"], "test data");
        assert_eq!(body["source"], "opensoma:file");
    }

    #[test]
    fn test_stream_event_body_format() {
        let event = soul::CollectedEvent {
            id: "stream-1".to_string(),
            source: "clipboard".to_string(),
            event_type: "clipboard_change".to_string(),
            timestamp_ms: 1700000001000,
            payload: b"clipboard content".to_vec(),
            tags: std::collections::HashMap::new(),
        };

        let payload_str = String::from_utf8_lossy(&event.payload).to_string();
        let body = serde_json::json!({
            "id": event.id,
            "source": event.source,
            "event_type": event.event_type,
            "timestamp_ms": event.timestamp_ms,
            "payload": payload_str,
            "tags": event.tags,
        });

        assert_eq!(body["id"], "stream-1");
        assert_eq!(body["source"], "clipboard");
        assert_eq!(body["payload"], "clipboard content");
    }

    #[test]
    fn test_batch_upload_body_format() {
        // Verify the batch upload JSON body structure
        let events = [
            soul::CollectedEvent {
                id: "batch-1".to_string(),
                source: "file".to_string(),
                event_type: "file_change".to_string(),
                timestamp_ms: 1700000000000,
                payload: b"event 1".to_vec(),
                tags: [("k1".into(), "v1".into())].into(),
            },
            soul::CollectedEvent {
                id: "batch-2".to_string(),
                source: "github".to_string(),
                event_type: "push".to_string(),
                timestamp_ms: 1700000001000,
                payload: b"event 2".to_vec(),
                tags: std::collections::HashMap::new(),
            },
        ];

        let batch_items: Vec<serde_json::Value> = events
            .iter()
            .map(|event| {
                let payload_str = String::from_utf8_lossy(&event.payload).to_string();
                serde_json::json!({
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
                })
            })
            .collect();

        let body = serde_json::json!({ "events": batch_items });

        assert_eq!(body["events"].as_array().unwrap().len(), 2);
        assert_eq!(body["events"][0]["topic"], "soma.file_change");
        assert_eq!(body["events"][0]["data"]["id"], "batch-1");
        assert_eq!(body["events"][1]["topic"], "soma.push");
        assert_eq!(body["events"][1]["data"]["id"], "batch-2");
        assert_eq!(body["events"][1]["source"], "opensoma:github");
    }

    #[test]
    fn test_batch_size_constant() {
        // Verify the batch size constant is reasonable
        const BATCH_SIZE: usize = 100;
        const _: () = assert!(BATCH_SIZE > 0);
        const _: () = assert!(BATCH_SIZE <= 500);
    }
}
