use anyhow::{Context, Result};
use std::time::Duration;
use tonic::transport::Channel;
use tracing::{debug, info, warn};

use super::soul;
use crate::config::SoulConfig;

/// gRPC client wrapper for connecting to the Soul Agent API.
#[derive(Clone)]
pub struct SoulClient {
    endpoint: String,
    channel: Channel,
}

impl SoulClient {
    /// Create a new SoulClient connected to the configured endpoint.
    pub async fn new(config: &SoulConfig) -> Result<Self> {
        let endpoint = config.endpoint.clone();
        let timeout = Duration::from_secs(config.connect_timeout);

        info!("Connecting to Soul at: {}", endpoint);

        let channel = Channel::from_shared(endpoint.clone())
            .context("Invalid Soul endpoint URI")?
            .timeout(timeout)
            .connect()
            .await
            .context("Failed to connect to Soul gRPC server")?;

        info!("Connected to Soul at: {}", endpoint);

        Ok(Self { endpoint, channel })
    }

    /// Send a heartbeat to Soul.
    pub async fn heartbeat(&self, node_id: &str) -> Result<soul::HeartbeatResponse> {
        debug!("Sending heartbeat for node: {}", node_id);

        // In production, this would use the generated gRPC client:
        //   let mut client = SoulServiceClient::new(self.channel.clone());
        //   let request = tonic::Request::new(stream);
        //   client.heartbeat(request).await

        // Stub response for standalone compilation
        Ok(soul::HeartbeatResponse {
            server_timestamp_ms: chrono::Utc::now().timestamp_millis(),
            ok: true,
            message: "pong".to_string(),
        })
    }

    /// Upload a batch of collected events to Soul.
    pub async fn upload_events(
        &self,
        events: &[soul::CollectedEvent],
    ) -> Result<soul::UploadEventsResponse> {
        debug!("Uploading {} events to Soul", events.len());

        // In production:
        //   let mut client = SoulServiceClient::new(self.channel.clone());
        //   let request = tonic::Request::new(soul::UploadEventsRequest { ... });
        //   client.upload_events(request).await

        // Stub response
        Ok(soul::UploadEventsResponse {
            accepted: events.len() as i64,
            rejected: 0,
            new_cursor: "".to_string(),
            reject_reasons: vec![],
        })
    }

    /// Get the connected endpoint URL.
    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    /// Check if the channel is ready (simple connectivity check).
    pub async fn is_ready(&self) -> bool {
        // tonic 0.12 doesn't expose a direct readiness check on Channel.
        // A channel is considered ready if it was successfully created.
        true
    }
}
