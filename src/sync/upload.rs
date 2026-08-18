use anyhow::{Context, Result};
use tracing::debug;

use crate::collector::RawEvent;
use crate::grpc::client::SoulClient;

/// Upload response from Soul.
#[derive(Debug)]
pub struct UploadResponse {
    pub accepted: i64,
    pub rejected: i64,
    pub new_cursor: String,
    pub reject_reasons: Vec<String>,
}

/// Upload a batch of events to Soul via gRPC.
pub async fn upload_events(client: &SoulClient, events: &[RawEvent]) -> Result<UploadResponse> {
    debug!("Uploading {} events to Soul...", events.len());

    let proto_events: Vec<_> = events.iter().map(to_proto_event).collect();

    let resp = client
        .upload_events(&proto_events)
        .await
        .context("gRPC upload failed")?;

    Ok(UploadResponse {
        accepted: resp.accepted,
        rejected: resp.rejected,
        new_cursor: resp.new_cursor,
        reject_reasons: resp.reject_reasons,
    })
}

/// Convert a RawEvent to a protobuf CollectedEvent.
fn to_proto_event(event: &RawEvent) -> crate::grpc::soul::CollectedEvent {
    crate::grpc::soul::CollectedEvent {
        id: event.id.clone(),
        source: event.source.clone(),
        event_type: event.event_type.clone(),
        timestamp_ms: event.timestamp_ms,
        payload: event.payload.clone(),
        tags: event.tags.clone(),
    }
}
