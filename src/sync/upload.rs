use anyhow::Result;
use tracing::{debug, info, warn};

use crate::collector::RawEvent;
use crate::grpc::client::SoulClient;

/// Maximum events per single upload request to avoid HTTP payload limits.
const MAX_CHUNK_SIZE: usize = 100;

/// Upload response from Soul.
#[derive(Debug)]
pub struct UploadResponse {
    pub accepted: i64,
    pub rejected: i64,
    pub new_cursor: String,
    pub reject_reasons: Vec<String>,
}

/// Upload statistics for monitoring.
#[derive(Debug, Clone, Default)]
pub struct UploadStats {
    pub total_sent: u64,
    pub total_accepted: u64,
    pub total_rejected: u64,
    pub total_bytes: u64,
    pub upload_count: u64,
}

/// Upload a batch of events to Soul via gRPC/HTTP.
/// Automatically splits large batches into chunks to avoid payload limits.
pub async fn upload_events(client: &SoulClient, events: &[RawEvent]) -> Result<UploadResponse> {
    if events.is_empty() {
        return Ok(UploadResponse {
            accepted: 0,
            rejected: 0,
            new_cursor: String::new(),
            reject_reasons: Vec::new(),
        });
    }

    let total_bytes: usize = events.iter().map(|e| e.payload.len()).sum();
    debug!(
        "Uploading {} events to Soul ({} bytes total, {} chunks max)",
        events.len(),
        total_bytes,
        events.len().div_ceil(MAX_CHUNK_SIZE)
    );

    let mut total_accepted: i64 = 0;
    let mut total_rejected: i64 = 0;
    let mut all_reject_reasons: Vec<String> = Vec::new();
    let mut new_cursor = String::new();

    // Split into chunks
    for (chunk_idx, chunk) in events.chunks(MAX_CHUNK_SIZE).enumerate() {
        let proto_events: Vec<_> = chunk.iter().map(to_proto_event_shared).collect();

        match client.upload_events(&proto_events).await {
            Ok(resp) => {
                total_accepted += resp.accepted;
                total_rejected += resp.rejected;
                if !resp.new_cursor.is_empty() {
                    new_cursor = resp.new_cursor;
                }
                all_reject_reasons.extend(resp.reject_reasons);

                debug!(
                    "Chunk {}/{}: accepted={}, rejected={}",
                    chunk_idx + 1,
                    events.len().div_ceil(MAX_CHUNK_SIZE),
                    resp.accepted,
                    resp.rejected
                );
            }
            Err(e) => {
                warn!("Chunk {} upload failed: {}", chunk_idx + 1, e);
                // Mark all events in this chunk as rejected
                total_rejected += chunk.len() as i64;
                all_reject_reasons.push(format!("Chunk {} failed: {}", chunk_idx + 1, e));
            }
        }
    }

    if total_accepted > 0 {
        info!(
            "Upload complete: {}/{} accepted ({} bytes)",
            total_accepted,
            events.len(),
            total_bytes
        );
    }

    Ok(UploadResponse {
        accepted: total_accepted,
        rejected: total_rejected,
        new_cursor,
        reject_reasons: all_reject_reasons,
    })
}

/// Convert a RawEvent to a protobuf CollectedEvent.
pub fn to_proto_event_shared(event: &RawEvent) -> crate::grpc::soul::CollectedEvent {
    crate::grpc::soul::CollectedEvent {
        id: event.id.clone(),
        source: event.source.clone(),
        event_type: event.event_type.clone(),
        timestamp_ms: event.timestamp_ms,
        payload: event.payload.clone(),
        tags: event.tags.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn make_event(id: &str, payload_size: usize) -> RawEvent {
        RawEvent {
            id: id.to_string(),
            source: "test".to_string(),
            event_type: "test_event".to_string(),
            timestamp_ms: 1000,
            payload: vec![0u8; payload_size],
            tags: HashMap::new(),
        }
    }

    #[test]
    fn test_to_proto_event() {
        let event = make_event("evt-1", 10);
        let proto = to_proto_event_shared(&event);
        assert_eq!(proto.id, "evt-1");
        assert_eq!(proto.source, "test");
        assert_eq!(proto.event_type, "test_event");
        assert_eq!(proto.timestamp_ms, 1000);
        assert_eq!(proto.payload.len(), 10);
    }

    #[test]
    fn test_max_chunk_size_constant() {
        // Verify the constant is reasonable
        assert!(MAX_CHUNK_SIZE > 0);
        assert!(MAX_CHUNK_SIZE <= 1000);
    }

    #[test]
    fn test_chunk_count_calculation() {
        // 0 events = 0 chunks
        assert_eq!((0usize + MAX_CHUNK_SIZE - 1) / MAX_CHUNK_SIZE, 0);
        // 1 event = 1 chunk
        assert_eq!((1usize + MAX_CHUNK_SIZE - 1) / MAX_CHUNK_SIZE, 1);
        // MAX_CHUNK_SIZE events = 1 chunk
        assert_eq!((MAX_CHUNK_SIZE + MAX_CHUNK_SIZE - 1) / MAX_CHUNK_SIZE, 1);
        // MAX_CHUNK_SIZE + 1 events = 2 chunks
        assert_eq!((MAX_CHUNK_SIZE + 1 + MAX_CHUNK_SIZE - 1) / MAX_CHUNK_SIZE, 2);
    }

    #[test]
    fn test_events_chunks_split() {
        let events: Vec<RawEvent> = (0..250).map(|i| make_event(&format!("e{}", i), 10)).collect();
        let chunks: Vec<_> = events.chunks(MAX_CHUNK_SIZE).collect();
        assert_eq!(chunks.len(), 3); // 100 + 100 + 50
        assert_eq!(chunks[0].len(), MAX_CHUNK_SIZE);
        assert_eq!(chunks[1].len(), MAX_CHUNK_SIZE);
        assert_eq!(chunks[2].len(), 50);
    }
}
