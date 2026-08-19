pub mod client;

// Include generated protobuf code from tonic-build (compiled from proto/soul.proto).
// This provides: HeartbeatRequest, HeartbeatResponse, UploadEventsRequest,
// UploadEventsResponse, CollectedEvent, EventStream, SoulCommand, PingCommand,
// ConfigUpdate, NodeStatus enum, StreamControl enum, and SoulServiceClient.
pub mod soul {
    #![allow(dead_code)]
    #![allow(clippy::all)]

    tonic::include_proto!("soul");
}

#[cfg(test)]
mod tests {
    use super::soul::*;

    #[test]
    fn test_heartbeat_request_default() {
        let req = HeartbeatRequest::default();
        assert!(req.node_id.is_empty());
        assert_eq!(req.timestamp_ms, 0);
        assert_eq!(req.status, 0);
        assert!(req.metadata.is_empty());
    }

    #[test]
    fn test_heartbeat_request_fields() {
        let mut metadata = std::collections::HashMap::new();
        metadata.insert("version".to_string(), "0.1.0".to_string());

        let req = HeartbeatRequest {
            node_id: "node-1".to_string(),
            timestamp_ms: 1700000000000,
            status: 1,
            metadata,
        };
        assert_eq!(req.node_id, "node-1");
        assert_eq!(req.timestamp_ms, 1700000000000);
        assert_eq!(req.metadata.get("version").unwrap(), "0.1.0");
    }

    #[test]
    fn test_heartbeat_response_default() {
        let resp = HeartbeatResponse::default();
        assert!(!resp.ok);
        assert!(resp.message.is_empty());
        assert_eq!(resp.server_timestamp_ms, 0);
    }

    #[test]
    fn test_heartbeat_response_pong() {
        let resp = HeartbeatResponse {
            server_timestamp_ms: 1700000000000,
            ok: true,
            message: "pong".to_string(),
        };
        assert!(resp.ok);
        assert_eq!(resp.message, "pong");
    }

    #[test]
    fn test_upload_events_request_default() {
        let req = UploadEventsRequest::default();
        assert!(req.node_id.is_empty());
        assert!(req.events.is_empty());
        assert!(req.cursor.is_empty());
    }

    #[test]
    fn test_upload_events_response_default() {
        let resp = UploadEventsResponse::default();
        assert_eq!(resp.accepted, 0);
        assert_eq!(resp.rejected, 0);
        assert!(resp.new_cursor.is_empty());
        assert!(resp.reject_reasons.is_empty());
    }

    #[test]
    fn test_collected_event_default() {
        let event = CollectedEvent::default();
        assert!(event.id.is_empty());
        assert!(event.source.is_empty());
        assert!(event.event_type.is_empty());
        assert_eq!(event.timestamp_ms, 0);
        assert!(event.payload.is_empty());
        assert!(event.tags.is_empty());
    }

    #[test]
    fn test_collected_event_with_data() {
        let mut tags = std::collections::HashMap::new();
        tags.insert("key".to_string(), "value".to_string());

        let event = CollectedEvent {
            id: "evt-001".to_string(),
            source: "file".to_string(),
            event_type: "file_change".to_string(),
            timestamp_ms: 1700000000000,
            payload: b"hello world".to_vec(),
            tags,
        };
        assert_eq!(event.id, "evt-001");
        assert_eq!(event.payload, b"hello world");
        assert_eq!(event.tags.get("key").unwrap(), "value");
    }

    #[test]
    fn test_collected_event_clone() {
        let event = CollectedEvent {
            id: "evt-002".to_string(),
            source: "network".to_string(),
            event_type: "connection".to_string(),
            timestamp_ms: 1700000001000,
            payload: vec![0, 1, 2, 3],
            tags: std::collections::HashMap::new(),
        };
        let cloned = event.clone();
        assert_eq!(cloned.id, event.id);
        assert_eq!(cloned.payload, event.payload);
    }

    #[test]
    fn test_upload_events_response_with_rejections() {
        let resp = UploadEventsResponse {
            accepted: 8,
            rejected: 2,
            new_cursor: "cursor-abc".to_string(),
            reject_reasons: vec![
                "HTTP 400 for event 5".to_string(),
                "HTTP 500 for event 7".to_string(),
            ],
        };
        assert_eq!(resp.accepted, 8);
        assert_eq!(resp.rejected, 2);
        assert_eq!(resp.reject_reasons.len(), 2);
        assert_eq!(resp.new_cursor, "cursor-abc");
    }

    #[test]
    fn test_node_status_enum() {
        assert_eq!(NodeStatus::Unknown as i32, 0);
        assert_eq!(NodeStatus::Healthy as i32, 1);
        assert_eq!(NodeStatus::Degraded as i32, 2);
        assert_eq!(NodeStatus::Error as i32, 3);

        assert_eq!(NodeStatus::from_str_name("NODE_STATUS_HEALTHY"), Some(NodeStatus::Healthy));
        assert_eq!(NodeStatus::Healthy.as_str_name(), "NODE_STATUS_HEALTHY");
    }

    #[test]
    fn test_stream_control_enum() {
        assert_eq!(StreamControl::StreamResume as i32, 0);
        assert_eq!(StreamControl::StreamPause as i32, 1);
    }

    #[test]
    fn test_ping_command_default() {
        let ping = PingCommand::default();
        // PingCommand is an empty message
        assert_eq!(ping, PingCommand {});
    }

    #[test]
    fn test_config_update_with_data() {
        let update = ConfigUpdate {
            config_toml: b"[daemon]\nnode_id = \"test\"".to_vec(),
        };
        assert!(!update.config_toml.is_empty());
        let text = String::from_utf8(update.config_toml).unwrap();
        assert!(text.contains("node_id"));
    }

    #[test]
    fn test_collected_event_equality() {
        let a = CollectedEvent {
            id: "evt-1".to_string(),
            source: "file".to_string(),
            event_type: "change".to_string(),
            timestamp_ms: 100,
            payload: vec![1, 2, 3],
            tags: std::collections::HashMap::new(),
        };
        let b = a.clone();
        assert_eq!(a, b);
    }
}
