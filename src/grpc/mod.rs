pub mod client;

// Include generated protobuf code if proto file was compiled
pub mod soul {
    #![allow(dead_code)]
    #![allow(clippy::all)]

    // Stub types for when proto is not compiled.
    // When tonic-build generates from soul.proto, these get replaced.
    // For now, we provide manual stubs so the crate compiles standalone.

    #[derive(Debug, Clone, Default)]
    pub struct HeartbeatRequest {
        pub node_id: String,
        pub timestamp_ms: i64,
        pub status: i32,
        pub metadata: std::collections::HashMap<String, String>,
    }

    #[derive(Debug, Clone, Default)]
    pub struct HeartbeatResponse {
        pub server_timestamp_ms: i64,
        pub ok: bool,
        pub message: String,
    }

    #[derive(Debug, Clone, Default)]
    pub struct UploadEventsRequest {
        pub node_id: String,
        pub events: Vec<CollectedEvent>,
        pub cursor: String,
    }

    #[derive(Debug, Clone, Default)]
    pub struct UploadEventsResponse {
        pub accepted: i64,
        pub rejected: i64,
        pub new_cursor: String,
        pub reject_reasons: Vec<String>,
    }

    #[derive(Debug, Clone, Default)]
    pub struct CollectedEvent {
        pub id: String,
        pub source: String,
        pub event_type: String,
        pub timestamp_ms: i64,
        pub payload: Vec<u8>,
        pub tags: std::collections::HashMap<String, String>,
    }
}
