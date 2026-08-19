//! Integration tests for OpenSoma — end-to-end pipeline verification.
//!
//! Tests the full flow: collector → processor → sync engine,
//! plus config validation, status server, and connector wiring.

use std::collections::HashMap;
use std::time::Duration;

// ─────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────

fn make_event(id: &str, source: &str, event_type: &str, payload: &str) -> opensoma::collector::RawEvent {
    opensoma::collector::RawEvent {
        id: id.to_string(),
        source: source.to_string(),
        event_type: event_type.to_string(),
        timestamp_ms: chrono::Utc::now().timestamp_millis(),
        payload: payload.as_bytes().to_vec(),
        tags: HashMap::new(),
    }
}

// ─────────────────────────────────────────────
// Config Validation Tests
// ─────────────────────────────────────────────

#[test]
fn test_config_validation_valid() {
    let dir = tempfile::tempdir().unwrap();
    let config_path = dir.path().join("config.toml");
    std::fs::write(
        &config_path,
        r#"
[daemon]
node_id = "test-node"
data_dir = "/tmp/opensoma-test"
status_port = 0

[soul]
endpoint = "http://localhost:8090"

[collector]
watch_dirs = []

[connector]

[processor]
normalize_timestamps = true
max_event_size = 1048576
dedup_window_secs = 60
enable_classify = true
enable_enrich = true

[sync]
batch_size = 50
upload_interval = 10
max_retries = 3
retry_backoff_ms = 1000
cache_size_mb = 64
"#,
    )
    .unwrap();

    let config = opensoma::config::AppConfig::load(config_path.to_str().unwrap()).unwrap();
    let warnings = config.validate().unwrap();
    assert!(warnings.iter().any(|w| w.contains("watch_dirs")));
}

#[test]
fn test_config_validation_empty_node_id() {
    let dir = tempfile::tempdir().unwrap();
    let config_path = dir.path().join("config.toml");
    std::fs::write(
        &config_path,
        r#"
[daemon]
node_id = ""

[soul]
endpoint = "http://localhost:8090"

[collector]
watch_dirs = []

[connector]

[processor]
normalize_timestamps = true
max_event_size = 1048576
dedup_window_secs = 60
enable_classify = true
enable_enrich = true

[sync]
batch_size = 50
upload_interval = 10
max_retries = 3
retry_backoff_ms = 1000
cache_size_mb = 64
"#,
    )
    .unwrap();

    let config = opensoma::config::AppConfig::load(config_path.to_str().unwrap()).unwrap();
    assert!(config.validate().is_err(), "Expected validation to fail for empty node_id: {:?}", config.validate());
}

#[test]
fn test_config_validation_feishu_enabled_no_creds() {
    let dir = tempfile::tempdir().unwrap();
    let config_path = dir.path().join("config.toml");
    std::fs::write(
        &config_path,
        r#"
[daemon]
node_id = "test"

[soul]
endpoint = "http://localhost:8090"

[collector]
watch_dirs = []

[connector]

[connector.feishu]
enabled = true
app_id = ""
app_secret = ""

[processor]
normalize_timestamps = true
max_event_size = 1048576
dedup_window_secs = 60
enable_classify = true
enable_enrich = true

[sync]
batch_size = 50
upload_interval = 10
max_retries = 3
retry_backoff_ms = 1000
cache_size_mb = 64
"#,
    )
    .unwrap();

    let config = opensoma::config::AppConfig::load(config_path.to_str().unwrap()).unwrap();
    assert!(config.validate().is_err());
}

#[test]
fn test_config_missing_file() {
    let result = opensoma::config::AppConfig::load("/nonexistent/config.toml");
    assert!(result.is_err());
}

// ─────────────────────────────────────────────
// Processor Pipeline Integration
// ─────────────────────────────────────────────

#[tokio::test]
async fn test_full_pipeline_file_event() {
    use opensoma::processor;

    let (input_tx, input_rx) = tokio::sync::mpsc::channel(64);
    let (output_tx, mut output_rx) = tokio::sync::mpsc::channel(64);

    let config = opensoma::config::ProcessorConfig {
        normalize_timestamps: true,
        max_event_size: 1_048_576,
        dedup_window_secs: 60,
        enable_classify: true,
        enable_enrich: true,
    };

    let handle = processor::start_pipeline(input_rx, output_tx, &config);

    let mut event = make_event("file-001", "file", "file_change", r#"{"content":"hello world"}"#);
    event.tags.insert("file_path".to_string(), "/tmp/test.txt".to_string());

    input_tx.send(event).await.unwrap();

    let processed = tokio::time::timeout(Duration::from_secs(3), output_rx.recv())
        .await
        .expect("timeout waiting for processed event")
        .expect("channel closed");

    assert_eq!(processed.id, "file-001");
    assert_eq!(processed.source, "file");
    assert!(processed.tags.contains_key("class_category"));
    assert!(processed.tags.contains_key("word_count"));

    handle.abort();
}

#[tokio::test]
async fn test_full_pipeline_multiple_sources() {
    use opensoma::processor;

    let (input_tx, input_rx) = tokio::sync::mpsc::channel(64);
    let (output_tx, mut output_rx) = tokio::sync::mpsc::channel(64);

    let config = opensoma::config::ProcessorConfig {
        normalize_timestamps: true,
        max_event_size: 1_048_576,
        dedup_window_secs: 0,
        enable_classify: true,
        enable_enrich: true,
    };

    let handle = processor::start_pipeline(input_rx, output_tx, &config);

    let sources = vec![
        ("file", "file_change", "File content here"),
        ("process", "process_started", "Process started with PID 1234"),
        ("clipboard", "clipboard_change", "Copied text from browser"),
        ("network", "connection_established", "Connection to 192.168.1.1:443"),
        ("connector:daily-digest", "connector_event", "Daily digest from email"),
    ];

    for (i, (source, event_type, payload)) in sources.iter().enumerate() {
        let event = make_event(&format!("multi-{}", i), source, event_type, payload);
        input_tx.send(event).await.unwrap();
    }

    for i in 0..5 {
        let processed = tokio::time::timeout(Duration::from_secs(3), output_rx.recv())
            .await
            .expect(&format!("timeout waiting for event {}", i))
            .expect("channel closed");

        assert!(processed.tags.contains_key("class_category"));
        assert!(processed.tags.contains_key("word_count"));
    }

    handle.abort();
}

// ─────────────────────────────────────────────
// Deduplication Integration
// ─────────────────────────────────────────────

#[tokio::test]
async fn test_dedup_across_pipeline() {
    use opensoma::processor;

    let (input_tx, input_rx) = tokio::sync::mpsc::channel(64);
    let (output_tx, mut output_rx) = tokio::sync::mpsc::channel(64);

    let config = opensoma::config::ProcessorConfig {
        normalize_timestamps: true,
        max_event_size: 1_048_576,
        dedup_window_secs: 300,
        enable_classify: false,
        enable_enrich: false,
    };

    let handle = processor::start_pipeline(input_rx, output_tx, &config);

    for _ in 0..3 {
        let event = make_event("dup-test", "file", "file_change", "same content");
        input_tx.send(event).await.unwrap();
    }

    let first = tokio::time::timeout(Duration::from_secs(2), output_rx.recv())
        .await
        .expect("timeout")
        .expect("channel closed");
    assert_eq!(first.id, "dup-test");

    let second = tokio::time::timeout(Duration::from_millis(500), output_rx.recv()).await;
    assert!(second.is_err());

    handle.abort();
}

// ─────────────────────────────────────────────
// Sync Cache Integration
// ─────────────────────────────────────────────

#[test]
fn test_cache_put_get_stats() {
    let dir = tempfile::tempdir().unwrap();
    let cache = opensoma::sync::cache::Cache::open(dir.path().to_str().unwrap()).unwrap();

    let event = make_event("cache-001", "file", "file_change", "cached data");
    cache.put(&event).unwrap();

    let stats = cache.stats();
    assert_eq!(stats.total, 1);
    assert_eq!(stats.pending, 1);
    assert_eq!(stats.uploaded, 0);

    cache.mark_uploaded("cache-001").unwrap();

    let stats = cache.stats();
    assert_eq!(stats.uploaded, 1);
    assert_eq!(stats.pending, 0);
}

#[test]
fn test_cache_multiple_events() {
    let dir = tempfile::tempdir().unwrap();
    let cache = opensoma::sync::cache::Cache::open(dir.path().to_str().unwrap()).unwrap();

    for i in 0..10 {
        let event = make_event(&format!("evt-{}", i), "file", "file_change", &format!("data {}", i));
        cache.put(&event).unwrap();
    }

    let stats = cache.stats();
    assert_eq!(stats.total, 10);
    assert_eq!(stats.pending, 10);

    for i in 0..5 {
        cache.mark_uploaded(&format!("evt-{}", i)).unwrap();
    }

    let stats = cache.stats();
    assert_eq!(stats.uploaded, 5);
    assert_eq!(stats.pending, 5);
}

// ─────────────────────────────────────────────
// Conflict Resolution Integration
// ─────────────────────────────────────────────

#[test]
fn test_conflict_detection_same_id_different_content() {
    use opensoma::sync::conflict::{ConflictResolver, ConflictStrategy, EventSnapshot};

    let resolver = ConflictResolver::new(ConflictStrategy::NewestWins);

    let local = make_event("evt-1", "file", "file_change", "local content");

    let server = EventSnapshot {
        id: "evt-1".to_string(),
        source: "file".to_string(),
        event_type: "file_change".to_string(),
        timestamp_ms: 1000,
        content_hash: "different-hash".to_string(),
        tags: HashMap::new(),
    };

    let conflict = resolver.detect(&local, &server);
    assert!(conflict.is_some());
    assert_eq!(conflict.unwrap().event_id, "evt-1");
}

#[test]
fn test_conflict_no_conflict_different_id() {
    use opensoma::sync::conflict::{ConflictResolver, ConflictStrategy, EventSnapshot};

    let resolver = ConflictResolver::new(ConflictStrategy::NewestWins);

    let local = make_event("evt-1", "file", "file_change", "content");

    let server = EventSnapshot {
        id: "evt-2".to_string(),
        source: "file".to_string(),
        event_type: "file_change".to_string(),
        timestamp_ms: 1000,
        content_hash: "hash".to_string(),
        tags: HashMap::new(),
    };

    let conflict = resolver.detect(&local, &server);
    assert!(conflict.is_none());
}

// ─────────────────────────────────────────────
// Status Server Integration
// ─────────────────────────────────────────────

#[tokio::test]
async fn test_status_server_health_endpoint() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    let state = opensoma::status_server::StatusServerState {
        node_id: "test-node".to_string(),
        start_time: std::time::Instant::now(),
        events_collected: std::sync::Arc::new(tokio::sync::RwLock::new(42)),
        events_synced: std::sync::Arc::new(tokio::sync::RwLock::new(40)),
        connectors_active: std::sync::Arc::new(tokio::sync::RwLock::new(vec!["feishu".to_string()])),
        last_error: std::sync::Arc::new(tokio::sync::RwLock::new(None)),
        connector_enabled: std::sync::Arc::new(tokio::sync::RwLock::new(std::collections::HashMap::new())),
        connector_event_counts: std::sync::Arc::new(tokio::sync::RwLock::new(std::collections::HashMap::new())),
        cache_stats: std::sync::Arc::new(tokio::sync::RwLock::new(opensoma::status_server::CacheStatsSnapshot::default())),
        cache: None,
    };

    let app = opensoma::status_server::build_router(state);
    let response = app
        .oneshot(
            Request::builder()
                .uri("/health")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
}

// ─────────────────────────────────────────────
// Connector Retry Integration
// ─────────────────────────────────────────────

#[test]
fn test_retry_delay_exponential_backoff() {
    let d0 = opensoma::connector::retry_delay(0);
    let d1 = opensoma::connector::retry_delay(1);
    let d2 = opensoma::connector::retry_delay(2);
    let d3 = opensoma::connector::retry_delay(3);

    assert_eq!(d0.as_millis(), 500);
    assert_eq!(d1.as_millis(), 1000);
    assert_eq!(d2.as_millis(), 2000);
    assert_eq!(d3.as_millis(), 4000);
    assert!(d3 > d2);
    assert!(d2 > d1);
    assert!(d1 > d0);
}

// ─────────────────────────────────────────────
// Upload Chunking Integration
// ─────────────────────────────────────────────

#[test]
fn test_upload_chunk_count() {
    assert_eq!(50usize.div_ceil(100), 1);
    assert_eq!(100usize.div_ceil(100), 1);
    assert_eq!(101usize.div_ceil(100), 2);
    assert_eq!(250usize.div_ceil(100), 3);
}

// ─────────────────────────────────────────────
// Serialization Roundtrip Tests
// ─────────────────────────────────────────────

#[test]
fn test_raw_event_json_roundtrip() {
    let event = make_event("rt-001", "file", "file_change", r#"{"key":"value"}"#);
    let json = serde_json::to_string(&event).unwrap();
    let deserialized: opensoma::collector::RawEvent = serde_json::from_str(&json).unwrap();

    assert_eq!(deserialized.id, "rt-001");
    assert_eq!(deserialized.source, "file");
    assert_eq!(deserialized.event_type, "file_change");
    assert_eq!(deserialized.payload, b"{\"key\":\"value\"}");
}

#[test]
fn test_raw_event_with_tags_roundtrip() {
    let mut event = make_event("rt-002", "process", "process_started", "PID 1234");
    event.tags.insert("pid".to_string(), "1234".to_string());
    event.tags.insert("name".to_string(), "python3".to_string());
    event.tags.insert("class_category".to_string(), "process".to_string());

    let json = serde_json::to_string(&event).unwrap();
    let deserialized: opensoma::collector::RawEvent = serde_json::from_str(&json).unwrap();

    assert_eq!(deserialized.tags.len(), 3);
    assert_eq!(deserialized.tags.get("pid").unwrap(), "1234");
    assert_eq!(deserialized.tags.get("class_category").unwrap(), "process");
}
