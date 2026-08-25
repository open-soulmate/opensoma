//! Integration tests for OpenSoma — end-to-end pipeline verification.
//!
//! Tests the full flow: collector → processor → sync engine,
//! plus config validation, status server, and connector wiring.

use std::collections::HashMap;
use std::time::Duration;

// ─────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────

fn make_event(
    id: &str,
    source: &str,
    event_type: &str,
    payload: &str,
) -> opensoma::collector::RawEvent {
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
    assert!(
        config.validate().is_err(),
        "Expected validation to fail for empty node_id: {:?}",
        config.validate()
    );
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

    let handle = processor::start_pipeline(input_rx, output_tx, &config, None);

    let mut event = make_event(
        "file-001",
        "file",
        "file_change",
        r#"{"content":"hello world"}"#,
    );
    event
        .tags
        .insert("file_path".to_string(), "/tmp/test.txt".to_string());

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

    let handle = processor::start_pipeline(input_rx, output_tx, &config, None);

    let sources = [
        ("file", "file_change", "File content here"),
        (
            "process",
            "process_started",
            "Process started with PID 1234",
        ),
        ("clipboard", "clipboard_change", "Copied text from browser"),
        (
            "network",
            "connection_established",
            "Connection to 192.168.1.1:443",
        ),
        (
            "connector:daily-digest",
            "connector_event",
            "Daily digest from email",
        ),
    ];

    for (i, (source, event_type, payload)) in sources.iter().enumerate() {
        let event = make_event(&format!("multi-{}", i), source, event_type, payload);
        input_tx.send(event).await.unwrap();
    }

    for i in 0..5 {
        let processed = tokio::time::timeout(Duration::from_secs(3), output_rx.recv())
            .await
            .unwrap_or_else(|_| panic!("timeout waiting for event {}", i))
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

    let handle = processor::start_pipeline(input_rx, output_tx, &config, None);

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
        let event = make_event(
            &format!("evt-{}", i),
            "file",
            "file_change",
            &format!("data {}", i),
        );
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
        connectors_active: std::sync::Arc::new(tokio::sync::RwLock::new(
            vec!["feishu".to_string()],
        )),
        last_error: std::sync::Arc::new(tokio::sync::RwLock::new(None)),
        connector_enabled: std::sync::Arc::new(tokio::sync::RwLock::new(
            std::collections::HashMap::new(),
        )),
        connector_event_counts: std::sync::Arc::new(tokio::sync::RwLock::new(
            std::collections::HashMap::new(),
        )),
        cache_stats: std::sync::Arc::new(tokio::sync::RwLock::new(
            opensoma::status_server::CacheStatsSnapshot::default(),
        )),
        cache: None,
        pipeline_metrics: None,
        health_checker: None,
        plugin_registry: None,
        config_snapshot: None,
        circuit_breakers: None,
        rate_limiters: None,
        collector_event_counts: std::sync::Arc::new(tokio::sync::RwLock::new(std::collections::HashMap::new())),
        collector_running: std::sync::Arc::new(tokio::sync::RwLock::new(std::collections::HashMap::new())),
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
    event
        .tags
        .insert("class_category".to_string(), "process".to_string());

    let json = serde_json::to_string(&event).unwrap();
    let deserialized: opensoma::collector::RawEvent = serde_json::from_str(&json).unwrap();

    assert_eq!(deserialized.tags.len(), 3);
    assert_eq!(deserialized.tags.get("pid").unwrap(), "1234");
    assert_eq!(deserialized.tags.get("class_category").unwrap(), "process");
}

// ─────────────────────────────────────────────
// Processor Classify Tests
// ─────────────────────────────────────────────

#[test]
fn test_classify_file_event() {
    use opensoma::processor::classify::{classify_event, ContentType};

    let mut event = make_event("cls-001", "file", "file_change", "some file content");
    event
        .tags
        .insert("extension".to_string(), "json".to_string());

    let classification = classify_event(&event);
    assert_eq!(classification.source_category, "file");
    assert_eq!(classification.content_type, ContentType::Data);
}

#[test]
fn test_classify_process_event() {
    use opensoma::processor::classify::{classify_event, ContentType};

    let event = make_event("cls-002", "process", "process_started", "PID 1234 started");
    let classification = classify_event(&event);
    assert_eq!(classification.source_category, "process");
    assert_eq!(classification.content_type, ContentType::System);
}

#[test]
fn test_classify_network_event() {
    use opensoma::processor::classify::{classify_event, ContentType};

    let event = make_event(
        "cls-003",
        "network",
        "network_new_connection",
        "Connection to 10.0.0.1",
    );
    let classification = classify_event(&event);
    assert_eq!(classification.source_category, "network");
    assert_eq!(classification.content_type, ContentType::Network);
}

#[test]
fn test_classify_clipboard_event() {
    use opensoma::processor::classify::{classify_event, ContentType};

    let event = make_event("cls-004", "clipboard", "clipboard_change", "copied text");
    let classification = classify_event(&event);
    assert_eq!(classification.source_category, "clipboard");
    assert_eq!(classification.content_type, ContentType::Clipboard);
}

#[test]
fn test_classify_webhook_notification() {
    use opensoma::processor::classify::{classify_event, ContentType};

    let event = make_event("cls-005", "webhook", "webhook", "incoming notification");
    let classification = classify_event(&event);
    assert_eq!(classification.content_type, ContentType::Notification);
}

#[test]
fn test_classify_log_file() {
    use opensoma::processor::classify::{classify_event, ContentType};

    let mut event = make_event("cls-006", "file", "file_change", "log output");
    event
        .tags
        .insert("extension".to_string(), "log".to_string());

    let classification = classify_event(&event);
    assert_eq!(classification.content_type, ContentType::Log);
}

#[test]
fn test_classify_config_file() {
    use opensoma::processor::classify::{classify_event, ContentType};

    let mut event = make_event("cls-007", "file", "file_change", "config data");
    event
        .tags
        .insert("extension".to_string(), "yaml".to_string());

    let classification = classify_event(&event);
    assert_eq!(classification.content_type, ContentType::Data);
}

#[test]
fn test_classify_unknown_source() {
    use opensoma::processor::classify::{classify_event, ContentType};

    let event = make_event("cls-008", "unknown-source", "custom_event", "payload");
    let classification = classify_event(&event);
    assert_eq!(classification.source_category, "unknown-source");
    assert_eq!(classification.content_type, ContentType::Generic);
}

#[test]
fn test_classify_apply_classification() {
    use opensoma::processor::classify::{apply_classification, classify_event};

    let event = make_event("cls-apply", "file", "file_change", "content");
    let classification = classify_event(&event);

    let mut event = make_event("cls-apply", "file", "file_change", "content");
    apply_classification(&mut event, &classification);

    assert!(event.tags.contains_key("class_category"));
    assert!(event.tags.contains_key("class_type"));
    assert!(event.tags.contains_key("class_urgency"));
}

// ─────────────────────────────────────────────
// Processor Enrich Tests
// ─────────────────────────────────────────────

#[test]
fn test_enrich_event_basic() {
    use opensoma::processor::enrich::enrich_event;

    let event = make_event(
        "enrich-001",
        "file",
        "file_change",
        "Hello world, this is a test document.",
    );
    let enrichment = enrich_event(&event);

    assert!(enrichment.word_count > 0);
    assert!(!enrichment.summary.is_empty());
}

#[test]
fn test_enrich_event_with_url() {
    use opensoma::processor::enrich::{enrich_event, EntityType};

    let event = make_event(
        "enrich-002",
        "file",
        "file_change",
        "Visit https://example.com for more info",
    );
    let enrichment = enrich_event(&event);

    let url_entities: Vec<_> = enrichment
        .entities
        .iter()
        .filter(|e| e.entity_type == EntityType::Url)
        .collect();
    assert!(!url_entities.is_empty());
    assert!(url_entities[0].value.contains("https://example.com"));
}

#[test]
fn test_enrich_event_with_email() {
    use opensoma::processor::enrich::{enrich_event, EntityType};

    let event = make_event(
        "enrich-003",
        "file",
        "file_change",
        "Contact user@example.com for support",
    );
    let enrichment = enrich_event(&event);

    let email_entities: Vec<_> = enrichment
        .entities
        .iter()
        .filter(|e| e.entity_type == EntityType::Email)
        .collect();
    assert!(!email_entities.is_empty());
    assert_eq!(email_entities[0].value, "user@example.com");
}

#[test]
fn test_enrich_event_with_ip() {
    use opensoma::processor::enrich::{enrich_event, EntityType};

    let event = make_event(
        "enrich-004",
        "network",
        "network_new_connection",
        "Connected to 192.168.1.100 on port 443",
    );
    let enrichment = enrich_event(&event);

    let ip_entities: Vec<_> = enrichment
        .entities
        .iter()
        .filter(|e| e.entity_type == EntityType::IpAddress)
        .collect();
    assert!(!ip_entities.is_empty());
}

#[test]
fn test_enrich_apply_to_event() {
    use opensoma::processor::enrich::{apply_enrichment, enrich_event};

    let event = make_event(
        "enrich-apply",
        "file",
        "file_change",
        "Visit https://example.com for details. Contact admin@test.org.",
    );
    let enrichment = enrich_event(&event);

    let mut event = make_event(
        "enrich-apply",
        "file",
        "file_change",
        "Visit https://example.com for details. Contact admin@test.org.",
    );
    apply_enrichment(&mut event, &enrichment);

    assert!(event.tags.contains_key("word_count"));
    assert!(event.tags.contains_key("summary"));
}

#[test]
fn test_enrich_empty_payload() {
    use opensoma::processor::enrich::enrich_event;

    let event = make_event("enrich-empty", "file", "file_change", "");
    let enrichment = enrich_event(&event);

    assert_eq!(enrichment.word_count, 0);
    assert!(enrichment.entities.is_empty());
}

// ─────────────────────────────────────────────
// Processor Normalize Tests
// ─────────────────────────────────────────────

#[test]
fn test_normalize_fixes_zero_timestamp() {
    use opensoma::processor::normalize::normalize_event;

    let mut event = make_event("norm-001", "file", "file_change", "content");
    event.timestamp_ms = 0;

    let config = opensoma::config::ProcessorConfig {
        normalize_timestamps: true,
        max_event_size: 1_048_576,
        dedup_window_secs: 60,
        enable_classify: true,
        enable_enrich: true,
    };

    normalize_event(&mut event, &config);
    assert!(event.timestamp_ms > 0);
}

#[test]
fn test_normalize_preserves_valid_timestamp() {
    use opensoma::processor::normalize::normalize_event;

    let ts = 1700000000000i64;
    let mut event = make_event("norm-002", "file", "file_change", "content");
    event.timestamp_ms = ts;

    let config = opensoma::config::ProcessorConfig {
        normalize_timestamps: true,
        max_event_size: 1_048_576,
        dedup_window_secs: 60,
        enable_classify: true,
        enable_enrich: true,
    };

    normalize_event(&mut event, &config);
    assert_eq!(event.timestamp_ms, ts);
}

#[test]
fn test_normalize_removes_empty_tags() {
    use opensoma::processor::normalize::normalize_event;

    let mut event = make_event("norm-003", "file", "file_change", "content");
    event.tags.insert("valid".to_string(), "value".to_string());
    event.tags.insert("empty".to_string(), "".to_string());

    let config = opensoma::config::ProcessorConfig {
        normalize_timestamps: true,
        max_event_size: 1_048_576,
        dedup_window_secs: 60,
        enable_classify: true,
        enable_enrich: true,
    };

    normalize_event(&mut event, &config);
    assert!(event.tags.contains_key("valid"));
    assert!(!event.tags.contains_key("empty"));
}

#[test]
fn test_normalize_format_timestamp() {
    use opensoma::processor::normalize::format_timestamp;

    let ts = 1700000000000i64; // 2023-11-14T22:13:20Z
    let formatted = format_timestamp(ts);
    assert!(formatted.contains("2023"));
    assert!(!formatted.is_empty());
}

// ─────────────────────────────────────────────
// Connector Module Structure Tests
// ─────────────────────────────────────────────

#[test]
fn test_connector_retry_delay_bounds() {
    // Verify retry delays are reasonable
    for attempt in 0..10 {
        let delay = opensoma::connector::retry_delay(attempt);
        assert!(
            delay.as_millis() >= 500,
            "delay too short for attempt {}",
            attempt
        );
        assert!(
            delay.as_secs() <= 300,
            "delay too long for attempt {}",
            attempt
        );
    }
}

#[test]
fn test_connector_retry_delay_increases() {
    let mut prev = std::time::Duration::ZERO;
    for attempt in 0..6 {
        let delay = opensoma::connector::retry_delay(attempt);
        assert!(delay > prev, "delay should increase at attempt {}", attempt);
        prev = delay;
    }
}
