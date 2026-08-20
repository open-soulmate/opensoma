use chrono::{DateTime, Utc};

use crate::collector::RawEvent;
use crate::config::ProcessorConfig;

/// Maximum length for a single tag value (prevents memory bloat from payloads leaking into tags).
const MAX_TAG_VALUE_LEN: usize = 4096;

/// Maximum length for a single tag key.
const MAX_TAG_KEY_LEN: usize = 256;

/// Maximum number of tags per event.
const MAX_TAGS_COUNT: usize = 64;

/// Normalize a raw event in-place:
/// - Ensure timestamps are UTC
/// - Sanitize tags (remove empty, truncate oversized keys/values, cap count)
/// - Normalize source and event_type
pub fn normalize_event(event: &mut RawEvent, config: &ProcessorConfig) {
    // Normalize timestamp to UTC millis
    if config.normalize_timestamps && event.timestamp_ms <= 0 {
        event.timestamp_ms = Utc::now().timestamp_millis();
    }

    // Sanitize: remove empty tags and truncate oversized values
    event.tags.retain(|_, v| !v.is_empty());

    // Truncate oversized tag keys (collect first to avoid borrow issues)
    let oversized_keys: Vec<String> = event
        .tags
        .keys()
        .filter(|k| k.len() > MAX_TAG_KEY_LEN)
        .cloned()
        .collect();
    for key in oversized_keys {
        if let Some(val) = event.tags.remove(&key) {
            let truncated_key: String = key.chars().take(MAX_TAG_KEY_LEN).collect();
            event.tags.insert(truncated_key, val);
        }
    }

    // Truncate oversized tag values
    for val in event.tags.values_mut() {
        if val.len() > MAX_TAG_VALUE_LEN {
            let truncated: String = val.chars().take(MAX_TAG_VALUE_LEN).collect();
            *val = format!("{}…", truncated);
        }
    }

    // Cap the number of tags (keep the first MAX_TAGS_COUNT after sorting by key)
    if event.tags.len() > MAX_TAGS_COUNT {
        let mut keys: Vec<String> = event.tags.keys().cloned().collect();
        keys.sort();
        for key in keys.into_iter().skip(MAX_TAGS_COUNT) {
            event.tags.remove(&key);
        }
    }

    // Ensure source is not empty
    if event.source.is_empty() {
        event.source = "unknown".to_string();
    }

    // Ensure event_type is not empty
    if event.event_type.is_empty() {
        event.event_type = "generic".to_string();
    }
}

/// Convert a timestamp in milliseconds to a human-readable UTC string.
pub fn format_timestamp(ts_ms: i64) -> String {
    DateTime::from_timestamp_millis(ts_ms)
        .map(|dt| dt.format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string())
        .unwrap_or_else(|| "unknown".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn make_config() -> ProcessorConfig {
        ProcessorConfig {
            normalize_timestamps: true,
            max_event_size: 1_048_576,
            dedup_window_secs: 300,
            enable_classify: true,
            enable_enrich: true,
        }
    }

    #[test]
    fn test_normalize_empty_source() {
        let config = make_config();
        let mut event = RawEvent {
            id: "test".into(),
            source: "".into(),
            event_type: "msg".into(),
            timestamp_ms: 1000,
            payload: vec![],
            tags: HashMap::new(),
        };
        normalize_event(&mut event, &config);
        assert_eq!(event.source, "unknown");
    }

    #[test]
    fn test_normalize_removes_empty_tags() {
        let config = make_config();
        let mut event = RawEvent {
            id: "test".into(),
            source: "file:test".into(),
            event_type: "msg".into(),
            timestamp_ms: 1000,
            payload: vec![],
            tags: {
                let mut m = HashMap::new();
                m.insert("good".into(), "value".into());
                m.insert("bad".into(), "".into());
                m
            },
        };
        normalize_event(&mut event, &config);
        assert_eq!(event.tags.len(), 1);
        assert!(event.tags.contains_key("good"));
    }

    #[test]
    fn test_normalize_fixes_zero_timestamp() {
        let config = make_config();
        let mut event = RawEvent {
            id: "test".into(),
            source: "file:test".into(),
            event_type: "msg".into(),
            timestamp_ms: 0,
            payload: vec![],
            tags: HashMap::new(),
        };
        normalize_event(&mut event, &config);
        assert!(event.timestamp_ms > 0);
    }

    #[test]
    fn test_normalize_fixes_negative_timestamp() {
        let config = make_config();
        let mut event = RawEvent {
            id: "test".into(),
            source: "file:test".into(),
            event_type: "msg".into(),
            timestamp_ms: -1,
            payload: vec![],
            tags: HashMap::new(),
        };
        normalize_event(&mut event, &config);
        assert!(event.timestamp_ms > 0);
    }

    #[test]
    fn test_normalize_preserves_valid_timestamp() {
        let config = make_config();
        let mut event = RawEvent {
            id: "test".into(),
            source: "file:test".into(),
            event_type: "msg".into(),
            timestamp_ms: 1700000000000,
            payload: vec![],
            tags: HashMap::new(),
        };
        normalize_event(&mut event, &config);
        assert_eq!(event.timestamp_ms, 1700000000000);
    }

    #[test]
    fn test_normalize_empty_event_type() {
        let config = make_config();
        let mut event = RawEvent {
            id: "test".into(),
            source: "file:test".into(),
            event_type: "".into(),
            timestamp_ms: 1000,
            payload: vec![],
            tags: HashMap::new(),
        };
        normalize_event(&mut event, &config);
        assert_eq!(event.event_type, "generic");
    }

    #[test]
    fn test_normalize_preserves_valid_event_type() {
        let config = make_config();
        let mut event = RawEvent {
            id: "test".into(),
            source: "file:test".into(),
            event_type: "file_change".into(),
            timestamp_ms: 1000,
            payload: vec![],
            tags: HashMap::new(),
        };
        normalize_event(&mut event, &config);
        assert_eq!(event.event_type, "file_change");
    }

    #[test]
    fn test_normalize_truncates_oversized_tag_value() {
        let config = make_config();
        let long_value = "x".repeat(5000);
        let mut tags = HashMap::new();
        tags.insert("key".into(), long_value);
        let mut event = RawEvent {
            id: "test".into(),
            source: "test".into(),
            event_type: "test".into(),
            timestamp_ms: 1000,
            payload: vec![],
            tags,
        };
        normalize_event(&mut event, &config);
        let val = event.tags.get("key").unwrap();
        // Should be truncated to MAX_TAG_VALUE_LEN + "…" marker
        assert!(val.len() <= 4100); // 4096 chars + "…" (3 bytes UTF-8)
        assert!(val.ends_with('…'));
    }

    #[test]
    fn test_normalize_preserves_short_tag_value() {
        let config = make_config();
        let mut tags = HashMap::new();
        tags.insert("key".into(), "short".into());
        let mut event = RawEvent {
            id: "test".into(),
            source: "test".into(),
            event_type: "test".into(),
            timestamp_ms: 1000,
            payload: vec![],
            tags,
        };
        normalize_event(&mut event, &config);
        assert_eq!(event.tags.get("key").unwrap(), "short");
    }

    #[test]
    fn test_normalize_caps_tag_count() {
        let config = make_config();
        let mut tags = HashMap::new();
        for i in 0..100 {
            tags.insert(format!("tag_{:03}", i), format!("val_{}", i));
        }
        let mut event = RawEvent {
            id: "test".into(),
            source: "test".into(),
            event_type: "test".into(),
            timestamp_ms: 1000,
            payload: vec![],
            tags,
        };
        normalize_event(&mut event, &config);
        assert!(event.tags.len() <= 64);
    }

    #[test]
    fn test_normalize_noop_when_disabled() {
        let config = ProcessorConfig {
            normalize_timestamps: false,
            max_event_size: 1_048_576,
            dedup_window_secs: 300,
            enable_classify: true,
            enable_enrich: true,
        };
        let mut event = RawEvent {
            id: "test".into(),
            source: "file:test".into(),
            event_type: "msg".into(),
            timestamp_ms: 0,
            payload: vec![],
            tags: HashMap::new(),
        };
        normalize_event(&mut event, &config);
        assert_eq!(event.timestamp_ms, 0); // not fixed when disabled
    }

    #[test]
    fn test_format_timestamp_valid() {
        let ts = 1700000000000; // 2023-11-14T22:13:20.000Z
        let formatted = format_timestamp(ts);
        assert!(formatted.starts_with("2023-11-14"));
        assert!(formatted.ends_with('Z'));
    }

    #[test]
    fn test_format_timestamp_zero() {
        let formatted = format_timestamp(0);
        assert!(formatted.starts_with("1970-01-01"));
    }
}
