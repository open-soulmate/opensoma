use chrono::{DateTime, Utc};

use crate::collector::RawEvent;
use crate::config::ProcessorConfig;

/// Normalize a raw event in-place:
/// - Ensure timestamps are UTC
/// - Sanitize tags
/// - Trim payload if needed
pub fn normalize_event(event: &mut RawEvent, config: &ProcessorConfig) {
    // Normalize timestamp to UTC millis
    if config.normalize_timestamps && event.timestamp_ms <= 0 {
        event.timestamp_ms = Utc::now().timestamp_millis();
    }

    // Sanitize: remove empty tags
    event.tags.retain(|_, v| !v.is_empty());

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
}
