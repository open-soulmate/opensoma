use serde::{Deserialize, Serialize};

use crate::collector::RawEvent;

/// Content classification result.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Classification {
    /// Source category: "file", "process", "network", "clipboard", "webhook", "connector", etc.
    pub source_category: String,
    /// Content type: "data", "log", "config", "metric", "notification", "error", etc.
    pub content_type: ContentType,
    /// Urgency level based on content analysis.
    pub urgency: Urgency,
    /// Additional classification labels.
    pub labels: Vec<String>,
}

/// Content type classification.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum ContentType {
    /// Structured data (JSON, CSV, etc.)
    Data,
    /// Log output or trace
    Log,
    /// Configuration change
    Config,
    /// Metric or measurement
    Metric,
    /// User notification or message
    Notification,
    /// Error or warning
    Error,
    /// Process/system event
    System,
    /// Network event
    Network,
    /// Clipboard content
    Clipboard,
    /// Generic / unclassified
    Generic,
}

/// Urgency level.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub enum Urgency {
    /// Low — background info, routine updates
    Low,
    /// Normal — standard events
    Normal,
    /// High — requires attention
    High,
    /// Critical — immediate action needed
    Critical,
}

/// Classify a raw event based on its source, type, payload, and tags.
pub fn classify_event(event: &RawEvent) -> Classification {
    let source_category = extract_source_category(&event.source);
    let content_type = detect_content_type(event);
    let urgency = detect_urgency(event, &content_type);
    let labels = extract_labels(event);

    Classification {
        source_category,
        content_type,
        urgency,
        labels,
    }
}

/// Extract the top-level source category from the source field.
/// E.g., "file:/tmp/data.json" → "file", "process:1234" → "process"
fn extract_source_category(source: &str) -> String {
    source.split(':').next().unwrap_or("unknown").to_string()
}

/// Detect the content type from event metadata.
fn detect_content_type(event: &RawEvent) -> ContentType {
    // Use event_type first
    match event.event_type.as_str() {
        "file_change" => {
            // Check file extension from tags
            if let Some(ext) = event.tags.get("extension") {
                match ext.as_str() {
                    "json" | "csv" | "xml" | "yaml" | "yml" | "toml" => return ContentType::Data,
                    "log" | "out" => return ContentType::Log,
                    "conf" | "cfg" | "ini" | "env" => return ContentType::Config,
                    "err" => return ContentType::Error,
                    _ => {}
                }
            }
            ContentType::Generic
        }
        "process_started" | "process_exited" | "process_resource_change" => ContentType::System,
        "network_new_connection" | "network_closed_connection" | "network_state_change" => {
            ContentType::Network
        }
        "clipboard_change" => ContentType::Clipboard,
        "webhook" => ContentType::Notification,
        "rss_item" => ContentType::Notification,
        "email_message" => ContentType::Notification,
        "git_push" | "git_tag" | "git_release" => ContentType::Notification,
        _ => ContentType::Generic,
    }
}

/// Detect urgency level from event content.
fn detect_urgency(event: &RawEvent, content_type: &ContentType) -> Urgency {
    // Error-type events are high urgency
    if *content_type == ContentType::Error {
        return Urgency::High;
    }

    // Process events with high resource usage
    if event.event_type == "process_resource_change" {
        if let Some(cpu_str) = event.tags.get("cpu_usage") {
            if let Ok(cpu) = cpu_str.parse::<f32>() {
                if cpu > 90.0 {
                    return Urgency::Critical;
                }
                if cpu > 70.0 {
                    return Urgency::High;
                }
            }
        }
        if let Some(mem_str) = event.tags.get("memory_mb") {
            if let Ok(mem_mb) = mem_str.parse::<u64>() {
                if mem_mb > 4096 {
                    return Urgency::High;
                }
            }
        }
    }

    // Network events from unusual ports
    if let Some(remote) = event.tags.get("remote") {
        // Flag connections to known suspicious ports
        if let Some(port_str) = remote.split(':').next_back() {
            if let Ok(port) = port_str.parse::<u16>() {
                match port {
                    4444 | 5555 | 6666 | 1234 | 31337 => return Urgency::Critical,
                    23 | 21 | 513 => return Urgency::High, // unencrypted protocols
                    _ => {}
                }
            }
        }
    }

    // Payload-based urgency (check for keywords)
    let payload_str = String::from_utf8_lossy(&event.payload);
    let lower = payload_str.to_lowercase();
    if lower.contains("critical") || lower.contains("fatal") || lower.contains("panic") {
        return Urgency::Critical;
    }
    if lower.contains("error") || lower.contains("fail") || lower.contains("exception") {
        return Urgency::High;
    }
    if lower.contains("warning") || lower.contains("warn") {
        return Urgency::Normal;
    }

    Urgency::Normal
}

/// Extract classification labels from event tags and content.
fn extract_labels(event: &RawEvent) -> Vec<String> {
    let mut labels = Vec::new();

    // Add source as label
    if let Some(ext) = event.tags.get("extension") {
        labels.push(format!("filetype:{}", ext));
    }

    // Add state labels from network events
    if let Some(state) = event.tags.get("state") {
        labels.push(format!("net:{}", state.to_lowercase()));
    }
    if let Some(change_type) = event.tags.get("change_type") {
        labels.push(change_type.clone());
    }

    // Add protocol label
    if let Some(proto) = event.tags.get("protocol") {
        labels.push(format!("proto:{}", proto));
    }

    labels
}

/// Add classification tags to an event. Modifies event.tags in place.
pub fn apply_classification(event: &mut RawEvent, classification: &Classification) {
    event.tags.insert(
        "class_category".to_string(),
        classification.source_category.clone(),
    );
    event.tags.insert(
        "class_type".to_string(),
        format!("{:?}", classification.content_type).to_lowercase(),
    );
    event.tags.insert(
        "class_urgency".to_string(),
        format!("{:?}", classification.urgency).to_lowercase(),
    );
    if !classification.labels.is_empty() {
        event
            .tags
            .insert("class_labels".to_string(), classification.labels.join(","));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn make_event(event_type: &str, source: &str, tags: HashMap<String, String>) -> RawEvent {
        RawEvent {
            id: "test".into(),
            source: source.into(),
            event_type: event_type.into(),
            timestamp_ms: 1000,
            payload: vec![],
            tags,
        }
    }

    #[test]
    fn test_classify_file_json() {
        let mut tags = HashMap::new();
        tags.insert("extension".into(), "json".into());
        let event = make_event("file_change", "file:/tmp/data.json", tags);
        let c = classify_event(&event);
        assert_eq!(c.source_category, "file");
        assert_eq!(c.content_type, ContentType::Data);
    }

    #[test]
    fn test_classify_process() {
        let event = make_event("process_started", "process:1234", HashMap::new());
        let c = classify_event(&event);
        assert_eq!(c.source_category, "process");
        assert_eq!(c.content_type, ContentType::System);
    }

    #[test]
    fn test_classify_network_critical_port() {
        let mut tags = HashMap::new();
        tags.insert("remote".into(), "192.168.1.100:4444".into());
        let event = make_event("network_new_connection", "network", tags);
        let c = classify_event(&event);
        assert_eq!(c.urgency, Urgency::Critical);
    }

    #[test]
    fn test_classify_high_cpu() {
        let mut tags = HashMap::new();
        tags.insert("cpu_usage".into(), "95.0".into());
        let event = make_event("process_resource_change", "process:5678", tags);
        let c = classify_event(&event);
        assert_eq!(c.urgency, Urgency::Critical);
    }

    #[test]
    fn test_extract_source_category() {
        assert_eq!(extract_source_category("file:/tmp/test"), "file");
        assert_eq!(extract_source_category("process:1234"), "process");
        assert_eq!(extract_source_category("clipboard"), "clipboard");
    }

    // ── Content type: file extensions ───────────────────────────────

    #[test]
    fn test_classify_file_log_extension() {
        let mut tags = HashMap::new();
        tags.insert("extension".into(), "log".into());
        let event = make_event("file_change", "file:/var/log/app.log", tags);
        let c = classify_event(&event);
        assert_eq!(c.content_type, ContentType::Log);
    }

    #[test]
    fn test_classify_file_config_extensions() {
        for ext in &["conf", "cfg", "ini", "env"] {
            let mut tags = HashMap::new();
            tags.insert("extension".into(), ext.to_string());
            let event = make_event("file_change", "file:/etc/app.conf", tags);
            let c = classify_event(&event);
            assert_eq!(c.content_type, ContentType::Config, "ext={}", ext);
        }
    }

    #[test]
    fn test_classify_file_data_extensions() {
        for ext in &["csv", "xml", "yaml", "yml", "toml"] {
            let mut tags = HashMap::new();
            tags.insert("extension".into(), ext.to_string());
            let event = make_event("file_change", "file:/data/file", tags);
            let c = classify_event(&event);
            assert_eq!(c.content_type, ContentType::Data, "ext={}", ext);
        }
    }

    #[test]
    fn test_classify_file_err_extension() {
        let mut tags = HashMap::new();
        tags.insert("extension".into(), "err".into());
        let event = make_event("file_change", "file:/tmp/out.err", tags);
        let c = classify_event(&event);
        assert_eq!(c.content_type, ContentType::Error);
    }

    #[test]
    fn test_classify_file_unknown_extension() {
        let mut tags = HashMap::new();
        tags.insert("extension".into(), "xyz".into());
        let event = make_event("file_change", "file:/tmp/mystery.xyz", tags);
        let c = classify_event(&event);
        assert_eq!(c.content_type, ContentType::Generic);
    }

    #[test]
    fn test_classify_file_no_extension() {
        let event = make_event("file_change", "file:/tmp/README", HashMap::new());
        let c = classify_event(&event);
        assert_eq!(c.content_type, ContentType::Generic);
    }

    // ── Content type: non-file event types ─────────────────────────

    #[test]
    fn test_classify_process_events() {
        for etype in &[
            "process_started",
            "process_exited",
            "process_resource_change",
        ] {
            let event = make_event(etype, "process:1234", HashMap::new());
            let c = classify_event(&event);
            assert_eq!(c.content_type, ContentType::System, "type={}", etype);
        }
    }

    #[test]
    fn test_classify_network_events() {
        for etype in &[
            "network_new_connection",
            "network_closed_connection",
            "network_state_change",
        ] {
            let event = make_event(etype, "network", HashMap::new());
            let c = classify_event(&event);
            assert_eq!(c.content_type, ContentType::Network, "type={}", etype);
        }
    }

    #[test]
    fn test_classify_clipboard() {
        let event = make_event("clipboard_change", "clipboard", HashMap::new());
        let c = classify_event(&event);
        assert_eq!(c.content_type, ContentType::Clipboard);
    }

    #[test]
    fn test_classify_notification_types() {
        for etype in &[
            "webhook",
            "rss_item",
            "email_message",
            "git_push",
            "git_tag",
            "git_release",
        ] {
            let event = make_event(etype, "test", HashMap::new());
            let c = classify_event(&event);
            assert_eq!(c.content_type, ContentType::Notification, "type={}", etype);
        }
    }

    #[test]
    fn test_classify_unknown_type() {
        let event = make_event("custom_event", "custom:source", HashMap::new());
        let c = classify_event(&event);
        assert_eq!(c.content_type, ContentType::Generic);
    }

    // ── Urgency detection ──────────────────────────────────────────

    #[test]
    fn test_urgency_error_content_type() {
        let mut tags = HashMap::new();
        tags.insert("extension".into(), "err".into());
        let event = make_event("file_change", "file:/tmp/out.err", tags);
        let c = classify_event(&event);
        assert_eq!(c.urgency, Urgency::High);
    }

    #[test]
    fn test_urgency_critical_cpu() {
        let mut tags = HashMap::new();
        tags.insert("cpu_usage".into(), "95.0".into());
        let event = make_event("process_resource_change", "process:1", tags);
        let c = classify_event(&event);
        assert_eq!(c.urgency, Urgency::Critical);
    }

    #[test]
    fn test_urgency_high_cpu() {
        let mut tags = HashMap::new();
        tags.insert("cpu_usage".into(), "75.0".into());
        let event = make_event("process_resource_change", "process:1", tags);
        let c = classify_event(&event);
        assert_eq!(c.urgency, Urgency::High);
    }

    #[test]
    fn test_urgency_normal_cpu() {
        let mut tags = HashMap::new();
        tags.insert("cpu_usage".into(), "30.0".into());
        let event = make_event("process_resource_change", "process:1", tags);
        let c = classify_event(&event);
        assert_eq!(c.urgency, Urgency::Normal);
    }

    #[test]
    fn test_urgency_high_memory() {
        let mut tags = HashMap::new();
        tags.insert("memory_mb".into(), "8192".into());
        let event = make_event("process_resource_change", "process:1", tags);
        let c = classify_event(&event);
        assert_eq!(c.urgency, Urgency::High);
    }

    #[test]
    fn test_urgency_normal_memory() {
        let mut tags = HashMap::new();
        tags.insert("memory_mb".into(), "512".into());
        let event = make_event("process_resource_change", "process:1", tags);
        let c = classify_event(&event);
        assert_eq!(c.urgency, Urgency::Normal);
    }

    #[test]
    fn test_urgency_suspicious_ports() {
        for port in &[5555, 6666, 1234, 31337] {
            let mut tags = HashMap::new();
            tags.insert("remote".into(), format!("10.0.0.1:{}", port));
            let event = make_event("network_new_connection", "network", tags);
            let c = classify_event(&event);
            assert_eq!(c.urgency, Urgency::Critical, "port={}", port);
        }
    }

    #[test]
    fn test_urgency_unencrypted_protocols() {
        for port in &[23, 21, 513] {
            let mut tags = HashMap::new();
            tags.insert("remote".into(), format!("10.0.0.1:{}", port));
            let event = make_event("network_new_connection", "network", tags);
            let c = classify_event(&event);
            assert_eq!(c.urgency, Urgency::High, "port={}", port);
        }
    }

    #[test]
    fn test_urgency_payload_critical_keyword() {
        let mut tags = HashMap::new();
        let event = RawEvent {
            id: "test".into(),
            source: "test".into(),
            event_type: "custom".into(),
            timestamp_ms: 1000,
            payload: b"FATAL: system crash detected".to_vec(),
            tags,
        };
        let c = classify_event(&event);
        assert_eq!(c.urgency, Urgency::Critical);
    }

    #[test]
    fn test_urgency_payload_error_keyword() {
        let event = RawEvent {
            id: "test".into(),
            source: "test".into(),
            event_type: "custom".into(),
            timestamp_ms: 1000,
            payload: b"Connection failure detected".to_vec(),
            tags: HashMap::new(),
        };
        let c = classify_event(&event);
        assert_eq!(c.urgency, Urgency::High);
    }

    #[test]
    fn test_urgency_payload_warning_keyword() {
        let event = RawEvent {
            id: "test".into(),
            source: "test".into(),
            event_type: "custom".into(),
            timestamp_ms: 1000,
            payload: b"Warning: disk space low".to_vec(),
            tags: HashMap::new(),
        };
        let c = classify_event(&event);
        assert_eq!(c.urgency, Urgency::Normal);
    }

    #[test]
    fn test_urgency_normal_default() {
        let event = make_event("clipboard_change", "clipboard", HashMap::new());
        let c = classify_event(&event);
        assert_eq!(c.urgency, Urgency::Normal);
    }

    #[test]
    fn test_urgency_invalid_cpu_value() {
        let mut tags = HashMap::new();
        tags.insert("cpu_usage".into(), "not_a_number".into());
        let event = make_event("process_resource_change", "process:1", tags);
        let c = classify_event(&event);
        // Invalid cpu_usage should not affect urgency
        assert_eq!(c.urgency, Urgency::Normal);
    }

    #[test]
    fn test_urgency_invalid_memory_value() {
        let mut tags = HashMap::new();
        tags.insert("memory_mb".into(), "NaN".into());
        let event = make_event("process_resource_change", "process:1", tags);
        let c = classify_event(&event);
        assert_eq!(c.urgency, Urgency::Normal);
    }

    // ── Label extraction ───────────────────────────────────────────

    #[test]
    fn test_labels_file_extension() {
        let mut tags = HashMap::new();
        tags.insert("extension".into(), "json".into());
        let event = make_event("file_change", "file:/tmp/data.json", tags);
        let c = classify_event(&event);
        assert!(c.labels.contains(&"filetype:json".to_string()));
    }

    #[test]
    fn test_labels_network_state() {
        let mut tags = HashMap::new();
        tags.insert("state".into(), "ESTABLISHED".into());
        let event = make_event("network_new_connection", "network", tags);
        let c = classify_event(&event);
        assert!(c.labels.contains(&"net:established".to_string()));
    }

    #[test]
    fn test_labels_change_type() {
        let mut tags = HashMap::new();
        tags.insert("change_type".into(), "created".into());
        let event = make_event("file_change", "file:/tmp/new.txt", tags);
        let c = classify_event(&event);
        assert!(c.labels.contains(&"created".to_string()));
    }

    #[test]
    fn test_labels_protocol() {
        let mut tags = HashMap::new();
        tags.insert("protocol".into(), "tcp".into());
        let event = make_event("network_new_connection", "network", tags);
        let c = classify_event(&event);
        assert!(c.labels.contains(&"proto:tcp".to_string()));
    }

    #[test]
    fn test_labels_empty_when_no_tags() {
        let event = make_event("clipboard_change", "clipboard", HashMap::new());
        let c = classify_event(&event);
        assert!(c.labels.is_empty());
    }

    // ── apply_classification ────────────────────────────────────────

    #[test]
    fn test_apply_classification_adds_tags() {
        let mut event = make_event("file_change", "file:/tmp/data.json", HashMap::new());
        let classification = Classification {
            source_category: "file".into(),
            content_type: ContentType::Data,
            urgency: Urgency::Normal,
            labels: vec!["filetype:json".into()],
        };
        apply_classification(&mut event, &classification);
        assert_eq!(event.tags.get("class_category").unwrap(), "file");
        assert_eq!(event.tags.get("class_type").unwrap(), "data");
        assert_eq!(event.tags.get("class_urgency").unwrap(), "normal");
        assert_eq!(event.tags.get("class_labels").unwrap(), "filetype:json");
    }

    #[test]
    fn test_apply_classification_no_labels() {
        let mut event = make_event("clipboard_change", "clipboard", HashMap::new());
        let classification = Classification {
            source_category: "clipboard".into(),
            content_type: ContentType::Clipboard,
            urgency: Urgency::Normal,
            labels: vec![],
        };
        apply_classification(&mut event, &classification);
        assert_eq!(event.tags.get("class_category").unwrap(), "clipboard");
        assert!(!event.tags.contains_key("class_labels"));
    }

    // ── Edge cases ─────────────────────────────────────────────────

    #[test]
    fn test_source_category_no_colon() {
        assert_eq!(extract_source_category("clipboard"), "clipboard");
        assert_eq!(extract_source_category(""), "");
    }

    #[test]
    fn test_classify_empty_source() {
        let event = make_event("unknown", "", HashMap::new());
        let c = classify_event(&event);
        assert_eq!(c.source_category, "");
    }

    #[test]
    fn test_classify_multiple_colons_in_source() {
        assert_eq!(
            extract_source_category("connector:dingtalk:approval:inst-002"),
            "connector"
        );
    }

    #[test]
    fn test_urgency_critical_in_lowercase_payload() {
        let event = RawEvent {
            id: "test".into(),
            source: "test".into(),
            event_type: "custom".into(),
            timestamp_ms: 1000,
            payload: b"CRITICAL: database unreachable".to_vec(),
            tags: HashMap::new(),
        };
        let c = classify_event(&event);
        assert_eq!(c.urgency, Urgency::Critical);
    }

    #[test]
    fn test_urgency_exception_in_payload() {
        let event = RawEvent {
            id: "test".into(),
            source: "test".into(),
            event_type: "custom".into(),
            timestamp_ms: 1000,
            payload: b"Unhandled exception in worker thread".to_vec(),
            tags: HashMap::new(),
        };
        let c = classify_event(&event);
        assert_eq!(c.urgency, Urgency::High);
    }

    #[test]
    fn test_network_port_no_match() {
        let mut tags = HashMap::new();
        tags.insert("remote".into(), "10.0.0.1:8080".into());
        let event = make_event("network_new_connection", "network", tags);
        let c = classify_event(&event);
        assert_eq!(c.urgency, Urgency::Normal);
    }

    #[test]
    fn test_urgency_boundary_cpu_90() {
        let mut tags = HashMap::new();
        tags.insert("cpu_usage".into(), "90.0".into());
        let event = make_event("process_resource_change", "process:1", tags);
        let c = classify_event(&event);
        assert_eq!(c.urgency, Urgency::High); // exactly 90 → High, not Critical
    }

    #[test]
    fn test_urgency_boundary_cpu_91() {
        let mut tags = HashMap::new();
        tags.insert("cpu_usage".into(), "91.0".into());
        let event = make_event("process_resource_change", "process:1", tags);
        let c = classify_event(&event);
        assert_eq!(c.urgency, Urgency::Critical);
    }
}
