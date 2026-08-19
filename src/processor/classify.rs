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
}
