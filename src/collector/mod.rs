pub mod clipboard;
pub mod file;
pub mod network;
pub mod process;

use anyhow::Result;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

/// A raw collected event before processing.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RawEvent {
    pub id: String,
    pub source: String,
    pub event_type: String,
    pub timestamp_ms: i64,
    pub payload: Vec<u8>,
    pub tags: std::collections::HashMap<String, String>,
}

/// Channel type for passing raw events between subsystems.
pub type EventTx = mpsc::Sender<RawEvent>;
pub type EventRx = mpsc::Receiver<RawEvent>;

/// Collector runtime status for monitoring.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CollectorStatus {
    pub name: String,
    pub running: bool,
    pub events_collected: u64,
    pub last_error: Option<String>,
}

/// Start all configured collectors. Events are sent to the provided `tx`.
pub async fn start_all(
    config: &crate::config::CollectorConfig,
    tx: EventTx,
) -> Result<JoinHandle<()>> {
    let watch_dirs = config.watch_dirs.clone();
    let debounce_ms = config.debounce_ms;
    let include = config.include.clone();
    let exclude = config.exclude.clone();
    let process_interval = config.process_interval_ms;
    let network_interval = config.network_interval_ms;
    let clipboard_interval = config.clipboard_interval_ms;

    // Start process monitor in a separate task
    let process_tx = tx.clone();
    tokio::spawn(async move {
        if let Err(e) = process::start_process_monitor(process_interval, process_tx).await {
            tracing::error!("Process collector failed: {}", e);
        }
    });

    // Start network monitor in a separate task
    let network_tx = tx.clone();
    tokio::spawn(async move {
        if let Err(e) = network::start_network_monitor(network_interval, network_tx).await {
            tracing::error!("Network collector failed: {}", e);
        }
    });

    // Start clipboard monitor in a separate task
    let clipboard_tx = tx.clone();
    tokio::spawn(async move {
        if let Err(e) = clipboard::start_clipboard_monitor(clipboard_interval, clipboard_tx).await {
            tracing::error!("Clipboard collector failed: {}", e);
        }
    });

    // Start file watcher (consumes the remaining tx)
    let handle = tokio::spawn(async move {
        if let Err(e) = file::start_watcher(&watch_dirs, debounce_ms, &include, &exclude, tx).await
        {
            tracing::error!("File collector failed: {}", e);
        }
    });

    Ok(handle)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_raw_event_serialization() {
        let event = RawEvent {
            id: "test-123".to_string(),
            source: "file".to_string(),
            event_type: "file_change".to_string(),
            timestamp_ms: 1700000000000,
            payload: b"hello world".to_vec(),
            tags: [("key".into(), "value".into())].into(),
        };

        let json = serde_json::to_string(&event).unwrap();
        assert!(json.contains("test-123"));
        assert!(json.contains("file_change"));

        let deserialized: RawEvent = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.id, "test-123");
        assert_eq!(deserialized.payload, b"hello world");
    }

    #[test]
    fn test_collector_status_serialization() {
        let status = CollectorStatus {
            name: "file".to_string(),
            running: true,
            events_collected: 42,
            last_error: None,
        };

        let json = serde_json::to_string(&status).unwrap();
        assert!(json.contains("\"running\":true"));
        assert!(json.contains("\"events_collected\":42"));
    }

    #[test]
    fn test_raw_event_empty_tags() {
        let event = RawEvent {
            id: "no-tags".to_string(),
            source: "clipboard".to_string(),
            event_type: "clipboard_change".to_string(),
            timestamp_ms: 1700000000000,
            payload: b"clipboard text".to_vec(),
            tags: std::collections::HashMap::new(),
        };
        let json = serde_json::to_string(&event).unwrap();
        assert!(json.contains("no-tags"));
        let deserialized: RawEvent = serde_json::from_str(&json).unwrap();
        assert!(deserialized.tags.is_empty());
    }

    #[test]
    fn test_raw_event_large_payload() {
        let large_data = vec![0u8; 1024 * 1024]; // 1MB
        let event = RawEvent {
            id: "large-1".to_string(),
            source: "file".to_string(),
            event_type: "file_change".to_string(),
            timestamp_ms: 1700000000000,
            payload: large_data.clone(),
            tags: std::collections::HashMap::new(),
        };
        assert_eq!(event.payload.len(), 1024 * 1024);
        let json = serde_json::to_string(&event).unwrap();
        let deserialized: RawEvent = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.payload.len(), 1024 * 1024);
    }

    #[test]
    fn test_collector_status_with_error() {
        let status = CollectorStatus {
            name: "network".to_string(),
            running: false,
            events_collected: 0,
            last_error: Some("connection refused".to_string()),
        };
        let json = serde_json::to_string(&status).unwrap();
        assert!(json.contains("connection refused"));
        assert!(json.contains("false"));
    }
}
