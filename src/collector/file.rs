use anyhow::Result;
use chrono::Utc;
use notify::{RecursiveMode, Watcher};
use notify_debouncer_mini::{new_debouncer, DebouncedEventKind};
use std::path::Path;
use std::time::Duration;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

use super::{EventTx, RawEvent};

/// Start a debounced file system watcher on the given directories.
pub async fn start_watcher(
    watch_dirs: &[String],
    debounce_ms: u64,
    include: &[String],
    exclude: &[String],
    tx: EventTx,
) -> Result<()> {
    let (debouncer_tx, debouncer_rx) = std::sync::mpsc::channel();

    let mut debouncer = new_debouncer(
        Duration::from_millis(debounce_ms),
        debouncer_tx,
    )?;

    for dir in watch_dirs {
        let path = Path::new(dir);
        if !path.exists() {
            warn!("Watch directory does not exist: {}", dir);
            continue;
        }
        debouncer
            .watcher()
            .watch(path, RecursiveMode::Recursive)?;
        info!("Watching directory: {}", dir);
    }

    // Process debounced events in a blocking thread (notify is sync)
    let include = include.to_vec();
    let exclude = exclude.to_vec();

    tokio::task::spawn_blocking(move || {
        for result in debouncer_rx {
            match result {
                Ok(events) => {
                    for event in events {
                        if let DebouncedEventKind::Any = event.kind {
                            let path_str = event.path.to_string_lossy().to_string();

                            if !matches_pattern(&path_str, &include, &exclude) {
                                debug!("Skipping non-matching file: {}", path_str);
                                continue;
                            }

                            let raw_event = RawEvent {
                                id: Uuid::new_v4().to_string(),
                                source: format!("file:{}", path_str),
                                event_type: "file_change".to_string(),
                                timestamp_ms: Utc::now().timestamp_millis(),
                                payload: read_file_payload(&event.path),
                                tags: build_tags(&event.path),
                            };

                            // Non-blocking send — if channel is full, drop the event
                            match tx.try_send(raw_event) {
                                Ok(()) => {
                                    debug!("Collected event from: {}", path_str);
                                }
                                Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                                    warn!("Event channel full, dropping event for: {}", path_str);
                                }
                                Err(e) => {
                                    error!("Failed to send event: {}", e);
                                    break;
                                }
                            }
                        }
                    }
                }
                Err(errors) => {
                    for e in errors {
                        error!("File watch error: {}", e);
                    }
                }
            }
        }
    });

    Ok(())
}

/// Check if a file path matches include/exclude glob patterns.
fn matches_pattern(path: &str, include: &[String], exclude: &[String]) -> bool {
    let filename = Path::new(path)
        .file_name()
        .map(|f| f.to_string_lossy().to_string())
        .unwrap_or_default();

    // Check exclude first
    for pattern in exclude {
        if simple_glob_match(pattern, &filename) {
            return false;
        }
    }

    // If include is empty, accept all
    if include.is_empty() {
        return true;
    }

    // Must match at least one include pattern
    include
        .iter()
        .any(|pattern| simple_glob_match(pattern, &filename))
}

/// Simple glob matching supporting only `*` wildcard.
fn simple_glob_match(pattern: &str, text: &str) -> bool {
    if pattern == "*" {
        return true;
    }
    if let Some(suffix) = pattern.strip_prefix("*.") {
        return text.ends_with(&format!(".{}", suffix));
    }
    if let Some(prefix) = pattern.strip_suffix(".*") {
        return text.starts_with(prefix);
    }
    pattern == text
}

/// Read file contents as payload bytes (limited to 1MB).
fn read_file_payload(path: &Path) -> Vec<u8> {
    match std::fs::read(path) {
        Ok(data) => {
            if data.len() > 1_048_576 {
                data[..1_048_576].to_vec()
            } else {
                data
            }
        }
        Err(e) => {
            debug!("Could not read file {}: {}", path.display(), e);
            Vec::new()
        }
    }
}

/// Build metadata tags from file path.
fn build_tags(path: &Path) -> std::collections::HashMap<String, String> {
    let mut tags = std::collections::HashMap::new();
    tags.insert(
        "filename".to_string(),
        path.file_name()
            .map(|f| f.to_string_lossy().to_string())
            .unwrap_or_default(),
    );
    tags.insert(
        "extension".to_string(),
        path.extension()
            .map(|e| e.to_string_lossy().to_string())
            .unwrap_or_default(),
    );
    if let Some(parent) = path.parent() {
        tags.insert(
            "directory".to_string(),
            parent.to_string_lossy().to_string(),
        );
    }
    tags
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_glob_match() {
        assert!(simple_glob_match("*.json", "data.json"));
        assert!(simple_glob_match("*.txt", "readme.txt"));
        assert!(!simple_glob_match("*.json", "data.csv"));
        assert!(simple_glob_match("*", "anything"));
    }

    #[test]
    fn test_matches_pattern() {
        let include = vec!["*.json".to_string(), "*.csv".to_string()];
        let exclude = vec!["*.tmp".to_string()];

        assert!(matches_pattern("/data/file.json", &include, &exclude));
        assert!(matches_pattern("/data/file.csv", &include, &exclude));
        assert!(!matches_pattern("/data/file.txt", &include, &exclude));
        assert!(!matches_pattern("/data/file.tmp", &include, &exclude));
    }
}
