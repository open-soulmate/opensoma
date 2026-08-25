/// In-memory ring buffer for recent log messages.
/// A tracing `Layer` captures all log events into a fixed-size ring buffer.
/// The status server exposes them via `GET /api/logs` for the dashboard Logs page.
use std::collections::VecDeque;
use std::fmt;
use std::sync::{Arc, Mutex};
use tracing::Subscriber;
use tracing_subscriber::layer::Context;
use tracing_subscriber::Layer;

/// Maximum number of log entries kept in the ring buffer.
const DEFAULT_CAPACITY: usize = 2000;

/// A single captured log entry.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct LogEntry {
    /// ISO-8601 timestamp (UTC)
    pub ts: String,
    /// Log level: TRACE, DEBUG, INFO, WARN, ERROR
    pub level: String,
    /// Target/module path (e.g. `opensoma::connector::github`)
    pub target: String,
    /// The formatted log message
    pub message: String,
}

/// Thread-safe ring buffer of recent log entries.
#[derive(Clone)]
pub struct LogBuffer {
    inner: Arc<Mutex<VecDeque<LogEntry>>>,
    capacity: usize,
}

impl LogBuffer {
    /// Create a new log buffer with the default capacity.
    pub fn new() -> Self {
        Self::with_capacity(DEFAULT_CAPACITY)
    }

    /// Create a new log buffer with a custom capacity.
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            inner: Arc::new(Mutex::new(VecDeque::with_capacity(capacity))),
            capacity,
        }
    }

    /// Push a log entry, evicting the oldest if at capacity.
    pub fn push(&self, entry: LogEntry) {
        let mut buf = self.inner.lock().unwrap();
        if buf.len() >= self.capacity {
            buf.pop_front();
        }
        buf.push_back(entry);
    }

    /// Retrieve the most recent `limit` entries (newest last).
    pub fn recent(&self, limit: usize) -> Vec<LogEntry> {
        let buf = self.inner.lock().unwrap();
        let len = buf.len();
        let skip = len.saturating_sub(limit);
        buf.iter().skip(skip).cloned().collect()
    }

    /// Retrieve entries filtered by level (case-insensitive).
    pub fn recent_filtered(&self, limit: usize, level: &str) -> Vec<LogEntry> {
        let buf = self.inner.lock().unwrap();
        let level_upper = level.to_uppercase();
        buf.iter()
            .filter(|e| e.level == level_upper)
            .rev()
            .take(limit)
            .collect::<Vec<_>>()
            .into_iter()
            .rev()
            .cloned()
            .collect()
    }

    /// Total number of entries currently stored.
    pub fn len(&self) -> usize {
        self.inner.lock().unwrap().len()
    }

    /// Returns true if the buffer is empty.
    pub fn is_empty(&self) -> bool {
        self.inner.lock().unwrap().is_empty()
    }

    /// Clear all entries.
    pub fn clear(&self) {
        self.inner.lock().unwrap().clear();
    }

    /// The configured capacity.
    pub fn capacity(&self) -> usize {
        self.capacity
    }
}

impl Default for LogBuffer {
    fn default() -> Self {
        Self::new()
    }
}

/// A tracing Layer that forwards events to a `LogBuffer`.
pub struct LogBufferLayer {
    buffer: LogBuffer,
}

impl LogBufferLayer {
    pub fn new(buffer: LogBuffer) -> Self {
        Self { buffer }
    }
}

impl<S: Subscriber> Layer<S> for LogBufferLayer {
    fn on_event(&self, event: &tracing::Event<'_>, _ctx: Context<'_, S>) {
        let metadata = event.metadata();
        let level = format!("{}", metadata.level());
        let target = metadata.target().to_string();

        // Extract the message from the event fields
        let mut visitor = MessageVisitor(String::new());
        event.record(&mut visitor);
        let message = visitor.0;

        // Skip empty messages (spans, etc.)
        if message.is_empty() {
            return;
        }

        let ts = chrono::Utc::now()
            .format("%Y-%m-%dT%H:%M:%S%.3fZ")
            .to_string();

        self.buffer.push(LogEntry {
            ts,
            level,
            target,
            message,
        });
    }
}

/// Visitor that extracts the message field from a tracing event.
struct MessageVisitor(String);

impl tracing::field::Visit for MessageVisitor {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn fmt::Debug) {
        if field.name() == "message" && self.0.is_empty() {
            self.0 = format!("{:?}", value);
            // Remove surrounding quotes added by Debug for &str
            if self.0.starts_with('"') && self.0.ends_with('"') {
                self.0 = self.0[1..self.0.len() - 1].to_string();
            }
        }
    }

    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        if field.name() == "message" && self.0.is_empty() {
            self.0 = value.to_string();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_log_buffer_push_and_recent() {
        let buf = LogBuffer::with_capacity(5);
        for i in 0..3 {
            buf.push(LogEntry {
                ts: format!("2025-01-01T00:00:0{}.000Z", i),
                level: "INFO".into(),
                target: "test".into(),
                message: format!("msg {}", i),
            });
        }
        let entries = buf.recent(10);
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0].message, "msg 0");
        assert_eq!(entries[2].message, "msg 2");
    }

    #[test]
    fn test_log_buffer_eviction() {
        let buf = LogBuffer::with_capacity(3);
        for i in 0..5 {
            buf.push(LogEntry {
                ts: format!("ts-{}", i),
                level: "INFO".into(),
                target: "test".into(),
                message: format!("msg {}", i),
            });
        }
        assert_eq!(buf.len(), 3);
        let entries = buf.recent(10);
        assert_eq!(entries[0].message, "msg 2");
        assert_eq!(entries[2].message, "msg 4");
    }

    #[test]
    fn test_log_buffer_filter_by_level() {
        let buf = LogBuffer::with_capacity(10);
        buf.push(LogEntry {
            ts: "t1".into(),
            level: "INFO".into(),
            target: "test".into(),
            message: "info msg".into(),
        });
        buf.push(LogEntry {
            ts: "t2".into(),
            level: "ERROR".into(),
            target: "test".into(),
            message: "error msg".into(),
        });
        buf.push(LogEntry {
            ts: "t3".into(),
            level: "INFO".into(),
            target: "test".into(),
            message: "info msg 2".into(),
        });

        let errors = buf.recent_filtered(10, "error");
        assert_eq!(errors.len(), 1);
        assert_eq!(errors[0].message, "error msg");

        let infos = buf.recent_filtered(10, "info");
        assert_eq!(infos.len(), 2);
    }

    #[test]
    fn test_log_buffer_capacity() {
        let buf = LogBuffer::with_capacity(100);
        assert_eq!(buf.capacity(), 100);
        assert!(buf.is_empty());
        assert_eq!(buf.len(), 0);
    }

    #[test]
    fn test_log_buffer_clear() {
        let buf = LogBuffer::with_capacity(10);
        buf.push(LogEntry {
            ts: "t".into(),
            level: "INFO".into(),
            target: "test".into(),
            message: "msg".into(),
        });
        assert_eq!(buf.len(), 1);
        buf.clear();
        assert!(buf.is_empty());
    }

    #[test]
    fn test_log_buffer_limit() {
        let buf = LogBuffer::with_capacity(10);
        for i in 0..10 {
            buf.push(LogEntry {
                ts: format!("ts-{}", i),
                level: "INFO".into(),
                target: "test".into(),
                message: format!("msg {}", i),
            });
        }
        let entries = buf.recent(3);
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0].message, "msg 7");
        assert_eq!(entries[2].message, "msg 9");
    }

    #[test]
    fn test_log_entry_serialization() {
        let entry = LogEntry {
            ts: "2025-01-01T00:00:00.000Z".into(),
            level: "INFO".into(),
            target: "opensoma::test".into(),
            message: "hello".into(),
        };
        let json = serde_json::to_string(&entry).unwrap();
        assert!(json.contains("INFO"));
        assert!(json.contains("hello"));
        let _: LogEntry = serde_json::from_str(&json).unwrap();
    }
}
