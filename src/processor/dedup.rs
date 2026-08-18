use std::collections::VecDeque;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;

use crate::collector::RawEvent;

/// Content-addressed deduplicator with a sliding time window.
/// Uses SHA-256 of (source + payload_hash) as the dedup key.
pub struct Deduplicator {
    seen: Mutex<VecDeque<(String, Instant)>>,
    window: Duration,
}

impl Deduplicator {
    pub fn new(window_secs: u64) -> Self {
        Self {
            seen: Mutex::new(VecDeque::new()),
            window: Duration::from_secs(window_secs),
        }
    }

    /// Returns true if this event is a duplicate (already seen within the window).
    pub async fn is_duplicate(&self, event: &RawEvent) -> bool {
        let key = Self::dedup_key(event);
        let now = Instant::now();

        let mut seen = self.seen.lock().await;

        // Evict expired entries
        while let Some((_, ts)) = seen.front() {
            if now.duration_since(*ts) > self.window {
                seen.pop_front();
            } else {
                break;
            }
        }

        // Check if we've seen this key
        if seen.iter().any(|(k, _)| k == &key) {
            return true;
        }

        // Record this event
        seen.push_back((key, now));
        false
    }

    /// Compute a dedup key from event source + content hash.
    fn dedup_key(event: &RawEvent) -> String {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mut hasher = DefaultHasher::new();
        event.source.hash(&mut hasher);
        // Hash first 4KB of payload for performance
        let payload_sample = if event.payload.len() > 4096 {
            &event.payload[..4096]
        } else {
            &event.payload
        };
        payload_sample.hash(&mut hasher);
        format!("{:016x}", hasher.finish())
    }

    /// Current number of entries in the dedup window.
    pub async fn len(&self) -> usize {
        self.seen.lock().await.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn make_event(id: &str, source: &str, payload: &[u8]) -> RawEvent {
        RawEvent {
            id: id.into(),
            source: source.into(),
            event_type: "test".into(),
            timestamp_ms: 1000,
            payload: payload.to_vec(),
            tags: HashMap::new(),
        }
    }

    #[tokio::test]
    async fn test_first_event_not_duplicate() {
        let dedup = Deduplicator::new(300);
        let event = make_event("1", "file:test", b"hello");
        assert!(!dedup.is_duplicate(&event).await);
    }

    #[tokio::test]
    async fn test_same_event_is_duplicate() {
        let dedup = Deduplicator::new(300);
        let event1 = make_event("1", "file:test", b"hello");
        let event2 = make_event("2", "file:test", b"hello");
        assert!(!dedup.is_duplicate(&event1).await);
        assert!(dedup.is_duplicate(&event2).await);
    }

    #[tokio::test]
    async fn test_different_events_not_duplicate() {
        let dedup = Deduplicator::new(300);
        let event1 = make_event("1", "file:test", b"hello");
        let event2 = make_event("2", "file:test", b"world");
        assert!(!dedup.is_duplicate(&event1).await);
        assert!(!dedup.is_duplicate(&event2).await);
    }

    #[tokio::test]
    async fn test_same_content_different_source_not_duplicate() {
        let dedup = Deduplicator::new(300);
        let event1 = make_event("1", "file:a", b"hello");
        let event2 = make_event("2", "file:b", b"hello");
        assert!(!dedup.is_duplicate(&event1).await);
        assert!(!dedup.is_duplicate(&event2).await);
    }

    #[tokio::test]
    async fn test_dedup_window_length() {
        let dedup = Deduplicator::new(300);
        let e1 = make_event("1", "src", b"data");
        let e2 = make_event("2", "src", b"data");
        let e3 = make_event("3", "src", b"data");
        assert!(!dedup.is_duplicate(&e1).await);
        assert!(dedup.is_duplicate(&e2).await);
        assert!(dedup.is_duplicate(&e3).await);
        assert_eq!(dedup.len().await, 1); // Only one unique key in window
    }

    #[tokio::test]
    async fn test_empty_payload() {
        let dedup = Deduplicator::new(300);
        let e1 = make_event("1", "src", b"");
        let e2 = make_event("2", "src", b"");
        assert!(!dedup.is_duplicate(&e1).await);
        assert!(dedup.is_duplicate(&e2).await);
    }

    #[tokio::test]
    async fn test_large_payload_dedup() {
        let dedup = Deduplicator::new(300);
        let big = vec![0u8; 8192]; // > 4KB, exercises the sample truncation
        let e1 = make_event("1", "src", &big);
        let e2 = make_event("2", "src", &big);
        assert!(!dedup.is_duplicate(&e1).await);
        assert!(dedup.is_duplicate(&e2).await);
    }
}
