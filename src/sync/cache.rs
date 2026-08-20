use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::path::Path;
use tracing::{debug, info};

use crate::collector::RawEvent;

/// Metadata for a cached event.
#[derive(Debug, Serialize, Deserialize)]
struct CacheEntry {
    event: RawEvent,
    /// SHA-256 hash of the event payload for deduplication.
    content_hash: String,
    uploaded: bool,
    created_at: i64,
    #[serde(default)]
    retry_count: u32,
}

/// Local event cache backed by sled.
/// Clone is cheap — sled::Db uses Arc internally.
#[derive(Clone)]
pub struct Cache {
    db: sled::Db,
}

impl Cache {
    /// Open or create a cache database at `{data_dir}/cache`.
    pub fn open(data_dir: &str) -> Result<Self> {
        let cache_path = Path::new(data_dir).join("cache");
        std::fs::create_dir_all(&cache_path)
            .with_context(|| format!("Failed to create cache dir: {:?}", cache_path))?;

        let db = sled::Config::new()
            .path(&cache_path)
            .cache_capacity(64 * 1024 * 1024) // 64MB page cache
            .open()
            .with_context(|| format!("Failed to open sled DB at: {:?}", cache_path))?;

        info!("Cache opened at: {:?}", cache_path);
        Ok(Self { db })
    }

    /// Compute SHA-256 hash of event payload.
    pub fn hash_event(event: &RawEvent) -> String {
        let mut hasher = Sha256::new();
        hasher.update(&event.payload);
        hex::encode(hasher.finalize())
    }

    /// Store an event in the cache. Skips if an event with the same content
    /// hash already exists (deduplication).
    pub fn put(&self, event: &RawEvent) -> Result<()> {
        let content_hash = Self::hash_event(event);

        // Check for duplicate content
        if let Some(existing) = self.db.get(event.id.as_bytes())? {
            let entry: CacheEntry = serde_json::from_slice(&existing)?;
            if entry.content_hash == content_hash && entry.uploaded {
                debug!(
                    "Skipping already-uploaded event with same hash: {}",
                    event.id
                );
                return Ok(());
            }
        }

        let entry = CacheEntry {
            event: event.clone(),
            content_hash,
            uploaded: false,
            created_at: chrono::Utc::now().timestamp_millis(),
            retry_count: 0,
        };
        let value = serde_json::to_vec(&entry)?;
        self.db.insert(event.id.as_bytes(), value)?;
        Ok(())
    }

    /// Mark an event as uploaded.
    pub fn mark_uploaded(&self, event_id: &str) -> Result<()> {
        if let Some(existing) = self.db.get(event_id.as_bytes())? {
            let mut entry: CacheEntry = serde_json::from_slice(&existing)?;
            entry.uploaded = true;
            let value = serde_json::to_vec(&entry)?;
            self.db.insert(event_id.as_bytes(), value)?;
        }
        Ok(())
    }

    /// Increment the retry count for an event.
    pub fn increment_retry(&self, event_id: &str) -> Result<()> {
        if let Some(existing) = self.db.get(event_id.as_bytes())? {
            let mut entry: CacheEntry = serde_json::from_slice(&existing)?;
            entry.retry_count += 1;
            let value = serde_json::to_vec(&entry)?;
            self.db.insert(event_id.as_bytes(), value)?;
        }
        Ok(())
    }

    /// Get all events that haven't been uploaded yet (for retry on restart).
    pub fn get_pending(&self) -> Result<Vec<RawEvent>> {
        let mut pending = Vec::new();
        for item in self.db.iter() {
            let (_, value) = item?;
            let entry: CacheEntry = serde_json::from_slice(&value)?;
            if !entry.uploaded {
                pending.push(entry.event);
            }
        }
        Ok(pending)
    }

    /// Get the cached content hash for an event by ID.
    /// Returns None if the event is not in the cache.
    pub fn get_cached_hash(&self, event_id: &str) -> Result<Option<String>> {
        if let Some(raw) = self.db.get(event_id.as_bytes())? {
            let entry: CacheEntry = serde_json::from_slice(&raw)?;
            Ok(Some(entry.content_hash))
        } else {
            Ok(None)
        }
    }

    /// Get the cached event snapshot for conflict detection.
    /// Returns None if the event is not in the cache.
    pub fn get_snapshot(
        &self,
        event_id: &str,
    ) -> Result<Option<crate::sync::conflict::EventSnapshot>> {
        if let Some(raw) = self.db.get(event_id.as_bytes())? {
            let entry: CacheEntry = serde_json::from_slice(&raw)?;
            Ok(Some(crate::sync::conflict::EventSnapshot {
                id: entry.event.id.clone(),
                source: entry.event.source.clone(),
                event_type: entry.event.event_type.clone(),
                timestamp_ms: entry.event.timestamp_ms,
                content_hash: entry.content_hash,
                tags: entry.event.tags.clone(),
            }))
        } else {
            Ok(None)
        }
    }

    /// Check if an event with the given content hash already exists.
    pub fn contains_hash(&self, content_hash: &str) -> Result<bool> {
        for item in self.db.iter() {
            let (_, value) = item?;
            let entry: CacheEntry = serde_json::from_slice(&value)?;
            if entry.content_hash == content_hash {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Get the number of pending (un-uploaded) events.
    pub fn pending_count(&self) -> Result<usize> {
        let mut count = 0;
        for item in self.db.iter() {
            let (_, value) = item?;
            let entry: CacheEntry = serde_json::from_slice(&value)?;
            if !entry.uploaded {
                count += 1;
            }
        }
        Ok(count)
    }

    /// Remove events older than the given timestamp (only if uploaded).
    pub fn evict_before(&self, cutoff_ms: i64) -> Result<usize> {
        let mut count = 0;
        let mut to_remove = Vec::new();

        for item in self.db.iter() {
            let (key, value) = item?;
            let entry: CacheEntry = serde_json::from_slice(&value)?;
            if entry.uploaded && entry.created_at < cutoff_ms {
                to_remove.push(key);
            }
        }

        for key in &to_remove {
            self.db.remove(key)?;
            count += 1;
        }

        if count > 0 {
            debug!("Evicted {} old cache entries", count);
        }
        Ok(count)
    }

    /// Remove a single event from the cache.
    pub fn remove(&self, event_id: &str) -> Result<()> {
        self.db.remove(event_id.as_bytes())?;
        Ok(())
    }

    /// Get cache statistics.
    pub fn stats(&self) -> CacheStats {
        let mut total = 0usize;
        let mut uploaded = 0usize;
        let mut pending = 0usize;
        let mut cache_size_bytes = 0u64;

        for (_, value) in self.db.iter().flatten() {
            if let Ok(entry) = serde_json::from_slice::<CacheEntry>(&value) {
                total += 1;
                cache_size_bytes += value.len() as u64;
                if entry.uploaded {
                    uploaded += 1;
                } else {
                    pending += 1;
                }
            }
        }

        CacheStats {
            total,
            uploaded,
            pending,
            cache_size_bytes,
        }
    }

    /// Flush all pending writes to disk.
    pub fn flush(&self) -> Result<()> {
        self.db.flush()?;
        Ok(())
    }

    /// Get the N most recent events (for the event search API).
    /// Returns events sorted by creation time (newest first).
    pub fn get_recent(&self, limit: usize) -> Result<Vec<RawEvent>> {
        let mut entries: Vec<(i64, RawEvent)> = Vec::new();
        for item in self.db.iter().flatten() {
            if let Ok(entry) = serde_json::from_slice::<CacheEntry>(&item.1) {
                entries.push((entry.created_at, entry.event));
            }
        }
        // Sort newest first
        entries.sort_by_key(|b| std::cmp::Reverse(b.0));
        Ok(entries.into_iter().take(limit).map(|(_, e)| e).collect())
    }

    /// Search events by source prefix (e.g. "file:", "process:", "connector:github").
    pub fn search_by_source(&self, source_prefix: &str, limit: usize) -> Result<Vec<RawEvent>> {
        let mut results = Vec::new();
        for item in self.db.iter().flatten() {
            if let Ok(entry) = serde_json::from_slice::<CacheEntry>(&item.1) {
                if entry.event.source.starts_with(source_prefix) {
                    results.push((entry.created_at, entry.event));
                }
            }
        }
        results.sort_by_key(|b| std::cmp::Reverse(b.0));
        Ok(results.into_iter().take(limit).map(|(_, e)| e).collect())
    }

    /// Search events by event type (exact match or prefix).
    pub fn search_by_type(&self, event_type: &str, limit: usize) -> Result<Vec<RawEvent>> {
        let mut results = Vec::new();
        for item in self.db.iter().flatten() {
            if let Ok(entry) = serde_json::from_slice::<CacheEntry>(&item.1) {
                if entry.event.event_type == event_type
                    || entry.event.event_type.starts_with(event_type)
                {
                    results.push((entry.created_at, entry.event));
                }
            }
        }
        results.sort_by_key(|b| std::cmp::Reverse(b.0));
        Ok(results.into_iter().take(limit).map(|(_, e)| e).collect())
    }

    /// Search events by time range (inclusive).
    pub fn search_by_time_range(
        &self,
        after_ms: i64,
        before_ms: i64,
        limit: usize,
    ) -> Result<Vec<RawEvent>> {
        let mut results = Vec::new();
        for item in self.db.iter().flatten() {
            if let Ok(entry) = serde_json::from_slice::<CacheEntry>(&item.1) {
                let ts = entry.event.timestamp_ms;
                if ts >= after_ms && ts <= before_ms {
                    results.push((entry.created_at, entry.event));
                }
            }
        }
        results.sort_by_key(|b| std::cmp::Reverse(b.0));
        Ok(results.into_iter().take(limit).map(|(_, e)| e).collect())
    }

    /// Full-text search on event payload (simple substring match).
    pub fn search_by_payload(&self, query: &str, limit: usize) -> Result<Vec<RawEvent>> {
        let query_lower = query.to_lowercase();
        let mut results = Vec::new();
        for item in self.db.iter().flatten() {
            if let Ok(entry) = serde_json::from_slice::<CacheEntry>(&item.1) {
                let payload_str = String::from_utf8_lossy(&entry.event.payload).to_lowercase();
                if payload_str.contains(&query_lower) {
                    results.push((entry.created_at, entry.event));
                }
            }
        }
        results.sort_by_key(|b| std::cmp::Reverse(b.0));
        Ok(results.into_iter().take(limit).map(|(_, e)| e).collect())
    }
}

#[derive(Debug)]
pub struct CacheStats {
    pub total: usize,
    pub uploaded: usize,
    pub pending: usize,
    pub cache_size_bytes: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn temp_cache() -> Cache {
        let dir = tempfile::tempdir().unwrap();
        Cache::open(dir.path().to_str().unwrap()).unwrap()
    }

    fn make_event(id: &str, payload: &[u8]) -> RawEvent {
        RawEvent {
            id: id.to_string(),
            source: "test".to_string(),
            event_type: "test.event".to_string(),
            timestamp_ms: chrono::Utc::now().timestamp_millis(),
            payload: payload.to_vec(),
            tags: HashMap::new(),
        }
    }

    #[test]
    fn test_put_and_get_pending() {
        let cache = temp_cache();
        let event = make_event("ev1", b"hello");
        cache.put(&event).unwrap();

        let pending = cache.get_pending().unwrap();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].id, "ev1");
    }

    #[test]
    fn test_mark_uploaded() {
        let cache = temp_cache();
        let event = make_event("ev2", b"data");
        cache.put(&event).unwrap();
        cache.mark_uploaded("ev2").unwrap();

        let pending = cache.get_pending().unwrap();
        assert_eq!(pending.len(), 0);
    }

    #[test]
    fn test_pending_count() {
        let cache = temp_cache();
        assert_eq!(cache.pending_count().unwrap(), 0);

        cache.put(&make_event("a", b"1")).unwrap();
        cache.put(&make_event("b", b"2")).unwrap();
        assert_eq!(cache.pending_count().unwrap(), 2);

        cache.mark_uploaded("a").unwrap();
        assert_eq!(cache.pending_count().unwrap(), 1);
    }

    #[test]
    fn test_increment_retry() {
        let cache = temp_cache();
        cache.put(&make_event("ev3", b"data")).unwrap();
        cache.increment_retry("ev3").unwrap();
        cache.increment_retry("ev3").unwrap();
        // No panic = success; retry count is internal
    }

    #[test]
    fn test_hash_event() {
        let event = make_event("ev4", b"same content");
        let hash1 = Cache::hash_event(&event);
        let hash2 = Cache::hash_event(&event);
        assert_eq!(hash1, hash2);
        assert_eq!(hash1.len(), 64); // SHA-256 hex = 64 chars
    }

    #[test]
    fn test_contains_hash() {
        let cache = temp_cache();
        let event = make_event("ev5", b"unique payload");
        let hash = Cache::hash_event(&event);

        assert!(!cache.contains_hash(&hash).unwrap());
        cache.put(&event).unwrap();
        assert!(cache.contains_hash(&hash).unwrap());
    }

    #[test]
    fn test_stats() {
        let cache = temp_cache();
        cache.put(&make_event("a", b"1")).unwrap();
        cache.put(&make_event("b", b"2")).unwrap();
        cache.mark_uploaded("a").unwrap();

        let stats = cache.stats();
        assert_eq!(stats.total, 2);
        assert_eq!(stats.uploaded, 1);
        assert_eq!(stats.pending, 1);
    }

    #[test]
    fn test_evict_before() {
        let cache = temp_cache();
        cache.put(&make_event("old", b"old data")).unwrap();
        cache.mark_uploaded("old").unwrap();

        // Evict everything older than far future
        let future = chrono::Utc::now().timestamp_millis() + 100_000;
        let evicted = cache.evict_before(future).unwrap();
        assert_eq!(evicted, 1);
        assert_eq!(cache.stats().total, 0);
    }

    #[test]
    fn test_evict_before_skips_unuploaded() {
        let cache = temp_cache();
        cache.put(&make_event("pending", b"data")).unwrap();
        // Don't mark as uploaded

        let future = chrono::Utc::now().timestamp_millis() + 100_000;
        let evicted = cache.evict_before(future).unwrap();
        assert_eq!(evicted, 0); // Should not evict unuploaded
        assert_eq!(cache.stats().total, 1);
    }

    #[test]
    fn test_remove() {
        let cache = temp_cache();
        cache.put(&make_event("ev6", b"data")).unwrap();
        cache.remove("ev6").unwrap();
        assert_eq!(cache.stats().total, 0);
    }
}

// Additional tests for search methods
#[cfg(test)]
mod search_tests {
    use super::*;
    use std::collections::HashMap;

    fn temp_cache() -> Cache {
        let dir = tempfile::tempdir().unwrap();
        Cache::open(dir.path().to_str().unwrap()).unwrap()
    }

    fn make_event(id: &str, payload: &[u8]) -> RawEvent {
        RawEvent {
            id: id.to_string(),
            source: "test".to_string(),
            event_type: "test.event".to_string(),
            timestamp_ms: chrono::Utc::now().timestamp_millis(),
            payload: payload.to_vec(),
            tags: HashMap::new(),
        }
    }

    fn make_event_with_source(
        id: &str,
        source: &str,
        event_type: &str,
        payload: &[u8],
    ) -> RawEvent {
        RawEvent {
            id: id.to_string(),
            source: source.to_string(),
            event_type: event_type.to_string(),
            timestamp_ms: chrono::Utc::now().timestamp_millis(),
            payload: payload.to_vec(),
            tags: HashMap::new(),
        }
    }

    #[test]
    fn test_get_recent() {
        let cache = temp_cache();
        cache
            .put(&make_event_with_source(
                "a",
                "file:1",
                "file_change",
                b"data1",
            ))
            .unwrap();
        cache
            .put(&make_event_with_source(
                "b",
                "process:2",
                "process_started",
                b"data2",
            ))
            .unwrap();
        cache
            .put(&make_event_with_source(
                "c",
                "file:3",
                "file_change",
                b"data3",
            ))
            .unwrap();

        let recent = cache.get_recent(10).unwrap();
        assert_eq!(recent.len(), 3);

        let recent_limited = cache.get_recent(2).unwrap();
        assert_eq!(recent_limited.len(), 2);
    }

    #[test]
    fn test_search_by_source() {
        let cache = temp_cache();
        cache
            .put(&make_event_with_source(
                "a",
                "file:/tmp/test.txt",
                "file_change",
                b"data",
            ))
            .unwrap();
        cache
            .put(&make_event_with_source(
                "b",
                "process:1234",
                "process_started",
                b"data",
            ))
            .unwrap();
        cache
            .put(&make_event_with_source(
                "c",
                "file:/tmp/other.txt",
                "file_change",
                b"data",
            ))
            .unwrap();

        let file_events = cache.search_by_source("file:", 10).unwrap();
        assert_eq!(file_events.len(), 2);

        let proc_events = cache.search_by_source("process:", 10).unwrap();
        assert_eq!(proc_events.len(), 1);

        let none_events = cache.search_by_source("clipboard:", 10).unwrap();
        assert_eq!(none_events.len(), 0);
    }

    #[test]
    fn test_search_by_type() {
        let cache = temp_cache();
        cache
            .put(&make_event_with_source("a", "src", "file_change", b"data"))
            .unwrap();
        cache
            .put(&make_event_with_source(
                "b",
                "src",
                "process_started",
                b"data",
            ))
            .unwrap();
        cache
            .put(&make_event_with_source("c", "src", "file_change", b"data"))
            .unwrap();
        cache
            .put(&make_event_with_source(
                "d",
                "src",
                "clipboard_change",
                b"data",
            ))
            .unwrap();

        let file_changes = cache.search_by_type("file_change", 10).unwrap();
        assert_eq!(file_changes.len(), 2);

        let clipboard = cache.search_by_type("clipboard_change", 10).unwrap();
        assert_eq!(clipboard.len(), 1);
    }

    #[test]
    fn test_search_by_payload() {
        let cache = temp_cache();
        cache
            .put(&make_event_with_source("a", "src", "test", b"Hello World"))
            .unwrap();
        cache
            .put(&make_event_with_source(
                "b",
                "src",
                "test",
                b"Goodbye World",
            ))
            .unwrap();
        cache
            .put(&make_event_with_source("c", "src", "test", b"Hello Again"))
            .unwrap();

        let hello = cache.search_by_payload("hello", 10).unwrap();
        assert_eq!(hello.len(), 2); // case-insensitive

        let world = cache.search_by_payload("World", 10).unwrap();
        assert_eq!(world.len(), 2);

        let nomatch = cache.search_by_payload("xyz", 10).unwrap();
        assert_eq!(nomatch.len(), 0);
    }

    #[test]
    fn test_search_by_time_range() {
        let cache = temp_cache();

        // Use fixed timestamps
        let mut e1 = make_event_with_source("a", "src", "test", b"data");
        e1.timestamp_ms = 1000;
        let mut e2 = make_event_with_source("b", "src", "test", b"data");
        e2.timestamp_ms = 2000;
        let mut e3 = make_event_with_source("c", "src", "test", b"data");
        e3.timestamp_ms = 3000;

        cache.put(&e1).unwrap();
        cache.put(&e2).unwrap();
        cache.put(&e3).unwrap();

        let range = cache.search_by_time_range(1500, 2500, 10).unwrap();
        assert_eq!(range.len(), 1);
        assert_eq!(range[0].id, "b");

        let all = cache.search_by_time_range(0, 5000, 10).unwrap();
        assert_eq!(all.len(), 3);
    }

    #[test]
    fn test_get_cached_hash_existing() {
        let cache = temp_cache();
        let event = make_event("evt-1", b"test_payload");
        cache.put(&event).unwrap();

        let hash = cache.get_cached_hash("evt-1").unwrap();
        assert!(hash.is_some());
        assert_eq!(hash.unwrap(), Cache::hash_event(&event));
    }

    #[test]
    fn test_get_cached_hash_nonexistent() {
        let cache = temp_cache();
        let hash = cache.get_cached_hash("nonexistent").unwrap();
        assert!(hash.is_none());
    }

    #[test]
    fn test_get_snapshot_existing() {
        let cache = temp_cache();
        let event = make_event("evt-1", b"snapshot_test");
        cache.put(&event).unwrap();

        let snap = cache.get_snapshot("evt-1").unwrap();
        assert!(snap.is_some());
        let snap = snap.unwrap();
        assert_eq!(snap.id, "evt-1");
        assert_eq!(snap.source, event.source);
        assert_eq!(snap.event_type, event.event_type);
        assert_eq!(snap.content_hash, Cache::hash_event(&event));
    }

    #[test]
    fn test_get_snapshot_nonexistent() {
        let cache = temp_cache();
        let snap = cache.get_snapshot("nonexistent").unwrap();
        assert!(snap.is_none());
    }

    #[test]
    fn test_get_snapshot_after_content_change() {
        let cache = temp_cache();

        // Put original event
        let original = make_event("evt-1", b"original");
        cache.put(&original).unwrap();

        // Get snapshot of original
        let snap1 = cache.get_snapshot("evt-1").unwrap().unwrap();
        let hash1 = snap1.content_hash.clone();

        // Put modified event with same ID
        let modified = make_event("evt-1", b"modified_content");
        cache.put(&modified).unwrap();

        // Snapshot should now reflect the modified content
        let snap2 = cache.get_snapshot("evt-1").unwrap().unwrap();
        let hash2 = snap2.content_hash.clone();

        assert_ne!(hash1, hash2);
        assert_eq!(hash2, Cache::hash_event(&modified));
    }
}
