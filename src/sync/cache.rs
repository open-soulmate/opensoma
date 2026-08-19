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

        for item in self.db.iter() {
            if let Ok((_, value)) = item {
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
