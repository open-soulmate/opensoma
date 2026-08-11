use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::path::Path;
use tracing::{debug, info, warn};

use crate::collector::RawEvent;

/// Metadata for a cached event.
#[derive(Debug, Serialize, Deserialize)]
struct CacheEntry {
    event: RawEvent,
    uploaded: bool,
    created_at: i64,
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

    /// Store an event in the cache.
    pub fn put(&self, event: &RawEvent) -> Result<()> {
        let entry = CacheEntry {
            event: event.clone(),
            uploaded: false,
            created_at: chrono::Utc::now().timestamp_millis(),
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

    /// Remove events older than the given timestamp.
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

    /// Get cache statistics.
    pub fn stats(&self) -> CacheStats {
        let mut total = 0usize;
        let mut uploaded = 0usize;
        let mut pending = 0usize;

        for item in self.db.iter() {
            if let Ok((_, value)) = item {
                if let Ok(entry) = serde_json::from_slice::<CacheEntry>(&value) {
                    total += 1;
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
}
