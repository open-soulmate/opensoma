use serde::{Deserialize, Serialize};
use tracing::debug;

use crate::collector::RawEvent;

/// Conflict resolution strategy.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ConflictStrategy {
    /// Server always wins — discard local if server has newer version.
    ServerWins,
    /// Local always wins — overwrite server with local data.
    LocalWins,
    /// Newest timestamp wins.
    NewestWins,
    /// Merge both (concatenate payloads, union tags).
    Merge,
    /// Keep both versions (create duplicate with conflict marker).
    KeepBoth,
}

impl Default for ConflictStrategy {
    fn default() -> Self {
        ConflictStrategy::NewestWins
    }
}

/// Represents a conflict between a local event and a server-side version.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Conflict {
    pub event_id: String,
    pub local_event: EventSnapshot,
    pub server_event: EventSnapshot,
    pub resolution: Resolution,
}

/// A lightweight snapshot of an event for conflict comparison.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventSnapshot {
    pub id: String,
    pub source: String,
    pub event_type: String,
    pub timestamp_ms: i64,
    pub content_hash: String,
    pub tags: std::collections::HashMap<String, String>,
}

/// How a conflict was resolved.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Resolution {
    /// Used local version.
    UsedLocal,
    /// Used server version.
    UsedServer,
    /// Used the version with the newest timestamp.
    UsedNewest { winner: String },
    /// Merged both versions.
    Merged,
    /// Kept both versions.
    KeptBoth { new_local_id: String },
    /// Pending resolution.
    Pending,
}

/// Conflict detector and resolver.
pub struct ConflictResolver {
    strategy: ConflictStrategy,
    /// History of resolved conflicts for audit trail.
    history: Vec<Conflict>,
}

impl ConflictResolver {
    pub fn new(strategy: ConflictStrategy) -> Self {
        Self {
            strategy,
            history: Vec::new(),
        }
    }

    /// Detect if a local event conflicts with a server-side event.
    /// Returns a Conflict if they have the same ID but different content.
    pub fn detect(&self, local: &RawEvent, server: &EventSnapshot) -> Option<Conflict> {
        if local.id != server.id {
            return None;
        }

        let local_hash = crate::sync::cache::Cache::hash_event(local);

        if local_hash == server.content_hash {
            // Same content — no conflict
            return None;
        }

        Some(Conflict {
            event_id: local.id.clone(),
            local_event: EventSnapshot {
                id: local.id.clone(),
                source: local.source.clone(),
                event_type: local.event_type.clone(),
                timestamp_ms: local.timestamp_ms,
                content_hash: local_hash,
                tags: local.tags.clone(),
            },
            server_event: server.clone(),
            resolution: Resolution::Pending,
        })
    }

    /// Resolve a conflict according to the configured strategy.
    pub fn resolve(&mut self, mut conflict: Conflict) -> ResolvedConflict {
        let resolution = match self.strategy {
            ConflictStrategy::ServerWins => {
                debug!(
                    "Conflict resolved: server wins for event {}",
                    conflict.event_id
                );
                Resolution::UsedServer
            }
            ConflictStrategy::LocalWins => {
                debug!(
                    "Conflict resolved: local wins for event {}",
                    conflict.event_id
                );
                Resolution::UsedLocal
            }
            ConflictStrategy::NewestWins => {
                if conflict.local_event.timestamp_ms >= conflict.server_event.timestamp_ms {
                    debug!(
                        "Conflict resolved: newest (local) for event {}",
                        conflict.event_id
                    );
                    Resolution::UsedNewest {
                        winner: "local".to_string(),
                    }
                } else {
                    debug!(
                        "Conflict resolved: newest (server) for event {}",
                        conflict.event_id
                    );
                    Resolution::UsedNewest {
                        winner: "server".to_string(),
                    }
                }
            }
            ConflictStrategy::Merge => {
                debug!("Conflict resolved: merge for event {}", conflict.event_id);
                Resolution::Merged
            }
            ConflictStrategy::KeepBoth => {
                let new_id = uuid::Uuid::new_v4().to_string();
                debug!(
                    "Conflict resolved: keep both for event {} (new id: {})",
                    conflict.event_id, new_id
                );
                Resolution::KeptBoth {
                    new_local_id: new_id.clone(),
                }
            }
        };

        conflict.resolution = resolution.clone();
        let event_id = conflict.event_id.clone();
        self.history.push(conflict);

        ResolvedConflict {
            event_id,
            resolution,
        }
    }

    /// Get the number of conflicts resolved so far.
    pub fn conflict_count(&self) -> usize {
        self.history.len()
    }

    /// Get recent conflict history.
    pub fn recent_conflicts(&self, limit: usize) -> &[Conflict] {
        let start = self.history.len().saturating_sub(limit);
        &self.history[start..]
    }
}

/// Result of resolving a conflict.
#[derive(Debug, Clone)]
pub struct ResolvedConflict {
    pub event_id: String,
    pub resolution: Resolution,
}

/// Check for conflicts against a batch of server responses.
/// Returns conflicts that need resolution.
pub fn detect_batch_conflicts(
    local_events: &[RawEvent],
    server_snapshots: &[EventSnapshot],
    resolver: &ConflictResolver,
) -> Vec<Conflict> {
    let mut conflicts = Vec::new();

    let server_map: std::collections::HashMap<&str, &EventSnapshot> = server_snapshots
        .iter()
        .map(|s| (s.id.as_str(), s))
        .collect();

    for local in local_events {
        if let Some(server) = server_map.get(local.id.as_str()) {
            if let Some(conflict) = resolver.detect(local, server) {
                conflicts.push(conflict);
            }
        }
    }

    conflicts
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn make_event(id: &str, payload: &[u8]) -> RawEvent {
        RawEvent {
            id: id.into(),
            source: "file:test".into(),
            event_type: "test".into(),
            timestamp_ms: 1000,
            payload: payload.to_vec(),
            tags: HashMap::new(),
        }
    }

    fn make_snapshot(id: &str, hash: &str, ts: i64) -> EventSnapshot {
        EventSnapshot {
            id: id.into(),
            source: "file:test".into(),
            event_type: "test".into(),
            timestamp_ms: ts,
            content_hash: hash.into(),
            tags: HashMap::new(),
        }
    }

    #[test]
    fn test_no_conflict_same_hash() {
        let resolver = ConflictResolver::new(ConflictStrategy::NewestWins);
        let event = make_event("1", b"hello");
        let hash = crate::sync::cache::Cache::hash_event(&event);
        let snapshot = make_snapshot("1", &hash, 1000);

        assert!(resolver.detect(&event, &snapshot).is_none());
    }

    #[test]
    fn test_conflict_different_hash() {
        let resolver = ConflictResolver::new(ConflictStrategy::NewestWins);
        let event = make_event("1", b"hello");
        let snapshot = make_snapshot("1", "different_hash", 1000);

        let conflict = resolver.detect(&event, &snapshot);
        assert!(conflict.is_some());
    }

    #[test]
    fn test_no_conflict_different_id() {
        let resolver = ConflictResolver::new(ConflictStrategy::NewestWins);
        let event = make_event("1", b"hello");
        let snapshot = make_snapshot("2", "any_hash", 1000);

        assert!(resolver.detect(&event, &snapshot).is_none());
    }

    #[test]
    fn test_resolve_server_wins() {
        let mut resolver = ConflictResolver::new(ConflictStrategy::ServerWins);
        let event = make_event("1", b"local");
        let snapshot = make_snapshot("1", "server_hash", 2000);

        let conflict = resolver.detect(&event, &snapshot).unwrap();
        let result = resolver.resolve(conflict);
        assert!(matches!(result.resolution, Resolution::UsedServer));
        assert_eq!(resolver.conflict_count(), 1);
    }

    #[test]
    fn test_resolve_newest_wins_local() {
        let mut resolver = ConflictResolver::new(ConflictStrategy::NewestWins);
        let mut event = make_event("1", b"local");
        event.timestamp_ms = 5000;
        let snapshot = make_snapshot("1", "server_hash", 2000);

        let conflict = resolver.detect(&event, &snapshot).unwrap();
        let result = resolver.resolve(conflict);
        if let Resolution::UsedNewest { winner } = result.resolution {
            assert_eq!(winner, "local");
        } else {
            panic!("Expected UsedNewest resolution");
        }
    }

    #[test]
    fn test_resolve_keep_both() {
        let mut resolver = ConflictResolver::new(ConflictStrategy::KeepBoth);
        let event = make_event("1", b"local");
        let snapshot = make_snapshot("1", "server_hash", 2000);

        let conflict = resolver.detect(&event, &snapshot).unwrap();
        let result = resolver.resolve(conflict);
        if let Resolution::KeptBoth { new_local_id } = result.resolution {
            assert!(!new_local_id.is_empty());
        } else {
            panic!("Expected KeptBoth resolution");
        }
    }
}
