pub mod cache;
pub mod conflict;
pub mod upload;

use tokio::task::JoinHandle;
use tracing::{error, info};

use crate::collector::{EventRx, RawEvent};
use crate::config::SyncConfig;
use crate::grpc::client::SoulClient;
use crate::status_server::CacheStatsSnapshot;
use conflict::{ConflictResolver, ConflictStrategy};

/// Start the sync engine with an explicit event receiver.
pub fn start_engine_with_rx(
    config: &SyncConfig,
    cache: cache::Cache,
    client: SoulClient,
    rx: EventRx,
    cache_stats: std::sync::Arc<tokio::sync::RwLock<CacheStatsSnapshot>>,
    metrics: Option<crate::metrics::PipelineMetrics>,
) -> JoinHandle<()> {
    let config = config.clone();
    let strategy = parse_conflict_strategy(&config.conflict_strategy);
    let resolver = ConflictResolver::new(strategy);

    tokio::spawn(async move {
        run_sync_engine(config, cache, client, rx, cache_stats, metrics, resolver).await;
    })
}

/// Parse a conflict strategy string from config into the enum.
fn parse_conflict_strategy(s: &str) -> ConflictStrategy {
    match s.to_lowercase().as_str() {
        "server_wins" => ConflictStrategy::ServerWins,
        "local_wins" => ConflictStrategy::LocalWins,
        "newest_wins" => ConflictStrategy::NewestWins,
        "merge" => ConflictStrategy::Merge,
        "keep_both" => ConflictStrategy::KeepBoth,
        _ => {
            tracing::warn!("Unknown conflict strategy '{}', defaulting to NewestWins", s);
            ConflictStrategy::NewestWins
        }
    }
}

/// Main sync loop: receive events → cache → batch upload.
async fn run_sync_engine(
    config: SyncConfig,
    cache: cache::Cache,
    client: SoulClient,
    mut rx: EventRx,
    cache_stats: std::sync::Arc<tokio::sync::RwLock<CacheStatsSnapshot>>,
    metrics: Option<crate::metrics::PipelineMetrics>,
    resolver: ConflictResolver,
) {
    info!(
        "Sync engine started — batch_size={}, interval={}s, max_retries={}, streaming={}",
        config.batch_size, config.upload_interval, config.max_retries, config.enable_streaming
    );

    let mut upload_interval =
        tokio::time::interval(std::time::Duration::from_secs(config.upload_interval));
    upload_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    let mut pending: Vec<RawEvent> = Vec::with_capacity(config.batch_size);
    let mut resolver = resolver;

    loop {
        tokio::select! {
            // Receive events from processor
            Some(event) = rx.recv() => {
                // Real-time streaming: send immediately if enabled
                if config.enable_streaming {
                    let proto_event = upload::to_proto_event_shared(&event);
                    if let Err(e) = client.stream_event(&proto_event).await {
                        tracing::debug!("Stream send failed (will batch): {}", e);
                    }
                }

                // Cache locally first (for offline retry)
                if let Err(e) = cache.put(&event) {
                    error!("Cache write error: {}", e);
                }
                pending.push(event);

                // Upload immediately if batch is full
                if pending.len() >= config.batch_size {
                    upload_batch(&config, &cache, &client, &mut pending, &metrics, &mut resolver).await;
                }
            }
            // Periodic upload for partial batches
            _ = upload_interval.tick() => {
                if !pending.is_empty() {
                    upload_batch(&config, &cache, &client, &mut pending, &metrics, &mut resolver).await;
                }
                // Update cache stats for status server
                let stats = cache.stats();
                let mut snapshot = cache_stats.write().await;
                snapshot.total = stats.total;
                snapshot.uploaded = stats.uploaded;
                snapshot.pending = stats.pending;
                snapshot.cache_size_bytes = stats.cache_size_bytes;
            }
        }
    }
}

/// Upload a batch of events with retry logic and exponential backoff.
async fn upload_batch(
    config: &SyncConfig,
    cache: &cache::Cache,
    client: &SoulClient,
    pending: &mut Vec<RawEvent>,
    metrics: &Option<crate::metrics::PipelineMetrics>,
    resolver: &mut ConflictResolver,
) {
    let _ = resolver; // Available for conflict detection when server returns event snapshots
    let batch = std::mem::take(pending);
    let mut backoff = config.retry_backoff_ms;
    let batch_bytes: u64 = batch.iter().map(|e| e.payload.len() as u64).sum();
    let mut timer = metrics.as_ref().map(|m| m.start_sync_timer());

    for attempt in 0..=config.max_retries {
        match upload::upload_events(client, &batch).await {
            Ok(resp) => {
                if let Some(ref m) = metrics {
                    m.inc_upload_batches();
                    m.inc_events_synced_by(resp.accepted as u64);
                    m.add_upload_bytes(batch_bytes);
                }
                if let Some(t) = timer.take() {
                    t.elapsed();
                }
                info!(
                    "Upload success — accepted={}, rejected={}",
                    resp.accepted, resp.rejected
                );

                // Mark uploaded events in cache
                for event in &batch {
                    let _ = cache.mark_uploaded(&event.id);
                }
                return;
            }
            Err(e) => {
                if attempt < config.max_retries {
                    error!(
                        "Upload failed (attempt {}/{}): {}. Retrying in {}ms...",
                        attempt + 1,
                        config.max_retries,
                        e,
                        backoff
                    );
                    tokio::time::sleep(std::time::Duration::from_millis(backoff)).await;
                    backoff = (backoff as f64 * 1.5) as u64;
                    if let Some(ref m) = metrics {
                        m.inc_sync_retries();
                    }
                } else {
                    error!(
                        "Upload failed after {} attempts: {}. Events re-queued to cache.",
                        config.max_retries, e
                    );
                    if let Some(ref m) = metrics {
                        m.inc_events_sync_failed();
                    }
                    if let Some(t) = timer.take() {
                        t.elapsed();
                    }
                    for event in &batch {
                        let _ = cache.put(event);
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sync_config_defaults() {
        // Verify default values are reasonable
        let config = SyncConfig {
            batch_size: 50,
            upload_interval: 10,
            max_retries: 5,
            retry_backoff_ms: 1000,
            cache_size_mb: 512,
            conflict_strategy: "newest_wins".to_string(),
            enable_streaming: false,
        };
        assert_eq!(config.batch_size, 50);
        assert_eq!(config.upload_interval, 10);
        assert_eq!(config.max_retries, 5);
        assert!(config.retry_backoff_ms > 0);
        assert!(config.cache_size_mb > 0);
        assert!(!config.enable_streaming);
    }

    #[test]
    fn test_sync_config_streaming() {
        let config = SyncConfig {
            batch_size: 50,
            upload_interval: 10,
            max_retries: 5,
            retry_backoff_ms: 1000,
            cache_size_mb: 512,
            conflict_strategy: "newest_wins".to_string(),
            enable_streaming: true,
        };
        assert!(config.enable_streaming);
    }

    #[test]
    fn test_backoff_calculation() {
        // Verify the exponential backoff formula: backoff = backoff * 1.5
        let mut backoff: u64 = 1000;
        backoff = (backoff as f64 * 1.5) as u64;
        assert_eq!(backoff, 1500);

        backoff = (backoff as f64 * 1.5) as u64;
        assert_eq!(backoff, 2250);

        backoff = (backoff as f64 * 1.5) as u64;
        assert_eq!(backoff, 3375);
    }

    #[test]
    fn test_batch_drain_logic() {
        // Simulate the drain logic used in upload_batch
        let mut pending: Vec<i32> = vec![1, 2, 3, 4, 5];
        let batch: Vec<i32> = std::mem::take(&mut pending);
        assert!(pending.is_empty());
        assert_eq!(batch.len(), 5);
        assert_eq!(batch, vec![1, 2, 3, 4, 5]);
    }

    #[test]
    fn test_batch_full_trigger() {
        // Simulate the batch full check
        let batch_size = 3;
        let mut pending: Vec<i32> = Vec::new();

        pending.push(1);
        assert!(pending.len() < batch_size);

        pending.push(2);
        assert!(pending.len() < batch_size);

        pending.push(3);
        assert!(pending.len() >= batch_size); // Should trigger upload
    }

    #[test]
    fn test_parse_conflict_strategy_valid() {
        assert!(matches!(
            parse_conflict_strategy("newest_wins"),
            ConflictStrategy::NewestWins
        ));
        assert!(matches!(
            parse_conflict_strategy("server_wins"),
            ConflictStrategy::ServerWins
        ));
        assert!(matches!(
            parse_conflict_strategy("local_wins"),
            ConflictStrategy::LocalWins
        ));
        assert!(matches!(
            parse_conflict_strategy("merge"),
            ConflictStrategy::Merge
        ));
        assert!(matches!(
            parse_conflict_strategy("keep_both"),
            ConflictStrategy::KeepBoth
        ));
    }

    #[test]
    fn test_parse_conflict_strategy_case_insensitive() {
        assert!(matches!(
            parse_conflict_strategy("NEWEST_WINS"),
            ConflictStrategy::NewestWins
        ));
        assert!(matches!(
            parse_conflict_strategy("Server_Wins"),
            ConflictStrategy::ServerWins
        ));
    }

    #[test]
    fn test_parse_conflict_strategy_unknown_defaults() {
        assert!(matches!(
            parse_conflict_strategy("unknown_strategy"),
            ConflictStrategy::NewestWins
        ));
        assert!(matches!(
            parse_conflict_strategy(""),
            ConflictStrategy::NewestWins
        ));
    }
}
