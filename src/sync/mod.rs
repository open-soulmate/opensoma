pub mod cache;
pub mod conflict;
pub mod upload;

use tokio::task::JoinHandle;
use tracing::{error, info};

use crate::collector::{EventRx, RawEvent};
use crate::config::SyncConfig;
use crate::grpc::client::SoulClient;

/// Start the sync engine with an explicit event receiver.
pub fn start_engine_with_rx(
    config: &SyncConfig,
    cache: cache::Cache,
    client: SoulClient,
    rx: EventRx,
) -> JoinHandle<()> {
    let config = config.clone();

    tokio::spawn(async move {
        run_sync_engine(config, cache, client, rx).await;
    })
}

/// Main sync loop: receive events → cache → batch upload.
async fn run_sync_engine(
    config: SyncConfig,
    cache: cache::Cache,
    client: SoulClient,
    mut rx: EventRx,
) {
    info!(
        "Sync engine started — batch_size={}, interval={}s, max_retries={}, streaming={}",
        config.batch_size, config.upload_interval, config.max_retries, config.enable_streaming
    );

    let mut upload_interval =
        tokio::time::interval(std::time::Duration::from_secs(config.upload_interval));
    upload_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    let mut pending: Vec<RawEvent> = Vec::with_capacity(config.batch_size);

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
                    upload_batch(&config, &cache, &client, &mut pending).await;
                }
            }
            // Periodic upload for partial batches
            _ = upload_interval.tick() => {
                if !pending.is_empty() {
                    upload_batch(&config, &cache, &client, &mut pending).await;
                }
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
) {
    let batch: Vec<_> = pending.drain(..).collect();
    let mut backoff = config.retry_backoff_ms;

    for attempt in 0..=config.max_retries {
        match upload::upload_events(client, &batch).await {
            Ok(resp) => {
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
                } else {
                    error!(
                        "Upload failed after {} attempts: {}. Events re-queued to cache.",
                        config.max_retries, e
                    );
                    for event in &batch {
                        let _ = cache.put(event);
                    }
                }
            }
        }
    }
}
