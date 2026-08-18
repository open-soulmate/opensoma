pub mod classify;
pub mod dedup;
pub mod enrich;
pub mod normalize;

use tokio::task::JoinHandle;
use tracing::{debug, error, info};

use crate::collector::{EventRx, EventTx};
use crate::config::ProcessorConfig;

/// Start the processing pipeline: raw events → normalize → classify → enrich → dedup → output.
/// Returns the output sender (for sync engine to consume) and the pipeline handle.
pub fn start_pipeline(
    raw_rx: EventRx,
    output_tx: EventTx,
    config: &ProcessorConfig,
) -> JoinHandle<()> {
    let config = config.clone();

    tokio::spawn(async move {
        run_pipeline(raw_rx, output_tx, config).await;
    })
}

/// Run the processing pipeline.
async fn run_pipeline(mut input: EventRx, output: EventTx, config: ProcessorConfig) {
    let dedup = dedup::Deduplicator::new(config.dedup_window_secs);
    info!(
        "Processor pipeline started — normalize={}, classify={}, enrich={}, dedup_window={}s",
        config.normalize_timestamps,
        config.enable_classify,
        config.enable_enrich,
        config.dedup_window_secs
    );

    while let Some(mut event) = input.recv().await {
        // Step 1: Normalize
        normalize::normalize_event(&mut event, &config);

        // Step 2: Size check
        if event.payload.len() > config.max_event_size {
            debug!("Dropping oversized event: {} bytes", event.payload.len());
            continue;
        }

        // Step 3: Classify (if enabled)
        if config.enable_classify {
            let classification = classify::classify_event(&event);
            classify::apply_classification(&mut event, &classification);
        }

        // Step 4: Enrich (if enabled)
        if config.enable_enrich {
            let enrichment = enrich::enrich_event(&event);
            enrich::apply_enrichment(&mut event, &enrichment);
        }

        // Step 5: Dedup check
        if dedup.is_duplicate(&event).await {
            debug!("Dropping duplicate event: {}", event.id);
            continue;
        }

        // Step 6: Forward to output
        if let Err(e) = output.send(event).await {
            error!("Pipeline output send error: {}", e);
            break;
        }
    }

    info!("Processor pipeline stopped.");
}
