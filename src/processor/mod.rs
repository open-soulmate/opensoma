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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::collector::RawEvent;
    use std::collections::HashMap;

    fn make_test_event(id: &str, event_type: &str, source: &str, payload: &str) -> RawEvent {
        RawEvent {
            id: id.to_string(),
            source: source.to_string(),
            event_type: event_type.to_string(),
            timestamp_ms: 1000,
            payload: payload.as_bytes().to_vec(),
            tags: HashMap::new(),
        }
    }

    #[tokio::test]
    async fn test_pipeline_basic_flow() {
        let (input_tx, input_rx) = tokio::sync::mpsc::channel(64);
        let (output_tx, mut output_rx) = tokio::sync::mpsc::channel(64);

        let config = ProcessorConfig {
            normalize_timestamps: true,
            max_event_size: 1_048_576,
            dedup_window_secs: 60,
            enable_classify: true,
            enable_enrich: true,
        };

        let handle = start_pipeline(input_rx, output_tx, &config);

        // Send a test event
        let event = make_test_event("evt-1", "file_change", "file:/tmp/test.json", r#"{"key":"value"}"#);
        input_tx.send(event).await.unwrap();

        // Receive the processed event
        let processed = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            output_rx.recv(),
        )
        .await
        .expect("timeout waiting for processed event")
        .expect("channel closed");

        assert_eq!(processed.id, "evt-1");
        // Should have classification tags
        assert!(processed.tags.contains_key("class_category"));
        assert!(processed.tags.contains_key("class_type"));
        // Should have enrichment tags
        assert!(processed.tags.contains_key("word_count"));

        handle.abort();
    }

    #[tokio::test]
    async fn test_pipeline_dedup() {
        let (input_tx, input_rx) = tokio::sync::mpsc::channel(64);
        let (output_tx, mut output_rx) = tokio::sync::mpsc::channel(64);

        let config = ProcessorConfig {
            normalize_timestamps: true,
            max_event_size: 1_048_576,
            dedup_window_secs: 300,
            enable_classify: false,
            enable_enrich: false,
        };

        let handle = start_pipeline(input_rx, output_tx, &config);

        // Send the same event twice
        let event1 = make_test_event("evt-dup", "file_change", "file:/tmp/test.txt", "hello");
        let event2 = make_test_event("evt-dup", "file_change", "file:/tmp/test.txt", "hello");

        input_tx.send(event1).await.unwrap();
        input_tx.send(event2).await.unwrap();

        // Should receive only the first event (second is deduped)
        let first = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            output_rx.recv(),
        )
        .await
        .expect("timeout")
        .expect("channel closed");
        assert_eq!(first.id, "evt-dup");

        // Second event should not come through (deduped)
        let second = tokio::time::timeout(
            std::time::Duration::from_millis(500),
            output_rx.recv(),
        )
        .await;
        assert!(second.is_err()); // timeout = no event = deduped

        handle.abort();
    }

    #[tokio::test]
    async fn test_pipeline_oversized_event_dropped() {
        let (input_tx, input_rx) = tokio::sync::mpsc::channel(64);
        let (output_tx, mut output_rx) = tokio::sync::mpsc::channel(64);

        let config = ProcessorConfig {
            normalize_timestamps: true,
            max_event_size: 10, // Very small max size
            dedup_window_secs: 60,
            enable_classify: false,
            enable_enrich: false,
        };

        let handle = start_pipeline(input_rx, output_tx, &config);

        // Send an oversized event
        let event = make_test_event("evt-big", "test", "test", "this payload is definitely larger than 10 bytes");
        input_tx.send(event).await.unwrap();

        // Should not receive anything (event was dropped)
        let result = tokio::time::timeout(
            std::time::Duration::from_millis(500),
            output_rx.recv(),
        )
        .await;
        assert!(result.is_err()); // timeout = dropped

        handle.abort();
    }

    #[tokio::test]
    async fn test_pipeline_classify_and_enrich() {
        let (input_tx, input_rx) = tokio::sync::mpsc::channel(64);
        let (output_tx, mut output_rx) = tokio::sync::mpsc::channel(64);

        let config = ProcessorConfig {
            normalize_timestamps: true,
            max_event_size: 1_048_576,
            dedup_window_secs: 60,
            enable_classify: true,
            enable_enrich: true,
        };

        let handle = start_pipeline(input_rx, output_tx, &config);

        // Send an event with detectable content
        let event = make_test_event(
            "evt-cls",
            "process_started",
            "process:1234",
            "Error: connection to 192.168.1.1 failed. Visit https://example.com for details.",
        );
        input_tx.send(event).await.unwrap();

        let processed = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            output_rx.recv(),
        )
        .await
        .expect("timeout")
        .expect("channel closed");

        // Classification
        assert_eq!(processed.tags.get("class_category").unwrap(), "process");
        assert_eq!(processed.tags.get("class_type").unwrap(), "system");

        // Enrichment should find entities
        assert!(processed.tags.contains_key("word_count"));

        handle.abort();
    }

    #[tokio::test]
    async fn test_pipeline_multiple_events_ordering() {
        let (input_tx, input_rx) = tokio::sync::mpsc::channel(64);
        let (output_tx, mut output_rx) = tokio::sync::mpsc::channel(64);

        let config = ProcessorConfig {
            normalize_timestamps: true,
            max_event_size: 1_048_576,
            dedup_window_secs: 60,
            enable_classify: false,
            enable_enrich: false,
        };

        let handle = start_pipeline(input_rx, output_tx, &config);

        // Send 5 distinct events
        for i in 0..5 {
            let event = make_test_event(
                &format!("evt-{}", i),
                "test",
                "test",
                &format!("payload {}", i),
            );
            input_tx.send(event).await.unwrap();
        }

        // Receive all 5 in order
        for i in 0..5 {
            let received = tokio::time::timeout(
                std::time::Duration::from_secs(2),
                output_rx.recv(),
            )
            .await
            .expect("timeout")
            .expect("channel closed");
            assert_eq!(received.id, format!("evt-{}", i));
        }

        handle.abort();
    }
}
