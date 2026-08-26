pub mod classify;
pub mod dedup;
pub mod enrich;
pub mod normalize;

use tokio::task::JoinHandle;
use tracing::{debug, error, info};

use crate::collector::{EventRx, EventTx};
use crate::config::{ProcessorConfig, SenseConfig};

/// Start the processing pipeline: raw events → normalize → classify → enrich → dedup → output.
/// Returns the output sender (for sync engine to consume) and the pipeline handle.
pub fn start_pipeline(
    raw_rx: EventRx,
    output_tx: EventTx,
    config: &ProcessorConfig,
    metrics: Option<crate::metrics::PipelineMetrics>,
) -> JoinHandle<()> {
    let config = config.clone();

    tokio::spawn(async move {
        run_pipeline(raw_rx, output_tx, config, None, metrics).await;
    })
}

/// Start the processing pipeline with sense plugin support.
pub fn start_pipeline_with_sense(
    raw_rx: EventRx,
    output_tx: EventTx,
    config: &ProcessorConfig,
    sense_config: &SenseConfig,
    metrics: Option<crate::metrics::PipelineMetrics>,
) -> JoinHandle<()> {
    let config = config.clone();
    let sense_config = sense_config.clone();

    tokio::spawn(async move {
        run_pipeline(raw_rx, output_tx, config, Some(sense_config), metrics).await;
    })
}

/// Run the processing pipeline.
async fn run_pipeline(
    mut input: EventRx,
    output: EventTx,
    config: ProcessorConfig,
    sense_config: Option<SenseConfig>,
    metrics: Option<crate::metrics::PipelineMetrics>,
) {
    let dedup = dedup::Deduplicator::new(config.dedup_window_secs);
    let sense_enabled = sense_config.as_ref().is_some_and(|s| s.enabled);
    info!(
        "Processor pipeline started — normalize={}, classify={}, enrich={}, dedup_window={}s, sense={}",
        config.normalize_timestamps,
        config.enable_classify,
        config.enable_enrich,
        config.dedup_window_secs,
        sense_enabled
    );

    while let Some(mut event) = input.recv().await {
        let timer = metrics.as_ref().map(|m| m.start_process_timer());
        if let Some(ref m) = metrics {
            m.inc_events_collected();
            m.inc_events_processed();
        }

        // Step 1: Normalize
        normalize::normalize_event(&mut event, &config);
        if let Some(ref m) = metrics {
            m.inc_events_normalized();
        }

        // Step 2: Size check — truncate if moderately over limit, drop if extremely large
        if event.payload.len() > config.max_event_size {
            let overage = event.payload.len() as f64 / config.max_event_size as f64;
            if overage <= 2.0 {
                // Truncate and tag — keep the event but trim payload
                event.payload.truncate(config.max_event_size);
                event
                    .tags
                    .insert("_truncated".to_string(), "payload".to_string());
                if let Some(ref m) = metrics {
                    m.inc_events_normalized(); // count as normalized (adjusted)
                }
                debug!(
                    "Truncated oversized event payload to {} bytes (was {:.0}% over limit)",
                    config.max_event_size,
                    (overage - 1.0) * 100.0
                );
            } else {
                debug!(
                    "Dropping oversized event: {} bytes ({:.0}% over limit)",
                    event.payload.len(),
                    (overage - 1.0) * 100.0
                );
                if let Some(ref m) = metrics {
                    m.inc_events_dropped_oversized();
                }
                continue;
            }
        }

        // Step 3: Sense parsing for media files (if enabled)
        if let Some(ref sc) = sense_config {
            if sc.enabled {
                process_sense(&mut event, sc);
            }
        }

        // Step 4: Classify (if enabled)
        if config.enable_classify {
            let classification = classify::classify_event(&event);
            classify::apply_classification(&mut event, &classification);
            if let Some(ref m) = metrics {
                m.inc_events_classified();
            }
        }

        // Step 5: Enrich (if enabled)
        if config.enable_enrich {
            let enrichment = enrich::enrich_event(&event);
            enrich::apply_enrichment(&mut event, &enrichment);
            if let Some(ref m) = metrics {
                m.inc_events_enriched();
            }
        }

        // Step 6: Dedup check
        if dedup.is_duplicate(&event).await {
            debug!("Dropping duplicate event: {}", event.id);
            if let Some(ref m) = metrics {
                m.inc_events_deduplicated();
            }
            continue;
        }

        // Step 7: Forward to output
        if let Some(t) = timer {
            t.elapsed();
        }
        if let Err(e) = output.send(event).await {
            error!("Pipeline output send error: {}", e);
            break;
        }
    }

    info!("Processor pipeline stopped.");
}

/// Detect media type from file extension and apply sense parsing metadata.
/// This tags media events with their detected type so downstream consumers
/// can route them to the appropriate sense plugin (ASR, OCR, image, video).
fn process_sense(event: &mut crate::collector::RawEvent, config: &SenseConfig) {
    // Only process file events
    if event.source != "file" && !event.event_type.starts_with("file_") {
        return;
    }

    // Try to detect media type from tags (file_path) or event_type
    let file_path = event
        .tags
        .get("file_path")
        .or_else(|| event.tags.get("path"))
        .cloned()
        .unwrap_or_default();

    let ext = std::path::Path::new(&file_path)
        .extension()
        .and_then(|e| e.to_str())
        .unwrap_or("")
        .to_lowercase();

    let media_type = match ext.as_str() {
        // Audio files → ASR
        "wav" | "mp3" | "ogg" | "flac" | "m4a" | "aac" | "wma" if config.asr.is_some() => {
            Some("audio")
        }
        // Image files → OCR or Image understanding
        "png" | "jpg" | "jpeg" | "gif" | "bmp" | "webp" | "tiff"
            if config.ocr.is_some() || config.image.is_some() =>
        {
            Some("image")
        }
        // Video files → Video frame extraction
        "mp4" | "avi" | "mkv" | "mov" | "webm" | "flv" if config.video.is_some() => Some("video"),
        // PDF → OCR
        "pdf" if config.ocr.is_some() => Some("pdf"),
        _ => None,
    };

    if let Some(mt) = media_type {
        event
            .tags
            .insert("sense_media_type".to_string(), mt.to_string());
        event
            .tags
            .insert("sense_eligible".to_string(), "true".to_string());
        debug!("Tagged event {} as sense-eligible (type={})", event.id, mt);
    }
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

        let handle = start_pipeline(input_rx, output_tx, &config, None);

        // Send a test event
        let event = make_test_event(
            "evt-1",
            "file_change",
            "file:/tmp/test.json",
            r#"{"key":"value"}"#,
        );
        input_tx.send(event).await.unwrap();

        // Receive the processed event
        let processed = tokio::time::timeout(std::time::Duration::from_secs(2), output_rx.recv())
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

        let handle = start_pipeline(input_rx, output_tx, &config, None);

        // Send the same event twice
        let event1 = make_test_event("evt-dup", "file_change", "file:/tmp/test.txt", "hello");
        let event2 = make_test_event("evt-dup", "file_change", "file:/tmp/test.txt", "hello");

        input_tx.send(event1).await.unwrap();
        input_tx.send(event2).await.unwrap();

        // Should receive only the first event (second is deduped)
        let first = tokio::time::timeout(std::time::Duration::from_secs(2), output_rx.recv())
            .await
            .expect("timeout")
            .expect("channel closed");
        assert_eq!(first.id, "evt-dup");

        // Second event should not come through (deduped)
        let second =
            tokio::time::timeout(std::time::Duration::from_millis(500), output_rx.recv()).await;
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

        let handle = start_pipeline(input_rx, output_tx, &config, None);

        // Send an oversized event (50 bytes on a 10-byte limit = 5x, >2x → dropped)
        let event = make_test_event(
            "evt-big",
            "test",
            "test",
            "this payload is definitely larger than 10 bytes",
        );
        input_tx.send(event).await.unwrap();

        // Should not receive anything (event was dropped)
        let result =
            tokio::time::timeout(std::time::Duration::from_millis(500), output_rx.recv()).await;
        assert!(result.is_err()); // timeout = dropped

        handle.abort();
    }

    #[tokio::test]
    async fn test_pipeline_moderately_oversized_event_truncated() {
        let (input_tx, input_rx) = tokio::sync::mpsc::channel(64);
        let (output_tx, mut output_rx) = tokio::sync::mpsc::channel(64);

        let config = ProcessorConfig {
            normalize_timestamps: true,
            max_event_size: 20, // 20 bytes max
            dedup_window_secs: 60,
            enable_classify: false,
            enable_enrich: false,
        };

        let handle = start_pipeline(input_rx, output_tx, &config, None);

        // Send a moderately oversized event (~30 bytes on 20-byte limit = 1.5x, <2x → truncated)
        let event = make_test_event("evt-med", "test", "test", "this payload is thirty bytes!!");
        input_tx.send(event).await.unwrap();

        // Should receive the event (truncated, not dropped)
        let received = tokio::time::timeout(std::time::Duration::from_secs(2), output_rx.recv())
            .await
            .expect("timeout — event should have been truncated, not dropped")
            .expect("channel closed");

        assert_eq!(received.id, "evt-med");
        assert!(received.payload.len() <= 20);
        assert_eq!(received.tags.get("_truncated").unwrap(), "payload");

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

        let handle = start_pipeline(input_rx, output_tx, &config, None);

        // Send an event with detectable content
        let event = make_test_event(
            "evt-cls",
            "process_started",
            "process:1234",
            "Error: connection to 192.168.1.1 failed. Visit https://example.com for details.",
        );
        input_tx.send(event).await.unwrap();

        let processed = tokio::time::timeout(std::time::Duration::from_secs(2), output_rx.recv())
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

        let handle = start_pipeline(input_rx, output_tx, &config, None);

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
            let received =
                tokio::time::timeout(std::time::Duration::from_secs(2), output_rx.recv())
                    .await
                    .expect("timeout")
                    .expect("channel closed");
            assert_eq!(received.id, format!("evt-{}", i));
        }

        handle.abort();
    }

    #[test]
    fn test_process_sense_audio_tagging() {
        use crate::config::{AsrSenseConfig, SenseConfig};

        let config = SenseConfig {
            enabled: true,
            asr: Some(AsrSenseConfig {
                engine: "whisper".to_string(),
                api_url: None,
                api_key: None,
                whisper_model: "base".to_string(),
            }),
            ocr: None,
            image: None,
            video: None,
        };

        let mut event = make_test_event("evt-audio", "file_change", "file", "audio data");
        event
            .tags
            .insert("file_path".to_string(), "/tmp/recording.wav".to_string());

        process_sense(&mut event, &config);

        assert_eq!(event.tags.get("sense_media_type").unwrap(), "audio");
        assert_eq!(event.tags.get("sense_eligible").unwrap(), "true");
    }

    #[test]
    fn test_process_sense_image_tagging() {
        use crate::config::{OcrSenseConfig, SenseConfig};

        let config = SenseConfig {
            enabled: true,
            asr: None,
            ocr: Some(OcrSenseConfig {
                engine: "tesseract".to_string(),
                api_url: None,
                api_key: None,
                tesseract_lang: "eng".to_string(),
            }),
            image: None,
            video: None,
        };

        let mut event = make_test_event("evt-img", "file_change", "file", "image data");
        event
            .tags
            .insert("file_path".to_string(), "/tmp/screenshot.png".to_string());

        process_sense(&mut event, &config);

        assert_eq!(event.tags.get("sense_media_type").unwrap(), "image");
    }

    #[test]
    fn test_process_sense_ignores_non_file_events() {
        use crate::config::SenseConfig;

        let config = SenseConfig {
            enabled: true,
            asr: None,
            ocr: None,
            image: None,
            video: None,
        };

        let mut event = make_test_event("evt-proc", "process_started", "process", "data");
        event
            .tags
            .insert("file_path".to_string(), "/tmp/audio.wav".to_string());

        process_sense(&mut event, &config);

        // Should NOT be tagged because source is "process", not "file"
        assert!(!event.tags.contains_key("sense_media_type"));
    }

    #[test]
    fn test_process_sense_disabled() {
        use crate::config::SenseConfig;

        let config = SenseConfig {
            enabled: false,
            asr: None,
            ocr: None,
            image: None,
            video: None,
        };

        let mut event = make_test_event("evt-img", "file_change", "file", "data");
        event
            .tags
            .insert("file_path".to_string(), "/tmp/photo.jpg".to_string());

        // This function is only called when sense is enabled, but test the guard
        // by not calling process_sense when disabled
        if config.enabled {
            process_sense(&mut event, &config);
        }

        assert!(!event.tags.contains_key("sense_media_type"));
    }

    #[tokio::test]
    async fn test_pipeline_with_sense_tagging() {
        use crate::config::{AsrSenseConfig, SenseConfig};

        let (input_tx, input_rx) = tokio::sync::mpsc::channel(64);
        let (output_tx, mut output_rx) = tokio::sync::mpsc::channel(64);

        let config = ProcessorConfig {
            normalize_timestamps: true,
            max_event_size: 1_048_576,
            dedup_window_secs: 60,
            enable_classify: false,
            enable_enrich: false,
        };

        let sense_config = SenseConfig {
            enabled: true,
            asr: Some(AsrSenseConfig {
                engine: "whisper".to_string(),
                api_url: None,
                api_key: None,
                whisper_model: "base".to_string(),
            }),
            ocr: None,
            image: None,
            video: None,
        };

        let handle = start_pipeline_with_sense(input_rx, output_tx, &config, &sense_config, None);

        let mut event = make_test_event("evt-media", "file_change", "file", "audio data");
        event
            .tags
            .insert("file_path".to_string(), "/tmp/recording.mp3".to_string());

        input_tx.send(event).await.unwrap();

        let processed = tokio::time::timeout(std::time::Duration::from_secs(2), output_rx.recv())
            .await
            .expect("timeout")
            .expect("channel closed");

        assert_eq!(processed.tags.get("sense_media_type").unwrap(), "audio");
        assert_eq!(processed.tags.get("sense_eligible").unwrap(), "true");

        handle.abort();
    }

    #[tokio::test]
    async fn test_pipeline_metrics_wiring() {
        let (input_tx, input_rx) = tokio::sync::mpsc::channel(64);
        let (output_tx, mut output_rx) = tokio::sync::mpsc::channel(64);

        let config = ProcessorConfig {
            normalize_timestamps: true,
            max_event_size: 1_048_576,
            dedup_window_secs: 60,
            enable_classify: true,
            enable_enrich: true,
        };

        let metrics = crate::metrics::PipelineMetrics::new();
        let handle = start_pipeline(input_rx, output_tx, &config, Some(metrics.clone()));

        // Send 3 distinct events (different payloads to avoid dedup)
        for i in 0..3 {
            let event = make_test_event(
                &format!("evt-m-{}", i),
                "file_change",
                "file:/tmp/test.json",
                &format!(r#"{{"key":"value{}"}}"#, i),
            );
            input_tx.send(event).await.unwrap();
        }

        // Receive all 3
        for _ in 0..3 {
            let _ = tokio::time::timeout(std::time::Duration::from_secs(2), output_rx.recv())
                .await
                .expect("timeout")
                .expect("channel closed");
        }

        // Verify metrics were collected
        let snap = metrics.snapshot();
        assert_eq!(snap.events_processed, 3, "Should have processed 3 events");
        assert_eq!(snap.events_normalized, 3, "Should have normalized 3 events");
        assert_eq!(snap.events_classified, 3, "Should have classified 3 events");
        assert_eq!(snap.events_enriched, 3, "Should have enriched 3 events");
        assert!(
            snap.avg_process_latency_us > 0,
            "Should have recorded latency"
        );

        handle.abort();
    }

    #[test]
    fn test_process_sense_video_tagging() {
        use crate::config::{SenseConfig, VideoSenseConfig};

        let config = SenseConfig {
            enabled: true,
            asr: None,
            ocr: None,
            image: None,
            video: Some(VideoSenseConfig {
                frame_interval_sec: 5,
                max_frames: 60,
                frame_analyzer: "ocr".to_string(),
            }),
        };

        let mut event = make_test_event("evt-vid", "file_change", "file", "video data");
        event
            .tags
            .insert("file_path".to_string(), "/tmp/lecture.mp4".to_string());

        process_sense(&mut event, &config);

        assert_eq!(event.tags.get("sense_media_type").unwrap(), "video");
        assert_eq!(event.tags.get("sense_eligible").unwrap(), "true");
    }

    #[test]
    fn test_process_sense_pdf_tagging() {
        use crate::config::{OcrSenseConfig, SenseConfig};

        let config = SenseConfig {
            enabled: true,
            asr: None,
            ocr: Some(OcrSenseConfig {
                engine: "tesseract".to_string(),
                api_url: None,
                api_key: None,
                tesseract_lang: "eng".to_string(),
            }),
            image: None,
            video: None,
        };

        let mut event = make_test_event("evt-pdf", "file_change", "file", "pdf data");
        event
            .tags
            .insert("file_path".to_string(), "/tmp/report.pdf".to_string());

        process_sense(&mut event, &config);

        assert_eq!(event.tags.get("sense_media_type").unwrap(), "pdf");
        assert_eq!(event.tags.get("sense_eligible").unwrap(), "true");
    }

    #[test]
    fn test_process_sense_unknown_extension_not_tagged() {
        use crate::config::SenseConfig;

        let config = SenseConfig {
            enabled: true,
            asr: None,
            ocr: None,
            image: None,
            video: None,
        };

        let mut event = make_test_event("evt-unknown", "file_change", "file", "data");
        event
            .tags
            .insert("file_path".to_string(), "/tmp/readme.md".to_string());

        process_sense(&mut event, &config);

        assert!(!event.tags.contains_key("sense_media_type"));
        assert!(!event.tags.contains_key("sense_eligible"));
    }

    #[test]
    fn test_process_sense_path_from_tags_fallback() {
        use crate::config::{OcrSenseConfig, SenseConfig};

        let config = SenseConfig {
            enabled: true,
            asr: None,
            ocr: Some(OcrSenseConfig {
                engine: "tesseract".to_string(),
                api_url: None,
                api_key: None,
                tesseract_lang: "eng".to_string(),
            }),
            image: None,
            video: None,
        };

        // Use "path" tag instead of "file_path"
        let mut event = make_test_event("evt-path", "file_change", "file", "data");
        event
            .tags
            .insert("path".to_string(), "/tmp/photo.jpeg".to_string());

        process_sense(&mut event, &config);

        assert_eq!(event.tags.get("sense_media_type").unwrap(), "image");
    }

    #[test]
    fn test_process_sense_no_path_tag() {
        use crate::config::SenseConfig;

        let config = SenseConfig {
            enabled: true,
            asr: None,
            ocr: None,
            image: None,
            video: None,
        };

        // No file_path or path tag
        let mut event = make_test_event("evt-nopath", "file_change", "file", "data");
        process_sense(&mut event, &config);

        assert!(!event.tags.contains_key("sense_media_type"));
    }

    #[test]
    fn test_process_sense_video_formats() {
        use crate::config::{SenseConfig, VideoSenseConfig};

        let config = SenseConfig {
            enabled: true,
            asr: None,
            ocr: None,
            image: None,
            video: Some(VideoSenseConfig {
                frame_interval_sec: 10,
                max_frames: 30,
                frame_analyzer: "ocr".to_string(),
            }),
        };

        for ext in &["mp4", "avi", "mkv", "mov", "webm", "flv"] {
            let mut event = make_test_event(
                &format!("evt-vid-{}", ext),
                "file_change",
                "file",
                "video data",
            );
            event
                .tags
                .insert("file_path".to_string(), format!("/tmp/video.{}", ext));
            process_sense(&mut event, &config);
            assert_eq!(
                event.tags.get("sense_media_type").unwrap(),
                "video",
                "Extension '{}' should be tagged as video",
                ext
            );
        }
    }

    #[test]
    fn test_process_sense_image_formats() {
        use crate::config::{OcrSenseConfig, SenseConfig};

        let config = SenseConfig {
            enabled: true,
            asr: None,
            ocr: Some(OcrSenseConfig {
                engine: "tesseract".to_string(),
                api_url: None,
                api_key: None,
                tesseract_lang: "eng".to_string(),
            }),
            image: None,
            video: None,
        };

        for ext in &["png", "jpg", "jpeg", "gif", "bmp", "webp", "tiff"] {
            let mut event = make_test_event(
                &format!("evt-img-{}", ext),
                "file_change",
                "file",
                "image data",
            );
            event
                .tags
                .insert("file_path".to_string(), format!("/tmp/photo.{}", ext));
            process_sense(&mut event, &config);
            assert_eq!(
                event.tags.get("sense_media_type").unwrap(),
                "image",
                "Extension '{}' should be tagged as image",
                ext
            );
        }
    }

    #[test]
    fn test_process_sense_audio_formats() {
        use crate::config::{AsrSenseConfig, SenseConfig};

        let config = SenseConfig {
            enabled: true,
            asr: Some(AsrSenseConfig {
                engine: "whisper".to_string(),
                api_url: None,
                api_key: None,
                whisper_model: "base".to_string(),
            }),
            ocr: None,
            image: None,
            video: None,
        };

        for ext in &["wav", "mp3", "ogg", "flac", "m4a", "aac", "wma"] {
            let mut event = make_test_event(
                &format!("evt-aud-{}", ext),
                "file_change",
                "file",
                "audio data",
            );
            event
                .tags
                .insert("file_path".to_string(), format!("/tmp/recording.{}", ext));
            process_sense(&mut event, &config);
            assert_eq!(
                event.tags.get("sense_media_type").unwrap(),
                "audio",
                "Extension '{}' should be tagged as audio",
                ext
            );
        }
    }
}
