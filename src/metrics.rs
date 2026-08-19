//! Internal metrics collection for the OpenSoma pipeline.
//!
//! Provides atomic counters and histograms for tracking events through
//! the collector → processor → sync pipeline. Complements the Prometheus
//! endpoint in status_server with more granular pipeline-internal metrics.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;
use serde::Serialize;

/// Atomic metrics collector for the OpenSoma pipeline.
///
/// All counters use relaxed atomics for minimal overhead on the hot path.
/// Clone is cheap (Arc-based sharing).
#[derive(Clone)]
pub struct PipelineMetrics {
    inner: Arc<MetricsInner>,
}

struct MetricsInner {
    // Collector counters
    pub events_collected: AtomicU64,
    pub events_dropped_collector: AtomicU64,
    pub collector_errors: AtomicU64,

    // Processor counters
    pub events_processed: AtomicU64,
    pub events_normalized: AtomicU64,
    pub events_classified: AtomicU64,
    pub events_enriched: AtomicU64,
    pub events_deduplicated: AtomicU64,
    pub events_dropped_oversized: AtomicU64,
    pub processor_errors: AtomicU64,

    // Sync counters
    pub events_synced: AtomicU64,
    pub events_sync_failed: AtomicU64,
    pub sync_retries: AtomicU64,
    pub upload_batches: AtomicU64,
    pub upload_bytes: AtomicU64,

    // Connector counters (per-connector)
    // We use a simple approach: store counts in a Vec with known indices
    pub connector_events: [AtomicU64; 11],
    pub connector_errors: [AtomicU64; 11],

    // Latency tracking (microseconds)
    pub process_latency_sum_us: AtomicU64,
    pub process_latency_count: AtomicU64,
    pub sync_latency_sum_us: AtomicU64,
    pub sync_latency_count: AtomicU64,

    // Cache stats
    pub cache_hits: AtomicU64,
    pub cache_misses: AtomicU64,
    pub cache_evictions: AtomicU64,

    // Conflict resolution
    pub conflicts_detected: AtomicU64,
    pub conflicts_resolved: AtomicU64,
}

/// Snapshot of all metrics for API responses.
#[derive(Debug, Clone, Serialize, Default)]
pub struct MetricsSnapshot {
    // Collector
    pub events_collected: u64,
    pub events_dropped_collector: u64,
    pub collector_errors: u64,

    // Processor
    pub events_processed: u64,
    pub events_normalized: u64,
    pub events_classified: u64,
    pub events_enriched: u64,
    pub events_deduplicated: u64,
    pub events_dropped_oversized: u64,
    pub processor_errors: u64,

    // Sync
    pub events_synced: u64,
    pub events_sync_failed: u64,
    pub sync_retries: u64,
    pub upload_batches: u64,
    pub upload_bytes: u64,

    // Connector breakdown
    pub connector_events: Vec<(String, u64)>,
    pub connector_errors: Vec<(String, u64)>,

    // Latency
    pub avg_process_latency_us: u64,
    pub avg_sync_latency_us: u64,

    // Cache
    pub cache_hits: u64,
    pub cache_misses: u64,
    pub cache_evictions: u64,
    pub cache_hit_rate: f64,

    // Conflicts
    pub conflicts_detected: u64,
    pub conflicts_resolved: u64,
}

/// Connector index mapping for array-based counters.
const CONNECTOR_NAMES: &[&str] = &[
    "feishu",    // 0
    "dingtalk",  // 1
    "wecom",     // 2
    "rss",       // 3
    "email",     // 4
    "webhook",   // 5
    "github",    // 6
    "notion",     // 7
    "git",       // 8
    "obsidian",  // 9
    "slack",     // 10
];

fn connector_index(name: &str) -> Option<usize> {
    CONNECTOR_NAMES.iter().position(|&n| n == name)
}

impl Default for PipelineMetrics {
    fn default() -> Self {
        Self::new()
    }
}

impl PipelineMetrics {
    /// Create a new metrics collector with all counters at zero.
    pub fn new() -> Self {
        Self {
            inner: Arc::new(MetricsInner {
                events_collected: AtomicU64::new(0),
                events_dropped_collector: AtomicU64::new(0),
                collector_errors: AtomicU64::new(0),
                events_processed: AtomicU64::new(0),
                events_normalized: AtomicU64::new(0),
                events_classified: AtomicU64::new(0),
                events_enriched: AtomicU64::new(0),
                events_deduplicated: AtomicU64::new(0),
                events_dropped_oversized: AtomicU64::new(0),
                processor_errors: AtomicU64::new(0),
                events_synced: AtomicU64::new(0),
                events_sync_failed: AtomicU64::new(0),
                sync_retries: AtomicU64::new(0),
                upload_batches: AtomicU64::new(0),
                upload_bytes: AtomicU64::new(0),
                connector_events: std::array::from_fn(|_| AtomicU64::new(0)),
                connector_errors: std::array::from_fn(|_| AtomicU64::new(0)),
                process_latency_sum_us: AtomicU64::new(0),
                process_latency_count: AtomicU64::new(0),
                sync_latency_sum_us: AtomicU64::new(0),
                sync_latency_count: AtomicU64::new(0),
                cache_hits: AtomicU64::new(0),
                cache_misses: AtomicU64::new(0),
                cache_evictions: AtomicU64::new(0),
                conflicts_detected: AtomicU64::new(0),
                conflicts_resolved: AtomicU64::new(0),
            }),
        }
    }

    // ── Collector counters ───────────────────────────────────────

    pub fn inc_events_collected(&self) {
        self.inner.events_collected.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_events_collected_by(&self, n: u64) {
        self.inner.events_collected.fetch_add(n, Ordering::Relaxed);
    }

    pub fn inc_events_dropped_collector(&self) {
        self.inner.events_dropped_collector.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_collector_errors(&self) {
        self.inner.collector_errors.fetch_add(1, Ordering::Relaxed);
    }

    // ── Processor counters ──────────────────────────────────────

    pub fn inc_events_processed(&self) {
        self.inner.events_processed.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_events_normalized(&self) {
        self.inner.events_normalized.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_events_classified(&self) {
        self.inner.events_classified.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_events_enriched(&self) {
        self.inner.events_enriched.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_events_deduplicated(&self) {
        self.inner.events_deduplicated.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_events_dropped_oversized(&self) {
        self.inner.events_dropped_oversized.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_processor_errors(&self) {
        self.inner.processor_errors.fetch_add(1, Ordering::Relaxed);
    }

    // ── Sync counters ───────────────────────────────────────────

    pub fn inc_events_synced(&self) {
        self.inner.events_synced.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_events_synced_by(&self, n: u64) {
        self.inner.events_synced.fetch_add(n, Ordering::Relaxed);
    }

    pub fn inc_events_sync_failed(&self) {
        self.inner.events_sync_failed.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_sync_retries(&self) {
        self.inner.sync_retries.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_upload_batches(&self) {
        self.inner.upload_batches.fetch_add(1, Ordering::Relaxed);
    }

    pub fn add_upload_bytes(&self, bytes: u64) {
        self.inner.upload_bytes.fetch_add(bytes, Ordering::Relaxed);
    }

    // ── Connector counters ──────────────────────────────────────

    pub fn inc_connector_events(&self, connector: &str) {
        if let Some(idx) = connector_index(connector) {
            self.inner.connector_events[idx].fetch_add(1, Ordering::Relaxed);
        }
    }

    pub fn inc_connector_errors(&self, connector: &str) {
        if let Some(idx) = connector_index(connector) {
            self.inner.connector_errors[idx].fetch_add(1, Ordering::Relaxed);
        }
    }

    // ── Latency tracking ────────────────────────────────────────

    /// Record a processing latency sample.
    pub fn record_process_latency(&self, duration: std::time::Duration) {
        let us = duration.as_micros() as u64;
        self.inner.process_latency_sum_us.fetch_add(us, Ordering::Relaxed);
        self.inner.process_latency_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a sync latency sample.
    pub fn record_sync_latency(&self, duration: std::time::Duration) {
        let us = duration.as_micros() as u64;
        self.inner.sync_latency_sum_us.fetch_add(us, Ordering::Relaxed);
        self.inner.sync_latency_count.fetch_add(1, Ordering::Relaxed);
    }

    // ── Cache counters ──────────────────────────────────────────

    pub fn inc_cache_hits(&self) {
        self.inner.cache_hits.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_cache_misses(&self) {
        self.inner.cache_misses.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_cache_evictions(&self) {
        self.inner.cache_evictions.fetch_add(1, Ordering::Relaxed);
    }

    // ── Conflict counters ───────────────────────────────────────

    pub fn inc_conflicts_detected(&self) {
        self.inner.conflicts_detected.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_conflicts_resolved(&self) {
        self.inner.conflicts_resolved.fetch_add(1, Ordering::Relaxed);
    }

    // ── Snapshot ────────────────────────────────────────────────

    /// Take a point-in-time snapshot of all metrics.
    pub fn snapshot(&self) -> MetricsSnapshot {
        let connector_events: Vec<(String, u64)> = CONNECTOR_NAMES
            .iter()
            .enumerate()
            .map(|(i, &name)| {
                (name.to_string(), self.inner.connector_events[i].load(Ordering::Relaxed))
            })
            .collect();

        let connector_errors: Vec<(String, u64)> = CONNECTOR_NAMES
            .iter()
            .enumerate()
            .map(|(i, &name)| {
                (name.to_string(), self.inner.connector_errors[i].load(Ordering::Relaxed))
            })
            .collect();

        let process_count = self.inner.process_latency_count.load(Ordering::Relaxed);
        let sync_count = self.inner.sync_latency_count.load(Ordering::Relaxed);

        let avg_process_latency_us = if process_count > 0 {
            self.inner.process_latency_sum_us.load(Ordering::Relaxed) / process_count
        } else {
            0
        };

        let avg_sync_latency_us = if sync_count > 0 {
            self.inner.sync_latency_sum_us.load(Ordering::Relaxed) / sync_count
        } else {
            0
        };

        let hits = self.inner.cache_hits.load(Ordering::Relaxed);
        let misses = self.inner.cache_misses.load(Ordering::Relaxed);
        let total = hits + misses;
        let cache_hit_rate = if total > 0 {
            hits as f64 / total as f64
        } else {
            0.0
        };

        MetricsSnapshot {
            events_collected: self.inner.events_collected.load(Ordering::Relaxed),
            events_dropped_collector: self.inner.events_dropped_collector.load(Ordering::Relaxed),
            collector_errors: self.inner.collector_errors.load(Ordering::Relaxed),
            events_processed: self.inner.events_processed.load(Ordering::Relaxed),
            events_normalized: self.inner.events_normalized.load(Ordering::Relaxed),
            events_classified: self.inner.events_classified.load(Ordering::Relaxed),
            events_enriched: self.inner.events_enriched.load(Ordering::Relaxed),
            events_deduplicated: self.inner.events_deduplicated.load(Ordering::Relaxed),
            events_dropped_oversized: self.inner.events_dropped_oversized.load(Ordering::Relaxed),
            processor_errors: self.inner.processor_errors.load(Ordering::Relaxed),
            events_synced: self.inner.events_synced.load(Ordering::Relaxed),
            events_sync_failed: self.inner.events_sync_failed.load(Ordering::Relaxed),
            sync_retries: self.inner.sync_retries.load(Ordering::Relaxed),
            upload_batches: self.inner.upload_batches.load(Ordering::Relaxed),
            upload_bytes: self.inner.upload_bytes.load(Ordering::Relaxed),
            connector_events,
            connector_errors,
            avg_process_latency_us,
            avg_sync_latency_us,
            cache_hits: hits,
            cache_misses: misses,
            cache_evictions: self.inner.cache_evictions.load(Ordering::Relaxed),
            cache_hit_rate,
            conflicts_detected: self.inner.conflicts_detected.load(Ordering::Relaxed),
            conflicts_resolved: self.inner.conflicts_resolved.load(Ordering::Relaxed),
        }
    }

    /// Render metrics in Prometheus text exposition format.
    pub fn to_prometheus(&self) -> String {
        let snap = self.snapshot();
        let mut lines = Vec::new();

        // Collector metrics
        lines.push("# HELP opensoma_pipeline_collected_total Events entering the pipeline.".into());
        lines.push("# TYPE opensoma_pipeline_collected_total counter".into());
        lines.push(format!("opensoma_pipeline_collected_total {}", snap.events_collected));

        lines.push("# HELP opensoma_pipeline_dropped_collector_total Events dropped at collector stage.".into());
        lines.push("# TYPE opensoma_pipeline_dropped_collector_total counter".into());
        lines.push(format!("opensoma_pipeline_dropped_collector_total {}", snap.events_dropped_collector));

        // Processor metrics
        lines.push("# HELP opensoma_pipeline_processed_total Events passing through processor.".into());
        lines.push("# TYPE opensoma_pipeline_processed_total counter".into());
        lines.push(format!("opensoma_pipeline_processed_total {}", snap.events_processed));

        lines.push("# HELP opensoma_pipeline_deduplicated_total Duplicate events removed.".into());
        lines.push("# TYPE opensoma_pipeline_deduplicated_total counter".into());
        lines.push(format!("opensoma_pipeline_deduplicated_total {}", snap.events_deduplicated));

        lines.push("# HELP opensoma_pipeline_oversized_total Oversized events dropped.".into());
        lines.push("# TYPE opensoma_pipeline_oversized_total counter".into());
        lines.push(format!("opensoma_pipeline_oversized_total {}", snap.events_dropped_oversized));

        // Sync metrics
        lines.push("# HELP opensoma_pipeline_synced_total Events synced to Soul.".into());
        lines.push("# TYPE opensoma_pipeline_synced_total counter".into());
        lines.push(format!("opensoma_pipeline_synced_total {}", snap.events_synced));

        lines.push("# HELP opensoma_pipeline_sync_failed_total Failed sync attempts.".into());
        lines.push("# TYPE opensoma_pipeline_sync_failed_total counter".into());
        lines.push(format!("opensoma_pipeline_sync_failed_total {}", snap.events_sync_failed));

        // Latency
        lines.push("# HELP opensoma_process_latency_avg_us Average processing latency in microseconds.".into());
        lines.push("# TYPE opensoma_process_latency_avg_us gauge".into());
        lines.push(format!("opensoma_process_latency_avg_us {}", snap.avg_process_latency_us));

        lines.push("# HELP opensoma_sync_latency_avg_us Average sync latency in microseconds.".into());
        lines.push("# TYPE opensoma_sync_latency_avg_us gauge".into());
        lines.push(format!("opensoma_sync_latency_avg_us {}", snap.avg_sync_latency_us));

        // Cache
        lines.push("# HELP opensoma_cache_hit_rate Cache hit rate (0.0-1.0).".into());
        lines.push("# TYPE opensoma_cache_hit_rate gauge".into());
        lines.push(format!("opensoma_cache_hit_rate {:.4}", snap.cache_hit_rate));

        // Per-connector events
        lines.push("# HELP opensoma_connector_events_total Events per connector.".into());
        lines.push("# TYPE opensoma_connector_events_total counter".into());
        for (name, count) in &snap.connector_events {
            lines.push(format!("opensoma_connector_events_total{{connector=\"{}\"}} {}", name, count));
        }

        lines.push("".into()); // trailing newline
        lines.join("\n")
    }
}

/// Latency timer — call `.elapsed()` to record the duration.
pub struct LatencyTimer {
    start: Instant,
    metrics: PipelineMetrics,
    timer_type: LatencyType,
}

enum LatencyType {
    Process,
    Sync,
}

impl PipelineMetrics {
    /// Start a latency timer for processing.
    pub fn start_process_timer(&self) -> LatencyTimer {
        LatencyTimer {
            start: Instant::now(),
            metrics: self.clone(),
            timer_type: LatencyType::Process,
        }
    }

    /// Start a latency timer for sync.
    pub fn start_sync_timer(&self) -> LatencyTimer {
        LatencyTimer {
            start: Instant::now(),
            metrics: self.clone(),
            timer_type: LatencyType::Sync,
        }
    }
}

impl LatencyTimer {
    /// Stop the timer and record the latency.
    pub fn elapsed(self) {
        let duration = self.start.elapsed();
        match self.timer_type {
            LatencyType::Process => self.metrics.record_process_latency(duration),
            LatencyType::Sync => self.metrics.record_sync_latency(duration),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metrics_new() {
        let m = PipelineMetrics::new();
        let snap = m.snapshot();
        assert_eq!(snap.events_collected, 0);
        assert_eq!(snap.events_processed, 0);
        assert_eq!(snap.events_synced, 0);
    }

    #[test]
    fn test_collector_counters() {
        let m = PipelineMetrics::new();
        m.inc_events_collected();
        m.inc_events_collected();
        m.inc_events_collected_by(5);
        m.inc_events_dropped_collector();
        m.inc_collector_errors();

        let snap = m.snapshot();
        assert_eq!(snap.events_collected, 7);
        assert_eq!(snap.events_dropped_collector, 1);
        assert_eq!(snap.collector_errors, 1);
    }

    #[test]
    fn test_processor_counters() {
        let m = PipelineMetrics::new();
        m.inc_events_processed();
        m.inc_events_normalized();
        m.inc_events_classified();
        m.inc_events_enriched();
        m.inc_events_deduplicated();
        m.inc_events_dropped_oversized();
        m.inc_processor_errors();

        let snap = m.snapshot();
        assert_eq!(snap.events_processed, 1);
        assert_eq!(snap.events_normalized, 1);
        assert_eq!(snap.events_classified, 1);
        assert_eq!(snap.events_enriched, 1);
        assert_eq!(snap.events_deduplicated, 1);
        assert_eq!(snap.events_dropped_oversized, 1);
        assert_eq!(snap.processor_errors, 1);
    }

    #[test]
    fn test_sync_counters() {
        let m = PipelineMetrics::new();
        m.inc_events_synced();
        m.inc_events_synced_by(10);
        m.inc_events_sync_failed();
        m.inc_sync_retries();
        m.inc_upload_batches();
        m.add_upload_bytes(1024);

        let snap = m.snapshot();
        assert_eq!(snap.events_synced, 11);
        assert_eq!(snap.events_sync_failed, 1);
        assert_eq!(snap.sync_retries, 1);
        assert_eq!(snap.upload_batches, 1);
        assert_eq!(snap.upload_bytes, 1024);
    }

    #[test]
    fn test_connector_counters() {
        let m = PipelineMetrics::new();
        m.inc_connector_events("github");
        m.inc_connector_events("github");
        m.inc_connector_events("feishu");
        m.inc_connector_errors("github");

        let snap = m.snapshot();
        // Find github count
        let github_events = snap.connector_events.iter().find(|(n, _)| n == "github").unwrap();
        assert_eq!(github_events.1, 2);

        let feishu_events = snap.connector_events.iter().find(|(n, _)| n == "feishu").unwrap();
        assert_eq!(feishu_events.1, 1);

        let github_errors = snap.connector_errors.iter().find(|(n, _)| n == "github").unwrap();
        assert_eq!(github_errors.1, 1);
    }

    #[test]
    fn test_connector_index_unknown() {
        let m = PipelineMetrics::new();
        // Unknown connector should be silently ignored
        m.inc_connector_events("unknown_connector");
        let snap = m.snapshot();
        let total: u64 = snap.connector_events.iter().map(|(_, c)| c).sum();
        assert_eq!(total, 0);
    }

    #[test]
    fn test_latency_tracking() {
        let m = PipelineMetrics::new();
        m.record_process_latency(std::time::Duration::from_micros(100));
        m.record_process_latency(std::time::Duration::from_micros(200));
        m.record_sync_latency(std::time::Duration::from_micros(500));

        let snap = m.snapshot();
        assert_eq!(snap.avg_process_latency_us, 150); // (100+200)/2
        assert_eq!(snap.avg_sync_latency_us, 500);
    }

    #[test]
    fn test_cache_counters() {
        let m = PipelineMetrics::new();
        m.inc_cache_hits();
        m.inc_cache_hits();
        m.inc_cache_misses();
        m.inc_cache_evictions();

        let snap = m.snapshot();
        assert_eq!(snap.cache_hits, 2);
        assert_eq!(snap.cache_misses, 1);
        assert_eq!(snap.cache_evictions, 1);
        assert!((snap.cache_hit_rate - 0.6667).abs() < 0.001);
    }

    #[test]
    fn test_conflict_counters() {
        let m = PipelineMetrics::new();
        m.inc_conflicts_detected();
        m.inc_conflicts_detected();
        m.inc_conflicts_resolved();

        let snap = m.snapshot();
        assert_eq!(snap.conflicts_detected, 2);
        assert_eq!(snap.conflicts_resolved, 1);
    }

    #[test]
    fn test_latency_timer() {
        let m = PipelineMetrics::new();
        let timer = m.start_process_timer();
        std::thread::sleep(std::time::Duration::from_millis(1));
        timer.elapsed();

        let snap = m.snapshot();
        assert!(snap.avg_process_latency_us > 0);
    }

    #[test]
    fn test_clone_shares_state() {
        let m1 = PipelineMetrics::new();
        let m2 = m1.clone();
        m1.inc_events_collected();
        m2.inc_events_collected();

        let snap = m1.snapshot();
        assert_eq!(snap.events_collected, 2);
    }

    #[test]
    fn test_prometheus_format() {
        let m = PipelineMetrics::new();
        m.inc_events_collected();
        m.inc_events_synced();

        let prom = m.to_prometheus();
        assert!(prom.contains("opensoma_pipeline_collected_total 1"));
        assert!(prom.contains("opensoma_pipeline_synced_total 1"));
        assert!(prom.contains("# TYPE"));
        assert!(prom.contains("# HELP"));
    }

    #[test]
    fn test_all_connector_names() {
        let m = PipelineMetrics::new();
        for name in CONNECTOR_NAMES {
            m.inc_connector_events(name);
        }
        let snap = m.snapshot();
        let total: u64 = snap.connector_events.iter().map(|(_, c)| c).sum();
        assert_eq!(total, 11);
    }
}
