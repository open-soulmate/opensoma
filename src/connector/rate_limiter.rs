//! Rate Limiter — token-bucket rate limiting for connector API calls.
//!
//! Prevents connectors from exceeding upstream API rate limits (e.g. GitHub
//! 5000 req/hour, DingTalk 40 req/s). Uses a token-bucket algorithm with
//! configurable refill rate and burst capacity.
//!
//! # Usage
//! ```ignore
//! let limiter = RateLimiter::new("github", RateLimiterConfig {
//!     max_tokens: 30,
//!     refill_rate: 1.0,  // 1 token per second
//!     refill_interval: Duration::from_secs(1),
//! });
//!
//! // Before making an API call
//! limiter.acquire().await;
//! let response = client.get(url).send().await?;
//!
//! // Or try without waiting
//! if limiter.try_acquire() {
//!     let response = client.get(url).send().await?;
//! }
//! ```
//!
//! Each connector owns its own `RateLimiter` instance. The limiter is
//! cheaply cloneable (Arc-based) so it can be shared across async tasks.

use serde::Serialize;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;
use tracing::debug;

/// Configuration for a rate limiter.
#[derive(Debug, Clone)]
pub struct RateLimiterConfig {
    /// Maximum number of tokens in the bucket (burst capacity).
    pub max_tokens: u32,
    /// Tokens added per refill interval.
    pub refill_rate: f64,
    /// How often tokens are refilled.
    pub refill_interval: Duration,
}

impl Default for RateLimiterConfig {
    fn default() -> Self {
        Self {
            max_tokens: 10,
            refill_rate: 1.0,
            refill_interval: Duration::from_secs(1),
        }
    }
}

/// Predefined rate limit configs for well-known APIs.
impl RateLimiterConfig {
    /// GitHub REST API: 5000 requests/hour ≈ 1.39/s, burst 30.
    pub fn github() -> Self {
        Self {
            max_tokens: 30,
            refill_rate: 1.39,
            refill_interval: Duration::from_secs(1),
        }
    }

    /// DingTalk API: ~40 requests/second, burst 20.
    pub fn dingtalk() -> Self {
        Self {
            max_tokens: 20,
            refill_rate: 40.0,
            refill_interval: Duration::from_secs(1),
        }
    }

    /// Feishu API: ~50 requests/second, burst 25.
    pub fn feishu() -> Self {
        Self {
            max_tokens: 25,
            refill_rate: 50.0,
            refill_interval: Duration::from_secs(1),
        }
    }

    /// Notion API: 3 requests/second.
    pub fn notion() -> Self {
        Self {
            max_tokens: 3,
            refill_rate: 3.0,
            refill_interval: Duration::from_secs(1),
        }
    }

    /// Slack API: Tier 3 (~50/min for most methods) ≈ 0.83/s.
    pub fn slack() -> Self {
        Self {
            max_tokens: 5,
            refill_rate: 0.83,
            refill_interval: Duration::from_secs(1),
        }
    }

    /// Discord API: 50 requests/second globally.
    pub fn discord() -> Self {
        Self {
            max_tokens: 25,
            refill_rate: 50.0,
            refill_interval: Duration::from_secs(1),
        }
    }

    /// WeCom API: ~200 requests/second.
    pub fn wecom() -> Self {
        Self {
            max_tokens: 50,
            refill_rate: 200.0,
            refill_interval: Duration::from_secs(1),
        }
    }

    /// Microsoft Graph API: ~2000 requests/minute ≈ 33/s.
    pub fn teams() -> Self {
        Self {
            max_tokens: 50,
            refill_rate: 33.0,
            refill_interval: Duration::from_secs(1),
        }
    }

    /// Conservative rate limit for unknown APIs.
    pub fn conservative() -> Self {
        Self {
            max_tokens: 5,
            refill_rate: 2.0,
            refill_interval: Duration::from_secs(1),
        }
    }
}

/// Snapshot of rate limiter state for monitoring.
#[derive(Debug, Clone, Serialize)]
pub struct RateLimiterSnapshot {
    pub connector: String,
    pub available_tokens: u32,
    pub max_tokens: u32,
    pub total_acquired: u64,
    pub total_waited_ms: u64,
    pub total_rejected: u64,
}

/// Inner mutable state of the rate limiter.
struct RateLimiterInner {
    /// Current number of available tokens (as fixed-point for sub-integer rates).
    tokens_fp: u64,
    /// Last time tokens were refilled.
    last_refill: Instant,
}

/// Thread-safe token-bucket rate limiter.
///
/// Uses a Mutex for the mutable bucket state and atomics for high-frequency counters.
#[derive(Clone)]
pub struct RateLimiter {
    connector: String,
    config: RateLimiterConfig,
    inner: Arc<Mutex<RateLimiterInner>>,
    /// Fixed-point multiplier for sub-integer token tracking (1000 = 3 decimal places).
    max_tokens_fp: u64,
    /// Tokens added per interval in fixed-point.
    refill_amount_fp: u64,
    total_acquired: Arc<AtomicU64>,
    total_waited_ms: Arc<AtomicU64>,
    total_rejected: Arc<AtomicU64>,
}

/// Fixed-point multiplier — allows tracking fractional tokens (e.g. 0.83 tokens/sec for Slack).
const FP_MULTIPLIER: u64 = 1000;

impl RateLimiter {
    /// Create a new rate limiter for a connector.
    pub fn new(connector: impl Into<String>, config: RateLimiterConfig) -> Self {
        let max_tokens_fp = (config.max_tokens as f64 * FP_MULTIPLIER as f64) as u64;
        let refill_amount_fp =
            (config.refill_rate * config.refill_interval.as_secs_f64() * FP_MULTIPLIER as f64)
                as u64;

        Self {
            connector: connector.into(),
            config: config.clone(),
            inner: Arc::new(Mutex::new(RateLimiterInner {
                tokens_fp: max_tokens_fp, // Start with a full bucket
                last_refill: Instant::now(),
            })),
            max_tokens_fp,
            refill_amount_fp,
            total_acquired: Arc::new(AtomicU64::new(0)),
            total_waited_ms: Arc::new(AtomicU64::new(0)),
            total_rejected: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Acquire a token, waiting if necessary.
    ///
    /// This is the primary method for rate-limited operations. It blocks (async)
    /// until a token is available, refilling tokens based on elapsed time.
    pub async fn acquire(&self) {
        let wait_start = Instant::now();

        loop {
            let wait_ms = {
                let mut inner = self.inner.lock().await;
                self.refill(&mut inner);

                if inner.tokens_fp >= FP_MULTIPLIER {
                    // Consume one token
                    inner.tokens_fp -= FP_MULTIPLIER;
                    self.total_acquired.fetch_add(1, Ordering::Relaxed);
                    None
                } else {
                    // Calculate how long until the next token is available
                    let deficit_fp = FP_MULTIPLIER - inner.tokens_fp;
                    let intervals_needed =
                        deficit_fp as f64 / self.refill_amount_fp.max(1) as f64;
                    let wait_duration = self
                        .config
                        .refill_interval
                        .mul_f64(intervals_needed)
                        .max(Duration::from_millis(10));
                    Some(wait_duration)
                }
            };

            match wait_ms {
                None => {
                    // Token acquired
                    let waited = wait_start.elapsed().as_millis() as u64;
                    if waited > 0 {
                        self.total_waited_ms.fetch_add(waited, Ordering::Relaxed);
                    }
                    return;
                }
                Some(duration) => {
                    debug!(
                        "Rate limiter '{}': waiting {}ms for token",
                        self.connector,
                        duration.as_millis()
                    );
                    tokio::time::sleep(duration).await;
                }
            }
        }
    }

    /// Try to acquire a token without waiting.
    ///
    /// Returns `true` if a token was available and consumed, `false` otherwise.
    /// Use this for non-critical operations that should be skipped when rate-limited.
    pub async fn try_acquire(&self) -> bool {
        let mut inner = self.inner.lock().await;
        self.refill(&mut inner);

        if inner.tokens_fp >= FP_MULTIPLIER {
            inner.tokens_fp -= FP_MULTIPLIER;
            self.total_acquired.fetch_add(1, Ordering::Relaxed);
            true
        } else {
            self.total_rejected.fetch_add(1, Ordering::Relaxed);
            false
        }
    }

    /// Refill tokens based on elapsed time since last refill.
    fn refill(&self, inner: &mut RateLimiterInner) {
        let elapsed = inner.last_refill.elapsed();
        let intervals = elapsed.as_secs_f64() / self.config.refill_interval.as_secs_f64();
        let new_tokens = (intervals * self.refill_amount_fp as f64) as u64;

        if new_tokens > 0 {
            inner.tokens_fp = (inner.tokens_fp + new_tokens).min(self.max_tokens_fp);
            inner.last_refill = Instant::now();
        }
    }

    /// Get a snapshot of the rate limiter state for monitoring.
    pub async fn snapshot(&self) -> RateLimiterSnapshot {
        let inner = self.inner.lock().await;
        RateLimiterSnapshot {
            connector: self.connector.clone(),
            available_tokens: (inner.tokens_fp / FP_MULTIPLIER) as u32,
            max_tokens: self.config.max_tokens,
            total_acquired: self.total_acquired.load(Ordering::Relaxed),
            total_waited_ms: self.total_waited_ms.load(Ordering::Relaxed),
            total_rejected: self.total_rejected.load(Ordering::Relaxed),
        }
    }

    /// Get the connector name.
    pub fn connector_name(&self) -> &str {
        &self.connector
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_acquire_with_full_bucket() {
        let limiter = RateLimiter::new("test", RateLimiterConfig {
            max_tokens: 5,
            refill_rate: 10.0,
            refill_interval: Duration::from_secs(1),
        });

        // Should acquire immediately since bucket starts full
        limiter.acquire().await;
        let snap = limiter.snapshot().await;
        assert_eq!(snap.total_acquired, 1);
        assert_eq!(snap.available_tokens, 4);
    }

    #[tokio::test]
    async fn test_try_acquire_success() {
        let limiter = RateLimiter::new("test", RateLimiterConfig {
            max_tokens: 3,
            refill_rate: 1.0,
            refill_interval: Duration::from_secs(1),
        });

        assert!(limiter.try_acquire().await);
        assert!(limiter.try_acquire().await);
        assert!(limiter.try_acquire().await);
        // Bucket should be empty now
        assert!(!limiter.try_acquire().await);

        let snap = limiter.snapshot().await;
        assert_eq!(snap.total_acquired, 3);
        assert_eq!(snap.total_rejected, 1);
    }

    #[tokio::test]
    async fn test_refill_over_time() {
        let limiter = RateLimiter::new("test", RateLimiterConfig {
            max_tokens: 5,
            refill_rate: 100.0, // Very fast refill for testing
            refill_interval: Duration::from_millis(10),
        });

        // Drain all tokens
        for _ in 0..5 {
            assert!(limiter.try_acquire().await);
        }
        assert!(!limiter.try_acquire().await);

        // Wait for refill
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Should have tokens again
        assert!(limiter.try_acquire().await);
    }

    #[tokio::test]
    async fn test_max_tokens_cap() {
        let limiter = RateLimiter::new("test", RateLimiterConfig {
            max_tokens: 3,
            refill_rate: 100.0,
            refill_interval: Duration::from_millis(10),
        });

        // Wait a long time to accumulate tokens
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Should still cap at max_tokens
        let snap = limiter.snapshot().await;
        assert!(snap.available_tokens <= 3);
    }

    #[tokio::test]
    async fn test_acquire_waits_for_refill() {
        let limiter = RateLimiter::new("test", RateLimiterConfig {
            max_tokens: 1,
            refill_rate: 100.0, // Fast refill
            refill_interval: Duration::from_millis(10),
        });

        // Drain the single token
        limiter.acquire().await;

        // This should wait briefly then succeed
        let start = Instant::now();
        limiter.acquire().await;
        let elapsed = start.elapsed();

        assert!(elapsed >= Duration::from_millis(5));
        assert_eq!(limiter.snapshot().await.total_acquired, 2);
    }

    #[test]
    fn test_default_config() {
        let config = RateLimiterConfig::default();
        assert_eq!(config.max_tokens, 10);
        assert_eq!(config.refill_rate, 1.0);
        assert_eq!(config.refill_interval, Duration::from_secs(1));
    }

    #[test]
    fn test_preset_configs() {
        let gh = RateLimiterConfig::github();
        assert_eq!(gh.max_tokens, 30);
        assert!(gh.refill_rate > 1.0);

        let dt = RateLimiterConfig::dingtalk();
        assert_eq!(dt.max_tokens, 20);

        let notion = RateLimiterConfig::notion();
        assert_eq!(notion.max_tokens, 3);

        let slack = RateLimiterConfig::slack();
        assert!(slack.refill_rate < 1.0);

        let conservative = RateLimiterConfig::conservative();
        assert_eq!(conservative.max_tokens, 5);
    }

    #[test]
    fn test_connector_name() {
        let limiter = RateLimiter::new("github", RateLimiterConfig::default());
        assert_eq!(limiter.connector_name(), "github");
    }

    #[tokio::test]
    async fn test_snapshot_fields() {
        let limiter = RateLimiter::new("test", RateLimiterConfig {
            max_tokens: 10,
            refill_rate: 5.0,
            refill_interval: Duration::from_secs(1),
        });

        limiter.acquire().await;
        let snap = limiter.snapshot().await;
        assert_eq!(snap.connector, "test");
        assert_eq!(snap.max_tokens, 10);
        assert_eq!(snap.available_tokens, 9);
        assert_eq!(snap.total_acquired, 1);
        assert_eq!(snap.total_rejected, 0);
    }

    #[tokio::test]
    async fn test_clone_shares_state() {
        let limiter = RateLimiter::new("test", RateLimiterConfig {
            max_tokens: 5,
            refill_rate: 1.0,
            refill_interval: Duration::from_secs(1),
        });

        let limiter2 = limiter.clone();

        limiter.acquire().await;
        limiter2.acquire().await;

        let snap = limiter.snapshot().await;
        assert_eq!(snap.total_acquired, 2);
        assert_eq!(snap.available_tokens, 3);
    }

    #[test]
    fn test_rate_limiter_snapshot_serializable() {
        let snapshot = RateLimiterSnapshot {
            connector: "github".to_string(),
            available_tokens: 25,
            max_tokens: 30,
            total_acquired: 100,
            total_waited_ms: 500,
            total_rejected: 5,
        };

        let json = serde_json::to_string(&snapshot).unwrap();
        assert!(json.contains("github"));
        assert!(json.contains("25"));
    }

    #[tokio::test]
    async fn test_sub_integer_rate() {
        // Test with a rate less than 1 token/sec (like Slack's 0.83/s)
        let limiter = RateLimiter::new("slack", RateLimiterConfig {
            max_tokens: 2,
            refill_rate: 0.5, // 0.5 tokens/sec
            refill_interval: Duration::from_secs(1),
        });

        // Drain 2 tokens
        assert!(limiter.try_acquire().await);
        assert!(limiter.try_acquire().await);
        assert!(!limiter.try_acquire().await);

        // Wait 2 seconds → should get 1 token (0.5 * 2 = 1.0)
        tokio::time::sleep(Duration::from_secs(2)).await;
        assert!(limiter.try_acquire().await);
    }

    #[tokio::test]
    async fn test_concurrent_acquire() {
        let limiter = RateLimiter::new("test", RateLimiterConfig {
            max_tokens: 5,
            refill_rate: 100.0,
            refill_interval: Duration::from_millis(10),
        });

        let mut handles = Vec::new();
        for _ in 0..5 {
            let l = limiter.clone();
            handles.push(tokio::spawn(async move {
                l.acquire().await;
            }));
        }

        for h in handles {
            h.await.unwrap();
        }

        let snap = limiter.snapshot().await;
        assert_eq!(snap.total_acquired, 5);
    }
}
