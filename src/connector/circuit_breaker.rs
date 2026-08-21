//! Circuit Breaker — prevents cascading failures for connector upstream calls.
//!
//! Implements the standard three-state circuit breaker pattern:
//! - **Closed**: requests pass through normally; failures are counted.
//! - **Open**: requests are rejected immediately; after a cooldown, transitions to Half-Open.
//! - **Half-Open**: a single probe request is allowed; success → Closed, failure → Open.
//!
//! # Usage
//! ```ignore
//! let cb = CircuitBreaker::new("github", CircuitBreakerConfig::default());
//! if cb.allow_request().is_err() {
//!     // Circuit is open — skip this connector poll
//!     return;
//! }
//! match fetch_data().await {
//!     Ok(data) => { cb.record_success(); /* process data */ }
//!     Err(e)   => { cb.record_failure(); /* log error */ }
//! }
//! ```
//!
//! Each connector owns its own `CircuitBreaker` instance. The breaker is
//! cheaply cloneable (Arc-based) so it can be shared across async tasks.

use serde::Serialize;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;
use tracing::{info, warn};

/// Circuit breaker state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum CircuitState {
    /// Normal operation — requests pass through.
    Closed,
    /// Too many failures — requests are rejected.
    Open,
    /// Probing — one request is allowed to test recovery.
    HalfOpen,
}

/// Configuration for a circuit breaker.
#[derive(Debug, Clone)]
pub struct CircuitBreakerConfig {
    /// Number of consecutive failures before opening the circuit.
    pub failure_threshold: u32,
    /// Duration to wait in Open state before transitioning to Half-Open.
    pub cooldown_duration: Duration,
    /// Number of consecutive successes in Half-Open before closing.
    pub success_threshold: u32,
}

impl Default for CircuitBreakerConfig {
    fn default() -> Self {
        Self {
            failure_threshold: 5,
            cooldown_duration: Duration::from_secs(30),
            success_threshold: 2,
        }
    }
}

/// Snapshot of circuit breaker state for API responses.
#[derive(Debug, Clone, Serialize)]
pub struct CircuitBreakerSnapshot {
    pub connector: String,
    pub state: CircuitState,
    pub consecutive_failures: u32,
    pub consecutive_successes: u32,
    pub total_failures: u64,
    pub total_successes: u64,
    pub total_rejected: u64,
    pub last_failure_ms: Option<i64>,
    pub last_success_ms: Option<i64>,
    pub opened_at_ms: Option<i64>,
}

/// Inner mutable state of the circuit breaker.
struct CircuitBreakerInner {
    state: CircuitState,
    consecutive_failures: u32,
    consecutive_successes: u32,
    last_failure: Option<Instant>,
    last_success: Option<Instant>,
    opened_at: Option<Instant>,
}

/// Thread-safe circuit breaker.
///
/// Uses a Mutex for the mutable state and atomics for high-frequency counters.
#[derive(Clone)]
pub struct CircuitBreaker {
    connector: String,
    config: CircuitBreakerConfig,
    inner: Arc<Mutex<CircuitBreakerInner>>,
    total_failures: Arc<AtomicU64>,
    total_successes: Arc<AtomicU64>,
    total_rejected: Arc<AtomicU64>,
}

impl CircuitBreaker {
    /// Create a new circuit breaker for a connector.
    pub fn new(connector: impl Into<String>, config: CircuitBreakerConfig) -> Self {
        Self {
            connector: connector.into(),
            config,
            inner: Arc::new(Mutex::new(CircuitBreakerInner {
                state: CircuitState::Closed,
                consecutive_failures: 0,
                consecutive_successes: 0,
                last_failure: None,
                last_success: None,
                opened_at: None,
            })),
            total_failures: Arc::new(AtomicU64::new(0)),
            total_successes: Arc::new(AtomicU64::new(0)),
            total_rejected: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Check if a request is allowed to proceed.
    ///
    /// Returns `Ok(())` if the request should proceed, `Err(reason)` if rejected.
    pub async fn allow_request(&self) -> Result<(), &'static str> {
        let mut inner = self.inner.lock().await;

        match inner.state {
            CircuitState::Closed => Ok(()),
            CircuitState::Open => {
                // Check if cooldown has elapsed
                if let Some(opened) = inner.opened_at {
                    if opened.elapsed() >= self.config.cooldown_duration {
                        info!(
                            "Circuit breaker '{}' cooldown elapsed — transitioning to Half-Open",
                            self.connector
                        );
                        inner.state = CircuitState::HalfOpen;
                        inner.consecutive_successes = 0;
                        Ok(())
                    } else {
                        self.total_rejected.fetch_add(1, Ordering::Relaxed);
                        Err("circuit open")
                    }
                } else {
                    // Shouldn't happen, but treat as open
                    self.total_rejected.fetch_add(1, Ordering::Relaxed);
                    Err("circuit open")
                }
            }
            CircuitState::HalfOpen => {
                // Allow the probe request
                Ok(())
            }
        }
    }

    /// Record a successful request.
    pub async fn record_success(&self) {
        let mut inner = self.inner.lock().await;
        inner.consecutive_failures = 0;
        inner.consecutive_successes += 1;
        inner.last_success = Some(Instant::now());
        self.total_successes.fetch_add(1, Ordering::Relaxed);

        match inner.state {
            CircuitState::HalfOpen => {
                if inner.consecutive_successes >= self.config.success_threshold {
                    info!(
                        "Circuit breaker '{}' recovered — transitioning to Closed",
                        self.connector
                    );
                    inner.state = CircuitState::Closed;
                    inner.opened_at = None;
                }
            }
            CircuitState::Open => {
                // Shouldn't happen (we shouldn't be recording success while open)
                // but gracefully transition to closed
                inner.state = CircuitState::Closed;
                inner.opened_at = None;
            }
            CircuitState::Closed => {
                // Already closed, nothing to do
            }
        }
    }

    /// Record a failed request.
    pub async fn record_failure(&self) {
        let mut inner = self.inner.lock().await;
        inner.consecutive_successes = 0;
        inner.consecutive_failures += 1;
        inner.last_failure = Some(Instant::now());
        self.total_failures.fetch_add(1, Ordering::Relaxed);

        match inner.state {
            CircuitState::Closed => {
                if inner.consecutive_failures >= self.config.failure_threshold {
                    warn!(
                        "Circuit breaker '{}' tripped — {} consecutive failures, transitioning to Open (cooldown {}s)",
                        self.connector,
                        inner.consecutive_failures,
                        self.config.cooldown_duration.as_secs()
                    );
                    inner.state = CircuitState::Open;
                    inner.opened_at = Some(Instant::now());
                }
            }
            CircuitState::HalfOpen => {
                warn!(
                    "Circuit breaker '{}' probe failed — back to Open",
                    self.connector
                );
                inner.state = CircuitState::Open;
                inner.opened_at = Some(Instant::now());
            }
            CircuitState::Open => {
                // Already open, keep counting
            }
        }
    }

    /// Get the current state.
    pub async fn state(&self) -> CircuitState {
        let inner = self.inner.lock().await;
        inner.state
    }

    /// Get a snapshot for API responses.
    pub async fn snapshot(&self) -> CircuitBreakerSnapshot {
        let inner = self.inner.lock().await;
        CircuitBreakerSnapshot {
            connector: self.connector.clone(),
            state: inner.state,
            consecutive_failures: inner.consecutive_failures,
            consecutive_successes: inner.consecutive_successes,
            total_failures: self.total_failures.load(Ordering::Relaxed),
            total_successes: self.total_successes.load(Ordering::Relaxed),
            total_rejected: self.total_rejected.load(Ordering::Relaxed),
            last_failure_ms: inner
                .last_failure
                .map(|t| (chrono::Utc::now() - t.elapsed()).timestamp_millis()),
            last_success_ms: inner
                .last_success
                .map(|t| (chrono::Utc::now() - t.elapsed()).timestamp_millis()),
            opened_at_ms: inner
                .opened_at
                .map(|t| (chrono::Utc::now() - t.elapsed()).timestamp_millis()),
        }
    }

    /// Manually reset the circuit breaker to Closed state.
    pub async fn reset(&self) {
        let mut inner = self.inner.lock().await;
        info!(
            "Circuit breaker '{}' manually reset to Closed",
            self.connector
        );
        inner.state = CircuitState::Closed;
        inner.consecutive_failures = 0;
        inner.consecutive_successes = 0;
        inner.opened_at = None;
    }

    /// Get the connector name.
    pub fn connector_name(&self) -> &str {
        &self.connector
    }
}

/// Manages circuit breakers for all connectors.
#[derive(Clone)]
pub struct CircuitBreakerRegistry {
    breakers: std::collections::HashMap<String, CircuitBreaker>,
}

impl CircuitBreakerRegistry {
    /// Create a new registry with breakers for all known connectors.
    pub fn new() -> Self {
        let connectors = [
            "feishu", "dingtalk", "wecom", "rss", "email", "webhook", "github", "notion", "git",
            "obsidian", "slack",
        ];
        let mut breakers = std::collections::HashMap::new();
        for name in &connectors {
            breakers.insert(
                name.to_string(),
                CircuitBreaker::new(*name, CircuitBreakerConfig::default()),
            );
        }
        Self { breakers }
    }

    /// Get a circuit breaker by connector name.
    pub fn get(&self, connector: &str) -> Option<&CircuitBreaker> {
        self.breakers.get(connector)
    }

    /// Get snapshots of all circuit breakers.
    pub async fn snapshot_all(&self) -> Vec<CircuitBreakerSnapshot> {
        let mut snapshots = Vec::with_capacity(self.breakers.len());
        for breaker in self.breakers.values() {
            snapshots.push(breaker.snapshot().await);
        }
        snapshots
    }

    /// Reset all circuit breakers to Closed state.
    pub async fn reset_all(&self) {
        for breaker in self.breakers.values() {
            breaker.reset().await;
        }
    }
}

impl Default for CircuitBreakerRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_circuit_breaker_starts_closed() {
        let cb = CircuitBreaker::new("test", CircuitBreakerConfig::default());
        assert_eq!(cb.state().await, CircuitState::Closed);
        assert!(cb.allow_request().await.is_ok());
    }

    #[tokio::test]
    async fn test_circuit_breaker_opens_after_threshold() {
        let cb = CircuitBreaker::new(
            "test",
            CircuitBreakerConfig {
                failure_threshold: 3,
                cooldown_duration: Duration::from_secs(60),
                success_threshold: 2,
            },
        );

        // Record failures up to threshold
        cb.record_failure().await;
        assert_eq!(cb.state().await, CircuitState::Closed);
        cb.record_failure().await;
        assert_eq!(cb.state().await, CircuitState::Closed);
        cb.record_failure().await;
        assert_eq!(cb.state().await, CircuitState::Open);

        // Requests should be rejected
        assert!(cb.allow_request().await.is_err());
    }

    #[tokio::test]
    async fn test_circuit_breaker_transitions_to_half_open() {
        let cb = CircuitBreaker::new(
            "test",
            CircuitBreakerConfig {
                failure_threshold: 2,
                cooldown_duration: Duration::from_millis(50),
                success_threshold: 2,
            },
        );

        // Trip the circuit
        cb.record_failure().await;
        cb.record_failure().await;
        assert_eq!(cb.state().await, CircuitState::Open);

        // Wait for cooldown
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Should allow probe request (transitions to Half-Open)
        assert!(cb.allow_request().await.is_ok());
        assert_eq!(cb.state().await, CircuitState::HalfOpen);
    }

    #[tokio::test]
    async fn test_circuit_breaker_closes_after_success_threshold() {
        let cb = CircuitBreaker::new(
            "test",
            CircuitBreakerConfig {
                failure_threshold: 2,
                cooldown_duration: Duration::from_millis(50),
                success_threshold: 2,
            },
        );

        // Trip and wait
        cb.record_failure().await;
        cb.record_failure().await;
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Probe and succeed
        cb.allow_request().await.unwrap();
        cb.record_success().await;
        assert_eq!(cb.state().await, CircuitState::HalfOpen);

        cb.record_success().await;
        assert_eq!(cb.state().await, CircuitState::Closed);
    }

    #[tokio::test]
    async fn test_circuit_breaker_half_open_failure_reopens() {
        let cb = CircuitBreaker::new(
            "test",
            CircuitBreakerConfig {
                failure_threshold: 2,
                cooldown_duration: Duration::from_millis(50),
                success_threshold: 2,
            },
        );

        // Trip and wait
        cb.record_failure().await;
        cb.record_failure().await;
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Probe fails
        cb.allow_request().await.unwrap();
        cb.record_failure().await;
        assert_eq!(cb.state().await, CircuitState::Open);
    }

    #[tokio::test]
    async fn test_circuit_breaker_success_resets_failure_count() {
        let cb = CircuitBreaker::new(
            "test",
            CircuitBreakerConfig {
                failure_threshold: 3,
                cooldown_duration: Duration::from_secs(60),
                success_threshold: 2,
            },
        );

        // Fail twice, succeed, fail twice more — should still be closed
        cb.record_failure().await;
        cb.record_failure().await;
        cb.record_success().await; // resets failure count
        cb.record_failure().await;
        cb.record_failure().await;
        assert_eq!(cb.state().await, CircuitState::Closed);
    }

    #[tokio::test]
    async fn test_circuit_breaker_snapshot() {
        let cb = CircuitBreaker::new("github", CircuitBreakerConfig::default());
        cb.record_failure().await;
        cb.record_success().await;

        let snap = cb.snapshot().await;
        assert_eq!(snap.connector, "github");
        assert_eq!(snap.state, CircuitState::Closed);
        assert_eq!(snap.total_failures, 1);
        assert_eq!(snap.total_successes, 1);
        assert_eq!(snap.consecutive_failures, 0);
        assert_eq!(snap.consecutive_successes, 1);
    }

    #[tokio::test]
    async fn test_circuit_breaker_manual_reset() {
        let cb = CircuitBreaker::new(
            "test",
            CircuitBreakerConfig {
                failure_threshold: 1,
                cooldown_duration: Duration::from_secs(60),
                success_threshold: 1,
            },
        );

        cb.record_failure().await;
        assert_eq!(cb.state().await, CircuitState::Open);

        cb.reset().await;
        assert_eq!(cb.state().await, CircuitState::Closed);
        assert!(cb.allow_request().await.is_ok());
    }

    #[tokio::test]
    async fn test_circuit_breaker_registry() {
        let registry = CircuitBreakerRegistry::new();

        // Should have breakers for all connectors
        assert!(registry.get("github").is_some());
        assert!(registry.get("feishu").is_some());
        assert!(registry.get("slack").is_some());
        assert!(registry.get("nonexistent").is_none());

        let snapshots = registry.snapshot_all().await;
        assert_eq!(snapshots.len(), 11);
    }

    #[tokio::test]
    async fn test_circuit_breaker_registry_reset_all() {
        let registry = CircuitBreakerRegistry::new();

        // Trip one breaker
        if let Some(cb) = registry.get("github") {
            cb.record_failure().await;
            cb.record_failure().await;
            cb.record_failure().await;
            cb.record_failure().await;
            cb.record_failure().await;
            assert_eq!(cb.state().await, CircuitState::Open);
        }

        // Reset all
        registry.reset_all().await;

        if let Some(cb) = registry.get("github") {
            assert_eq!(cb.state().await, CircuitState::Closed);
        }
    }

    #[test]
    fn test_circuit_breaker_config_default() {
        let config = CircuitBreakerConfig::default();
        assert_eq!(config.failure_threshold, 5);
        assert_eq!(config.cooldown_duration, Duration::from_secs(30));
        assert_eq!(config.success_threshold, 2);
    }

    #[test]
    fn test_circuit_state_serialize() {
        let state = CircuitState::Closed;
        let json = serde_json::to_string(&state).unwrap();
        assert_eq!(json, "\"Closed\"");

        let state = CircuitState::Open;
        let json = serde_json::to_string(&state).unwrap();
        assert_eq!(json, "\"Open\"");
    }

    #[test]
    fn test_circuit_breaker_clone() {
        let cb = CircuitBreaker::new("test", CircuitBreakerConfig::default());
        let cb2 = cb.clone();
        assert_eq!(cb.connector_name(), cb2.connector_name());
    }
}
