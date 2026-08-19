//! Connector health check system.
//!
//! Periodically pings each enabled connector and tracks health status.
//! Provides a health summary for the status server API.

use serde::Serialize;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, warn};

/// Health status of a single connector.
#[derive(Debug, Clone, Serialize)]
pub struct ConnectorHealth {
    pub name: String,
    pub status: HealthStatus,
    pub last_check_ms: i64,
    pub last_healthy_ms: Option<i64>,
    pub consecutive_failures: u32,
    pub error_message: Option<String>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub enum HealthStatus {
    Healthy,
    Degraded,
    Unhealthy,
    Unknown,
}

/// Manages health checks for all connectors.
pub struct HealthChecker {
    /// Connector name → health state
    states: Arc<RwLock<HashMap<String, ConnectorHealth>>>,
}

impl Clone for HealthChecker {
    fn clone(&self) -> Self {
        Self {
            states: self.states.clone(),
        }
    }
}

impl HealthChecker {
    pub fn new() -> Self {
        Self {
            states: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Record a successful health check for a connector.
    pub async fn record_healthy(&self, name: &str) {
        let now = chrono::Utc::now().timestamp_millis();
        let mut states = self.states.write().await;
        let entry = states.entry(name.to_string()).or_insert_with(|| ConnectorHealth {
            name: name.to_string(),
            status: HealthStatus::Unknown,
            last_check_ms: 0,
            last_healthy_ms: None,
            consecutive_failures: 0,
            error_message: None,
        });

        entry.status = HealthStatus::Healthy;
        entry.last_check_ms = now;
        entry.last_healthy_ms = Some(now);
        entry.consecutive_failures = 0;
        entry.error_message = None;
        debug!("Connector '{}' health: OK", name);
    }

    /// Record a failed health check for a connector.
    pub async fn record_unhealthy(&self, name: &str, error: &str) {
        let now = chrono::Utc::now().timestamp_millis();
        let mut states = self.states.write().await;
        let entry = states.entry(name.to_string()).or_insert_with(|| ConnectorHealth {
            name: name.to_string(),
            status: HealthStatus::Unknown,
            last_check_ms: 0,
            last_healthy_ms: None,
            consecutive_failures: 0,
            error_message: None,
        });

        entry.consecutive_failures += 1;
        entry.last_check_ms = now;
        entry.error_message = Some(error.to_string());

        // 1 failure = degraded, 3+ = unhealthy
        entry.status = if entry.consecutive_failures >= 3 {
            HealthStatus::Unhealthy
        } else {
            HealthStatus::Degraded
        };

        warn!(
            "Connector '{}' health: {} (failures: {}, error: {})",
            name,
            serde_json::to_string(&entry.status).unwrap_or_default(),
            entry.consecutive_failures,
            error
        );
    }

    /// Get health status of all connectors.
    pub async fn get_all(&self) -> Vec<ConnectorHealth> {
        self.states.read().await.values().cloned().collect()
    }

    /// Get health status of a specific connector.
    pub async fn get(&self, name: &str) -> Option<ConnectorHealth> {
        self.states.read().await.get(name).cloned()
    }

    /// Get a summary of overall system health.
    pub async fn summary(&self) -> HealthSummary {
        let states = self.states.read().await;
        let total = states.len();
        let healthy = states.values().filter(|h| h.status == HealthStatus::Healthy).count();
        let degraded = states.values().filter(|h| h.status == HealthStatus::Degraded).count();
        let unhealthy = states.values().filter(|h| h.status == HealthStatus::Unhealthy).count();

        HealthSummary {
            total,
            healthy,
            degraded,
            unhealthy,
            overall: if unhealthy > 0 {
                HealthStatus::Unhealthy
            } else if degraded > 0 {
                HealthStatus::Degraded
            } else if healthy > 0 {
                HealthStatus::Healthy
            } else {
                HealthStatus::Unknown
            },
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct HealthSummary {
    pub total: usize,
    pub healthy: usize,
    pub degraded: usize,
    pub unhealthy: usize,
    pub overall: HealthStatus,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_healthy_record() {
        let checker = HealthChecker::new();
        checker.record_healthy("github").await;

        let health = checker.get("github").await.unwrap();
        assert_eq!(health.status, HealthStatus::Healthy);
        assert_eq!(health.consecutive_failures, 0);
        assert!(health.last_healthy_ms.is_some());
    }

    #[tokio::test]
    async fn test_unhealthy_degraded_then_unhealthy() {
        let checker = HealthChecker::new();

        checker.record_unhealthy("feishu", "timeout").await;
        let h = checker.get("feishu").await.unwrap();
        assert_eq!(h.status, HealthStatus::Degraded);
        assert_eq!(h.consecutive_failures, 1);

        checker.record_unhealthy("feishu", "timeout").await;
        let h = checker.get("feishu").await.unwrap();
        assert_eq!(h.status, HealthStatus::Degraded);
        assert_eq!(h.consecutive_failures, 2);

        checker.record_unhealthy("feishu", "timeout").await;
        let h = checker.get("feishu").await.unwrap();
        assert_eq!(h.status, HealthStatus::Unhealthy);
        assert_eq!(h.consecutive_failures, 3);
    }

    #[tokio::test]
    async fn test_recovery_resets_failures() {
        let checker = HealthChecker::new();

        checker.record_unhealthy("rss", "err1").await;
        checker.record_unhealthy("rss", "err2").await;
        checker.record_healthy("rss").await;

        let h = checker.get("rss").await.unwrap();
        assert_eq!(h.status, HealthStatus::Healthy);
        assert_eq!(h.consecutive_failures, 0);
        assert!(h.error_message.is_none());
    }

    #[tokio::test]
    async fn test_summary() {
        let checker = HealthChecker::new();

        checker.record_healthy("github").await;
        checker.record_healthy("feishu").await;
        checker.record_unhealthy("rss", "down").await;
        checker.record_unhealthy("rss", "down").await;
        checker.record_unhealthy("rss", "down").await;

        let summary = checker.summary().await;
        assert_eq!(summary.total, 3);
        assert_eq!(summary.healthy, 2);
        assert_eq!(summary.unhealthy, 1);
        assert_eq!(summary.overall, HealthStatus::Unhealthy);
    }

    #[tokio::test]
    async fn test_get_all() {
        let checker = HealthChecker::new();
        checker.record_healthy("a").await;
        checker.record_healthy("b").await;

        let all = checker.get_all().await;
        assert_eq!(all.len(), 2);
    }

    #[tokio::test]
    async fn test_get_unknown_connector() {
        let checker = HealthChecker::new();
        assert!(checker.get("nonexistent").await.is_none());
    }

    #[tokio::test]
    async fn test_clone_shares_state() {
        let c1 = HealthChecker::new();
        let c2 = c1.clone();

        c1.record_healthy("test").await;
        let h = c2.get("test").await.unwrap();
        assert_eq!(h.status, HealthStatus::Healthy);
    }

    #[tokio::test]
    async fn test_empty_summary() {
        let checker = HealthChecker::new();
        let summary = checker.summary().await;
        assert_eq!(summary.total, 0);
        assert_eq!(summary.overall, HealthStatus::Unknown);
    }
}
