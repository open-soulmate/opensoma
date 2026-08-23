pub mod circuit_breaker;
pub mod rate_limiter;
pub mod discord;
pub mod teams;
pub mod dingtalk;
pub mod email;
pub mod feishu;
pub mod git;
pub mod github;
pub mod notion;
pub mod obsidian;
pub mod rss;
pub mod slack;
pub mod telegram;
pub mod webhook;
pub mod wecom;

use anyhow::Result;
use async_trait::async_trait;
use tokio::task::JoinHandle;
use tracing::info;

use crate::collector::EventTx;
use crate::config::ConnectorConfig;

/// Unified connector trait for health checking and identification.
/// Each connector implements this to provide:
/// - A human-readable name for logging and status
/// - A health-check that verifies connectivity to the data source
#[async_trait]
pub trait Connector: Send + Sync {
    /// Human-readable connector name (e.g. "dingtalk", "feishu", "github")
    fn name(&self) -> &str;

    /// Health check — verifies the connector can reach its data source.
    /// Returns Ok(()) if healthy, Err with details if not.
    async fn ping(&self) -> Result<()>;
}

/// Retry an async block with exponential backoff.
///
/// The body block must evaluate to `Result<T>`. Must be called from an async context.
///
/// # Usage
/// ```ignore
/// let token = retry_async!("dingtalk_token", 3, {
///     fetch_access_token(&client, &config).await
/// })?;
/// ```
#[macro_export]
macro_rules! retry_async {
    ($label:expr, $max:expr, { $($body:tt)* }) => {{
        let mut _retry_last_err: Option<anyhow::Error> = None;
        let mut _retry_result: Option<_> = None;
        for _retry_attempt in 0u32..$max {
            match { $($body)* } {
                Ok(val) => { _retry_result = Some(val); break; }
                Err(e) => {
                    let delay_ms = 500u64 * 2u64.pow(_retry_attempt);
                    tracing::warn!(
                        "{} failed (attempt {}/{}): {}; retrying in {}ms",
                        $label, _retry_attempt + 1, $max, e, delay_ms
                    );
                    tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
                    _retry_last_err = Some(e.into());
                }
            }
        }
        match _retry_result {
            Some(val) => Ok(val),
            None => Err(_retry_last_err.unwrap_or_else(|| anyhow::anyhow!("{}: no attempts made", $label))),
        }
    }};
}

/// Retry delay for manual retry loops (exponential backoff starting at 500ms).
pub fn retry_delay(attempt: u32) -> std::time::Duration {
    std::time::Duration::from_millis(500 * 2u64.pow(attempt))
}

/// Start all enabled connectors. Each connector runs its own HTTP server or
/// polling loop, forwarding events into the shared collector channel.
///
/// When a `HealthChecker` is provided, a background health-check loop is also
/// spawned that periodically pings each enabled connector and records the
/// result.  This feeds the `/api/health/connectors` status-server endpoint.
pub async fn start_all(
    config: &ConnectorConfig,
    tx: EventTx,
    health_checker: Option<crate::health::HealthChecker>,
    circuit_breakers: Option<circuit_breaker::CircuitBreakerRegistry>,
) -> Result<JoinHandle<()>> {
    let config = config.clone();

    // Collect connector names for background health checking
    let connector_names: Vec<String> = build_enabled_connector_names(&config);
    // Clone config for the health-check loop (the main handle consumes `config`)
    let health_config = config.clone();

    let handle = tokio::spawn(async move {
        let mut handles: Vec<JoinHandle<()>> = Vec::new();

        // Helper to extract circuit breaker from registry
        let get_cb = |name: &str| -> Option<circuit_breaker::CircuitBreaker> {
            circuit_breakers.as_ref().and_then(|r| r.get(name).cloned())
        };

        // Feishu connector
        if let Some(ref feishu_cfg) = config.feishu {
            if feishu_cfg.enabled {
                match feishu::start(feishu_cfg.clone(), tx.clone(), get_cb("feishu")).await {
                    Ok(h) => {
                        info!("Feishu connector started.");
                        handles.push(h);
                    }
                    Err(e) => tracing::error!("Failed to start Feishu connector: {}", e),
                }
            }
        }

        // DingTalk connector
        if let Some(ref dingtalk_cfg) = config.dingtalk {
            if dingtalk_cfg.enabled {
                match dingtalk::start(dingtalk_cfg.clone(), tx.clone(), get_cb("dingtalk")).await {
                    Ok(h) => {
                        info!("DingTalk connector started.");
                        handles.push(h);
                    }
                    Err(e) => tracing::error!("Failed to start DingTalk connector: {}", e),
                }
            }
        }

        // WeCom connector
        if let Some(ref wecom_cfg) = config.wecom {
            if wecom_cfg.enabled {
                match wecom::start(wecom_cfg.clone(), tx.clone(), get_cb("wecom")).await {
                    Ok(h) => {
                        info!("WeCom connector started.");
                        handles.push(h);
                    }
                    Err(e) => tracing::error!("Failed to start WeCom connector: {}", e),
                }
            }
        }

        // RSS connector
        if let Some(ref rss_cfg) = config.rss {
            if rss_cfg.enabled {
                match rss::start(rss_cfg.clone(), tx.clone(), get_cb("rss")).await {
                    Ok(h) => {
                        info!("RSS connector started.");
                        handles.push(h);
                    }
                    Err(e) => tracing::error!("Failed to start RSS connector: {}", e),
                }
            }
        }

        // Email connector
        if let Some(ref email_cfg) = config.email {
            if email_cfg.enabled {
                match email::start(email_cfg.clone(), tx.clone(), get_cb("email")).await {
                    Ok(h) => {
                        info!("Email connector started.");
                        handles.push(h);
                    }
                    Err(e) => tracing::error!("Failed to start Email connector: {}", e),
                }
            }
        }

        // Webhook connector
        if let Some(ref webhook_cfg) = config.webhook {
            if webhook_cfg.enabled {
                match webhook::start(webhook_cfg.clone(), tx.clone(), get_cb("webhook")).await {
                    Ok(h) => {
                        info!("Webhook connector started.");
                        handles.push(h);
                    }
                    Err(e) => tracing::error!("Failed to start Webhook connector: {}", e),
                }
            }
        }

        // GitHub connector
        if let Some(ref github_cfg) = config.github {
            if github_cfg.enabled {
                match github::start(github_cfg.clone(), tx.clone(), get_cb("github")).await {
                    Ok(h) => {
                        info!("GitHub connector started.");
                        handles.push(h);
                    }
                    Err(e) => tracing::error!("Failed to start GitHub connector: {}", e),
                }
            }
        }

        // Notion connector
        if let Some(ref notion_cfg) = config.notion {
            if notion_cfg.enabled {
                match notion::start(notion_cfg.clone(), tx.clone(), get_cb("notion")).await {
                    Ok(h) => {
                        info!("Notion connector started.");
                        handles.push(h);
                    }
                    Err(e) => tracing::error!("Failed to start Notion connector: {}", e),
                }
            }
        }

        // Git connector
        if let Some(ref git_cfg) = config.git {
            if git_cfg.enabled {
                match git::start(git_cfg.clone(), tx.clone(), get_cb("git")).await {
                    Ok(h) => {
                        info!("Git connector started.");
                        handles.push(h);
                    }
                    Err(e) => tracing::error!("Failed to start Git connector: {}", e),
                }
            }
        }

        // Obsidian connector
        if let Some(ref obsidian_cfg) = config.obsidian {
            if obsidian_cfg.enabled {
                match obsidian::start(obsidian_cfg.clone(), tx.clone(), get_cb("obsidian")).await {
                    Ok(h) => {
                        info!("Obsidian connector started.");
                        handles.push(h);
                    }
                    Err(e) => tracing::error!("Failed to start Obsidian connector: {}", e),
                }
            }
        }

        // Slack connector
        if let Some(ref slack_cfg) = config.slack {
            if slack_cfg.enabled {
                match slack::start(slack_cfg.clone(), tx.clone(), get_cb("slack")).await {
                    Ok(h) => {
                        info!("Slack connector started.");
                        handles.push(h);
                    }
                    Err(e) => tracing::error!("Failed to start Slack connector: {}", e),
                }
            }
        }

        // Telegram connector
        if let Some(ref telegram_cfg) = config.telegram {
            if telegram_cfg.enabled {
                match telegram::start(telegram_cfg.clone(), tx.clone(), get_cb("telegram")).await {
                    Ok(h) => {
                        info!("Telegram connector started.");
                        handles.push(h);
                    }
                    Err(e) => tracing::error!("Failed to start Telegram connector: {}", e),
                }
            }
        }

        // Discord connector
        if let Some(ref discord_cfg) = config.discord {
            if discord_cfg.enabled {
                match discord::start(discord_cfg.clone(), tx.clone(), get_cb("discord")).await {
                    Ok(h) => {
                        info!("Discord connector started.");
                        handles.push(h);
                    }
                    Err(e) => tracing::error!("Failed to start Discord connector: {}", e),
                }
            }
        }

        // Teams connector
        if let Some(ref teams_cfg) = config.teams {
            if teams_cfg.enabled {
                match teams::start(teams_cfg.clone(), tx.clone(), get_cb("teams")).await {
                    Ok(h) => {
                        info!("Teams connector started.");
                        handles.push(h);
                    }
                    Err(e) => tracing::error!("Failed to start Teams connector: {}", e),
                }
            }
        }

        // Wait for all connector tasks
        for h in handles {
            let _ = h.await;
        }
    });

    // Spawn background health-check loop when a HealthChecker is provided
    if let Some(checker) = health_checker {
        if !connector_names.is_empty() {
            let names = connector_names;
            let cfg = health_config;
            tokio::spawn(async move {
                // Wait a bit before first health check to let connectors initialize
                tokio::time::sleep(std::time::Duration::from_secs(10)).await;
                info!(
                    "Connector health-check loop started — {} connectors, interval=60s",
                    names.len()
                );

                let mut interval =
                    tokio::time::interval(std::time::Duration::from_secs(60));
                interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

                loop {
                    interval.tick().await;
                    for name in &names {
                        let result =
                            ping_connector_by_name(name, &cfg).await;
                        match result {
                            Ok(()) => checker.record_healthy(name).await,
                            Err(e) => {
                                checker
                                    .record_unhealthy(name, &e.to_string())
                                    .await
                            }
                        }
                    }
                }
            });
        }
    }

    Ok(handle)
}

/// Build a list of enabled connector names from config (for health tracking).
fn build_enabled_connector_names(config: &ConnectorConfig) -> Vec<String> {
    let mut names = Vec::new();
    if config.feishu.as_ref().is_some_and(|c| c.enabled) {
        names.push("feishu".to_string());
    }
    if config.dingtalk.as_ref().is_some_and(|c| c.enabled) {
        names.push("dingtalk".to_string());
    }
    if config.wecom.as_ref().is_some_and(|c| c.enabled) {
        names.push("wecom".to_string());
    }
    if config.rss.as_ref().is_some_and(|c| c.enabled) {
        names.push("rss".to_string());
    }
    if config.email.as_ref().is_some_and(|c| c.enabled) {
        names.push("email".to_string());
    }
    if config.webhook.as_ref().is_some_and(|c| c.enabled) {
        names.push("webhook".to_string());
    }
    if config.github.as_ref().is_some_and(|c| c.enabled) {
        names.push("github".to_string());
    }
    if config.notion.as_ref().is_some_and(|c| c.enabled) {
        names.push("notion".to_string());
    }
    if config.git.as_ref().is_some_and(|c| c.enabled) {
        names.push("git".to_string());
    }
    if config.obsidian.as_ref().is_some_and(|c| c.enabled) {
        names.push("obsidian".to_string());
    }
    if config.slack.as_ref().is_some_and(|c| c.enabled) {
        names.push("slack".to_string());
    }
    if config.telegram.as_ref().is_some_and(|c| c.enabled) {
        names.push("telegram".to_string());
    }
    if config.discord.as_ref().is_some_and(|c| c.enabled) {
        names.push("discord".to_string());
    }
    if config.teams.as_ref().is_some_and(|c| c.enabled) {
        names.push("teams".to_string());
    }
    names
}

/// Ping a connector by name using its `Connector::ping()` implementation.
///
/// This instantiates a temporary connector object purely for the health probe
/// so we don't need to hold references to the running connector instances.
pub async fn ping_connector_by_name(
    name: &str,
    config: &ConnectorConfig,
) -> Result<()> {
    match name {
        "feishu" => {
            if let Some(ref cfg) = config.feishu {
                let c = feishu::FeishuConnector::new(cfg.clone());
                return c.ping().await;
            }
        }
        "dingtalk" => {
            if let Some(ref cfg) = config.dingtalk {
                let c = dingtalk::DingtalkConnector::new(cfg.clone())
                    .map_err(|e| anyhow::anyhow!("DingtalkConnector init: {}", e))?;
                return c.ping().await;
            }
        }
        "wecom" => {
            if let Some(ref cfg) = config.wecom {
                let c = wecom::WecomConnector::new(cfg.clone());
                return c.ping().await;
            }
        }
        "rss" => {
            if let Some(ref cfg) = config.rss {
                let c = rss::RssConnector::new(cfg.clone());
                return c.ping().await;
            }
        }
        "email" => {
            if let Some(ref cfg) = config.email {
                let c = email::EmailConnector::new(cfg.clone());
                return c.ping().await;
            }
        }
        "webhook" => {
            if let Some(ref cfg) = config.webhook {
                let c = webhook::WebhookConnector::new(cfg.clone());
                return c.ping().await;
            }
        }
        "github" => {
            if let Some(ref cfg) = config.github {
                let c = github::GitHubConnector::new(cfg.clone());
                return c.ping().await;
            }
        }
        "notion" => {
            if let Some(ref cfg) = config.notion {
                let c = notion::NotionConnector::new(cfg.clone());
                return c.ping().await;
            }
        }
        "git" => {
            if let Some(ref cfg) = config.git {
                let c = git::GitConnector::new(cfg.clone());
                return c.ping().await;
            }
        }
        "obsidian" => {
            if let Some(ref cfg) = config.obsidian {
                let c = obsidian::ObsidianConnector::new(cfg.clone());
                return c.ping().await;
            }
        }
        "slack" => {
            if let Some(ref cfg) = config.slack {
                let c = slack::SlackConnector::new(cfg.clone());
                return c.ping().await;
            }
        }
        "telegram" => {
            if let Some(ref cfg) = config.telegram {
                let c = telegram::TelegramConnector::new(cfg.clone());
                return c.ping().await;
            }
        }
        "discord" => {
            if let Some(ref cfg) = config.discord {
                let c = discord::DiscordConnector::new(cfg.clone());
                return c.ping().await;
            }
        }
        "teams" => {
            if let Some(ref cfg) = config.teams {
                let c = teams::TeamsConnector::new(cfg.clone());
                return c.ping().await;
            }
        }
        _ => {}
    }
    anyhow::bail!("Connector '{}' not found in config", name)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_retry_delay_exponential_backoff() {
        let d0 = retry_delay(0);
        let d1 = retry_delay(1);
        let d2 = retry_delay(2);
        let d3 = retry_delay(3);

        // 500 * 2^attempt
        assert_eq!(d0.as_millis(), 500);
        assert_eq!(d1.as_millis(), 1000);
        assert_eq!(d2.as_millis(), 2000);
        assert_eq!(d3.as_millis(), 4000);
    }

    #[tokio::test]
    async fn test_retry_async_succeeds_first_try() {
        let result: anyhow::Result<i32> =
            retry_async!("test_op", 3, { Ok::<i32, anyhow::Error>(42) });
        assert_eq!(result.unwrap(), 42);
    }

    #[tokio::test]
    async fn test_retry_async_succeeds_after_retries() {
        use std::sync::atomic::{AtomicU32, Ordering};
        use std::sync::Arc;
        let attempts = Arc::new(AtomicU32::new(0));
        let attempts_clone = attempts.clone();

        let result: anyhow::Result<i32> = retry_async!("test_op", 3, {
            let n = attempts_clone.fetch_add(1, Ordering::SeqCst);
            if n < 2 {
                Err(anyhow::anyhow!("not yet"))
            } else {
                Ok::<i32, anyhow::Error>(99)
            }
        });
        assert_eq!(result.unwrap(), 99);
        assert_eq!(attempts.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn test_retry_async_exhausts_retries() {
        let result: anyhow::Result<i32> =
            retry_async!("test_op", 2, { Err(anyhow::anyhow!("permanent failure")) });
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("permanent failure"));
    }

    #[test]
    fn test_retry_delay_zero_attempt() {
        let d = retry_delay(0);
        assert_eq!(d.as_millis(), 500);
    }

    #[test]
    fn test_retry_delay_large_attempt() {
        let d = retry_delay(10);
        // 500 * 2^10 = 512000ms
        assert_eq!(d.as_millis(), 512000);
    }

    #[test]
    fn test_retry_delay_monotonically_increasing() {
        for i in 0..8 {
            assert!(retry_delay(i) < retry_delay(i + 1));
        }
    }

    #[tokio::test]
    async fn test_retry_async_succeeds_first_try_value() {
        let result: anyhow::Result<String> = retry_async!("test_string", 3, {
            Ok::<String, anyhow::Error>("hello".to_string())
        });
        assert_eq!(result.unwrap(), "hello");
    }

    #[tokio::test]
    async fn test_retry_async_single_attempt_success() {
        let result: anyhow::Result<i32> =
            retry_async!("single", 1, { Ok::<i32, anyhow::Error>(7) });
        assert_eq!(result.unwrap(), 7);
    }

    #[tokio::test]
    async fn test_retry_async_single_attempt_failure() {
        let result: anyhow::Result<i32> =
            retry_async!("single_fail", 1, { Err(anyhow::anyhow!("oops")) });
        assert!(result.is_err());
    }

    // ── Health-check integration tests ────────────────────────────

    #[test]
    fn test_build_enabled_connector_names_all_disabled() {
        let config = ConnectorConfig {
            feishu: None,
            dingtalk: None,
            wecom: None,
            rss: None,
            email: None,
            webhook: None,
            github: None,
            notion: None,
            git: None,
            obsidian: None,
            slack: None,
            telegram: None,
            discord: None,
            teams: None,
        };
        let names = build_enabled_connector_names(&config);
        assert!(names.is_empty());
    }

    #[test]
    fn test_build_enabled_connector_names_some_enabled() {
        let config = ConnectorConfig {
            feishu: Some(crate::config::FeishuConfig {
                enabled: true,
                app_id: "id".into(),
                app_secret: "secret".into(),
                webhook_path: "/hook".into(),
                folder_token: None,
            }),
            dingtalk: None,
            wecom: None,
            rss: Some(crate::config::RssConfig {
                enabled: true,
                feeds: vec![],
                poll_interval_secs: 60,
            }),
            email: None,
            webhook: None,
            github: None,
            notion: None,
            git: None,
            obsidian: None,
            slack: None,
            telegram: None,
            discord: None,
            teams: None,
        };
        let names = build_enabled_connector_names(&config);
        assert_eq!(names.len(), 2);
        assert!(names.contains(&"feishu".to_string()));
        assert!(names.contains(&"rss".to_string()));
    }

    #[test]
    fn test_build_enabled_connector_names_disabled_not_included() {
        let config = ConnectorConfig {
            feishu: Some(crate::config::FeishuConfig {
                enabled: false,
                app_id: "id".into(),
                app_secret: "secret".into(),
                webhook_path: "/hook".into(),
                folder_token: None,
            }),
            dingtalk: None,
            wecom: None,
            rss: None,
            email: None,
            webhook: None,
            github: None,
            notion: None,
            git: None,
            obsidian: None,
            slack: None,
            telegram: None,
            discord: None,
            teams: None,
        };
        let names = build_enabled_connector_names(&config);
        assert!(names.is_empty());
    }

    #[tokio::test]
    async fn test_ping_connector_unknown_name() {
        let config = ConnectorConfig {
            feishu: None,
            dingtalk: None,
            wecom: None,
            rss: None,
            email: None,
            webhook: None,
            github: None,
            notion: None,
            git: None,
            obsidian: None,
            slack: None,
            telegram: None,
            discord: None,
            teams: None,
        };
        let result = ping_connector_by_name("nonexistent", &config).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("not found"));
    }
}
