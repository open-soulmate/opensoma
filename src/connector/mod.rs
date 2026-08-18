pub mod dingtalk;
pub mod email;
pub mod feishu;
pub mod git;
pub mod github;
pub mod notion;
pub mod obsidian;
pub mod rss;
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
pub async fn start_all(config: &ConnectorConfig, tx: EventTx) -> Result<JoinHandle<()>> {
    let config = config.clone();

    let handle = tokio::spawn(async move {
        let mut handles: Vec<JoinHandle<()>> = Vec::new();

        // Feishu connector
        if let Some(ref feishu_cfg) = config.feishu {
            if feishu_cfg.enabled {
                match feishu::start(feishu_cfg.clone(), tx.clone()).await {
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
                match dingtalk::start(dingtalk_cfg.clone(), tx.clone()).await {
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
                match wecom::start(wecom_cfg.clone(), tx.clone()).await {
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
                match rss::start(rss_cfg.clone(), tx.clone()).await {
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
                match email::start(email_cfg.clone(), tx.clone()).await {
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
                match webhook::start(webhook_cfg.clone(), tx.clone()).await {
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
                match github::start(github_cfg.clone(), tx.clone()).await {
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
                match notion::start(notion_cfg.clone(), tx.clone()).await {
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
                match git::start(git_cfg.clone(), tx.clone()).await {
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
                match obsidian::start(obsidian_cfg.clone(), tx.clone()).await {
                    Ok(h) => {
                        info!("Obsidian connector started.");
                        handles.push(h);
                    }
                    Err(e) => tracing::error!("Failed to start Obsidian connector: {}", e),
                }
            }
        }

        // Wait for all connector tasks
        for h in handles {
            let _ = h.await;
        }
    });

    Ok(handle)
}
