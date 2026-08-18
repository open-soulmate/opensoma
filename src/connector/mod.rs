pub mod feishu;
pub mod dingtalk;
pub mod wecom;
pub mod notion;
pub mod git;
pub mod obsidian;
pub mod rss;
pub mod email;
pub mod webhook;
pub mod github;

use anyhow::Result;
use tokio::task::JoinHandle;
use tracing::info;

use crate::collector::EventTx;
use crate::config::ConnectorConfig;

/// Start all enabled connectors. Each connector runs its own HTTP server or
/// polling loop, forwarding events into the shared collector channel.
pub async fn start_all(
    config: &ConnectorConfig,
    tx: EventTx,
) -> Result<JoinHandle<()>> {
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
