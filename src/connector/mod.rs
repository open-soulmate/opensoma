pub mod feishu;
pub mod dingtalk;
pub mod wecom;

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

        // Wait for all connector tasks
        for h in handles {
            let _ = h.await;
        }
    });

    Ok(handle)
}
