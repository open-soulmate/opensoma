use anyhow::Result;
use chrono::Utc;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use tokio::task::JoinHandle;
use tracing::{error, info, warn};
use uuid::Uuid;

use crate::collector::{EventTx, RawEvent};
use crate::config::FeishuConfig;

/// Feishu API tenant access token response.
#[derive(Debug, Deserialize)]
struct TokenResponse {
    tenant_access_token: String,
    #[allow(dead_code)]
    expire: u64,
}

/// Feishu event callback body (simplified).
#[derive(Debug, Deserialize)]
struct FeishuEvent {
    #[serde(rename = "type")]
    event_type: Option<String>,
    event: Option<serde_json::Value>,
    header: Option<serde_json::Value>,
}

/// Start the Feishu connector. Authenticates with Feishu API and begins
/// polling for messages or setting up a webhook listener.
pub async fn start(config: FeishuConfig, tx: EventTx) -> Result<JoinHandle<()>> {
    let http_client = Client::builder()
        .timeout(std::time::Duration::from_secs(30))
        .build()?;

    // Fetch initial access token
    let token = fetch_tenant_token(&http_client, &config).await?;
    info!("Feishu connector authenticated.");

    let handle = tokio::spawn(async move {
        let mut current_token = token;
        let mut refresh_interval =
            tokio::time::interval(std::time::Duration::from_secs(7000)); // Token valid ~2h

        loop {
            tokio::select! {
                _ = refresh_interval.tick() => {
                    match fetch_tenant_token(&http_client, &config).await {
                        Ok(new_token) => {
                            current_token = new_token;
                            info!("Feishu access token refreshed.");
                        }
                        Err(e) => {
                            error!("Failed to refresh Feishu token: {}", e);
                        }
                    }
                }
            }
        }
    });

    Ok(handle)
}

/// Fetch a tenant access token from Feishu.
async fn fetch_tenant_token(client: &Client, config: &FeishuConfig) -> Result<String> {
    let url = "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal";
    let body = serde_json::json!({
        "app_id": config.app_id,
        "app_secret": config.app_secret,
    });

    let resp = client
        .post(url)
        .json(&body)
        .send()
        .await?
        .json::<TokenResponse>()
        .await?;

    Ok(resp.tenant_access_token)
}

/// Convert a Feishu event into a RawEvent for the collector pipeline.
pub fn to_raw_event(event: FeishuEvent) -> RawEvent {
    RawEvent {
        id: Uuid::new_v4().to_string(),
        source: "connector:feishu".to_string(),
        event_type: "message".to_string(),
        timestamp_ms: Utc::now().timestamp_millis(),
        payload: serde_json::to_vec(&event).unwrap_or_default(),
        tags: {
            let mut m = std::collections::HashMap::new();
            m.insert("platform".to_string(), "feishu".to_string());
            m
        },
    }
}
