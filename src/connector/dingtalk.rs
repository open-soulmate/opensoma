use anyhow::Result;
use chrono::Utc;
use reqwest::Client;
use serde::Deserialize;
use tokio::task::JoinHandle;
use tracing::{error, info};
use uuid::Uuid;

use crate::collector::{EventTx, RawEvent};
use crate::config::DingtalkConfig;

#[derive(Debug, Deserialize)]
struct TokenResponse {
    access_token: String,
    #[allow(dead_code)]
    errcode: Option<i64>,
    #[allow(dead_code)]
    errmsg: Option<String>,
}

/// Start the DingTalk connector. Authenticates via OAuth and begins
/// polling or webhook listening for robot messages.
pub async fn start(config: DingtalkConfig, tx: EventTx) -> Result<JoinHandle<()>> {
    let http_client = Client::builder()
        .timeout(std::time::Duration::from_secs(30))
        .build()?;

    let token = fetch_access_token(&http_client, &config).await?;
    info!("DingTalk connector authenticated.");

    let handle = tokio::spawn(async move {
        let mut _current_token = token;
        // DingTalk tokens are valid for 7200s, refresh at 6000s
        let mut refresh_interval =
            tokio::time::interval(std::time::Duration::from_secs(6000));

        loop {
            tokio::select! {
                _ = refresh_interval.tick() => {
                    match fetch_access_token(&http_client, &config).await {
                        Ok(new_token) => {
                            _current_token = new_token;
                            info!("DingTalk access token refreshed.");
                        }
                        Err(e) => {
                            error!("Failed to refresh DingTalk token: {}", e);
                        }
                    }
                }
            }
        }
    });

    Ok(handle)
}

/// Fetch an access token from DingTalk.
async fn fetch_access_token(client: &Client, config: &DingtalkConfig) -> Result<String> {
    let url = format!(
        "https://oapi.dingtalk.com/gettoken?appkey={}&appsecret={}",
        config.app_key, config.app_secret
    );

    let resp = client.get(&url).send().await?.json::<TokenResponse>().await?;
    Ok(resp.access_token)
}

/// Send a message via DingTalk robot webhook.
pub async fn send_robot_message(
    client: &Client,
    webhook_url: &str,
    text: &str,
) -> Result<()> {
    let body = serde_json::json!({
        "msgtype": "text",
        "text": { "content": text }
    });

    client.post(webhook_url).json(&body).send().await?;
    Ok(())
}

/// Convert a DingTalk event into a RawEvent.
pub fn to_raw_event(payload: serde_json::Value) -> RawEvent {
    RawEvent {
        id: Uuid::new_v4().to_string(),
        source: "connector:dingtalk".to_string(),
        event_type: "message".to_string(),
        timestamp_ms: Utc::now().timestamp_millis(),
        payload: serde_json::to_vec(&payload).unwrap_or_default(),
        tags: {
            let mut m = std::collections::HashMap::new();
            m.insert("platform".to_string(), "dingtalk".to_string());
            m
        },
    }
}
