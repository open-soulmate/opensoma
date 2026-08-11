use anyhow::Result;
use chrono::Utc;
use reqwest::Client;
use serde::Deserialize;
use tokio::task::JoinHandle;
use tracing::{error, info};
use uuid::Uuid;

use crate::collector::{EventTx, RawEvent};
use crate::config::WecomConfig;

#[derive(Debug, Deserialize)]
struct TokenResponse {
    access_token: String,
    #[allow(dead_code)]
    expires_in: u64,
    #[allow(dead_code)]
    errcode: Option<i64>,
    #[allow(dead_code)]
    errmsg: Option<String>,
}

/// Start the WeCom (企业微信) connector. Fetches access token and
/// begins polling for callback events.
pub async fn start(config: WecomConfig, tx: EventTx) -> Result<JoinHandle<()>> {
    let http_client = Client::builder()
        .timeout(std::time::Duration::from_secs(30))
        .build()?;

    let token = fetch_access_token(&http_client, &config).await?;
    info!("WeCom connector authenticated.");

    let handle = tokio::spawn(async move {
        let mut _current_token = token;
        // WeCom tokens are valid for 7200s
        let mut refresh_interval =
            tokio::time::interval(std::time::Duration::from_secs(6000));

        loop {
            tokio::select! {
                _ = refresh_interval.tick() => {
                    match fetch_access_token(&http_client, &config).await {
                        Ok(new_token) => {
                            _current_token = new_token;
                            info!("WeCom access token refreshed.");
                        }
                        Err(e) => {
                            error!("Failed to refresh WeCom token: {}", e);
                        }
                    }
                }
            }
        }
    });

    Ok(handle)
}

/// Fetch an access token from WeCom.
async fn fetch_access_token(client: &Client, config: &WecomConfig) -> Result<String> {
    let url = format!(
        "https://qyapi.weixin.qq.com/cgi-bin/gettoken?corpid={}&corpsecret={}",
        config.corp_id, config.secret
    );

    let resp = client.get(&url).send().await?.json::<TokenResponse>().await?;
    Ok(resp.access_token)
}

/// Send a message via WeCom application API.
pub async fn send_app_message(
    client: &Client,
    token: &str,
    agent_id: &str,
    user_id: &str,
    content: &str,
) -> Result<()> {
    let url = format!(
        "https://qyapi.weixin.qq.com/cgi-bin/message/send?access_token={}",
        token
    );
    let body = serde_json::json!({
        "touser": user_id,
        "msgtype": "text",
        "agentid": agent_id,
        "text": { "content": content }
    });

    client.post(&url).json(&body).send().await?;
    Ok(())
}

/// Convert a WeCom event into a RawEvent.
pub fn to_raw_event(payload: serde_json::Value) -> RawEvent {
    RawEvent {
        id: Uuid::new_v4().to_string(),
        source: "connector:wecom".to_string(),
        event_type: "message".to_string(),
        timestamp_ms: Utc::now().timestamp_millis(),
        payload: serde_json::to_vec(&payload).unwrap_or_default(),
        tags: {
            let mut m = std::collections::HashMap::new();
            m.insert("platform".to_string(), "wecom".to_string());
            m
        },
    }
}
