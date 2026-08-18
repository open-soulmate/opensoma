use anyhow::Result;
use async_trait::async_trait;
use chrono::Utc;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use tokio::task::JoinHandle;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

use crate::collector::{EventTx, RawEvent};
use crate::connector::Connector;
use crate::config::DingtalkConfig;
use crate::retry_async;

#[derive(Debug, Deserialize)]
struct TokenResponse {
    access_token: String,
    #[allow(dead_code)]
    errcode: Option<i64>,
    #[allow(dead_code)]
    errmsg: Option<String>,
}

/// DingTalk approval process instance list response.
#[derive(Debug, Deserialize)]
struct ApprovalListResponse {
    #[serde(default)]
    result: ApprovalResult,
    #[serde(default)]
    errcode: Option<i64>,
    #[serde(default)]
    errmsg: Option<String>,
}

#[derive(Debug, Deserialize, Default)]
struct ApprovalResult {
    #[serde(default)]
    list: Vec<ApprovalInstance>,
    #[serde(default)]
    next_cursor: i64,
    #[serde(default)]
    has_more: bool,
}

/// A single DingTalk approval process instance.
#[derive(Debug, Deserialize, Serialize)]
struct ApprovalInstance {
    #[serde(default)]
    process_instance_id: String,
    #[serde(default)]
    title: String,
    #[serde(default)]
    status: String,
    #[serde(default)]
    originator_userid: String,
    #[serde(default)]
    create_time: i64,
    #[serde(default)]
    finish_time: i64,
    #[serde(default)]
    business_id: String,
}

/// DingTalk work notification response (topapi/message/corpconversation).
#[derive(Debug, Deserialize)]
struct WorkNotificationResponse {
    #[serde(default)]
    errcode: Option<i64>,
    #[serde(default)]
    errmsg: Option<String>,
}

/// DingTalk robot messages are received via callbacks. For polling mode,
/// we check the async send result of previously sent messages.
#[derive(Debug, Deserialize)]
struct SendResultResponse {
    #[serde(default)]
    errcode: Option<i64>,
    #[serde(default)]
    errmsg: Option<String>,
    #[serde(default)]
    send_result: Option<SendResult>,
}

#[derive(Debug, Deserialize)]
struct SendResult {
    #[serde(default)]
    invalid_user_id_list: Vec<String>,
    #[serde(default)]
    forbidden_user_id_list: Vec<String>,
    #[serde(default)]
    failed_user_id_list: Vec<String>,
}

/// Start the DingTalk connector. Authenticates via OAuth, then polls
/// approval process instances and forwards them as events.
pub async fn start(config: DingtalkConfig, tx: EventTx) -> Result<JoinHandle<()>> {
    let http_client = Client::builder()
        .timeout(std::time::Duration::from_secs(30))
        .build()?;

    let token = retry_async!("dingtalk_token", 3, {
        fetch_access_token(&http_client, &config).await
    })?;
    info!("DingTalk connector authenticated.");

    let poll_secs = config.poll_interval_secs;

    let handle = tokio::spawn(async move {
        let mut current_token = token;
        let mut token_refresh = tokio::time::interval(std::time::Duration::from_secs(6000));
        let mut poll_interval = tokio::time::interval(std::time::Duration::from_secs(poll_secs));
        poll_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        // Track seen instance IDs to avoid duplicate events
        let mut seen_approvals: std::collections::HashSet<String> = std::collections::HashSet::new();

        loop {
            tokio::select! {
                _ = token_refresh.tick() => {
                    match fetch_access_token(&http_client, &config).await {
                        Ok(new_token) => {
                            current_token = new_token;
                            info!("DingTalk access token refreshed.");
                        }
                        Err(e) => {
                            error!("Failed to refresh DingTalk token: {}", e);
                        }
                    }
                }
                _ = poll_interval.tick() => {
                    // Poll approval process instances
                    match fetch_approval_list(&http_client, &current_token).await {
                        Ok(instances) => {
                            debug!("Fetched {} DingTalk approval instances", instances.len());
                            for inst in instances {
                                if seen_approvals.contains(&inst.process_instance_id) {
                                    continue;
                                }
                                seen_approvals.insert(inst.process_instance_id.clone());

                                let raw_event = to_raw_event(&inst);
                                match tx.try_send(raw_event) {
                                    Ok(()) => {
                                        debug!("Forwarded DingTalk approval: {}", inst.title);
                                    }
                                    Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                                        warn!("Event channel full, dropping DingTalk approval: {}", inst.title);
                                    }
                                    Err(e) => {
                                        error!("Failed to send DingTalk event: {}", e);
                                    }
                                }
                            }
                        }
                        Err(e) => {
                            warn!("Failed to fetch DingTalk approvals: {}", e);
                        }
                    }

                    // Keep the seen set from growing unbounded (keep last 10000)
                    if seen_approvals.len() > 10000 {
                        let excess = seen_approvals.len() - 5000;
                        let to_remove: Vec<String> = seen_approvals.iter().take(excess).cloned().collect();
                        for id in to_remove {
                            seen_approvals.remove(&id);
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
    if let Some(code) = resp.errcode {
        if code != 0 {
            anyhow::bail!("DingTalk token error {}: {:?}", code, resp.errmsg);
        }
    }
    Ok(resp.access_token)
}

/// Fetch approval process instances from DingTalk.
/// Uses the topapi/processinstance/list API.
async fn fetch_approval_list(
    client: &Client,
    token: &str,
) -> Result<Vec<ApprovalInstance>> {
    let url = format!(
        "https://oapi.dingtalk.com/topapi/processinstance/list?access_token={}",
        token
    );

    // Query approval instances from the last 24 hours
    let now_ms = Utc::now().timestamp_millis();
    let day_ago_ms = now_ms - 86_400_000;

    let body = serde_json::json!({
        "start_time": day_ago_ms,
        "end_time": now_ms,
        "size": 100,
        "cursor": 0,
    });

    let resp = client
        .post(&url)
        .json(&body)
        .send()
        .await?
        .json::<ApprovalListResponse>()
        .await?;

    if let Some(code) = resp.errcode {
        if code != 0 {
            anyhow::bail!("DingTalk approval list error {}: {:?}", code, resp.errmsg);
        }
    }

    Ok(resp.result.list)
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

/// Convert a DingTalk approval instance into a RawEvent.
fn to_raw_event(inst: &ApprovalInstance) -> RawEvent {
    let mut tags = std::collections::HashMap::new();
    tags.insert("platform".to_string(), "dingtalk".to_string());
    tags.insert("type".to_string(), "approval".to_string());
    tags.insert("status".to_string(), inst.status.clone());
    tags.insert("instance_id".to_string(), inst.process_instance_id.clone());
    tags.insert("title".to_string(), inst.title.clone());

    let payload = serde_json::to_vec(&serde_json::json!({
        "process_instance_id": inst.process_instance_id,
        "title": inst.title,
        "status": inst.status,
        "originator_userid": inst.originator_userid,
        "create_time": inst.create_time,
        "finish_time": inst.finish_time,
        "business_id": inst.business_id,
    }))
    .unwrap_or_default();

    RawEvent {
        id: Uuid::new_v4().to_string(),
        source: format!("connector:dingtalk:approval:{}", inst.process_instance_id),
        event_type: "approval".to_string(),
        timestamp_ms: Utc::now().timestamp_millis(),
        payload,
        tags,
    }
}

/// DingTalk connector implementing the unified Connector trait.
pub struct DingtalkConnector {
    config: DingtalkConfig,
    client: Client,
}

impl DingtalkConnector {
    pub fn new(config: DingtalkConfig) -> Result<Self> {
        let client = Client::builder()
            .timeout(std::time::Duration::from_secs(30))
            .build()?;
        Ok(Self { config, client })
    }
}

#[async_trait]
impl Connector for DingtalkConnector {
    fn name(&self) -> &str { "dingtalk" }

    async fn ping(&self) -> Result<()> {
        fetch_access_token(&self.client, &self.config).await?;
        Ok(())
    }
}

/// Convert a raw DingTalk callback payload into a RawEvent.
pub fn to_raw_event_from_callback(payload: serde_json::Value) -> RawEvent {
    let event_type = payload
        .get("EventType")
        .or_else(|| payload.get("eventType"))
        .and_then(|v| v.as_str())
        .unwrap_or("callback")
        .to_string();

    RawEvent {
        id: Uuid::new_v4().to_string(),
        source: "connector:dingtalk:callback".to_string(),
        event_type,
        timestamp_ms: Utc::now().timestamp_millis(),
        payload: serde_json::to_vec(&payload).unwrap_or_default(),
        tags: {
            let mut m = std::collections::HashMap::new();
            m.insert("platform".to_string(), "dingtalk".to_string());
            m
        },
    }
}
