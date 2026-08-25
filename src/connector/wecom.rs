use anyhow::Result;
use chrono::Utc;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use tokio::task::JoinHandle;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

use crate::collector::{EventTx, RawEvent};
use crate::config::WecomConfig;
use crate::connector::circuit_breaker::CircuitBreaker;
use crate::connector::Connector;

/// WeCom connector implementing the unified Connector trait.
pub struct WecomConnector {
    config: WecomConfig,
}

impl WecomConnector {
    pub fn new(config: WecomConfig) -> Self {
        Self { config }
    }
}

#[async_trait::async_trait]
impl Connector for WecomConnector {
    fn name(&self) -> &str {
        "wecom"
    }

    async fn ping(&self) -> Result<()> {
        let client = Client::builder()
            .timeout(std::time::Duration::from_secs(10))
            .build()?;
        fetch_access_token(&client, &self.config).await?;
        Ok(())
    }
}

#[derive(Debug, Deserialize)]
struct TokenResponse {
    access_token: String,
    #[allow(dead_code)]
    expires_in: u64,
    #[serde(default)]
    errcode: Option<i64>,
    #[serde(default)]
    errmsg: Option<String>,
}

/// WeCom message list response for getting received messages.
#[derive(Debug, Deserialize)]
struct MessageListResponse {
    #[serde(default)]
    errcode: Option<i64>,
    #[serde(default)]
    errmsg: Option<String>,
    #[serde(default)]
    result: Option<MessageResult>,
}

#[derive(Debug, Deserialize, Default)]
struct MessageResult {
    #[serde(default)]
    msg_list: Vec<WeChatMessage>,
}

/// A single WeCom received message.
#[derive(Debug, Deserialize, Serialize)]
struct WeChatMessage {
    #[serde(default)]
    msgid: String,
    #[serde(default)]
    msg_type: String,
    #[serde(default)]
    content: Option<String>,
    #[serde(default)]
    from_user: String,
    #[serde(default)]
    create_time: i64,
    #[serde(default)]
    agentid: Option<i64>,
}

/// WeCom external contact list response.
#[allow(dead_code)]
#[derive(Debug, Deserialize)]
struct ExternalContactListResponse {
    #[serde(default)]
    errcode: Option<i64>,
    #[serde(default)]
    errmsg: Option<String>,
    #[serde(default)]
    external_userid: Vec<String>,
}

/// Start the WeCom connector. Authenticates via API, then
/// polls for received messages and forwards them as events.
pub async fn start(config: WecomConfig, tx: EventTx, circuit_breaker: Option<CircuitBreaker>) -> Result<JoinHandle<()>> {
    let http_client = Client::builder()
        .timeout(std::time::Duration::from_secs(30))
        .build()?;

    let token = fetch_access_token(&http_client, &config).await?;
    info!("WeCom connector authenticated.");

    let poll_secs = config.poll_interval_secs;

    let handle = tokio::spawn(async move {
        let cb = circuit_breaker;
        let mut current_token = token;
        let mut token_refresh = tokio::time::interval(std::time::Duration::from_secs(6000));
        let mut poll_interval = tokio::time::interval(std::time::Duration::from_secs(poll_secs));
        poll_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        // Track seen message IDs to avoid duplicates
        let mut seen_msgs: std::collections::HashSet<String> = std::collections::HashSet::new();

        loop {
            tokio::select! {
                _ = token_refresh.tick() => {
                    match fetch_access_token(&http_client, &config).await {
                        Ok(new_token) => {
                            current_token = new_token;
                            info!("WeCom access token refreshed.");
                            if let Some(ref c) = cb { c.record_success().await; }
                        }
                        Err(e) => {
                            error!("Failed to refresh WeCom token: {}", e);
                            if let Some(ref c) = cb { c.record_failure().await; }
                        }
                    }
                }
                _ = poll_interval.tick() => {
                    // Circuit breaker check
                    if let Some(ref c) = cb {
                        if c.allow_request().await.is_err() {
                            debug!("WeCom circuit breaker open — skipping poll cycle");
                            continue;
                        }
                    }
                    // Poll received messages for the agent
                    match fetch_received_messages(&http_client, &current_token, &config.agent_id).await {
                        Ok(messages) => {
                            debug!("Fetched {} WeCom messages", messages.len());
                            if let Some(ref c) = cb { c.record_success().await; }
                            for msg in messages {
                                if seen_msgs.contains(&msg.msgid) {
                                    continue;
                                }
                                seen_msgs.insert(msg.msgid.clone());

                                let raw_event = to_raw_event(&msg);
                                match tx.try_send(raw_event) {
                                    Ok(()) => {
                                        debug!("Forwarded WeCom msg: {} from {}", msg.msgid, msg.from_user);
                                    }
                                    Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                                        warn!("Event channel full, dropping WeCom msg: {}", msg.msgid);
                                    }
                                    Err(e) => {
                                        error!("Failed to send WeCom event: {}", e);
                                    }
                                }
                            }
                        }
                        Err(e) => {
                            warn!("Failed to fetch WeCom messages: {}", e);
                            if let Some(ref c) = cb { c.record_failure().await; }
                        }
                    }

                    // Keep the seen set from growing unbounded
                    if seen_msgs.len() > 10000 {
                        let excess = seen_msgs.len() - 5000;
                        let to_remove: Vec<String> = seen_msgs.iter().take(excess).cloned().collect();
                        for id in to_remove {
                            seen_msgs.remove(&id);
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

    let resp = client
        .get(&url)
        .send()
        .await?
        .json::<TokenResponse>()
        .await?;
    if let Some(code) = resp.errcode {
        if code != 0 {
            anyhow::bail!("WeCom token error {}: {:?}", code, resp.errmsg);
        }
    }
    Ok(resp.access_token)
}

/// Fetch received messages for a WeCom application.
/// Uses the /cgi-bin/message/list_msg API (enterprise internal API).
async fn fetch_received_messages(
    client: &Client,
    token: &str,
    _agent_id: &str,
) -> Result<Vec<WeChatMessage>> {
    let url = format!(
        "https://qyapi.weixin.qq.com/cgi-bin/message/list_msg?access_token={}",
        token
    );

    // Query messages from the last hour
    let now = Utc::now().timestamp();
    let hour_ago = now - 3600;

    let body = serde_json::json!({
        "chat_type": "single",
        "start_time": hour_ago,
        "end_time": now,
        "limit": 100,
        "cursor": "",
    });

    let resp = client
        .post(&url)
        .json(&body)
        .send()
        .await?
        .json::<MessageListResponse>()
        .await?;

    if let Some(code) = resp.errcode {
        if code != 0 {
            // Error code 45028 means no message access permission (common for some app types)
            // Return empty instead of error in that case
            if code == 45028 {
                debug!("WeCom message list not available (code 45028), skipping.");
                return Ok(Vec::new());
            }
            anyhow::bail!("WeCom message list error {}: {:?}", code, resp.errmsg);
        }
    }

    Ok(resp.result.map(|r| r.msg_list).unwrap_or_default())
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

/// Convert a WeCom message into a RawEvent for the collector pipeline.
fn to_raw_event(msg: &WeChatMessage) -> RawEvent {
    let mut tags = std::collections::HashMap::new();
    tags.insert("platform".to_string(), "wecom".to_string());
    tags.insert("msg_type".to_string(), msg.msg_type.clone());
    tags.insert("from_user".to_string(), msg.from_user.clone());
    tags.insert("msgid".to_string(), msg.msgid.clone());

    let payload = serde_json::to_vec(&serde_json::json!({
        "msgid": msg.msgid,
        "msg_type": msg.msg_type,
        "content": msg.content,
        "from_user": msg.from_user,
        "create_time": msg.create_time,
        "agentid": msg.agentid,
    }))
    .unwrap_or_default();

    RawEvent {
        id: Uuid::new_v4().to_string(),
        source: format!("connector:wecom:msg:{}", msg.msgid),
        event_type: "message".to_string(),
        timestamp_ms: Utc::now().timestamp_millis(),
        payload,
        tags,
    }
}

/// Convert a raw WeCom callback event into a RawEvent.
pub fn to_raw_event_from_callback(payload: serde_json::Value) -> RawEvent {
    let event_type = payload
        .get("MsgType")
        .or_else(|| payload.get("Event"))
        .and_then(|v| v.as_str())
        .unwrap_or("callback")
        .to_string();

    RawEvent {
        id: Uuid::new_v4().to_string(),
        source: "connector:wecom:callback".to_string(),
        event_type,
        timestamp_ms: Utc::now().timestamp_millis(),
        payload: serde_json::to_vec(&payload).unwrap_or_default(),
        tags: {
            let mut m = std::collections::HashMap::new();
            m.insert("platform".to_string(), "wecom".to_string());
            m
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::WecomConfig;

    #[test]
    fn test_to_raw_event_from_callback_msg_type() {
        let payload = serde_json::json!({
            "MsgType": "text",
            "Content": "Hello"
        });
        let event = to_raw_event_from_callback(payload);
        assert_eq!(event.event_type, "text");
        assert_eq!(event.source, "connector:wecom:callback");
        assert_eq!(event.tags.get("platform").unwrap(), "wecom");
    }

    #[test]
    fn test_to_raw_event_from_callback_event() {
        let payload = serde_json::json!({
            "Event": "subscribe"
        });
        let event = to_raw_event_from_callback(payload);
        assert_eq!(event.event_type, "subscribe");
    }

    #[test]
    fn test_to_raw_event_from_callback_fallback() {
        let payload = serde_json::json!({"data": "test"});
        let event = to_raw_event_from_callback(payload);
        assert_eq!(event.event_type, "callback");
    }

    #[test]
    fn test_to_raw_event_from_callback_payload_bytes() {
        let payload = serde_json::json!({"MsgType": "image", "MediaId": "123"});
        let event = to_raw_event_from_callback(payload.clone());
        let parsed: serde_json::Value = serde_json::from_slice(&event.payload).unwrap();
        assert_eq!(parsed, payload);
    }

    #[test]
    fn test_to_raw_event_from_message() {
        let msg = WeChatMessage {
            msgid: "msg-001".to_string(),
            msg_type: "text".to_string(),
            content: Some("Hello, world!".to_string()),
            from_user: "user-abc".to_string(),
            create_time: 1700000000,
            agentid: Some(1000002),
        };
        let event = to_raw_event(&msg);
        assert_eq!(event.event_type, "message");
        assert_eq!(event.source, "connector:wecom:msg:msg-001");
        assert_eq!(event.tags.get("platform").unwrap(), "wecom");
        assert_eq!(event.tags.get("msg_type").unwrap(), "text");
        assert_eq!(event.tags.get("from_user").unwrap(), "user-abc");
        assert_eq!(event.tags.get("msgid").unwrap(), "msg-001");

        let payload: serde_json::Value = serde_json::from_slice(&event.payload).unwrap();
        assert_eq!(payload["msgid"], "msg-001");
        assert_eq!(payload["content"], "Hello, world!");
        assert_eq!(payload["from_user"], "user-abc");
        assert_eq!(payload["create_time"], 1700000000);
        assert_eq!(payload["agentid"], 1000002);
    }

    #[test]
    fn test_to_raw_event_from_message_no_content() {
        let msg = WeChatMessage {
            msgid: "msg-002".to_string(),
            msg_type: "image".to_string(),
            content: None,
            from_user: "user-xyz".to_string(),
            create_time: 0,
            agentid: None,
        };
        let event = to_raw_event(&msg);
        let payload: serde_json::Value = serde_json::from_slice(&event.payload).unwrap();
        assert!(payload["content"].is_null());
        assert!(payload["agentid"].is_null());
    }

    #[test]
    fn test_wechat_message_deserialization() {
        let json = r#"{
            "msgid": "msg-123",
            "msg_type": "text",
            "content": "Test message",
            "from_user": "user1",
            "create_time": 1700000000,
            "agentid": 1000001
        }"#;
        let msg: WeChatMessage = serde_json::from_str(json).unwrap();
        assert_eq!(msg.msgid, "msg-123");
        assert_eq!(msg.msg_type, "text");
        assert_eq!(msg.content.unwrap(), "Test message");
        assert_eq!(msg.from_user, "user1");
        assert_eq!(msg.agentid.unwrap(), 1000001);
    }

    #[test]
    fn test_wechat_message_deserialization_minimal() {
        let json = r#"{}"#;
        let msg: WeChatMessage = serde_json::from_str(json).unwrap();
        assert_eq!(msg.msgid, "");
        assert_eq!(msg.msg_type, "");
        assert!(msg.content.is_none());
        assert_eq!(msg.from_user, "");
        assert_eq!(msg.create_time, 0);
        assert!(msg.agentid.is_none());
    }

    #[test]
    fn test_token_response_deserialization() {
        let json = r#"{
            "access_token": "token-abc123",
            "expires_in": 7200
        }"#;
        let resp: TokenResponse = serde_json::from_str(json).unwrap();
        assert_eq!(resp.access_token, "token-abc123");
        assert_eq!(resp.expires_in, 7200);
        assert!(resp.errcode.is_none());
        assert!(resp.errmsg.is_none());
    }

    #[test]
    fn test_token_response_with_error() {
        let json = r#"{
            "access_token": "",
            "expires_in": 0,
            "errcode": 40013,
            "errmsg": "invalid appid"
        }"#;
        let resp: TokenResponse = serde_json::from_str(json).unwrap();
        assert_eq!(resp.errcode.unwrap(), 40013);
        assert_eq!(resp.errmsg.unwrap(), "invalid appid");
    }

    #[test]
    fn test_message_list_response_deserialization() {
        let json = r#"{
            "errcode": 0,
            "errmsg": "ok",
            "result": {
                "msg_list": [
                    {"msgid": "m1", "msg_type": "text", "from_user": "u1", "create_time": 100},
                    {"msgid": "m2", "msg_type": "image", "from_user": "u2", "create_time": 200}
                ]
            }
        }"#;
        let resp: MessageListResponse = serde_json::from_str(json).unwrap();
        assert!(resp.errcode.is_none() || resp.errcode == Some(0));
        let result = resp.result.unwrap();
        assert_eq!(result.msg_list.len(), 2);
        assert_eq!(result.msg_list[0].msgid, "m1");
        assert_eq!(result.msg_list[1].msg_type, "image");
    }

    #[test]
    fn test_message_list_response_empty() {
        let json = r#"{"errcode": 0, "result": {"msg_list": []}}"#;
        let resp: MessageListResponse = serde_json::from_str(json).unwrap();
        let result = resp.result.unwrap();
        assert!(result.msg_list.is_empty());
    }

    #[test]
    fn test_message_list_response_no_result() {
        let json = r#"{"errcode": 45028, "errmsg": "no permission"}"#;
        let resp: MessageListResponse = serde_json::from_str(json).unwrap();
        assert_eq!(resp.errcode.unwrap(), 45028);
        assert!(resp.result.is_none());
    }

    #[test]
    fn test_external_contact_list_deserialization() {
        let json = r#"{
            "errcode": 0,
            "errmsg": "ok",
            "external_userid": ["ext-user-1", "ext-user-2", "ext-user-3"]
        }"#;
        let resp: ExternalContactListResponse = serde_json::from_str(json).unwrap();
        assert_eq!(resp.external_userid.len(), 3);
        assert_eq!(resp.external_userid[0], "ext-user-1");
    }

    #[test]
    fn test_external_contact_list_empty() {
        let json = r#"{"errcode": 0, "external_userid": []}"#;
        let resp: ExternalContactListResponse = serde_json::from_str(json).unwrap();
        assert!(resp.external_userid.is_empty());
    }

    #[test]
    fn test_wecom_connector_name() {
        let config = WecomConfig {
            enabled: true,
            corp_id: "test-corp".to_string(),
            agent_id: "1000001".to_string(),
            secret: "test-secret".to_string(),
            poll_interval_secs: 60,
        };
        let connector = WecomConnector::new(config);
        assert_eq!(connector.name(), "wecom");
    }

    #[test]
    fn test_callback_msg_type_priority_over_event() {
        // MsgType should take priority over Event
        let payload = serde_json::json!({
            "MsgType": "text",
            "Event": "subscribe"
        });
        let event = to_raw_event_from_callback(payload);
        assert_eq!(event.event_type, "text");
    }

    #[test]
    fn test_to_raw_event_unique_ids() {
        let msg = WeChatMessage {
            msgid: "same-msg".to_string(),
            msg_type: "text".to_string(),
            content: Some("test".to_string()),
            from_user: "user".to_string(),
            create_time: 0,
            agentid: None,
        };
        let e1 = to_raw_event(&msg);
        let e2 = to_raw_event(&msg);
        assert_ne!(e1.id, e2.id);
        assert_eq!(e1.source, e2.source);
    }
}
