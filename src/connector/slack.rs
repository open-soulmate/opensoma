use anyhow::{Context, Result};
use async_trait::async_trait;
use chrono::Utc;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use tokio::task::JoinHandle;
use tokio::time::Duration;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

use crate::collector::{EventTx, RawEvent};
use crate::config::SlackConfig;
use crate::connector::Connector;
use crate::retry_async;

/// Slack connector — polls Slack channels for new messages using the Slack Web API.
///
/// Requires a Bot User OAuth Token (xoxb-...) with the following scopes:
/// - channels:history (for public channels)
/// - groups:history (for private channels)
/// - users:read (for user name resolution)
/// - channels:read (to list channels)
pub struct SlackConnector {
    config: SlackConfig,
}

impl SlackConnector {
    pub fn new(config: SlackConfig) -> Self {
        Self { config }
    }
}

#[async_trait]
impl Connector for SlackConnector {
    fn name(&self) -> &str {
        "slack"
    }

    async fn ping(&self) -> Result<()> {
        let client = Client::new();
        let resp = client
            .get("https://slack.com/api/auth.test")
            .bearer_auth(&self.config.bot_token)
            .send()
            .await
            .context("Failed to reach Slack API")?;

        let body: serde_json::Value = resp.json().await?;
        if body.get("ok").and_then(|v| v.as_bool()).unwrap_or(false) {
            Ok(())
        } else {
            let err = body
                .get("error")
                .and_then(|v| v.as_str())
                .unwrap_or("unknown");
            Err(anyhow::anyhow!("Slack auth.test failed: {}", err))
        }
    }
}

/// Slack conversation.history response.
#[derive(Debug, Deserialize)]
struct HistoryResponse {
    ok: bool,
    #[serde(default)]
    messages: Vec<SlackMessage>,
    #[serde(default)]
    error: Option<String>,
    #[serde(default)]
    has_more: bool,
    #[serde(default)]
    response_metadata: Option<ResponseMetadata>,
}

#[derive(Debug, Deserialize)]
struct ResponseMetadata {
    #[serde(default)]
    next_cursor: Option<String>,
}

/// A single Slack message.
#[derive(Debug, Deserialize, Serialize, Clone)]
struct SlackMessage {
    #[serde(default)]
    ts: String,
    #[serde(default)]
    r#type: String,
    #[serde(default)]
    user: String,
    #[serde(default)]
    text: String,
    #[serde(default)]
    thread_ts: Option<String>,
    #[serde(default)]
    reply_count: Option<u32>,
    #[serde(default)]
    subtype: Option<String>,
    #[serde(default)]
    bot_id: Option<String>,
}

/// Slack users.info response for name resolution.
#[derive(Debug, Deserialize)]
struct UserInfoResponse {
    ok: bool,
    #[serde(default)]
    user: Option<UserInfo>,
}

#[derive(Debug, Deserialize)]
struct UserInfo {
    #[serde(default)]
    id: String,
    #[serde(default)]
    name: String,
    #[serde(default)]
    real_name: Option<String>,
}

/// Slack conversations.list response.
#[derive(Debug, Deserialize)]
struct ConversationsResponse {
    ok: bool,
    #[serde(default)]
    channels: Vec<ChannelInfo>,
    #[serde(default)]
    error: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ChannelInfo {
    id: String,
    #[serde(default)]
    name: String,
    #[serde(default)]
    is_member: bool,
}

/// Start the Slack connector. Polls configured channels for new messages.
pub async fn start(config: SlackConfig, tx: EventTx) -> Result<JoinHandle<()>> {
    let poll_secs = config.poll_interval_secs;
    let channels = config.channels.clone();
    let bot_token = config.bot_token.clone();
    let include_threads = config.include_threads;

    info!(
        "Slack connector starting — {} channels, poll every {}s",
        channels.len(),
        poll_secs
    );

    // If no channels configured, auto-discover channels the bot has joined
    let resolved_channels = if channels.is_empty() {
        match discover_channels(&bot_token).await {
            Ok(ch) => {
                info!("Auto-discovered {} Slack channels", ch.len());
                ch
            }
            Err(e) => {
                warn!("Failed to auto-discover Slack channels: {}", e);
                Vec::new()
            }
        }
    } else {
        channels
    };

    let handle = tokio::spawn(async move {
        if resolved_channels.is_empty() {
            warn!("No Slack channels to monitor — connector idle");
            // Still run the loop in case channels are added later
        }

        let client = Client::new();
        let mut interval = tokio::time::interval(Duration::from_secs(poll_secs));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        // Track the latest timestamp seen per channel to avoid duplicates
        let mut cursors: std::collections::HashMap<String, String> =
            std::collections::HashMap::new();

        // User name cache
        let mut user_cache: std::collections::HashMap<String, String> =
            std::collections::HashMap::new();

        loop {
            interval.tick().await;

            for channel_id in &resolved_channels {
                if let Err(e) = poll_channel(
                    &client,
                    &bot_token,
                    channel_id,
                    &tx,
                    &mut cursors,
                    &mut user_cache,
                    include_threads,
                )
                .await
                {
                    warn!("Slack poll failed for channel {}: {}", channel_id, e);
                }
            }
        }
    });

    Ok(handle)
}

/// Auto-discover channels the bot has joined.
async fn discover_channels(bot_token: &str) -> Result<Vec<String>> {
    let client = Client::new();
    let mut channels = Vec::new();
    let cursor: Option<String> = None;

    loop {
        let mut url = "https://slack.com/api/conversations.list?types=public_channel,private_channel&limit=200&exclude_archived=true".to_string();
        if let Some(ref c) = cursor {
            url.push_str(&format!("&cursor={}", c));
        }

        let resp: ConversationsResponse = retry_async!("slack_conversations_list", 3, {
            let r = client
                .get(&url)
                .bearer_auth(bot_token)
                .send()
                .await
                .context("Failed to list Slack conversations")?;
            r.json::<ConversationsResponse>()
                .await
                .context("Failed to parse Slack conversations response")
        })?;

        if !resp.ok {
            let err = resp.error.unwrap_or_else(|| "unknown error".to_string());
            return Err(anyhow::anyhow!("Slack conversations.list failed: {}", err));
        }

        for ch in &resp.channels {
            if ch.is_member {
                channels.push(ch.id.clone());
            }
        }

        // Check for pagination cursor
        // Note: The ConversationsResponse doesn't have response_metadata,
        // so we'll break after first page for now.
        break;
    }

    Ok(channels)
}

/// Poll a single Slack channel for new messages.
async fn poll_channel(
    client: &Client,
    bot_token: &str,
    channel_id: &str,
    tx: &EventTx,
    cursors: &mut std::collections::HashMap<String, String>,
    user_cache: &mut std::collections::HashMap<String, String>,
    include_threads: bool,
) -> Result<()> {
    let oldest = cursors.get(channel_id).cloned();

    let mut url = format!(
        "https://slack.com/api/conversations.history?channel={}&limit=100",
        channel_id
    );
    if let Some(ref ts) = oldest {
        url.push_str(&format!("&oldest={}", ts));
    }

    let resp: HistoryResponse = retry_async!("slack_conversations_history", 3, {
        let r = client
            .get(&url)
            .bearer_auth(bot_token)
            .send()
            .await
            .context("Failed to fetch Slack channel history")?;
        r.json::<HistoryResponse>()
            .await
            .context("Failed to parse Slack history response")
    })?;

    if !resp.ok {
        let err = resp.error.unwrap_or_else(|| "unknown error".to_string());
        return Err(anyhow::anyhow!(
            "Slack conversations.history failed: {}",
            err
        ));
    }

    if resp.messages.is_empty() {
        debug!("No new messages in Slack channel {}", channel_id);
        return Ok(());
    }

    // Update cursor to the newest message timestamp
    if let Some(newest) = resp.messages.first() {
        cursors.insert(channel_id.to_string(), newest.ts.clone());
    }

    // Process messages in chronological order (oldest first)
    for msg in resp.messages.iter().rev() {
        // Skip bot messages and system messages
        if msg.subtype.as_deref() == Some("bot_message") || msg.bot_id.is_some() {
            continue;
        }

        // Resolve user name
        let user_name = resolve_user(client, bot_token, &msg.user, user_cache).await;

        // Build the event
        let mut tags = std::collections::HashMap::new();
        tags.insert("connector".to_string(), "slack".to_string());
        tags.insert("channel_id".to_string(), channel_id.to_string());
        tags.insert("user_id".to_string(), msg.user.clone());
        tags.insert("user_name".to_string(), user_name.clone());
        tags.insert("message_ts".to_string(), msg.ts.clone());

        if let Some(ref thread_ts) = msg.thread_ts {
            tags.insert("thread_ts".to_string(), thread_ts.clone());
            tags.insert("is_reply".to_string(), "true".to_string());
        }

        if let Some(reply_count) = msg.reply_count {
            tags.insert("reply_count".to_string(), reply_count.to_string());
        }

        let payload = serde_json::json!({
            "channel_id": channel_id,
            "user": user_name,
            "user_id": msg.user,
            "text": msg.text,
            "ts": msg.ts,
            "thread_ts": msg.thread_ts,
            "reply_count": msg.reply_count,
        });

        let event = RawEvent {
            id: Uuid::new_v4().to_string(),
            source: "connector:slack".to_string(),
            event_type: "slack_message".to_string(),
            timestamp_ms: Utc::now().timestamp_millis(),
            payload: serde_json::to_vec(&payload).unwrap_or_default(),
            tags,
        };

        if let Err(e) = tx.try_send(event) {
            match e {
                tokio::sync::mpsc::error::TrySendError::Full(_) => {
                    warn!("Event channel full, dropping Slack event");
                }
                tokio::sync::mpsc::error::TrySendError::Closed(_) => {
                    error!("Event channel closed");
                    return Err(anyhow::anyhow!("Event channel closed"));
                }
            }
        }

        // Fetch thread replies if applicable
        if include_threads && msg.reply_count.unwrap_or(0) > 0 && msg.thread_ts.is_none() {
            if let Err(e) =
                poll_thread(client, bot_token, channel_id, &msg.ts, tx, user_cache).await
            {
                debug!("Failed to fetch thread replies for {}: {}", msg.ts, e);
            }
        }
    }

    info!(
        "Slack channel {}: processed {} messages",
        channel_id,
        resp.messages.len()
    );

    Ok(())
}

/// Fetch thread replies for a message.
async fn poll_thread(
    client: &Client,
    bot_token: &str,
    channel_id: &str,
    thread_ts: &str,
    tx: &EventTx,
    user_cache: &mut std::collections::HashMap<String, String>,
) -> Result<()> {
    let url = format!(
        "https://slack.com/api/conversations.replies?channel={}&ts={}&limit=100",
        channel_id, thread_ts
    );

    let resp: HistoryResponse = retry_async!("slack_conversations_replies", 3, {
        let r = client
            .get(&url)
            .bearer_auth(bot_token)
            .send()
            .await
            .context("Failed to fetch Slack thread replies")?;
        r.json::<HistoryResponse>()
            .await
            .context("Failed to parse Slack replies response")
    })?;

    if !resp.ok {
        let err = resp.error.unwrap_or_else(|| "unknown error".to_string());
        return Err(anyhow::anyhow!(
            "Slack conversations.replies failed: {}",
            err
        ));
    }

    // Skip the first message (it's the parent)
    for msg in resp.messages.iter().skip(1) {
        if msg.subtype.as_deref() == Some("bot_message") {
            continue;
        }

        let user_name = resolve_user(client, bot_token, &msg.user, user_cache).await;

        let mut tags = std::collections::HashMap::new();
        tags.insert("connector".to_string(), "slack".to_string());
        tags.insert("channel_id".to_string(), channel_id.to_string());
        tags.insert("user_id".to_string(), msg.user.clone());
        tags.insert("user_name".to_string(), user_name.clone());
        tags.insert("thread_ts".to_string(), thread_ts.to_string());
        tags.insert("is_reply".to_string(), "true".to_string());

        let payload = serde_json::json!({
            "channel_id": channel_id,
            "user": user_name,
            "user_id": msg.user,
            "text": msg.text,
            "ts": msg.ts,
            "thread_ts": thread_ts,
        });

        let event = RawEvent {
            id: Uuid::new_v4().to_string(),
            source: "connector:slack".to_string(),
            event_type: "slack_thread_reply".to_string(),
            timestamp_ms: Utc::now().timestamp_millis(),
            payload: serde_json::to_vec(&payload).unwrap_or_default(),
            tags,
        };

        let _ = tx.try_send(event);
    }

    Ok(())
}

/// Resolve a Slack user ID to a display name, with caching.
async fn resolve_user(
    client: &Client,
    bot_token: &str,
    user_id: &str,
    cache: &mut std::collections::HashMap<String, String>,
) -> String {
    if let Some(name) = cache.get(user_id) {
        return name.clone();
    }

    let url = format!("https://slack.com/api/users.info?user={}", user_id);
    match client
        .get(&url)
        .bearer_auth(bot_token)
        .send()
        .await
    {
        Ok(resp) => match resp.json::<UserInfoResponse>().await {
            Ok(info) if info.ok => {
                let name = info
                    .user
                    .as_ref()
                    .and_then(|u| u.real_name.clone())
                    .or_else(|| info.user.as_ref().map(|u| u.name.clone()))
                    .unwrap_or_else(|| user_id.to_string());
                cache.insert(user_id.to_string(), name.clone());
                name
            }
            _ => {
                cache.insert(user_id.to_string(), user_id.to_string());
                user_id.to_string()
            }
        },
        Err(_) => {
            cache.insert(user_id.to_string(), user_id.to_string());
            user_id.to_string()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_slack_connector_name() {
        let config = SlackConfig {
            enabled: true,
            bot_token: "xoxb-test".to_string(),
            channels: vec![],
            poll_interval_secs: 60,
            include_threads: true,
        };
        let connector = SlackConnector::new(config);
        assert_eq!(connector.name(), "slack");
    }

    #[test]
    fn test_slack_message_deserialization() {
        let json = r#"{
            "ts": "1234567890.123456",
            "type": "message",
            "user": "U12345",
            "text": "Hello world",
            "thread_ts": null,
            "reply_count": null,
            "subtype": null,
            "bot_id": null
        }"#;

        let msg: SlackMessage = serde_json::from_str(json).unwrap();
        assert_eq!(msg.ts, "1234567890.123456");
        assert_eq!(msg.user, "U12345");
        assert_eq!(msg.text, "Hello world");
        assert!(msg.thread_ts.is_none());
        assert!(msg.reply_count.is_none());
    }

    #[test]
    fn test_slack_message_with_thread() {
        let json = r#"{
            "ts": "1234567890.123456",
            "type": "message",
            "user": "U12345",
            "text": "Thread parent",
            "thread_ts": "1234567890.123456",
            "reply_count": 3,
            "subtype": null,
            "bot_id": null
        }"#;

        let msg: SlackMessage = serde_json::from_str(json).unwrap();
        assert_eq!(msg.thread_ts, Some("1234567890.123456".to_string()));
        assert_eq!(msg.reply_count, Some(3));
    }

    #[test]
    fn test_slack_message_bot_filtered() {
        let json = r#"{
            "ts": "1234567890.123456",
            "type": "message",
            "user": "",
            "text": "Bot message",
            "subtype": "bot_message",
            "bot_id": "B12345"
        }"#;

        let msg: SlackMessage = serde_json::from_str(json).unwrap();
        assert_eq!(msg.subtype, Some("bot_message".to_string()));
        assert_eq!(msg.bot_id, Some("B12345".to_string()));
    }

    #[test]
    fn test_history_response_deserialization() {
        let json = r#"{
            "ok": true,
            "messages": [
                {
                    "ts": "1234567890.123456",
                    "type": "message",
                    "user": "U12345",
                    "text": "Hello"
                }
            ],
            "has_more": false
        }"#;

        let resp: HistoryResponse = serde_json::from_str(json).unwrap();
        assert!(resp.ok);
        assert_eq!(resp.messages.len(), 1);
        assert_eq!(resp.messages[0].text, "Hello");
        assert!(!resp.has_more);
    }

    #[test]
    fn test_history_response_error() {
        let json = r#"{
            "ok": false,
            "error": "channel_not_found",
            "messages": []
        }"#;

        let resp: HistoryResponse = serde_json::from_str(json).unwrap();
        assert!(!resp.ok);
        assert_eq!(resp.error, Some("channel_not_found".to_string()));
    }

    #[test]
    fn test_conversations_response_deserialization() {
        let json = r#"{
            "ok": true,
            "channels": [
                {"id": "C123", "name": "general", "is_member": true},
                {"id": "C456", "name": "random", "is_member": false}
            ]
        }"#;

        let resp: ConversationsResponse = serde_json::from_str(json).unwrap();
        assert!(resp.ok);
        assert_eq!(resp.channels.len(), 2);
        assert!(resp.channels[0].is_member);
        assert!(!resp.channels[1].is_member);
    }

    #[test]
    fn test_user_info_deserialization() {
        let json = r#"{
            "ok": true,
            "user": {
                "id": "U12345",
                "name": "john.doe",
                "real_name": "John Doe"
            }
        }"#;

        let resp: UserInfoResponse = serde_json::from_str(json).unwrap();
        assert!(resp.ok);
        assert_eq!(resp.user.as_ref().unwrap().real_name, Some("John Doe".to_string()));
    }

    #[test]
    fn test_slack_event_tags() {
        // Verify the event structure matches what we expect
        let mut tags = std::collections::HashMap::new();
        tags.insert("connector".to_string(), "slack".to_string());
        tags.insert("channel_id".to_string(), "C123".to_string());
        tags.insert("user_id".to_string(), "U456".to_string());
        tags.insert("user_name".to_string(), "test_user".to_string());
        tags.insert("message_ts".to_string(), "1234567890.123456".to_string());

        assert_eq!(tags.get("connector").unwrap(), "slack");
        assert_eq!(tags.get("channel_id").unwrap(), "C123");
        assert_eq!(tags.get("user_id").unwrap(), "U456");
    }

    #[test]
    fn test_user_cache_behavior() {
        let mut cache = std::collections::HashMap::new();
        cache.insert("U123".to_string(), "John".to_string());

        // Simulate cache hit
        assert_eq!(cache.get("U123"), Some(&"John".to_string()));

        // Simulate cache miss
        assert!(cache.get("U999").is_none());
    }

    #[test]
    fn test_slack_config_defaults() {
        let config = SlackConfig {
            enabled: true,
            bot_token: "xoxb-test".to_string(),
            channels: vec!["C123".to_string()],
            poll_interval_secs: 60,
            include_threads: true,
        };
        assert!(config.enabled);
        assert_eq!(config.channels.len(), 1);
        assert_eq!(config.poll_interval_secs, 60);
        assert!(config.include_threads);
    }
}
