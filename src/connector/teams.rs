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
use crate::config::TeamsConfig;
use crate::connector::circuit_breaker::CircuitBreaker;
use crate::connector::Connector;
use crate::retry_async;

/// Microsoft Teams connector — polls Teams channels for new messages using
/// the Microsoft Graph API.
///
/// Requires an Azure AD app registration with the following application permissions:
/// - ChannelMessage.Read.All
/// - Team.ReadBasic.All
/// - Channel.ReadBasic.All
///
/// Uses the Microsoft Graph API v1.0 (https://graph.microsoft.com/v1.0/).
///
/// Authentication uses the OAuth 2.0 client credentials flow with a client secret.
pub struct TeamsConnector {
    config: TeamsConfig,
}

impl TeamsConnector {
    pub fn new(config: TeamsConfig) -> Self {
        Self { config }
    }
}

#[async_trait]
impl Connector for TeamsConnector {
    fn name(&self) -> &str {
        "teams"
    }

    async fn ping(&self) -> Result<()> {
        let client = Client::new();
        let token = fetch_access_token(&client, &self.config).await?;

        // Verify we can list teams
        let resp = client
            .get("https://graph.microsoft.com/v1.0/me/joinedTeams")
            .bearer_auth(&token)
            .send()
            .await
            .context("Failed to reach Microsoft Graph API")?;

        if resp.status().is_success() {
            Ok(())
        } else {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            Err(anyhow::anyhow!(
                "Teams API auth failed ({}): {}",
                status,
                body
            ))
        }
    }
}

/// OAuth 2.0 token response from Azure AD.
#[derive(Debug, Deserialize)]
struct TokenResponse {
    access_token: String,
    #[allow(dead_code)]
    token_type: Option<String>,
    #[allow(dead_code)]
    expires_in: Option<u64>,
    #[allow(dead_code)]
    error: Option<String>,
    #[allow(dead_code)]
    error_description: Option<String>,
}

/// Microsoft Graph team object (partial).
#[derive(Debug, Deserialize, Clone)]
#[allow(dead_code)]
struct Team {
    id: String,
    #[serde(default)]
    display_name: Option<String>,
    #[serde(default)]
    description: Option<String>,
}

/// Microsoft Graph channel object (partial).
#[derive(Debug, Deserialize, Clone, Default)]
#[allow(dead_code)]
struct Channel {
    id: String,
    #[serde(default)]
    display_name: Option<String>,
    #[serde(default)]
    description: Option<String>,
}

/// Microsoft Graph chat message object (partial).
#[derive(Debug, Deserialize, Serialize, Clone, Default)]
struct ChatMessage {
    id: String,
    #[serde(default)]
    message_type: Option<String>,
    #[serde(default)]
    subject: Option<String>,
    #[serde(default)]
    summary: Option<String>,
    #[serde(default)]
    body: Option<MessageBody>,
    #[serde(default)]
    from: Option<MessageFrom>,
    #[serde(default)]
    created_date_time: Option<String>,
    #[serde(default)]
    last_modified_date_time: Option<String>,
    #[serde(default)]
    attachments: Vec<ChatMessageAttachment>,
    #[serde(default)]
    mentions: Vec<serde_json::Value>,
}

/// Message body content.
#[derive(Debug, Deserialize, Serialize, Clone)]
struct MessageBody {
    #[serde(default)]
    content_type: Option<String>,
    #[serde(default)]
    content: String,
}

/// Message sender information.
#[derive(Debug, Deserialize, Serialize, Clone)]
struct MessageFrom {
    #[serde(default)]
    user: Option<GraphUser>,
}

/// Microsoft Graph user (partial).
#[derive(Debug, Deserialize, Serialize, Clone)]
struct GraphUser {
    #[serde(default)]
    id: Option<String>,
    #[serde(default)]
    display_name: Option<String>,
    #[serde(default)]
    user_id: Option<String>,
}

/// Chat message attachment.
#[derive(Debug, Deserialize, Serialize, Clone)]
struct ChatMessageAttachment {
    #[serde(default)]
    id: Option<String>,
    #[serde(default)]
    content_type: Option<String>,
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    content_url: Option<String>,
}

/// Graph API list response wrapper.
#[derive(Debug, Deserialize, Default)]
#[allow(dead_code)]
struct GraphListResponse<T: Default> {
    #[serde(default)]
    value: Vec<T>,
    #[serde(rename = "@odata.nextLink")]
    next_link: Option<String>,
}

/// Fetch an OAuth 2.0 access token using the client credentials flow.
async fn fetch_access_token(client: &Client, config: &TeamsConfig) -> Result<String> {
    let token_url = format!(
        "https://login.microsoftonline.com/{}/oauth2/v2.0/token",
        config.tenant_id
    );

    let params = [
        ("grant_type", "client_credentials"),
        ("client_id", &config.client_id),
        ("client_secret", &config.client_secret),
        ("scope", "https://graph.microsoft.com/.default"),
    ];

    let resp = retry_async!("teams_token", 3, {
        let r = client
            .post(&token_url)
            .form(&params)
            .send()
            .await
            .context("Failed to request Teams access token")?;

        let status = r.status();
        if !status.is_success() {
            let body = r.text().await.unwrap_or_default();
            anyhow::bail!("Teams token request failed ({}): {}", status, body);
        }

        let token_resp: TokenResponse = r
            .json()
            .await
            .context("Failed to parse Teams token response")?;

        if let Some(err) = &token_resp.error {
            anyhow::bail!("Teams token error: {} - {}", err, token_resp.error_description.as_deref().unwrap_or(""));
        }

        Ok::<String, anyhow::Error>(token_resp.access_token)
    })?;

    Ok(resp)
}

/// Fetch channels for a specific team.
async fn fetch_channels(
    client: &Client,
    token: &str,
    team_id: &str,
) -> Result<Vec<Channel>> {
    let url = format!(
        "https://graph.microsoft.com/v1.0/teams/{}/channels",
        team_id
    );

    let resp = retry_async!("teams_channels", 3, {
        let r = client
            .get(&url)
            .bearer_auth(token)
            .send()
            .await
            .context("Failed to fetch Teams channels")?;

        let status = r.status();
        if !status.is_success() {
            let body = r.text().await.unwrap_or_default();
            anyhow::bail!("Teams channels API error ({}): {}", status, body);
        }

        let list: GraphListResponse<Channel> = r
            .json()
            .await
            .context("Failed to parse Teams channels response")?;
        Ok::<Vec<Channel>, anyhow::Error>(list.value)
    })?;

    Ok(resp)
}

/// Fetch messages from a Teams channel.
/// Returns messages in reverse chronological order (newest first).
async fn fetch_messages(
    client: &Client,
    token: &str,
    team_id: &str,
    channel_id: &str,
    after: Option<&str>,
    limit: u32,
) -> Result<Vec<ChatMessage>> {
    let mut url = format!(
        "https://graph.microsoft.com/v1.0/teams/{}/channels/{}/messages?$top={}",
        team_id, channel_id, limit
    );

    // Filter for messages after a specific time if we have a last-seen timestamp
    if let Some(after_dt) = after {
        url = format!(
            "{}&$filter=createdDateTime gt {}",
            url, after_dt
        );
    }

    let resp = retry_async!("teams_messages", 3, {
        let r = client
            .get(&url)
            .bearer_auth(token)
            .send()
            .await
            .context("Failed to fetch Teams messages")?;

        let status = r.status();
        if !status.is_success() {
            let body = r.text().await.unwrap_or_default();
            anyhow::bail!("Teams messages API error ({}): {}", status, body);
        }

        let list: GraphListResponse<ChatMessage> = r
            .json()
            .await
            .context("Failed to parse Teams messages response")?;
        Ok::<Vec<ChatMessage>, anyhow::Error>(list.value)
    })?;

    Ok(resp)
}

/// Convert a Teams ChatMessage into a RawEvent.
fn message_to_event(msg: &ChatMessage, team_id: &str, channel_id: &str) -> RawEvent {
    let author_name = msg
        .from
        .as_ref()
        .and_then(|f| f.user.as_ref())
        .and_then(|u| u.display_name.clone())
        .unwrap_or_else(|| "unknown".to_string());

    let author_id = msg
        .from
        .as_ref()
        .and_then(|f| f.user.as_ref())
        .and_then(|u| u.id.clone().or_else(|| u.user_id.clone()))
        .unwrap_or_default();

    let body_content = msg
        .body
        .as_ref()
        .map(|b| {
            // Strip HTML tags from Teams message body to get plain text
            strip_html(&b.content)
        })
        .unwrap_or_default();

    let content_type = msg
        .body
        .as_ref()
        .and_then(|b| b.content_type.clone())
        .unwrap_or_else(|| "text".to_string());

    let mut payload = serde_json::json!({
        "source": "teams",
        "message_id": msg.id,
        "team_id": team_id,
        "channel_id": channel_id,
        "author_id": author_id,
        "author_name": author_name,
        "content": body_content,
        "content_type": content_type,
        "message_type": msg.message_type,
        "subject": msg.subject,
        "created_at": msg.created_date_time,
        "modified_at": msg.last_modified_date_time,
    });

    // Include attachments info
    if !msg.attachments.is_empty() {
        payload["attachments"] = serde_json::json!(
            msg.attachments
                .iter()
                .map(|a| {
                    serde_json::json!({
                        "id": a.id,
                        "name": a.name,
                        "content_type": a.content_type,
                        "content_url": a.content_url,
                    })
                })
                .collect::<Vec<_>>()
        );
    }

    // Build tags
    let mut tags = std::collections::HashMap::new();
    tags.insert("source".to_string(), "teams".to_string());
    tags.insert("team".to_string(), team_id.to_string());
    tags.insert("channel".to_string(), channel_id.to_string());
    tags.insert("author".to_string(), author_id.clone());
    tags.insert("sender".to_string(), author_name.clone());
    if msg.message_type.as_deref() == Some("systemEventMessage") {
        tags.insert("system_event".to_string(), "true".to_string());
    }
    if let Some(ref subject) = msg.subject {
        if !subject.is_empty() {
            tags.insert("subject".to_string(), subject.clone());
        }
    }

    RawEvent {
        id: Uuid::new_v4().to_string(),
        source: "teams".to_string(),
        event_type: "message".to_string(),
        payload: serde_json::to_vec(&payload).unwrap_or_default(),
        timestamp_ms: Utc::now().timestamp_millis(),
        tags,
    }
}

/// Strip HTML tags from content, preserving text.
fn strip_html(html: &str) -> String {
    let mut result = String::with_capacity(html.len());
    let mut in_tag = false;
    let mut in_entity = false;
    let mut entity_buf = String::new();

    for ch in html.chars() {
        match ch {
            '<' => {
                in_tag = true;
                // Add space for block-level elements
                if !result.is_empty() && !result.ends_with(' ') && !result.ends_with('\n') {
                    result.push(' ');
                }
            }
            '>' => in_tag = false,
            '&' if !in_tag => {
                in_entity = true;
                entity_buf.clear();
                entity_buf.push(ch);
            }
            ';' if in_entity => {
                entity_buf.push(ch);
                // Decode common HTML entities
                match entity_buf.as_str() {
                    "&amp;" => result.push('&'),
                    "&lt;" => result.push('<'),
                    "&gt;" => result.push('>'),
                    "&quot;" => result.push('"'),
                    "&nbsp;" => result.push(' '),
                    "&#39;" | "&apos;" => result.push('\''),
                    _ => result.push_str(&entity_buf),
                }
                in_entity = false;
                entity_buf.clear();
            }
            _ if in_tag => {} // Skip tag content
            _ if in_entity => entity_buf.push(ch),
            _ => result.push(ch),
        }
    }

    // Handle unclosed entity
    if in_entity {
        result.push_str(&entity_buf);
    }

    let collapsed: String = result.split_whitespace().collect::<Vec<_>>().join(" "); collapsed
}

/// Start the Microsoft Teams connector — spawns a background task that polls
/// configured teams and channels.
pub async fn start(
    config: TeamsConfig,
    tx: EventTx,
    circuit_breaker: Option<CircuitBreaker>,
) -> Result<JoinHandle<()>> {
    let client = Client::builder()
        .timeout(Duration::from_secs(30))
        .build()
        .context("Failed to create HTTP client for Teams connector")?;

    let team_ids = config.team_ids.clone();
    let channel_filter = config.channels.clone();
    let poll_interval = Duration::from_secs(config.poll_interval_secs);

    let handle = tokio::spawn(async move {
        info!(
            "Teams connector started — teams={}, poll_interval={}s",
            team_ids.len(),
            poll_interval.as_secs()
        );

        let _cb = circuit_breaker;

        // Track the last seen message timestamp per channel for incremental fetching
        let mut last_seen: std::collections::HashMap<String, String> =
            std::collections::HashMap::new();

        let mut interval = tokio::time::interval(poll_interval);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        // Skip the first immediate tick
        interval.tick().await;

        loop {
            // Fetch a fresh access token for this poll cycle
            let token = match fetch_access_token(&client, &config).await {
                Ok(t) => t,
                Err(e) => {
                    error!("Failed to get Teams access token: {}", e);
                    if let Some(ref cb) = _cb {
                        cb.record_failure().await;
                    }
                    interval.tick().await;
                    continue;
                }
            };

            interval.tick().await;

            // Circuit breaker check
            if let Some(ref cb) = _cb {
                if cb.allow_request().await.is_err() {
                    warn!("Teams connector circuit breaker open — skipping poll");
                    tokio::time::sleep(Duration::from_secs(30)).await;
                    continue;
                }
            }

            let mut total_count = 0u64;

            for team_id in &team_ids {
                // Discover channels or use configured ones
                let channels: Vec<String> = if channel_filter.is_empty() {
                    match fetch_channels(&client, &token, team_id).await {
                        Ok(ch) => {
                            let ids: Vec<String> = ch.iter().map(|c| c.id.clone()).collect();
                            debug!("Discovered {} channels in team {}", ids.len(), team_id);
                            ids
                        }
                        Err(e) => {
                            warn!("Failed to discover Teams channels for team {}: {}", team_id, e);
                            continue;
                        }
                    }
                } else {
                    channel_filter.clone()
                };

                for channel_id in &channels {
                    let cache_key = format!("{}:{}", team_id, channel_id);
                    let after_ts = last_seen.get(&cache_key).map(|s| s.as_str());

                    match fetch_messages(&client, &token, team_id, channel_id, after_ts, 50).await
                    {
                        Ok(messages) => {
                            if messages.is_empty() {
                                debug!(
                                    "No new messages in Teams channel {}/{}",
                                    team_id, channel_id
                                );
                                continue;
                            }

                            // Messages come newest-first; sort chronologically
                            let mut sorted = messages;
                            sorted.sort_by(|a, b| {
                                a.created_date_time
                                    .as_deref()
                                    .unwrap_or("")
                                    .cmp(b.created_date_time.as_deref().unwrap_or(""))
                            });

                            for msg in &sorted {
                                // Skip system events if configured
                                if config.ignore_system_events
                                    && msg.message_type.as_deref() == Some("systemEventMessage")
                                {
                                    continue;
                                }

                                // Skip empty messages
                                let body_empty = msg
                                    .body
                                    .as_ref()
                                    .map(|b| b.content.trim().is_empty())
                                    .unwrap_or(true);
                                if body_empty && msg.attachments.is_empty() {
                                    continue;
                                }

                                let event = message_to_event(msg, team_id, channel_id);
                                if let Err(e) = tx.send(event).await {
                                    error!("Failed to send Teams event: {}", e);
                                }
                                total_count += 1;
                            }

                            // Update last seen to the newest message timestamp
                            if let Some(newest) = sorted.last() {
                                if let Some(ref dt) = newest.created_date_time {
                                    last_seen.insert(cache_key, dt.clone());
                                }
                            }

                            if let Some(ref cb) = _cb {
                                cb.record_success().await;
                            }
                        }
                        Err(e) => {
                            warn!(
                                "Failed to fetch Teams messages in {}/{}: {}",
                                team_id, channel_id, e
                            );
                            if let Some(ref cb) = _cb {
                                cb.record_failure().await;
                            }
                        }
                    }

                    // Rate limit: Graph API allows ~2000 requests per minute
                    tokio::time::sleep(Duration::from_millis(200)).await;
                }
            }

            if total_count > 0 {
                info!("Teams connector: collected {} messages", total_count);
            }
        }
    });

    Ok(handle)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_teams_config() -> TeamsConfig {
        TeamsConfig {
            enabled: true,
            tenant_id: "test-tenant".to_string(),
            client_id: "test-client".to_string(),
            client_secret: "test-secret".to_string(),
            team_ids: vec!["team-1".to_string()],
            channels: vec![],
            ignore_system_events: true,
            poll_interval_secs: 60,
        }
    }

    #[test]
    fn test_teams_message_to_event_basic() {
        let msg = ChatMessage {
            id: "msg-001".to_string(),
            message_type: Some("message".to_string()),
            subject: None,
            summary: None,
            body: Some(MessageBody {
                content_type: Some("text".to_string()),
                content: "Hello, Teams!".to_string(),
            }),
            from: Some(MessageFrom {
                user: Some(GraphUser {
                    id: Some("user-1".to_string()),
                    display_name: Some("Alice".to_string()),
                    user_id: None,
                }),
            }),
            created_date_time: Some("2024-01-15T10:30:00Z".to_string()),
            last_modified_date_time: None,
            attachments: vec![],
            mentions: vec![],
        };

        let event = message_to_event(&msg, "team-1", "channel-1");
        assert_eq!(event.source, "teams");
        assert_eq!(event.event_type, "message");
        assert_eq!(event.tags.get("source").map(|s| s.as_str()), Some("teams"));
        assert_eq!(event.tags.get("team").map(|s| s.as_str()), Some("team-1"));
        assert_eq!(event.tags.get("channel").map(|s| s.as_str()), Some("channel-1"));
        assert_eq!(event.tags.get("author").map(|s| s.as_str()), Some("user-1"));
        assert_eq!(event.tags.get("sender").map(|s| s.as_str()), Some("Alice"));

        let payload: serde_json::Value = serde_json::from_slice(&event.payload).unwrap();
        assert_eq!(payload["content"], "Hello, Teams!");
        assert_eq!(payload["author_name"], "Alice");
    }

    #[test]
    fn test_teams_message_to_event_html_body() {
        let msg = ChatMessage {
            id: "msg-002".to_string(),
            message_type: Some("message".to_string()),
            subject: None,
            summary: None,
            body: Some(MessageBody {
                content_type: Some("html".to_string()),
                content: "<div><p>Hello <b>world</b>!</p></div>".to_string(),
            }),
            from: Some(MessageFrom {
                user: Some(GraphUser {
                    id: Some("user-2".to_string()),
                    display_name: Some("Bob".to_string()),
                    user_id: None,
                }),
            }),
            created_date_time: None,
            last_modified_date_time: None,
            attachments: vec![],
            mentions: vec![],
        };

        let event = message_to_event(&msg, "team-1", "channel-1");
        let payload: serde_json::Value = serde_json::from_slice(&event.payload).unwrap();
        // HTML should be stripped
        let content = payload["content"].as_str().unwrap();
        assert!(!content.contains("<"));
        assert!(content.contains("Hello"));
        assert!(content.contains("world"));
    }

    #[test]
    fn test_teams_message_to_event_system_event() {
        let msg = ChatMessage {
            id: "msg-003".to_string(),
            message_type: Some("systemEventMessage".to_string()),
            subject: None,
            summary: None,
            body: Some(MessageBody {
                content_type: Some("text".to_string()),
                content: "User joined the team".to_string(),
            }),
            from: None,
            created_date_time: None,
            last_modified_date_time: None,
            attachments: vec![],
            mentions: vec![],
        };

        let event = message_to_event(&msg, "team-1", "channel-1");
        assert_eq!(
            event.tags.get("system_event").map(|s| s.as_str()),
            Some("true")
        );
        assert_eq!(
            event.tags.get("author").map(|s| s.as_str()),
            Some("") // No author for system events
        );
    }

    #[test]
    fn test_teams_message_to_event_with_attachments() {
        let msg = ChatMessage {
            id: "msg-004".to_string(),
            message_type: Some("message".to_string()),
            subject: None,
            summary: None,
            body: Some(MessageBody {
                content_type: Some("text".to_string()),
                content: "Check this file".to_string(),
            }),
            from: Some(MessageFrom {
                user: Some(GraphUser {
                    id: Some("user-3".to_string()),
                    display_name: Some("Charlie".to_string()),
                    user_id: None,
                }),
            }),
            created_date_time: None,
            last_modified_date_time: None,
            attachments: vec![ChatMessageAttachment {
                id: Some("att-1".to_string()),
                content_type: Some("reference".to_string()),
                name: Some("report.xlsx".to_string()),
                content_url: Some("https://contoso.sharepoint.com/report.xlsx".to_string()),
            }],
            mentions: vec![],
        };

        let event = message_to_event(&msg, "team-1", "channel-1");
        let payload: serde_json::Value = serde_json::from_slice(&event.payload).unwrap();
        assert!(payload["attachments"].is_array());
        assert_eq!(payload["attachments"][0]["name"], "report.xlsx");
    }

    #[test]
    fn test_teams_message_to_event_with_subject() {
        let msg = ChatMessage {
            id: "msg-005".to_string(),
            message_type: Some("message".to_string()),
            subject: Some("Weekly Standup".to_string()),
            summary: None,
            body: Some(MessageBody {
                content_type: Some("text".to_string()),
                content: "Meeting notes".to_string(),
            }),
            from: None,
            created_date_time: None,
            last_modified_date_time: None,
            attachments: vec![],
            mentions: vec![],
        };

        let event = message_to_event(&msg, "team-1", "channel-1");
        assert_eq!(
            event.tags.get("subject").map(|s| s.as_str()),
            Some("Weekly Standup")
        );
    }

    #[test]
    fn test_strip_html_basic() {
        assert_eq!(strip_html("<p>Hello</p>"), "Hello");
        assert_eq!(strip_html("<b>Bold</b> text"), "Bold text");
        assert_eq!(strip_html("No tags"), "No tags");
        assert_eq!(strip_html(""), "");
    }

    #[test]
    fn test_strip_html_entities() {
        assert_eq!(strip_html("a &amp; b"), "a & b");
        assert_eq!(strip_html("&lt;div&gt;"), "<div>");
        assert_eq!(strip_html("hello&nbsp;world"), "hello world");
        assert_eq!(strip_html("it&apos;s"), "it's");
    }

    #[test]
    fn test_strip_html_nested() {
        assert_eq!(
            strip_html("<div><p><span>Deep</span> nested</p></div>"),
            "Deep nested"
        );
    }

    #[test]
    fn test_strip_html_with_links() {
        assert_eq!(
            strip_html(r#"<a href="https://example.com">Click here</a>"#),
            "Click here"
        );
    }

    #[test]
    fn test_teams_config_clone() {
        let config = make_teams_config();
        let cloned = config.clone();
        assert_eq!(cloned.tenant_id, "test-tenant");
        assert_eq!(cloned.client_id, "test-client");
        assert_eq!(cloned.team_ids.len(), 1);
        assert!(cloned.ignore_system_events);
    }

    #[test]
    fn test_teams_message_empty_body_skipped() {
        let msg = ChatMessage {
            id: "msg-empty".to_string(),
            message_type: None,
            subject: None,
            summary: None,
            body: Some(MessageBody {
                content_type: Some("text".to_string()),
                content: "   ".to_string(),
            }),
            from: None,
            created_date_time: None,
            last_modified_date_time: None,
            attachments: vec![],
            mentions: vec![],
        };

        // The message_to_event function always creates an event,
        // but the start() loop would skip it based on body_empty check
        let event = message_to_event(&msg, "t1", "c1");
        let payload: serde_json::Value = serde_json::from_slice(&event.payload).unwrap();
        assert_eq!(payload["content"], ""); // Stripped whitespace
    }

    #[test]
    fn test_teams_message_from_user_id_fallback() {
        let msg = ChatMessage {
            id: "msg-fallback".to_string(),
            message_type: None,
            subject: None,
            summary: None,
            body: Some(MessageBody {
                content_type: None,
                content: "test".to_string(),
            }),
            from: Some(MessageFrom {
                user: Some(GraphUser {
                    id: None,
                    display_name: None,
                    user_id: Some("alt-user-id".to_string()),
                }),
            }),
            created_date_time: None,
            last_modified_date_time: None,
            attachments: vec![],
            mentions: vec![],
        };

        let event = message_to_event(&msg, "t1", "c1");
        assert_eq!(
            event.tags.get("author").map(|s| s.as_str()),
            Some("alt-user-id")
        );
        assert_eq!(
            event.tags.get("sender").map(|s| s.as_str()),
            Some("unknown")
        );
    }

    #[test]
    fn test_teams_connector_name() {
        let connector = TeamsConnector::new(make_teams_config());
        assert_eq!(connector.name(), "teams");
    }

    #[test]
    fn test_teams_message_unicode_content() {
        let msg = ChatMessage {
            id: "msg-unicode".to_string(),
            message_type: Some("message".to_string()),
            subject: None,
            summary: None,
            body: Some(MessageBody {
                content_type: Some("text".to_string()),
                content: "你好世界 🌍 مرحبا".to_string(),
            }),
            from: Some(MessageFrom {
                user: Some(GraphUser {
                    id: Some("user-u".to_string()),
                    display_name: Some("Unicode User".to_string()),
                    user_id: None,
                }),
            }),
            created_date_time: None,
            last_modified_date_time: None,
            attachments: vec![],
            mentions: vec![],
        };

        let event = message_to_event(&msg, "t1", "c1");
        let payload: serde_json::Value = serde_json::from_slice(&event.payload).unwrap();
        assert_eq!(payload["content"], "你好世界 🌍 مرحبا");
    }

    #[test]
    fn test_teams_message_serialization_roundtrip() {
        let msg = ChatMessage {
            id: "msg-roundtrip".to_string(),
            message_type: Some("message".to_string()),
            subject: Some("Test Subject".to_string()),
            summary: None,
            body: Some(MessageBody {
                content_type: Some("html".to_string()),
                content: "<p>Roundtrip test 🎉</p>".to_string(),
            }),
            from: Some(MessageFrom {
                user: Some(GraphUser {
                    id: Some("user-rt".to_string()),
                    display_name: Some("Round Trip".to_string()),
                    user_id: None,
                }),
            }),
            created_date_time: Some("2024-08-01T15:30:00Z".to_string()),
            last_modified_date_time: None,
            attachments: vec![],
            mentions: vec![],
        };

        let event = message_to_event(&msg, "t1", "c1");
        let payload: serde_json::Value = serde_json::from_slice(&event.payload).unwrap();
        // Verify all fields are present and correct
        assert_eq!(payload["message_id"], "msg-roundtrip");
        assert_eq!(payload["author_name"], "Round Trip");
        assert_eq!(event.source, "teams");
        assert_eq!(event.event_type, "message");
    }

    #[test]
    fn test_teams_message_multiple_attachments() {
        let msg = ChatMessage {
            id: "msg-multi-att".to_string(),
            message_type: Some("message".to_string()),
            subject: None,
            summary: None,
            body: Some(MessageBody {
                content_type: Some("text".to_string()),
                content: "Multiple files attached".to_string(),
            }),
            from: Some(MessageFrom {
                user: Some(GraphUser {
                    id: Some("user-ma".to_string()),
                    display_name: Some("Multi Attach".to_string()),
                    user_id: None,
                }),
            }),
            created_date_time: None,
            last_modified_date_time: None,
            attachments: vec![
                ChatMessageAttachment {
                    id: Some("att-1".to_string()),
                    content_type: Some("reference".to_string()),
                    name: Some("report.xlsx".to_string()),
                    content_url: Some("https://contoso.sharepoint.com/report.xlsx".to_string()),
                },
                ChatMessageAttachment {
                    id: Some("att-2".to_string()),
                    content_type: Some("reference".to_string()),
                    name: Some("slides.pptx".to_string()),
                    content_url: Some("https://contoso.sharepoint.com/slides.pptx".to_string()),
                },
            ],
            mentions: vec![],
        };

        let event = message_to_event(&msg, "t1", "c1");
        let payload: serde_json::Value = serde_json::from_slice(&event.payload).unwrap();
        let att = payload["attachments"].as_array().unwrap();
        assert_eq!(att.len(), 2);
        assert_eq!(att[0]["name"], "report.xlsx");
        assert_eq!(att[1]["name"], "slides.pptx");
    }

    #[test]
    fn test_teams_message_no_body() {
        let msg = ChatMessage {
            id: "msg-no-body".to_string(),
            message_type: Some("message".to_string()),
            subject: None,
            summary: None,
            body: None,
            from: Some(MessageFrom {
                user: Some(GraphUser {
                    id: Some("user-nb".to_string()),
                    display_name: Some("No Body".to_string()),
                    user_id: None,
                }),
            }),
            created_date_time: None,
            last_modified_date_time: None,
            attachments: vec![],
            mentions: vec![],
        };

        let event = message_to_event(&msg, "t1", "c1");
        let payload: serde_json::Value = serde_json::from_slice(&event.payload).unwrap();
        assert_eq!(payload["content"], "");
    }

    #[test]
    fn test_teams_strip_html_script_tags() {
        let result = strip_html("<script>alert('xss')</script>Hello");
        assert!(!result.contains("script"));
        assert!(result.contains("Hello"));
    }

    #[test]
    fn test_teams_strip_html_multiple_entities() {
        assert_eq!(strip_html("&amp; &lt; &gt;"), "& < >");
        assert_eq!(strip_html("a&amp;b&lt;c&gt;d"), "a&b<c>d");
    }

    #[test]
    fn test_teams_message_mentions_present() {
        // Verify that a message with mentions is processed correctly.
        // Note: mentions are currently stored on the struct but not serialized into the event payload.
        let msg = ChatMessage {
            id: "msg-mentions".to_string(),
            message_type: Some("message".to_string()),
            subject: None,
            summary: None,
            body: Some(MessageBody {
                content_type: Some("text".to_string()),
                content: "Hey @Alice, check this!".to_string(),
            }),
            from: Some(MessageFrom {
                user: Some(GraphUser {
                    id: Some("user-ment".to_string()),
                    display_name: Some("Mentioner".to_string()),
                    user_id: None,
                }),
            }),
            created_date_time: None,
            last_modified_date_time: None,
            attachments: vec![],
            mentions: vec![serde_json::json!({
                "id": 1,
                "mentionText": "Alice",
                "mentioned": { "user": { "id": "user-alice", "displayName": "Alice" } }
            })],
        };

        let event = message_to_event(&msg, "t1", "c1");
        let payload: serde_json::Value = serde_json::from_slice(&event.payload).unwrap();
        // Content should include the mention text
        assert!(payload["content"].as_str().unwrap().contains("@Alice"));
        assert_eq!(event.event_type, "message");
    }

    #[test]
    fn test_teams_message_large_content() {
        let large_content = "B".repeat(10_000);
        let msg = ChatMessage {
            id: "msg-large".to_string(),
            message_type: Some("message".to_string()),
            subject: None,
            summary: None,
            body: Some(MessageBody {
                content_type: Some("text".to_string()),
                content: large_content.clone(),
            }),
            from: Some(MessageFrom {
                user: Some(GraphUser {
                    id: Some("user-lg".to_string()),
                    display_name: Some("Verbose".to_string()),
                    user_id: None,
                }),
            }),
            created_date_time: None,
            last_modified_date_time: None,
            attachments: vec![],
            mentions: vec![],
        };

        let event = message_to_event(&msg, "t1", "c1");
        let payload: serde_json::Value = serde_json::from_slice(&event.payload).unwrap();
        assert_eq!(payload["content"].as_str().unwrap().len(), 10_000);
        assert!(!event.payload.is_empty());
    }
}
