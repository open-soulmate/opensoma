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
use crate::config::DiscordConfig;
use crate::connector::circuit_breaker::CircuitBreaker;
use crate::connector::Connector;
use crate::retry_async;

/// Discord connector — polls Discord channels for new messages using the Discord Bot HTTP API.
///
/// Requires a Bot Token with the following intents/permissions:
/// - MESSAGE_CONTENT intent (privileged)
/// - READ_MESSAGE_HISTORY permission
/// - VIEW_CHANNEL permission
///
/// Uses the Discord REST API v10 (https://discord.com/api/v10/).
pub struct DiscordConnector {
    config: DiscordConfig,
}

impl DiscordConnector {
    pub fn new(config: DiscordConfig) -> Self {
        Self { config }
    }
}

#[async_trait]
impl Connector for DiscordConnector {
    fn name(&self) -> &str {
        "discord"
    }

    async fn ping(&self) -> Result<()> {
        let client = Client::new();
        let resp = client
            .get("https://discord.com/api/v10/users/@me")
            .header("Authorization", format!("Bot {}", self.config.bot_token))
            .send()
            .await
            .context("Failed to reach Discord API")?;

        if resp.status().is_success() {
            Ok(())
        } else {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            Err(anyhow::anyhow!(
                "Discord API auth failed ({}): {}",
                status,
                body
            ))
        }
    }
}

/// Discord channel object (partial).
#[derive(Debug, Deserialize, Clone)]
#[allow(dead_code)]
struct DiscordChannel {
    id: String,
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    r#type: Option<u32>,
}

/// Discord message object (partial).
#[derive(Debug, Deserialize, Serialize, Clone)]
struct DiscordMessage {
    id: String,
    #[serde(default)]
    content: String,
    #[serde(default)]
    channel_id: String,
    #[serde(default)]
    author: Option<DiscordUser>,
    #[serde(default)]
    timestamp: Option<String>,
    #[serde(default)]
    edited_timestamp: Option<String>,
    #[serde(default)]
    attachments: Vec<DiscordAttachment>,
    #[serde(default)]
    embeds: Vec<serde_json::Value>,
    #[serde(default)]
    r#type: Option<u32>,
}

/// Discord user object (partial).
#[derive(Debug, Deserialize, Serialize, Clone)]
struct DiscordUser {
    id: String,
    #[serde(default)]
    username: String,
    #[serde(default)]
    global_name: Option<String>,
    #[serde(default)]
    bot: Option<bool>,
}

/// Discord attachment object (partial).
#[derive(Debug, Deserialize, Serialize, Clone)]
struct DiscordAttachment {
    id: String,
    #[serde(default)]
    filename: String,
    #[serde(default)]
    url: String,
    #[serde(default)]
    content_type: Option<String>,
    #[serde(default)]
    size: Option<u64>,
}

/// Fetch channels from a Discord guild (server).
async fn fetch_channels(
    client: &Client,
    bot_token: &str,
    guild_id: &str,
) -> Result<Vec<DiscordChannel>> {
    let url = format!("https://discord.com/api/v10/guilds/{}/channels", guild_id);
    let resp = retry_async!("discord_channels", 3, {
        let r = client
            .get(&url)
            .header("Authorization", format!("Bot {}", bot_token))
            .send()
            .await
            .context("Failed to fetch Discord channels")?;

        let status = r.status();
        if !status.is_success() {
            let body = r.text().await.unwrap_or_default();
            anyhow::bail!("Discord channels API error ({}): {}", status, body);
        }

        let channels: Vec<DiscordChannel> = r
            .json()
            .await
            .context("Failed to parse Discord channels response")?;
        Ok::<Vec<DiscordChannel>, anyhow::Error>(channels)
    })?;

    // Filter to text channels only (type 0 = GUILD_TEXT, type 5 = GUILD_ANNOUNCEMENT)
    let text_channels: Vec<DiscordChannel> = resp
        .into_iter()
        .filter(|ch| matches!(ch.r#type, Some(0) | Some(5)))
        .collect();

    Ok(text_channels)
}

/// Fetch messages from a Discord channel.
/// Returns messages in reverse chronological order (newest first).
async fn fetch_messages(
    client: &Client,
    bot_token: &str,
    channel_id: &str,
    after: Option<&str>,
    limit: u32,
) -> Result<Vec<DiscordMessage>> {
    let mut url = format!(
        "https://discord.com/api/v10/channels/{}/messages?limit={}",
        channel_id, limit
    );
    if let Some(after_id) = after {
        url = format!("{}&after={}", url, after_id);
    }

    let resp = retry_async!("discord_messages", 3, {
        let r = client
            .get(&url)
            .header("Authorization", format!("Bot {}", bot_token))
            .send()
            .await
            .context("Failed to fetch Discord messages")?;

        let status = r.status();
        if !status.is_success() {
            let body = r.text().await.unwrap_or_default();
            anyhow::bail!("Discord messages API error ({}): {}", status, body);
        }

        let messages: Vec<DiscordMessage> = r
            .json()
            .await
            .context("Failed to parse Discord messages response")?;
        Ok::<Vec<DiscordMessage>, anyhow::Error>(messages)
    })?;

    Ok(resp)
}

/// Convert a DiscordMessage into a RawEvent.
fn message_to_event(msg: &DiscordMessage, guild_id: &str) -> RawEvent {
    let author_name = msg
        .author
        .as_ref()
        .map(|a| {
            a.global_name
                .clone()
                .unwrap_or_else(|| a.username.clone())
        })
        .unwrap_or_else(|| "unknown".to_string());

    let author_id = msg
        .author
        .as_ref()
        .map(|a| a.id.clone())
        .unwrap_or_default();

    let mut payload = serde_json::json!({
        "source": "discord",
        "message_id": msg.id,
        "channel_id": msg.channel_id,
        "guild_id": guild_id,
        "author_id": author_id,
        "author_name": author_name,
        "content": msg.content,
        "timestamp": msg.timestamp,
        "edited_timestamp": msg.edited_timestamp,
        "type": msg.r#type,
    });

    // Include attachment info
    if !msg.attachments.is_empty() {
        payload["attachments"] = serde_json::json!(
            msg.attachments
                .iter()
                .map(|a| {
                    serde_json::json!({
                        "filename": a.filename,
                        "url": a.url,
                        "content_type": a.content_type,
                        "size": a.size,
                    })
                })
                .collect::<Vec<_>>()
        );
    }

    // Include embeds
    if !msg.embeds.is_empty() {
        payload["embeds"] = serde_json::json!(msg.embeds);
    }

    // Build tags
    let mut tags = std::collections::HashMap::new();
    tags.insert("source".to_string(), "discord".to_string());
    tags.insert("channel".to_string(), msg.channel_id.clone());
    tags.insert("author".to_string(), author_id.clone());
    if let Some(ref author) = msg.author {
        if author.bot.unwrap_or(false) {
            tags.insert("bot".to_string(), "true".to_string());
        }
    }

    RawEvent {
        id: Uuid::new_v4().to_string(),
        source: "discord".to_string(),
        event_type: "message".to_string(),
        payload: serde_json::to_vec(&payload).unwrap_or_default(),
        timestamp_ms: Utc::now().timestamp_millis(),
        tags,
    }
}

/// Start the Discord connector — spawns a background task that polls configured channels.
pub async fn start(
    config: DiscordConfig,
    tx: EventTx,
    circuit_breaker: Option<CircuitBreaker>,
) -> Result<JoinHandle<()>> {
    let client = Client::builder()
        .timeout(Duration::from_secs(30))
        .build()
        .context("Failed to create HTTP client for Discord connector")?;

    let bot_token = config.bot_token.clone();
    let guild_id = config.guild_id.clone();
    let channels = config.channels.clone();
    let poll_interval = Duration::from_secs(config.poll_interval_secs);
    let channel_filter = channels.clone();

    let handle = tokio::spawn(async move {
        info!(
            "Discord connector started — guild={}, poll_interval={}s",
            guild_id,
            poll_interval.as_secs()
        );

        let _cb = circuit_breaker; // Circuit breaker integration point

        // Track the last seen message ID per channel for incremental fetching
        let mut last_seen: std::collections::HashMap<String, String> =
            std::collections::HashMap::new();

        // Resolve channel IDs: if specific channels are configured, use them;
        // otherwise discover all text channels in the guild
        let resolved_channels: Vec<String> = if channel_filter.is_empty() {
            // Discover channels from guild
            match fetch_channels(&client, &bot_token, &guild_id).await {
                Ok(ch) => {
                    let ids: Vec<String> = ch.iter().map(|c| c.id.clone()).collect();
                    info!("Discovered {} text channels in guild {}", ids.len(), guild_id);
                    ids
                }
                Err(e) => {
                    error!("Failed to discover Discord channels: {}", e);
                    return;
                }
            }
        } else {
            channel_filter
        };

        if resolved_channels.is_empty() {
            warn!("No Discord channels to monitor — connector idle");
        }

        let mut interval = tokio::time::interval(poll_interval);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        // Skip the first immediate tick
        interval.tick().await;

        loop {
            interval.tick().await;

            // Circuit breaker integration point
            if let Some(ref cb) = _cb {
                if cb.allow_request().await.is_err() {
                    warn!("Discord connector circuit breaker open — skipping poll");
                    tokio::time::sleep(Duration::from_secs(30)).await;
                    continue;
                }
            }

            let mut batch_count = 0u64;

            for channel_id in &resolved_channels {
                let after_id = last_seen.get(channel_id).map(|s| s.as_str());

                match fetch_messages(&client, &bot_token, channel_id, after_id, 50).await {
                    Ok(messages) => {
                        if messages.is_empty() {
                            debug!("No new messages in Discord channel {}", channel_id);
                            continue;
                        }

                        // Messages come newest-first; reverse for chronological order
                        let mut sorted = messages;
                        sorted.sort_by(|a, b| a.id.cmp(&b.id));

                        for msg in &sorted {
                            // Skip bot messages if configured
                            if config.ignore_bots
                                && msg
                                    .author
                                    .as_ref()
                                    .and_then(|a| a.bot)
                                    .unwrap_or(false)
                            {
                                continue;
                            }

                            // Skip empty messages (e.g. join/leave events)
                            if msg.content.is_empty() && msg.attachments.is_empty() {
                                continue;
                            }

                            let event = message_to_event(msg, &guild_id);
                            if let Err(e) = tx.send(event).await {
                                error!("Failed to send Discord event: {}", e);
                            }
                            batch_count += 1;
                        }

                        // Update last seen to the newest message ID
                        if let Some(newest) = sorted.last() {
                            last_seen.insert(channel_id.clone(), newest.id.clone());
                        }

                        if let Some(ref cb) = _cb {
                            cb.record_success().await;
                        }
                    }
                    Err(e) => {
                        warn!("Failed to fetch Discord channel {}: {}", channel_id, e);
                        if let Some(ref cb) = _cb {
                            cb.record_failure().await;
                        }
                    }
                }

                // Rate limit: Discord allows 50 requests/second for bots
                tokio::time::sleep(Duration::from_millis(100)).await;
            }

            if batch_count > 0 {
                info!("Discord connector: collected {} messages", batch_count);
            }
        }
    });

    Ok(handle)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_discord_message_to_event() {
        let msg = DiscordMessage {
            id: "1234567890".to_string(),
            content: "Hello, world!".to_string(),
            channel_id: "channel_1".to_string(),
            author: Some(DiscordUser {
                id: "user_1".to_string(),
                username: "testuser".to_string(),
                global_name: Some("Test User".to_string()),
                bot: Some(false),
            }),
            timestamp: Some("2024-01-15T10:30:00.000000+00:00".to_string()),
            edited_timestamp: None,
            attachments: vec![],
            embeds: vec![],
            r#type: Some(0),
        };

        let event = message_to_event(&msg, "guild_1");
        assert_eq!(event.source, "discord");
        assert_eq!(event.event_type, "message");
        assert!(event.tags.get("source").map(|s| s.as_str()) == Some("discord"));
        assert!(event.tags.get("channel").map(|s| s.as_str()) == Some("channel_1"));
        assert!(event.tags.get("author").map(|s| s.as_str()) == Some("user_1"));

        let payload: serde_json::Value = serde_json::from_slice(&event.payload).unwrap();
        assert_eq!(payload["content"], "Hello, world!");
        assert_eq!(payload["author_name"], "Test User");
    }

    #[test]
    fn test_discord_message_to_event_bot() {
        let msg = DiscordMessage {
            id: "999".to_string(),
            content: "I am a bot".to_string(),
            channel_id: "ch_2".to_string(),
            author: Some(DiscordUser {
                id: "bot_1".to_string(),
                username: "botuser".to_string(),
                global_name: None,
                bot: Some(true),
            }),
            timestamp: None,
            edited_timestamp: None,
            attachments: vec![],
            embeds: vec![],
            r#type: None,
        };

        let event = message_to_event(&msg, "guild_2");
        assert!(event.tags.get("bot").map(|s| s.as_str()) == Some("true"));

        let payload: serde_json::Value = serde_json::from_slice(&event.payload).unwrap();
        assert_eq!(payload["author_name"], "botuser"); // Falls back to username
    }

    #[test]
    fn test_discord_message_to_event_with_attachments() {
        let msg = DiscordMessage {
            id: "111".to_string(),
            content: "Check this out".to_string(),
            channel_id: "ch_3".to_string(),
            author: Some(DiscordUser {
                id: "user_2".to_string(),
                username: "uploader".to_string(),
                global_name: None,
                bot: None,
            }),
            timestamp: None,
            edited_timestamp: None,
            attachments: vec![DiscordAttachment {
                id: "att_1".to_string(),
                filename: "image.png".to_string(),
                url: "https://cdn.discordapp.com/attachments/image.png".to_string(),
                content_type: Some("image/png".to_string()),
                size: Some(1024),
            }],
            embeds: vec![],
            r#type: Some(0),
        };

        let event = message_to_event(&msg, "guild_3");
        let payload: serde_json::Value = serde_json::from_slice(&event.payload).unwrap();
        assert!(payload["attachments"].is_array());
        assert_eq!(payload["attachments"][0]["filename"], "image.png");
    }

    #[test]
    fn test_discord_message_to_event_empty_author() {
        let msg = DiscordMessage {
            id: "222".to_string(),
            content: "System message".to_string(),
            channel_id: "ch_4".to_string(),
            author: None,
            timestamp: None,
            edited_timestamp: None,
            attachments: vec![],
            embeds: vec![],
            r#type: Some(7), // system message
        };

        let event = message_to_event(&msg, "guild_4");
        let payload: serde_json::Value = serde_json::from_slice(&event.payload).unwrap();
        assert_eq!(payload["author_name"], "unknown");
        assert_eq!(payload["author_id"], "");
    }

    #[test]
    fn test_discord_message_with_embeds() {
        let msg = DiscordMessage {
            id: "333".to_string(),
            content: "Check this link".to_string(),
            channel_id: "ch_5".to_string(),
            author: Some(DiscordUser {
                id: "user_3".to_string(),
                username: "sharer".to_string(),
                global_name: Some("Link Sharer".to_string()),
                bot: Some(false),
            }),
            timestamp: Some("2024-06-01T12:00:00.000000+00:00".to_string()),
            edited_timestamp: None,
            attachments: vec![],
            embeds: vec![serde_json::json!({
                "title": "Example Page",
                "description": "A sample embed",
                "url": "https://example.com",
                "type": "rich"
            })],
            r#type: Some(0),
        };

        let event = message_to_event(&msg, "guild_5");
        let payload: serde_json::Value = serde_json::from_slice(&event.payload).unwrap();
        assert!(payload["embeds"].is_array());
        assert_eq!(payload["embeds"][0]["title"], "Example Page");
        assert_eq!(payload["embeds"][0]["url"], "https://example.com");
    }

    #[test]
    fn test_discord_message_edited() {
        let msg = DiscordMessage {
            id: "444".to_string(),
            content: "Edited content".to_string(),
            channel_id: "ch_6".to_string(),
            author: Some(DiscordUser {
                id: "user_4".to_string(),
                username: "editor".to_string(),
                global_name: None,
                bot: None,
            }),
            timestamp: Some("2024-06-01T12:00:00.000000+00:00".to_string()),
            edited_timestamp: Some("2024-06-01T12:05:00.000000+00:00".to_string()),
            attachments: vec![],
            embeds: vec![],
            r#type: Some(0),
        };

        let event = message_to_event(&msg, "guild_6");
        let payload: serde_json::Value = serde_json::from_slice(&event.payload).unwrap();
        assert_eq!(payload["content"], "Edited content");
        assert_eq!(
            payload["edited_timestamp"],
            "2024-06-01T12:05:00.000000+00:00"
        );
        assert_eq!(
            payload["timestamp"],
            "2024-06-01T12:00:00.000000+00:00"
        );
    }

    #[test]
    fn test_discord_message_multiple_attachments() {
        let msg = DiscordMessage {
            id: "555".to_string(),
            content: "Multiple files".to_string(),
            channel_id: "ch_7".to_string(),
            author: Some(DiscordUser {
                id: "user_5".to_string(),
                username: "multifile".to_string(),
                global_name: None,
                bot: None,
            }),
            timestamp: None,
            edited_timestamp: None,
            attachments: vec![
                DiscordAttachment {
                    id: "att_1".to_string(),
                    filename: "photo.jpg".to_string(),
                    url: "https://cdn.discordapp.com/photo.jpg".to_string(),
                    content_type: Some("image/jpeg".to_string()),
                    size: Some(2048),
                },
                DiscordAttachment {
                    id: "att_2".to_string(),
                    filename: "doc.pdf".to_string(),
                    url: "https://cdn.discordapp.com/doc.pdf".to_string(),
                    content_type: Some("application/pdf".to_string()),
                    size: Some(4096),
                },
            ],
            embeds: vec![],
            r#type: Some(0),
        };

        let event = message_to_event(&msg, "guild_7");
        let payload: serde_json::Value = serde_json::from_slice(&event.payload).unwrap();
        let att = payload["attachments"].as_array().unwrap();
        assert_eq!(att.len(), 2);
        assert_eq!(att[0]["filename"], "photo.jpg");
        assert_eq!(att[0]["content_type"], "image/jpeg");
        assert_eq!(att[1]["filename"], "doc.pdf");
        assert_eq!(att[1]["size"], 4096);
    }

    #[test]
    fn test_discord_message_empty_content_with_attachment() {
        // Discord messages can have empty content if they only contain attachments
        let msg = DiscordMessage {
            id: "666".to_string(),
            content: "".to_string(),
            channel_id: "ch_8".to_string(),
            author: Some(DiscordUser {
                id: "user_6".to_string(),
                username: "uploader".to_string(),
                global_name: None,
                bot: None,
            }),
            timestamp: None,
            edited_timestamp: None,
            attachments: vec![DiscordAttachment {
                id: "att_3".to_string(),
                filename: "image.gif".to_string(),
                url: "https://cdn.discordapp.com/image.gif".to_string(),
                content_type: Some("image/gif".to_string()),
                size: Some(512),
            }],
            embeds: vec![],
            r#type: Some(0),
        };

        let event = message_to_event(&msg, "guild_8");
        let payload: serde_json::Value = serde_json::from_slice(&event.payload).unwrap();
        assert_eq!(payload["content"], "");
        assert!(payload["attachments"].is_array());
        assert_eq!(payload["attachments"][0]["filename"], "image.gif");
    }

    #[test]
    fn test_discord_message_reply_type() {
        let msg = DiscordMessage {
            id: "777".to_string(),
            content: "This is a reply".to_string(),
            channel_id: "ch_9".to_string(),
            author: Some(DiscordUser {
                id: "user_7".to_string(),
                username: "replier".to_string(),
                global_name: Some("Reply User".to_string()),
                bot: Some(false),
            }),
            timestamp: Some("2024-07-01T09:00:00.000000+00:00".to_string()),
            edited_timestamp: None,
            attachments: vec![],
            embeds: vec![],
            r#type: Some(19), // reply type
        };

        let event = message_to_event(&msg, "guild_9");
        let payload: serde_json::Value = serde_json::from_slice(&event.payload).unwrap();
        assert_eq!(payload["type"], 19);
        assert_eq!(event.event_type, "message");
        assert_eq!(event.source, "discord");
    }

    #[test]
    fn test_discord_message_serialization_roundtrip() {
        let msg = DiscordMessage {
            id: "888".to_string(),
            content: "Roundtrip test 🎉".to_string(),
            channel_id: "ch_10".to_string(),
            author: Some(DiscordUser {
                id: "user_8".to_string(),
                username: "roundtrip".to_string(),
                global_name: Some("Round Trip".to_string()),
                bot: Some(false),
            }),
            timestamp: Some("2024-08-01T15:30:00.000000+00:00".to_string()),
            edited_timestamp: None,
            attachments: vec![],
            embeds: vec![],
            r#type: Some(0),
        };

        // Serialize to JSON and back
        let json = serde_json::to_string(&msg).unwrap();
        let deserialized: DiscordMessage = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.id, "888");
        assert_eq!(deserialized.content, "Roundtrip test 🎉");
        assert_eq!(
            deserialized.author.as_ref().unwrap().global_name,
            Some("Round Trip".to_string())
        );
    }

    #[test]
    fn test_discord_message_unicode_content() {
        let msg = DiscordMessage {
            id: "999".to_string(),
            content: "你好世界 🌍 مرحبا".to_string(),
            channel_id: "ch_11".to_string(),
            author: Some(DiscordUser {
                id: "user_9".to_string(),
                username: "unicode_user".to_string(),
                global_name: Some("Unicode 名前".to_string()),
                bot: None,
            }),
            timestamp: None,
            edited_timestamp: None,
            attachments: vec![],
            embeds: vec![],
            r#type: Some(0),
        };

        let event = message_to_event(&msg, "guild_10");
        let payload: serde_json::Value = serde_json::from_slice(&event.payload).unwrap();
        assert_eq!(payload["content"], "你好世界 🌍 مرحبا");
        assert_eq!(payload["author_name"], "Unicode 名前");
    }

    #[test]
    fn test_discord_channel_type_filtering() {
        // Verify channel type filtering logic: only type 0 (GUILD_TEXT) and 5 (GUILD_ANNOUNCEMENT) pass
        let channels = vec![
            DiscordChannel { id: "1".to_string(), name: Some("general".to_string()), r#type: Some(0) },
            DiscordChannel { id: "2".to_string(), name: Some("voice-room".to_string()), r#type: Some(2) },
            DiscordChannel { id: "3".to_string(), name: Some("announcements".to_string()), r#type: Some(5) },
            DiscordChannel { id: "4".to_string(), name: Some("stage".to_string()), r#type: Some(13) },
            DiscordChannel { id: "5".to_string(), name: Some("forum".to_string()), r#type: Some(15) },
            DiscordChannel { id: "6".to_string(), name: Some("unknown".to_string()), r#type: None },
        ];

        let text_channels: Vec<&DiscordChannel> = channels
            .iter()
            .filter(|ch| matches!(ch.r#type, Some(0) | Some(5)))
            .collect();

        assert_eq!(text_channels.len(), 2);
        assert_eq!(text_channels[0].id, "1");
        assert_eq!(text_channels[1].id, "3");
    }

    #[test]
    fn test_discord_message_id_ordering() {
        // Discord snowflake IDs are sortable chronologically
        let mut ids = vec!["100", "50", "200", "150", "75"];
        ids.sort();
        assert_eq!(ids, vec!["100", "150", "200", "50", "75"]); // lexicographic sort
        // Note: real Discord IDs are 64-bit integers, but string comparison works for same-length IDs
    }

    #[test]
    fn test_discord_connector_name() {
        let connector = DiscordConnector::new(DiscordConfig {
            enabled: true,
            bot_token: "xoxb-test".to_string(),
            guild_id: "guild_1".to_string(),
            channels: vec![],
            poll_interval_secs: 60,
            ignore_bots: false,
        });
        assert_eq!(connector.name(), "discord");
    }

    #[test]
    fn test_discord_attachment_no_content_type() {
        let msg = DiscordMessage {
            id: "att_no_ct".to_string(),
            content: "File upload".to_string(),
            channel_id: "ch_12".to_string(),
            author: Some(DiscordUser {
                id: "user_10".to_string(),
                username: "fileuser".to_string(),
                global_name: None,
                bot: None,
            }),
            timestamp: None,
            edited_timestamp: None,
            attachments: vec![DiscordAttachment {
                id: "att_4".to_string(),
                filename: "mystery.bin".to_string(),
                url: "https://cdn.discordapp.com/mystery.bin".to_string(),
                content_type: None,
                size: None,
            }],
            embeds: vec![],
            r#type: Some(0),
        };

        let event = message_to_event(&msg, "guild_11");
        let payload: serde_json::Value = serde_json::from_slice(&event.payload).unwrap();
        let att = payload["attachments"].as_array().unwrap();
        assert_eq!(att.len(), 1);
        assert_eq!(att[0]["filename"], "mystery.bin");
        assert!(att[0]["content_type"].is_null());
        assert!(att[0]["size"].is_null());
    }

    #[test]
    fn test_discord_event_tags_no_bot_tag_for_human() {
        let msg = DiscordMessage {
            id: "human_msg".to_string(),
            content: "I'm human".to_string(),
            channel_id: "ch_13".to_string(),
            author: Some(DiscordUser {
                id: "human_1".to_string(),
                username: "humanuser".to_string(),
                global_name: Some("Human".to_string()),
                bot: Some(false),
            }),
            timestamp: None,
            edited_timestamp: None,
            attachments: vec![],
            embeds: vec![],
            r#type: Some(0),
        };

        let event = message_to_event(&msg, "guild_12");
        // Human messages should NOT have the "bot" tag
        assert!(event.tags.get("bot").is_none());
    }

    #[test]
    fn test_discord_message_empty_payload_serialization() {
        let msg = DiscordMessage {
            id: "empty_all".to_string(),
            content: "".to_string(),
            channel_id: "".to_string(),
            author: None,
            timestamp: None,
            edited_timestamp: None,
            attachments: vec![],
            embeds: vec![],
            r#type: None,
        };

        let event = message_to_event(&msg, "");
        let payload: serde_json::Value = serde_json::from_slice(&event.payload).unwrap();
        assert_eq!(payload["content"], "");
        assert_eq!(payload["channel_id"], "");
        assert_eq!(payload["guild_id"], "");
        assert_eq!(payload["author_name"], "unknown");
        assert!(payload["attachments"].is_null());
        assert!(payload["embeds"].is_null());
    }

    #[test]
    fn test_discord_message_large_content() {
        let large_content = "A".repeat(10_000);
        let msg = DiscordMessage {
            id: "large_msg".to_string(),
            content: large_content.clone(),
            channel_id: "ch_14".to_string(),
            author: Some(DiscordUser {
                id: "user_11".to_string(),
                username: "verbose".to_string(),
                global_name: None,
                bot: None,
            }),
            timestamp: None,
            edited_timestamp: None,
            attachments: vec![],
            embeds: vec![],
            r#type: Some(0),
        };

        let event = message_to_event(&msg, "guild_13");
        let payload: serde_json::Value = serde_json::from_slice(&event.payload).unwrap();
        assert_eq!(payload["content"].as_str().unwrap().len(), 10_000);
        // Event payload should be serializable
        assert!(!event.payload.is_empty());
    }

    #[test]
    fn test_discord_user_global_name_fallback() {
        // When global_name is None, should fall back to username
        let user = DiscordUser {
            id: "u1".to_string(),
            username: "fallback_user".to_string(),
            global_name: None,
            bot: None,
        };
        let msg = DiscordMessage {
            id: "fallback_test".to_string(),
            content: "test".to_string(),
            channel_id: "ch".to_string(),
            author: Some(user),
            timestamp: None,
            edited_timestamp: None,
            attachments: vec![],
            embeds: vec![],
            r#type: Some(0),
        };

        let event = message_to_event(&msg, "g");
        let payload: serde_json::Value = serde_json::from_slice(&event.payload).unwrap();
        assert_eq!(payload["author_name"], "fallback_user");
    }
}
