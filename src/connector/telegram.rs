use anyhow::{Context, Result};
use reqwest::Client;
use tokio::task::JoinHandle;
use tokio::time::Duration;
use tracing::{debug, error, info, warn};

use crate::collector::{EventTx, RawEvent};
use crate::config::TelegramConfig;
use crate::connector::circuit_breaker::CircuitBreaker;
use crate::connector::Connector;

/// Telegram connector implementing the unified Connector trait.
/// Uses the Telegram Bot API (long-polling via getUpdates) to collect
/// messages, edited messages, channel posts, and callback queries.
pub struct TelegramConnector {
    config: TelegramConfig,
}

impl TelegramConnector {
    pub fn new(config: TelegramConfig) -> Self {
        Self { config }
    }
}

#[async_trait::async_trait]
impl Connector for TelegramConnector {
    fn name(&self) -> &str {
        "telegram"
    }

    async fn ping(&self) -> Result<()> {
        let client = Client::builder().timeout(Duration::from_secs(10)).build()?;
        let url = format!(
            "https://api.telegram.org/bot{}/getMe",
            self.config.bot_token
        );
        let resp = client
            .get(&url)
            .send()
            .await
            .context("Telegram API unreachable")?;
        if !resp.status().is_success() {
            anyhow::bail!("Telegram getMe returned {}", resp.status());
        }
        let data: serde_json::Value = resp.json().await?;
        if data["ok"] != true {
            anyhow::bail!(
                "Telegram getMe not ok: {}",
                data["description"].as_str().unwrap_or("unknown")
            );
        }
        Ok(())
    }
}

/// Start the Telegram connector. Uses long-polling (getUpdates) to receive
/// updates from the Bot API and forwards them into the collector pipeline.
pub async fn start(
    config: TelegramConfig,
    tx: EventTx,
    circuit_breaker: Option<CircuitBreaker>,
) -> Result<JoinHandle<()>> {
    let http_client = Client::builder()
        .timeout(Duration::from_secs(65)) // slightly > long-poll timeout
        .build()?;

    let poll_interval = Duration::from_secs(config.poll_interval_secs);
    let allowed_chats: Vec<i64> = config.allowed_chats.clone().unwrap_or_default();
    let include_edited = config.include_edited;
    let bot_token = config.bot_token.clone();

    info!(
        "Telegram connector starting — polling every {}s, {} allowed chats",
        config.poll_interval_secs,
        if allowed_chats.is_empty() {
            "all".to_string()
        } else {
            allowed_chats.len().to_string()
        }
    );

    let handle = tokio::spawn(async move {
        let cb = circuit_breaker;
        let mut offset: i64 = 0;
        let mut interval = tokio::time::interval(poll_interval);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        // Initial poll immediately
        match poll_updates(
            &http_client,
            &bot_token,
            offset,
            &allowed_chats,
            include_edited,
            &tx,
        )
        .await
        {
            Ok(new_offset) => {
                if new_offset > 0 {
                    offset = new_offset;
                }
            }
            Err(e) => error!("Telegram initial poll failed: {}", e),
        }

        loop {
            interval.tick().await;

            // Circuit breaker check
            if let Some(ref c) = cb {
                if c.allow_request().await.is_err() {
                    debug!("Telegram circuit breaker open — skipping poll cycle");
                    continue;
                }
            }

            match poll_updates(
                &http_client,
                &bot_token,
                offset,
                &allowed_chats,
                include_edited,
                &tx,
            )
            .await
            {
                Ok(new_offset) => {
                    if new_offset > 0 {
                        offset = new_offset;
                    }
                    if let Some(ref c) = cb {
                        c.record_success().await;
                    }
                }
                Err(e) => {
                    warn!("Telegram poll failed: {}", e);
                    if let Some(ref c) = cb {
                        c.record_failure().await;
                    }
                }
            }
        }
    });

    Ok(handle)
}

/// Poll Telegram getUpdates and forward new messages as RawEvents.
/// Returns the next offset to use (last update_id + 1).
async fn poll_updates(
    client: &Client,
    bot_token: &str,
    offset: i64,
    allowed_chats: &[i64],
    include_edited: bool,
    tx: &EventTx,
) -> Result<i64> {
    let url = format!("https://api.telegram.org/bot{}/getUpdates", bot_token);

    let mut params = serde_json::json!({
        "timeout": 30,
        "allowed_updates": ["message", "edited_message", "channel_post", "edited_channel_post", "callback_query"],
    });
    if offset > 0 {
        params["offset"] = serde_json::json!(offset);
    }

    let resp = client
        .post(&url)
        .json(&params)
        .send()
        .await
        .context("Telegram getUpdates request failed")?;

    // Handle rate-limited responses (429 Too Many Requests)
    if resp.status() == reqwest::StatusCode::TOO_MANY_REQUESTS {
        let retry_after = resp
            .headers()
            .get("retry-after")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(1);
        warn!(
            "Telegram rate limited (429), waiting {}s before retry",
            retry_after
        );
        tokio::time::sleep(Duration::from_secs(retry_after)).await;

        // Retry once after waiting
        let resp2 = client
            .post(&url)
            .json(&params)
            .send()
            .await
            .context("Telegram getUpdates retry failed")?;

        if !resp2.status().is_success() {
            anyhow::bail!(
                "Telegram getUpdates returned {} after retry",
                resp2.status()
            );
        }

        let data: serde_json::Value = resp2
            .json()
            .await
            .context("Failed to parse Telegram getUpdates retry response")?;

        if data["ok"] != true {
            anyhow::bail!(
                "Telegram getUpdates not ok after retry: {}",
                data["description"].as_str().unwrap_or("unknown")
            );
        }

        let updates = match data["result"].as_array() {
            Some(arr) => arr,
            None => return Ok(offset),
        };

        let mut max_offset = offset;
        for update in updates {
            let update_id = update["update_id"].as_i64().unwrap_or(0);
            if update_id >= max_offset {
                max_offset = update_id + 1;
            }
        }
        return Ok(max_offset);
    }

    if !resp.status().is_success() {
        anyhow::bail!("Telegram getUpdates returned {}", resp.status());
    }

    let data: serde_json::Value = resp
        .json()
        .await
        .context("Failed to parse Telegram getUpdates response")?;

    if data["ok"] != true {
        anyhow::bail!(
            "Telegram getUpdates not ok: {}",
            data["description"].as_str().unwrap_or("unknown")
        );
    }

    let updates = match data["result"].as_array() {
        Some(arr) => arr,
        None => return Ok(offset),
    };

    let mut max_offset = offset;

    for update in updates {
        let update_id = update["update_id"].as_i64().unwrap_or(0);
        if update_id >= max_offset {
            max_offset = update_id + 1;
        }

        // Determine the message object (message, edited_message, channel_post, etc.)
        let (msg_value, is_edited, update_type) = if update.get("message").is_some() {
            (&update["message"], false, "message")
        } else if update.get("edited_message").is_some() {
            if !include_edited {
                continue;
            }
            (&update["edited_message"], true, "edited_message")
        } else if update.get("channel_post").is_some() {
            (&update["channel_post"], false, "channel_post")
        } else if update.get("edited_channel_post").is_some() {
            if !include_edited {
                continue;
            }
            (&update["edited_channel_post"], true, "edited_channel_post")
        } else if update.get("callback_query").is_some() {
            // Handle callback queries (inline keyboard button presses)
            let cb = &update["callback_query"];
            let chat_id = cb["message"]["chat"]["id"].as_i64().unwrap_or(0);
            if !allowed_chats.is_empty() && !allowed_chats.contains(&chat_id) {
                debug!("Telegram: skipping callback_query from chat {}", chat_id);
                continue;
            }
            let event = RawEvent {
                id: format!("tg_{}", update_id),
                source: "connector:telegram".to_string(),
                event_type: "telegram_callback_query".to_string(),
                timestamp_ms: chrono::Utc::now().timestamp_millis(),
                payload: serde_json::to_vec(cb).unwrap_or_default(),
                tags: {
                    let mut tags = std::collections::HashMap::new();
                    tags.insert("connector".to_string(), "telegram".to_string());
                    tags.insert("update_type".to_string(), "callback_query".to_string());
                    tags.insert("chat_id".to_string(), chat_id.to_string());
                    if let Some(data) = cb["data"].as_str() {
                        tags.insert("callback_data".to_string(), data.to_string());
                    }
                    tags
                },
            };
            if tx.send(event).await.is_err() {
                error!("Telegram collector channel closed");
                return Ok(max_offset);
            }
            debug!("Telegram callback_query from chat {}", chat_id);
            continue;
        } else {
            debug!("Telegram: unknown update type for update_id {}", update_id);
            continue;
        };

        // Extract chat info
        let chat_id = msg_value["chat"]["id"].as_i64().unwrap_or(0);
        let chat_type = msg_value["chat"]["type"].as_str().unwrap_or("unknown");
        let chat_title = msg_value["chat"]["title"].as_str().unwrap_or("");
        let from_user = msg_value["from"]["username"]
            .as_str()
            .or_else(|| msg_value["from"]["first_name"].as_str())
            .unwrap_or("unknown");

        // Filter by allowed chats
        if !allowed_chats.is_empty() && !allowed_chats.contains(&chat_id) {
            debug!("Telegram: skipping message from chat {}", chat_id);
            continue;
        }

        // Extract message content
        let text = msg_value["text"]
            .as_str()
            .or_else(|| msg_value["caption"].as_str())
            .unwrap_or("");
        let message_id = msg_value["message_id"].as_i64().unwrap_or(0);
        let date = msg_value["date"].as_i64().unwrap_or(0);

        // Determine media type
        let media_type = if msg_value.get("photo").is_some() {
            "photo"
        } else if msg_value.get("video").is_some() {
            "video"
        } else if msg_value.get("document").is_some() {
            "document"
        } else if msg_value.get("audio").is_some() {
            "audio"
        } else if msg_value.get("voice").is_some() {
            "voice"
        } else if msg_value.get("sticker").is_some() {
            "sticker"
        } else if msg_value.get("location").is_some() {
            "location"
        } else {
            "text"
        };

        // Build payload JSON
        let payload = serde_json::json!({
            "update_id": update_id,
            "update_type": update_type,
            "is_edited": is_edited,
            "chat_id": chat_id,
            "chat_type": chat_type,
            "chat_title": chat_title,
            "message_id": message_id,
            "from": from_user,
            "text": text,
            "media_type": media_type,
            "date": date,
            "raw": msg_value,
        });

        let event = RawEvent {
            id: format!("tg_{}", update_id),
            source: format!("connector:telegram:{}", chat_id),
            event_type: format!("telegram_{}", update_type),
            timestamp_ms: if date > 0 {
                date * 1000
            } else {
                chrono::Utc::now().timestamp_millis()
            },
            payload: serde_json::to_vec(&payload).unwrap_or_default(),
            tags: {
                let mut tags = std::collections::HashMap::new();
                tags.insert("connector".to_string(), "telegram".to_string());
                tags.insert("chat_id".to_string(), chat_id.to_string());
                tags.insert("chat_type".to_string(), chat_type.to_string());
                tags.insert("from".to_string(), from_user.to_string());
                tags.insert("media_type".to_string(), media_type.to_string());
                if is_edited {
                    tags.insert("edited".to_string(), "true".to_string());
                }
                if !chat_title.is_empty() {
                    tags.insert("chat_title".to_string(), chat_title.to_string());
                }
                tags
            },
        };

        if tx.send(event).await.is_err() {
            error!("Telegram collector channel closed");
            return Ok(max_offset);
        }

        debug!(
            "Telegram {} from {} in chat {} ({})",
            update_type, from_user, chat_id, chat_type
        );
    }

    Ok(max_offset)
}

/// Send a message via the Telegram Bot API (for use by other modules).
pub async fn send_message(
    client: &Client,
    bot_token: &str,
    chat_id: i64,
    text: &str,
    parse_mode: Option<&str>,
) -> Result<serde_json::Value> {
    let url = format!("https://api.telegram.org/bot{}/sendMessage", bot_token);
    let mut body = serde_json::json!({
        "chat_id": chat_id,
        "text": text,
    });
    if let Some(mode) = parse_mode {
        body["parse_mode"] = serde_json::json!(mode);
    }

    let resp = client
        .post(&url)
        .json(&body)
        .send()
        .await
        .context("Failed to send Telegram message")?;

    let data: serde_json::Value = resp.json().await?;
    if data["ok"] != true {
        anyhow::bail!(
            "Telegram sendMessage failed: {}",
            data["description"].as_str().unwrap_or("unknown")
        );
    }
    Ok(data)
}

/// Get information about the bot (getMe).
pub async fn get_me(client: &Client, bot_token: &str) -> Result<serde_json::Value> {
    let url = format!("https://api.telegram.org/bot{}/getMe", bot_token);
    let resp = client
        .get(&url)
        .send()
        .await
        .context("Telegram getMe failed")?;
    let data: serde_json::Value = resp.json().await?;
    if data["ok"] != true {
        anyhow::bail!(
            "Telegram getMe failed: {}",
            data["description"].as_str().unwrap_or("unknown")
        );
    }
    Ok(data)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_telegram_connector_name() {
        let config = TelegramConfig {
            enabled: true,
            bot_token: "test:token".to_string(),
            allowed_chats: None,
            include_edited: true,
            poll_interval_secs: 30,
        };
        let connector = TelegramConnector::new(config);
        assert_eq!(connector.name(), "telegram");
    }

    #[test]
    fn test_telegram_connector_clone_config() {
        let config = TelegramConfig {
            enabled: true,
            bot_token: "123:ABC".to_string(),
            allowed_chats: Some(vec![123456, 789012]),
            include_edited: false,
            poll_interval_secs: 60,
        };
        let cloned = config.clone();
        assert_eq!(cloned.bot_token, "123:ABC");
        assert_eq!(cloned.allowed_chats.as_ref().unwrap().len(), 2);
        assert!(!cloned.include_edited);
    }

    #[test]
    fn test_update_type_detection_message() {
        let update = serde_json::json!({
            "update_id": 1,
            "message": {
                "message_id": 100,
                "from": {"id": 1, "username": "test"},
                "chat": {"id": 10, "type": "private"},
                "text": "hello",
                "date": 1700000000
            }
        });

        assert!(update.get("message").is_some());
        assert!(update.get("edited_message").is_none());
        assert!(update.get("callback_query").is_none());
    }

    #[test]
    fn test_update_type_detection_callback() {
        let update = serde_json::json!({
            "update_id": 2,
            "callback_query": {
                "id": "cb1",
                "from": {"id": 1, "username": "test"},
                "data": "button_click",
                "message": {
                    "chat": {"id": 10, "type": "private"}
                }
            }
        });

        assert!(update.get("message").is_none());
        assert!(update.get("callback_query").is_some());
    }

    #[test]
    fn test_chat_filtering_allowed() {
        let allowed_chats: Vec<i64> = vec![100, 200, 300];
        let chat_id = 200;
        assert!(allowed_chats.is_empty() || allowed_chats.contains(&chat_id));
    }

    #[test]
    fn test_chat_filtering_denied() {
        let allowed_chats: Vec<i64> = vec![100, 200, 300];
        let chat_id = 999;
        assert!(!allowed_chats.contains(&chat_id));
    }

    #[test]
    fn test_chat_filtering_empty_allows_all() {
        let allowed_chats: Vec<i64> = vec![];
        let chat_id = 999;
        assert!(allowed_chats.is_empty() || allowed_chats.contains(&chat_id));
    }

    #[test]
    fn test_media_type_detection() {
        let msg = serde_json::json!({
            "photo": [{"file_id": "abc"}],
            "caption": "photo caption"
        });
        let media_type = if msg.get("photo").is_some() {
            "photo"
        } else if msg.get("video").is_some() {
            "video"
        } else {
            "text"
        };
        assert_eq!(media_type, "photo");
    }

    #[test]
    fn test_media_type_text_default() {
        let msg = serde_json::json!({
            "text": "hello world"
        });
        let media_type = if msg.get("photo").is_some() {
            "photo"
        } else if msg.get("video").is_some() {
            "video"
        } else if msg.get("document").is_some() {
            "document"
        } else {
            "text"
        };
        assert_eq!(media_type, "text");
    }

    #[test]
    fn test_event_id_format() {
        let update_id = 42;
        let event_id = format!("tg_{}", update_id);
        assert_eq!(event_id, "tg_42");
    }

    #[test]
    fn test_source_format() {
        let chat_id = 123456i64;
        let source = format!("connector:telegram:{}", chat_id);
        assert_eq!(source, "connector:telegram:123456");
    }

    #[test]
    fn test_timestamp_conversion() {
        // Telegram dates are Unix seconds, we convert to milliseconds
        let date: i64 = 1700000000;
        let timestamp_ms = date * 1000;
        assert_eq!(timestamp_ms, 1700000000000);
    }

    #[test]
    fn test_send_message_body_format() {
        let body = serde_json::json!({
            "chat_id": 123456,
            "text": "Hello!",
            "parse_mode": "HTML"
        });
        assert_eq!(body["chat_id"], 123456);
        assert_eq!(body["text"], "Hello!");
        assert_eq!(body["parse_mode"], "HTML");
    }

    #[test]
    fn test_send_message_body_without_parse_mode() {
        let body = serde_json::json!({
            "chat_id": 123456,
            "text": "Hello!"
        });
        assert!(body.get("parse_mode").is_none());
    }

    #[test]
    fn test_poll_params_format() {
        let offset = 42i64;
        let mut params = serde_json::json!({
            "timeout": 30,
            "allowed_updates": ["message", "edited_message", "channel_post"],
        });
        if offset > 0 {
            params["offset"] = serde_json::json!(offset);
        }
        assert_eq!(params["offset"], 42);
        assert_eq!(params["timeout"], 30);
    }

    #[test]
    fn test_poll_params_no_offset() {
        let offset = 0i64;
        let mut params = serde_json::json!({
            "timeout": 30,
            "allowed_updates": ["message"],
        });
        if offset > 0 {
            params["offset"] = serde_json::json!(offset);
        }
        assert!(params.get("offset").is_none());
    }

    #[test]
    fn test_extract_chat_info() {
        let msg = serde_json::json!({
            "chat": {"id": 100, "type": "group", "title": "Test Group"},
            "from": {"username": "alice", "first_name": "Alice"},
            "text": "hello",
            "message_id": 42,
            "date": 1700000000
        });

        assert_eq!(msg["chat"]["id"].as_i64().unwrap(), 100);
        assert_eq!(msg["chat"]["type"].as_str().unwrap(), "group");
        assert_eq!(msg["chat"]["title"].as_str().unwrap(), "Test Group");
        assert_eq!(
            msg["from"]["username"]
                .as_str()
                .or_else(|| msg["from"]["first_name"].as_str())
                .unwrap(),
            "alice"
        );
    }

    #[test]
    fn test_extract_from_fallback_to_first_name() {
        let msg = serde_json::json!({
            "from": {"id": 1, "first_name": "Alice"}
        });
        let from_user = msg["from"]["username"]
            .as_str()
            .or_else(|| msg["from"]["first_name"].as_str())
            .unwrap_or("unknown");
        assert_eq!(from_user, "Alice");
    }

    #[test]
    fn test_extract_from_unknown() {
        let msg = serde_json::json!({
            "from": {"id": 1}
        });
        let from_user = msg["from"]["username"]
            .as_str()
            .or_else(|| msg["from"]["first_name"].as_str())
            .unwrap_or("unknown");
        assert_eq!(from_user, "unknown");
    }
}
