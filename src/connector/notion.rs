use anyhow::{Context, Result};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use tokio::task::JoinHandle;
use tokio::time::Duration;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

use crate::collector::{EventTx, RawEvent};
use crate::config::NotionConfig;
use crate::connector::circuit_breaker::CircuitBreaker;
use crate::connector::Connector;

/// Notion connector implementing the unified Connector trait.
pub struct NotionConnector {
    config: NotionConfig,
}

impl NotionConnector {
    pub fn new(config: NotionConfig) -> Self {
        Self { config }
    }
}

#[async_trait::async_trait]
impl Connector for NotionConnector {
    fn name(&self) -> &str {
        "notion"
    }

    async fn ping(&self) -> Result<()> {
        let client = Client::builder().timeout(Duration::from_secs(10)).build()?;
        // Try to query the database to verify credentials
        let url = format!("{}/databases/{}", NOTION_BASE_URL, self.config.database_id);
        let resp = client
            .get(&url)
            .header(
                "Authorization",
                format!("Bearer {}", self.config.integration_token),
            )
            .header("Notion-Version", NOTION_API_VERSION)
            .send()
            .await
            .context("Notion API unreachable")?;
        if !resp.status().is_success() {
            anyhow::bail!("Notion API returned {}", resp.status());
        }
        Ok(())
    }
}

const NOTION_API_VERSION: &str = "2022-06-28";
const NOTION_BASE_URL: &str = "https://api.notion.com/v1";

/// Notion page list response from database query.
#[derive(Debug, Deserialize)]
struct DatabaseQueryResponse {
    results: Vec<NotionPage>,
    #[serde(default)]
    has_more: bool,
    #[serde(default)]
    next_cursor: Option<String>,
}

/// A single Notion page object.
#[derive(Debug, Deserialize, Serialize)]
struct NotionPage {
    id: String,
    #[serde(default)]
    properties: serde_json::Value,
}

/// Notion block children response.
#[derive(Debug, Deserialize)]
struct BlockChildrenResponse {
    results: Vec<NotionBlock>,
    #[serde(default)]
    has_more: bool,
    #[serde(default)]
    next_cursor: Option<String>,
}

/// A single Notion block.
#[derive(Debug, Deserialize, Serialize)]
struct NotionBlock {
    id: String,
    #[serde(rename = "type")]
    block_type: String,
    #[serde(default)]
    rich_text: Option<Vec<RichText>>,
    #[serde(default)]
    paragraph: Option<BlockWithText>,
    #[serde(default)]
    heading_1: Option<BlockWithText>,
    #[serde(default)]
    heading_2: Option<BlockWithText>,
    #[serde(default)]
    heading_3: Option<BlockWithText>,
    #[serde(default)]
    bulleted_list_item: Option<BlockWithText>,
    #[serde(default)]
    numbered_list_item: Option<BlockWithText>,
    #[serde(default)]
    to_do: Option<BlockWithText>,
    #[serde(default)]
    code: Option<BlockWithText>,
    #[serde(default)]
    quote: Option<BlockWithText>,
    #[serde(default)]
    callout: Option<BlockWithText>,
    #[serde(default)]
    toggle: Option<BlockWithText>,
}

#[derive(Debug, Deserialize, Serialize)]
struct BlockWithText {
    #[serde(default)]
    rich_text: Vec<RichText>,
}

#[derive(Debug, Deserialize, Serialize)]
struct RichText {
    #[serde(default)]
    plain_text: String,
}

/// Start the Notion connector. Polls a configured database for pages,
/// fetches page content, and forwards events into the collector pipeline.
pub async fn start(config: NotionConfig, tx: EventTx, circuit_breaker: Option<CircuitBreaker>) -> Result<JoinHandle<()>> {
    let http_client = Client::builder().timeout(Duration::from_secs(30)).build()?;

    let handle = tokio::spawn(async move {
        let _cb = circuit_breaker; // Circuit breaker integration point
        let mut poll_interval =
            tokio::time::interval(Duration::from_secs(config.poll_interval_secs));
        poll_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        info!(
            "Notion connector started — polling database {} every {}s",
            config.database_id, config.poll_interval_secs
        );

        loop {
            poll_interval.tick().await;

            match query_database(&http_client, &config).await {
                Ok(pages) => {
                    debug!("Fetched {} pages from Notion database", pages.len());
                    for page in pages {
                        let page_id = &page.id;
                        match fetch_page_blocks(&http_client, &config, page_id).await {
                            Ok(content) => {
                                let raw_event = to_raw_event(page_id, &page.properties, &content);
                                match tx.try_send(raw_event) {
                                    Ok(()) => {
                                        debug!("Forwarded Notion page: {}", page_id);
                                    }
                                    Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                                        warn!(
                                            "Event channel full, dropping Notion page: {}",
                                            page_id
                                        );
                                    }
                                    Err(e) => {
                                        error!("Failed to send Notion event: {}", e);
                                    }
                                }
                            }
                            Err(e) => {
                                warn!("Failed to fetch Notion page blocks {}: {}", page_id, e);
                            }
                        }
                    }
                }
                Err(e) => {
                    warn!("Failed to query Notion database: {}", e);
                }
            }
        }
    });

    Ok(handle)
}

/// Query all pages from a Notion database.
async fn query_database(client: &Client, config: &NotionConfig) -> Result<Vec<NotionPage>> {
    let mut all_pages = Vec::new();
    let mut start_cursor: Option<String> = None;

    loop {
        let url = format!("{}/databases/{}/query", NOTION_BASE_URL, config.database_id);

        let mut body = serde_json::json!({ "page_size": 100 });
        if let Some(ref cursor) = start_cursor {
            body["start_cursor"] = serde_json::json!(cursor);
        }

        let resp = client
            .post(&url)
            .header(
                "Authorization",
                format!("Bearer {}", config.integration_token),
            )
            .header("Notion-Version", NOTION_API_VERSION)
            .json(&body)
            .send()
            .await?;

        let status = resp.status();
        if status == reqwest::StatusCode::TOO_MANY_REQUESTS {
            let retry_after = resp
                .headers()
                .get("retry-after")
                .and_then(|v| v.to_str().ok())
                .and_then(|v| v.parse::<u64>().ok())
                .unwrap_or(1);
            warn!("Notion rate limited on database query, waiting {}s", retry_after);
            tokio::time::sleep(Duration::from_secs(retry_after)).await;
            continue; // Retry the same page
        }
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            anyhow::bail!("Notion database query error {}: {}", status, body);
        }

        let resp = resp.json::<DatabaseQueryResponse>().await?;

        all_pages.extend(resp.results);

        if resp.has_more {
            start_cursor = resp.next_cursor;
        } else {
            break;
        }
    }

    Ok(all_pages)
}

/// Fetch all block children for a page and extract text content.
async fn fetch_page_blocks(
    client: &Client,
    config: &NotionConfig,
    page_id: &str,
) -> Result<String> {
    let mut all_text = Vec::new();
    let mut start_cursor: Option<String> = None;

    loop {
        let mut url = format!(
            "{}/blocks/{}/children?page_size=100",
            NOTION_BASE_URL, page_id
        );
        if let Some(ref cursor) = start_cursor {
            url = format!("{}&start_cursor={}", url, cursor);
        }

        let resp = client
            .get(&url)
            .header(
                "Authorization",
                format!("Bearer {}", config.integration_token),
            )
            .header("Notion-Version", NOTION_API_VERSION)
            .send()
            .await?;

        let status = resp.status();
        if status == reqwest::StatusCode::TOO_MANY_REQUESTS {
            let retry_after = resp
                .headers()
                .get("retry-after")
                .and_then(|v| v.to_str().ok())
                .and_then(|v| v.parse::<u64>().ok())
                .unwrap_or(1);
            warn!("Notion rate limited on page blocks, waiting {}s", retry_after);
            tokio::time::sleep(Duration::from_secs(retry_after)).await;
            continue; // Retry the same page
        }
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            anyhow::bail!("Notion blocks API error {}: {}", status, body);
        }

        let resp = resp.json::<BlockChildrenResponse>().await?;

        for block in &resp.results {
            if let Some(text) = extract_block_text(block) {
                all_text.push(text);
            }
        }

        if resp.has_more {
            start_cursor = resp.next_cursor;
        } else {
            break;
        }
    }

    Ok(all_text.join("\n"))
}

/// Extract plain text from a Notion block.
fn extract_block_text(block: &NotionBlock) -> Option<String> {
    let rich_texts = match block.block_type.as_str() {
        "paragraph" => block.paragraph.as_ref().map(|b| &b.rich_text),
        "heading_1" => block.heading_1.as_ref().map(|b| &b.rich_text),
        "heading_2" => block.heading_2.as_ref().map(|b| &b.rich_text),
        "heading_3" => block.heading_3.as_ref().map(|b| &b.rich_text),
        "bulleted_list_item" => block.bulleted_list_item.as_ref().map(|b| &b.rich_text),
        "numbered_list_item" => block.numbered_list_item.as_ref().map(|b| &b.rich_text),
        "to_do" => block.to_do.as_ref().map(|b| &b.rich_text),
        "code" => block.code.as_ref().map(|b| &b.rich_text),
        "quote" => block.quote.as_ref().map(|b| &b.rich_text),
        "callout" => block.callout.as_ref().map(|b| &b.rich_text),
        "toggle" => block.toggle.as_ref().map(|b| &b.rich_text),
        _ => None,
    };

    rich_texts.map(|rts| {
        rts.iter()
            .map(|rt| rt.plain_text.as_str())
            .collect::<Vec<_>>()
            .join("")
    })
}

/// Convert Notion page data into a RawEvent.
fn to_raw_event(page_id: &str, properties: &serde_json::Value, content: &str) -> RawEvent {
    let mut tags = std::collections::HashMap::new();
    tags.insert("platform".to_string(), "notion".to_string());
    tags.insert("page_id".to_string(), page_id.to_string());

    // Extract title from properties if possible
    if let Some(title) = extract_title_from_properties(properties) {
        tags.insert("title".to_string(), title);
    }

    RawEvent {
        id: Uuid::new_v4().to_string(),
        source: format!("connector:notion:{}", page_id),
        event_type: "document".to_string(),
        timestamp_ms: chrono::Utc::now().timestamp_millis(),
        payload: content.as_bytes().to_vec(),
        tags,
    }
}

/// Try to extract a title string from Notion page properties.
fn extract_title_from_properties(properties: &serde_json::Value) -> Option<String> {
    // Notion titles are typically in a "title" or "Name" property
    for key in &["title", "Name"] {
        if let Some(prop) = properties.get(key) {
            if let Some(title_arr) = prop.get("title") {
                if let Some(arr) = title_arr.as_array() {
                    let text: String = arr
                        .iter()
                        .filter_map(|t| t.get("plain_text").and_then(|v| v.as_str()))
                        .collect();
                    if !text.is_empty() {
                        return Some(text);
                    }
                }
            }
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_block_text_paragraph() {
        let block = NotionBlock {
            id: "test".to_string(),
            block_type: "paragraph".to_string(),
            rich_text: None,
            paragraph: Some(BlockWithText {
                rich_text: vec![RichText {
                    plain_text: "Hello world".to_string(),
                }],
            }),
            heading_1: None,
            heading_2: None,
            heading_3: None,
            bulleted_list_item: None,
            numbered_list_item: None,
            to_do: None,
            code: None,
            quote: None,
            callout: None,
            toggle: None,
        };
        assert_eq!(extract_block_text(&block), Some("Hello world".to_string()));
    }

    #[test]
    fn test_extract_block_text_heading() {
        let block = NotionBlock {
            id: "test".to_string(),
            block_type: "heading_1".to_string(),
            rich_text: None,
            paragraph: None,
            heading_1: Some(BlockWithText {
                rich_text: vec![RichText {
                    plain_text: "Title".to_string(),
                }],
            }),
            heading_2: None,
            heading_3: None,
            bulleted_list_item: None,
            numbered_list_item: None,
            to_do: None,
            code: None,
            quote: None,
            callout: None,
            toggle: None,
        };
        assert_eq!(extract_block_text(&block), Some("Title".to_string()));
    }

    #[test]
    fn test_extract_block_text_unsupported_type() {
        let block = NotionBlock {
            id: "test".to_string(),
            block_type: "unsupported_type".to_string(),
            rich_text: None,
            paragraph: None,
            heading_1: None,
            heading_2: None,
            heading_3: None,
            bulleted_list_item: None,
            numbered_list_item: None,
            to_do: None,
            code: None,
            quote: None,
            callout: None,
            toggle: None,
        };
        assert_eq!(extract_block_text(&block), None);
    }

    #[test]
    fn test_extract_title_from_properties_title_key() {
        let props = serde_json::json!({
            "title": {
                "title": [
                    { "plain_text": "My Page Title" }
                ]
            }
        });
        assert_eq!(
            extract_title_from_properties(&props),
            Some("My Page Title".to_string())
        );
    }

    #[test]
    fn test_extract_title_from_properties_name_key() {
        let props = serde_json::json!({
            "Name": {
                "title": [
                    { "plain_text": "Named Page" }
                ]
            }
        });
        assert_eq!(
            extract_title_from_properties(&props),
            Some("Named Page".to_string())
        );
    }

    #[test]
    fn test_extract_title_from_properties_empty() {
        let props = serde_json::json!({});
        assert_eq!(extract_title_from_properties(&props), None);
    }

    #[test]
    fn test_to_raw_event_structure() {
        let props = serde_json::json!({
            "title": {
                "title": [{ "plain_text": "Test Page" }]
            }
        });
        let event = to_raw_event("page-123", &props, "Content here");
        assert_eq!(event.source, "connector:notion:page-123");
        assert_eq!(event.event_type, "document");
        assert_eq!(event.tags.get("platform").unwrap(), "notion");
        assert_eq!(event.tags.get("page_id").unwrap(), "page-123");
        assert_eq!(event.tags.get("title").unwrap(), "Test Page");
    }

    #[test]
    fn test_extract_block_text_empty_rich_text() {
        let block = NotionBlock {
            id: "test".to_string(),
            block_type: "paragraph".to_string(),
            rich_text: None,
            paragraph: Some(BlockWithText { rich_text: vec![] }),
            heading_1: None,
            heading_2: None,
            heading_3: None,
            bulleted_list_item: None,
            numbered_list_item: None,
            to_do: None,
            code: None,
            quote: None,
            callout: None,
            toggle: None,
        };
        assert_eq!(extract_block_text(&block), Some("".to_string()));
    }

    #[test]
    fn test_extract_block_text_multiple_rich_texts() {
        let block = NotionBlock {
            id: "test".to_string(),
            block_type: "paragraph".to_string(),
            rich_text: None,
            paragraph: Some(BlockWithText {
                rich_text: vec![
                    RichText {
                        plain_text: "Hello ".to_string(),
                    },
                    RichText {
                        plain_text: "world".to_string(),
                    },
                ],
            }),
            heading_1: None,
            heading_2: None,
            heading_3: None,
            bulleted_list_item: None,
            numbered_list_item: None,
            to_do: None,
            code: None,
            quote: None,
            callout: None,
            toggle: None,
        };
        assert_eq!(extract_block_text(&block), Some("Hello world".to_string()));
    }

    #[test]
    fn test_extract_title_from_properties_empty_title_array() {
        let props = serde_json::json!({
            "title": {
                "title": []
            }
        });
        assert_eq!(extract_title_from_properties(&props), None);
    }

    #[test]
    fn test_to_raw_event_no_title() {
        let props = serde_json::json!({});
        let event = to_raw_event("page-no-title", &props, "Body text");
        assert_eq!(event.source, "connector:notion:page-no-title");
        assert!(!event.tags.contains_key("title"));
        assert_eq!(event.payload, b"Body text");
    }

    #[test]
    fn test_to_raw_event_payload_encoding() {
        let props = serde_json::json!({});
        let content = "中文内容 🎉";
        let event = to_raw_event("page-unicode", &props, content);
        let decoded = String::from_utf8(event.payload).unwrap();
        assert_eq!(decoded, "中文内容 🎉");
    }

    #[test]
    fn test_notion_api_version_constant() {
        assert_eq!(NOTION_API_VERSION, "2022-06-28");
    }

    #[test]
    fn test_notion_base_url_constant() {
        assert_eq!(NOTION_BASE_URL, "https://api.notion.com/v1");
    }
}
