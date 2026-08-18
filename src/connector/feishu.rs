use anyhow::Result;
use chrono::Utc;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use tokio::task::JoinHandle;
use tokio::time::Duration;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

use crate::collector::{EventTx, RawEvent};
use crate::config::FeishuConfig;
use crate::connector::Connector;
use crate::retry_async;

/// Feishu connector implementing the unified Connector trait.
pub struct FeishuConnector {
    config: FeishuConfig,
}

impl FeishuConnector {
    pub fn new(config: FeishuConfig) -> Self {
        Self { config }
    }
}

#[async_trait::async_trait]
impl Connector for FeishuConnector {
    fn name(&self) -> &str {
        "feishu"
    }

    async fn ping(&self) -> Result<()> {
        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(10))
            .build()?;
        fetch_tenant_token(&client, &self.config).await?;
        Ok(())
    }
}

/// Feishu API tenant access token response.
#[derive(Debug, Deserialize)]
struct TokenResponse {
    tenant_access_token: String,
    #[allow(dead_code)]
    expire: u64,
}

/// Feishu document list response.
#[derive(Debug, Deserialize)]
struct DocListResponse {
    #[serde(default)]
    items: Vec<DocItem>,
    #[serde(default)]
    has_more: bool,
    #[serde(default)]
    page_token: Option<String>,
}

/// A single document entry from the Feishu list API.
#[derive(Debug, Deserialize, Serialize)]
pub struct DocItem {
    #[serde(default)]
    document_id: String,
    #[serde(default)]
    title: String,
    #[serde(default)]
    doc_type: String,
    #[serde(default)]
    revision_id: Option<i64>,
}

/// Feishu document raw content response.
#[derive(Debug, Deserialize)]
struct DocContentResponse {
    #[serde(default)]
    content: Option<String>,
}

/// Start the Feishu connector. Authenticates with Feishu API, then periodically
/// polls a configured folder for documents and forwards new/updated content
/// into the collector pipeline.
pub async fn start(config: FeishuConfig, tx: EventTx) -> Result<JoinHandle<()>> {
    let http_client = Client::builder().timeout(Duration::from_secs(30)).build()?;

    // Fetch initial access token with retry
    let token = retry_async!("feishu_token", 3, {
        fetch_tenant_token(&http_client, &config).await
    })?;
    info!("Feishu connector authenticated.");

    let handle = tokio::spawn(async move {
        let mut current_token = token;
        let mut token_refresh = tokio::time::interval(Duration::from_secs(7000));
        let mut poll_interval = tokio::time::interval(Duration::from_secs(60));
        poll_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        // Track seen document IDs to avoid duplicate events
        let mut seen_docs: std::collections::HashSet<String> = std::collections::HashSet::new();

        loop {
            tokio::select! {
                _ = token_refresh.tick() => {
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
                _ = poll_interval.tick() => {
                    if let Some(ref folder_token) = config.folder_token {
                        if folder_token.is_empty() {
                            continue;
                        }
                        match fetch_document_list(&http_client, &current_token, folder_token).await {
                            Ok(docs) => {
                                debug!("Fetched {} documents from Feishu folder", docs.len());
                                for doc in docs {
                                    // Dedup: skip already-seen documents
                                    if seen_docs.contains(&doc.document_id) {
                                        continue;
                                    }
                                    match fetch_document_content(&http_client, &current_token, &doc.document_id).await {
                                        Ok(content) => {
                                            let raw_event = to_raw_event(&doc, &content);
                                            match tx.try_send(raw_event) {
                                                Ok(()) => {
                                                    seen_docs.insert(doc.document_id.clone());
                                                    debug!("Forwarded Feishu doc: {}", doc.title);
                                                }
                                                Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                                                    warn!("Event channel full, dropping Feishu doc: {}", doc.title);
                                                }
                                                Err(e) => {
                                                    error!("Failed to send Feishu event: {}", e);
                                                }
                                            }
                                        }
                                        Err(e) => {
                                            warn!("Failed to fetch doc content {}: {}", doc.document_id, e);
                                        }
                                    }
                                }
                            }
                            Err(e) => {
                                warn!("Failed to fetch Feishu document list: {}", e);
                            }
                        }
                    }
                }
            }

            // Evict old seen docs to prevent unbounded growth
            if seen_docs.len() > 10000 {
                let excess = seen_docs.len() - 5000;
                let to_remove: Vec<String> = seen_docs.iter().take(excess).cloned().collect();
                for id in to_remove {
                    seen_docs.remove(&id);
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

/// Fetch the list of documents in a Feishu folder.
pub async fn fetch_document_list(
    client: &Client,
    token: &str,
    folder_token: &str,
) -> Result<Vec<DocItem>> {
    let mut all_docs = Vec::new();
    let mut page_token: Option<String> = None;

    loop {
        let mut url = format!(
            "https://open.feishu.cn/open-apis/drive/v1/files?folder_token={}&page_size=50",
            folder_token
        );
        if let Some(ref pt) = page_token {
            url = format!("{}&page_token={}", url, pt);
        }

        let resp = client
            .get(&url)
            .bearer_auth(token)
            .send()
            .await?
            .json::<DocListResponse>()
            .await?;

        all_docs.extend(resp.items);

        if resp.has_more {
            page_token = resp.page_token;
        } else {
            break;
        }
    }

    Ok(all_docs)
}

/// Fetch the raw content of a single Feishu document.
pub async fn fetch_document_content(
    client: &Client,
    token: &str,
    document_id: &str,
) -> Result<String> {
    let url = format!(
        "https://open.feishu.cn/open-apis/docx/v1/documents/{}/raw_content",
        document_id
    );

    let resp = client
        .get(&url)
        .bearer_auth(token)
        .send()
        .await?
        .json::<DocContentResponse>()
        .await?;

    Ok(resp.content.unwrap_or_default())
}

/// Convert a Feishu document into a RawEvent for the collector pipeline.
fn to_raw_event(doc: &DocItem, content: &str) -> RawEvent {
    let mut tags = std::collections::HashMap::new();
    tags.insert("platform".to_string(), "feishu".to_string());
    tags.insert("doc_id".to_string(), doc.document_id.clone());
    tags.insert("doc_type".to_string(), doc.doc_type.clone());
    tags.insert("title".to_string(), doc.title.clone());

    RawEvent {
        id: Uuid::new_v4().to_string(),
        source: format!("connector:feishu:{}", doc.document_id),
        event_type: "document".to_string(),
        timestamp_ms: Utc::now().timestamp_millis(),
        payload: content.as_bytes().to_vec(),
        tags,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_to_raw_event_structure() {
        let doc = DocItem {
            document_id: "doc-abc123".to_string(),
            title: "Test Document".to_string(),
            doc_type: "docx".to_string(),
            revision_id: Some(1),
        };
        let event = to_raw_event(&doc, "Document content here");
        assert_eq!(event.source, "connector:feishu:doc-abc123");
        assert_eq!(event.event_type, "document");
        assert_eq!(event.tags.get("platform").unwrap(), "feishu");
        assert_eq!(event.tags.get("doc_id").unwrap(), "doc-abc123");
        assert_eq!(event.tags.get("doc_type").unwrap(), "docx");
        assert_eq!(event.tags.get("title").unwrap(), "Test Document");
        assert_eq!(event.payload, b"Document content here");
    }

    #[test]
    fn test_to_raw_event_empty_content() {
        let doc = DocItem {
            document_id: "empty-doc".to_string(),
            title: "Empty".to_string(),
            doc_type: "sheet".to_string(),
            revision_id: None,
        };
        let event = to_raw_event(&doc, "");
        assert!(event.payload.is_empty());
        assert_eq!(event.tags.get("doc_type").unwrap(), "sheet");
    }

    #[test]
    fn test_to_raw_event_unicode_content() {
        let doc = DocItem {
            document_id: "doc-cn".to_string(),
            title: "中文文档".to_string(),
            doc_type: "docx".to_string(),
            revision_id: Some(42),
        };
        let content = "这是一份中文内容的文档，包含特殊字符：émojis 🎉";
        let event = to_raw_event(&doc, content);
        assert_eq!(event.payload, content.as_bytes());
        assert_eq!(event.tags.get("title").unwrap(), "中文文档");
        // UUID should be valid format
        assert!(uuid::Uuid::parse_str(&event.id).is_ok());
    }

    #[test]
    fn test_to_raw_event_large_payload() {
        let doc = DocItem {
            document_id: "large-doc".to_string(),
            title: "Large Document".to_string(),
            doc_type: "docx".to_string(),
            revision_id: Some(10),
        };
        let content = "x".repeat(100_000);
        let event = to_raw_event(&doc, &content);
        assert_eq!(event.payload.len(), 100_000);
    }

    #[test]
    fn test_to_raw_event_all_doc_types() {
        for doc_type in &["docx", "sheet", "bitable", "mindnote", "file", "wiki"] {
            let doc = DocItem {
                document_id: format!("doc-{}", doc_type),
                title: format!("{} doc", doc_type),
                doc_type: doc_type.to_string(),
                revision_id: None,
            };
            let event = to_raw_event(&doc, "content");
            assert_eq!(event.tags.get("doc_type").unwrap(), doc_type);
        }
    }

    #[test]
    fn test_doc_item_deserialization() {
        let json = r#"{
            "document_id": "doc-123",
            "title": "My Doc",
            "doc_type": "docx",
            "revision_id": 5
        }"#;
        let doc: DocItem = serde_json::from_str(json).unwrap();
        assert_eq!(doc.document_id, "doc-123");
        assert_eq!(doc.title, "My Doc");
        assert_eq!(doc.doc_type, "docx");
        assert_eq!(doc.revision_id, Some(5));
    }

    #[test]
    fn test_doc_item_deserialization_missing_optional() {
        let json = r#"{
            "document_id": "doc-456",
            "title": "Minimal Doc",
            "doc_type": "sheet"
        }"#;
        let doc: DocItem = serde_json::from_str(json).unwrap();
        assert_eq!(doc.document_id, "doc-456");
        assert!(doc.revision_id.is_none());
    }

    #[test]
    fn test_doc_item_deserialization_empty_defaults() {
        let json = r#"{}"#;
        let doc: DocItem = serde_json::from_str(json).unwrap();
        assert_eq!(doc.document_id, "");
        assert_eq!(doc.title, "");
        assert_eq!(doc.doc_type, "");
        assert!(doc.revision_id.is_none());
    }

    #[test]
    fn test_doc_list_response_deserialization() {
        let json = r#"{
            "items": [
                {"document_id": "d1", "title": "Doc 1", "doc_type": "docx"},
                {"document_id": "d2", "title": "Doc 2", "doc_type": "sheet"}
            ],
            "has_more": true,
            "page_token": "next-page-token"
        }"#;
        let resp: DocListResponse = serde_json::from_str(json).unwrap();
        assert_eq!(resp.items.len(), 2);
        assert_eq!(resp.items[0].document_id, "d1");
        assert!(resp.has_more);
        assert_eq!(resp.page_token.unwrap(), "next-page-token");
    }

    #[test]
    fn test_doc_list_response_empty() {
        let json = r#"{"items": [], "has_more": false}"#;
        let resp: DocListResponse = serde_json::from_str(json).unwrap();
        assert!(resp.items.is_empty());
        assert!(!resp.has_more);
        assert!(resp.page_token.is_none());
    }

    #[test]
    fn test_doc_list_response_missing_fields() {
        let json = r#"{}"#;
        let resp: DocListResponse = serde_json::from_str(json).unwrap();
        assert!(resp.items.is_empty());
        assert!(!resp.has_more);
        assert!(resp.page_token.is_none());
    }

    #[test]
    fn test_token_response_deserialization() {
        let json = r#"{
            "tenant_access_token": "t-abc123xyz",
            "expire": 7200
        }"#;
        let resp: TokenResponse = serde_json::from_str(json).unwrap();
        assert_eq!(resp.tenant_access_token, "t-abc123xyz");
        assert_eq!(resp.expire, 7200);
    }

    #[test]
    fn test_doc_content_response_with_content() {
        let json = r#"{"content": "Hello, world!"}"#;
        let resp: DocContentResponse = serde_json::from_str(json).unwrap();
        assert_eq!(resp.content.unwrap(), "Hello, world!");
    }

    #[test]
    fn test_doc_content_response_no_content() {
        let json = r#"{}"#;
        let resp: DocContentResponse = serde_json::from_str(json).unwrap();
        assert!(resp.content.is_none());
    }

    #[test]
    fn test_doc_content_response_null_content() {
        let json = r#"{"content": null}"#;
        let resp: DocContentResponse = serde_json::from_str(json).unwrap();
        assert!(resp.content.is_none());
    }

    #[test]
    fn test_to_raw_event_timestamp_is_recent() {
        let doc = DocItem {
            document_id: "ts-doc".to_string(),
            title: "Timestamp Test".to_string(),
            doc_type: "docx".to_string(),
            revision_id: None,
        };
        let before = Utc::now().timestamp_millis();
        let event = to_raw_event(&doc, "test");
        let after = Utc::now().timestamp_millis();
        assert!(event.timestamp_ms >= before);
        assert!(event.timestamp_ms <= after);
    }

    #[test]
    fn test_to_raw_event_unique_ids() {
        let doc = DocItem {
            document_id: "same-doc".to_string(),
            title: "Same Doc".to_string(),
            doc_type: "docx".to_string(),
            revision_id: None,
        };
        let e1 = to_raw_event(&doc, "content");
        let e2 = to_raw_event(&doc, "content");
        // Same doc should produce different event IDs
        assert_ne!(e1.id, e2.id);
        // But same source
        assert_eq!(e1.source, e2.source);
    }

    #[test]
    fn test_feishu_connector_name() {
        let config = FeishuConfig {
            enabled: true,
            app_id: "test".to_string(),
            app_secret: "test".to_string(),
            webhook_path: "/webhook/feishu".to_string(),
            folder_token: None,
        };
        let connector = FeishuConnector::new(config);
        assert_eq!(connector.name(), "feishu");
    }
}
