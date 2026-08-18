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
        let seen_docs: std::collections::HashSet<String> = std::collections::HashSet::new();

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
