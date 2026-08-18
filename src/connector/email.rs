use anyhow::Result;
use async_trait::async_trait;
use tokio::task::JoinHandle;
use tokio::time::Duration;
use tracing::{debug, error, info, warn};

use crate::collector::{EventTx, RawEvent};
use crate::config::EmailConfig;
use crate::connector::Connector;

/// Start the Email connector. Polls IMAP accounts at regular intervals
/// and forwards new emails into the collector pipeline.
pub async fn start(config: EmailConfig, tx: EventTx) -> Result<JoinHandle<()>> {
    let poll_secs = config.poll_interval_secs;
    let accounts = config.accounts.clone();

    info!(
        "Email connector starting — {} accounts, poll every {}s",
        accounts.len(),
        poll_secs
    );

    let handle = tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(poll_secs));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        // Track seen message UIDs per account to avoid duplicates
        let mut seen: std::collections::HashMap<String, std::collections::HashSet<u32>> =
            std::collections::HashMap::new();

        // Initial poll
        for account in &accounts {
            let key = format!("{}:{}", account.name, account.username);
            let seen_set = seen.entry(key).or_default();
            if let Err(e) = poll_account(account, &tx, seen_set).await {
                error!("Email initial poll failed for '{}': {}", account.name, e);
            }
        }

        loop {
            interval.tick().await;
            for account in &accounts {
                let key = format!("{}:{}", account.name, account.username);
                let seen_set = seen.entry(key).or_default();
                if let Err(e) = poll_account(account, &tx, seen_set).await {
                    warn!("Email poll failed for '{}': {}", account.name, e);
                }
            }
        }
    });

    Ok(handle)
}

/// Poll a single IMAP account for new messages.
async fn poll_account(
    account: &crate::config::EmailAccountConfig,
    tx: &EventTx,
    seen: &mut std::collections::HashSet<u32>,
) -> Result<()> {
    debug!(
        "Polling email account '{}' ({}@{}:{})",
        account.name, account.username, account.imap_server, account.imap_port
    );

    // Use synchronous IMAP in a blocking task (the imap crate is sync)
    let account_clone = account.clone();
    let result = tokio::task::spawn_blocking(move || poll_account_sync(&account_clone)).await??;

    let new_messages: Vec<_> = result
        .into_iter()
        .filter(|(uid, _)| !seen.contains(uid))
        .collect();

    if new_messages.is_empty() {
        debug!("No new emails for '{}'", account.name);
        return Ok(());
    }

    info!("{}: {} new emails", account.name, new_messages.len());

    for (uid, (headers, body)) in new_messages {
        seen.insert(uid);

        let payload_json = serde_json::json!({
            "account": account.name,
            "folder": account.folder,
            "uid": uid,
            "headers": headers,
            "body_snippet": body.chars().take(2000).collect::<String>(),
        });

        let event = RawEvent {
            id: uuid::Uuid::new_v4().to_string(),
            source: format!("connector:email:{}", account.name),
            event_type: "email_message".to_string(),
            timestamp_ms: chrono::Utc::now().timestamp_millis(),
            payload: payload_json.to_string().into_bytes(),
            tags: {
                let mut tags = std::collections::HashMap::new();
                tags.insert("connector".to_string(), "email".to_string());
                tags.insert("account".to_string(), account.name.clone());
                tags.insert("folder".to_string(), account.folder.clone());
                tags
            },
        };

        if tx.send(event).await.is_err() {
            error!("Email collector channel closed");
            break;
        }
    }

    Ok(())
}

/// Synchronous IMAP polling (runs in a blocking thread).
fn poll_account_sync(
    account: &crate::config::EmailAccountConfig,
) -> Result<Vec<(u32, (String, String))>> {
    use imap::Session;
    use native_tls::TlsConnector;

    let tls = TlsConnector::builder().build()?;

    let client = imap::connect(
        (account.imap_server.as_str(), account.imap_port),
        &account.imap_server,
        &tls,
    )?;

    let mut session: Session<_> = client
        .login(&account.username, &account.password)
        .map_err(|e| anyhow::anyhow!("IMAP login failed: {}", e.0))?;

    session.select(&account.folder)?;

    // Search for unseen messages
    let uids = session.search("UNSEEN")?;
    let uid_vec: Vec<u32> = uids.iter().copied().collect();

    if uid_vec.is_empty() {
        session.logout().ok();
        return Ok(vec![]);
    }

    let mut messages = Vec::new();

    // Fetch headers and body for new messages (limit to 20)
    for uid in uid_vec.iter().take(20) {
        let _fetch_items = format!(
            "{} (BODY[HEADER.FIELDS (FROM SUBJECT DATE)]) BODY[TEXT]",
            uid
        );
        let fetch_result = session.fetch(
            uid.to_string(),
            "BODY[HEADER.FIELDS (FROM SUBJECT DATE)] BODY[TEXT]",
        )?;

        let mut headers = String::new();
        let mut body = String::new();

        for fetch in fetch_result.iter() {
            if let Some(h) = fetch.header() {
                headers = String::from_utf8_lossy(h).to_string();
            }
            if let Some(b) = fetch.text() {
                body = String::from_utf8_lossy(b).to_string();
            }
            // Also try body()
            if body.is_empty() {
                if let Some(b) = fetch.body() {
                    body = String::from_utf8_lossy(b)
                        .to_string()
                        .chars()
                        .take(5000)
                        .collect();
                }
            }
        }

        messages.push((*uid, (headers, body)));
    }

    session.logout().ok();
    Ok(messages)
}

/// Email connector implementing the unified Connector trait.
pub struct EmailConnector {
    config: EmailConfig,
}

impl EmailConnector {
    pub fn new(config: EmailConfig) -> Self {
        Self { config }
    }
}

#[async_trait]
impl Connector for EmailConnector {
    fn name(&self) -> &str {
        "email"
    }

    async fn ping(&self) -> Result<()> {
        if let Some(account) = self.config.accounts.first() {
            let account = account.clone();
            tokio::task::spawn_blocking(move || {
                let tls = native_tls::TlsConnector::builder().build()?;
                let client = imap::connect(
                    (account.imap_server.as_str(), account.imap_port),
                    &account.imap_server,
                    &tls,
                )?;
                let mut session = client
                    .login(&account.username, &account.password)
                    .map_err(|e| anyhow::anyhow!("IMAP login failed: {}", e.0))?;
                session.logout().ok();
                Ok::<(), anyhow::Error>(())
            })
            .await??
        } else {
            anyhow::bail!("No email accounts configured");
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    // IMAP tests require a real server, so we just verify the module compiles
    #[test]
    fn test_module_compiles() {
        assert!(true);
    }
}
