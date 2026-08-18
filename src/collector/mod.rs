pub mod file;
pub mod process;
pub mod network;
pub mod clipboard;

use anyhow::Result;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

/// A raw collected event before processing.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RawEvent {
    pub id: String,
    pub source: String,
    pub event_type: String,
    pub timestamp_ms: i64,
    pub payload: Vec<u8>,
    pub tags: std::collections::HashMap<String, String>,
}

/// Channel type for passing raw events between subsystems.
pub type EventTx = mpsc::Sender<RawEvent>;
pub type EventRx = mpsc::Receiver<RawEvent>;

/// Start all configured collectors. Events are sent to the provided `tx`.
pub async fn start_all(
    config: &crate::config::CollectorConfig,
    tx: EventTx,
) -> Result<JoinHandle<()>> {
    let watch_dirs = config.watch_dirs.clone();
    let debounce_ms = config.debounce_ms;
    let include = config.include.clone();
    let exclude = config.exclude.clone();

    // Start process monitor in a separate task
    let process_tx = tx.clone();
    tokio::spawn(async move {
        if let Err(e) = process::start_process_monitor(5000, process_tx).await {
            tracing::error!("Process collector failed: {}", e);
        }
    });

    // Start network monitor in a separate task
    let network_tx = tx.clone();
    tokio::spawn(async move {
        if let Err(e) = network::start_network_monitor(10000, network_tx).await {
            tracing::error!("Network collector failed: {}", e);
        }
    });

    // Start clipboard monitor in a separate task
    let clipboard_tx = tx.clone();
    tokio::spawn(async move {
        if let Err(e) = clipboard::start_clipboard_monitor(2000, clipboard_tx).await {
            tracing::error!("Clipboard collector failed: {}", e);
        }
    });

    // Start file watcher (consumes the remaining tx)
    let handle = tokio::spawn(async move {
        if let Err(e) = file::start_watcher(
            &watch_dirs,
            debounce_ms,
            &include,
            &exclude,
            tx,
        )
        .await
        {
            tracing::error!("File collector failed: {}", e);
        }
    });

    Ok(handle)
}
