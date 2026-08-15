use anyhow::Result;
use notify::{RecursiveMode, Watcher};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::Duration;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

use crate::collector::{EventTx, RawEvent};
use crate::config::ObsidianConfig;

/// Start the Obsidian connector. Watches the vault directory for file changes
/// and parses Markdown files with WikiLink resolution.
pub async fn start(config: ObsidianConfig, tx: EventTx) -> Result<JoinHandle<()>> {
    let vault_path = Path::new(&config.vault_path).to_path_buf();
    if !vault_path.is_dir() {
        anyhow::bail!(
            "Obsidian vault path does not exist: {}",
            config.vault_path
        );
    }

    let handle = tokio::spawn(async move {
        // Track file hashes for change detection
        let mut known_hashes: HashMap<String, String> = HashMap::new();

        // Initial full scan
        info!(
            "Obsidian connector started — watching vault at {}",
            config.vault_path
        );
        if let Err(e) = scan_vault(&vault_path, &tx, &mut known_hashes).await {
            warn!("Initial Obsidian vault scan failed: {}", e);
        }

        // Set up filesystem watcher
        let (notify_tx, notify_rx) = mpsc::channel();
        let mut watcher = match notify::RecommendedWatcher::new(
            notify_tx,
            notify::Config::default().with_poll_interval(Duration::from_secs(2)),
        ) {
            Ok(w) => w,
            Err(e) => {
                error!("Failed to create Obsidian filesystem watcher: {}", e);
                return;
            }
        };

        if let Err(e) = watcher.watch(&vault_path, RecursiveMode::Recursive) {
            error!("Failed to watch Obsidian vault: {}", e);
            return;
        }

        info!("Obsidian filesystem watcher active.");

        // Debounce timer: wait a bit after a change before scanning
        let mut debounce_deadline: Option<tokio::time::Instant> = None;
        let debounce_duration = Duration::from_millis(config.debounce_ms);
        let mut tick = tokio::time::interval(Duration::from_millis(200));

        loop {
            tokio::select! {
                // Drain filesystem events (non-blocking)
                _ = tick.tick() => {
                    // Drain all pending notify events
                    let mut got_event = false;
                    loop {
                        match notify_rx.try_recv() {
                            Ok(Ok(_events)) => {
                                got_event = true;
                            }
                            Ok(Err(e)) => {
                                warn!("Filesystem watch error: {}", e);
                            }
                            Err(mpsc::TryRecvError::Empty) => break,
                            Err(mpsc::TryRecvError::Disconnected) => {
                                error!("Filesystem watcher disconnected");
                                return;
                            }
                        }
                    }

                    if got_event {
                        debounce_deadline = Some(tokio::time::Instant::now() + debounce_duration);
                    }

                    // Check if debounce has elapsed
                    if let Some(deadline) = debounce_deadline {
                        if tokio::time::Instant::now() >= deadline {
                            debounce_deadline = None;
                            if let Err(e) = scan_vault(&vault_path, &tx, &mut known_hashes).await {
                                warn!("Obsidian vault scan failed: {}", e);
                            }
                        }
                    }
                }
            }
        }
    });

    Ok(handle)
}

/// Scan the entire vault for `.md` files and forward new/changed ones.
async fn scan_vault(
    vault_path: &Path,
    tx: &EventTx,
    known_hashes: &mut HashMap<String, String>,
) -> Result<()> {
    let md_files = find_markdown_files(vault_path)?;

    for file_path in md_files {
        let relative = file_path
            .strip_prefix(vault_path)
            .unwrap_or(&file_path)
            .to_string_lossy()
            .to_string();

        // Skip hidden files/dirs and Obsidian config
        if relative.starts_with('.') || relative.starts_with(".obsidian") {
            continue;
        }

        let content = match std::fs::read_to_string(&file_path) {
            Ok(c) => c,
            Err(e) => {
                warn!("Failed to read {}: {}", relative, e);
                continue;
            }
        };

        let hash = compute_hash(&content);

        // Skip unchanged files
        if known_hashes.get(&relative) == Some(&hash) {
            continue;
        }

        known_hashes.insert(relative.clone(), hash.clone());

        // Parse WikiLinks and build link graph metadata
        let wikilinks = extract_wikilinks(&content);

        let raw_event = to_raw_event(&relative, &content, &hash, &wikilinks);
        match tx.try_send(raw_event) {
            Ok(()) => {
                debug!("Forwarded Obsidian file: {}", relative);
            }
            Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                warn!("Event channel full, dropping Obsidian file: {}", relative);
            }
            Err(e) => {
                error!("Failed to send Obsidian event: {}", e);
            }
        }
    }

    Ok(())
}

/// Recursively find all `.md` files under a directory.
fn find_markdown_files(dir: &Path) -> Result<Vec<PathBuf>> {
    let mut results = Vec::new();
    let mut stack = vec![dir.to_path_buf()];

    while let Some(current) = stack.pop() {
        if !current.is_dir() {
            continue;
        }

        let entries = match std::fs::read_dir(&current) {
            Ok(e) => e,
            Err(_) => continue,
        };

        for entry in entries {
            let entry = entry?;
            let path = entry.path();

            // Skip .obsidian config directory
            if path
                .file_name()
                .map_or(false, |n| n.to_string_lossy() == ".obsidian")
            {
                continue;
            }

            if path.is_dir() {
                stack.push(path);
            } else if path.extension().map_or(false, |ext| ext == "md") {
                results.push(path);
            }
        }
    }

    Ok(results)
}

/// Extract WikiLinks from Markdown content.
/// WikiLinks look like: [[Note Title]] or [[Note Title|Display Text]]
fn extract_wikilinks(content: &str) -> Vec<String> {
    let mut links = Vec::new();

    for line in content.lines() {
        let mut remaining = line;
        while let Some(start) = remaining.find("[[") {
            let after_start = &remaining[start + 2..];
            if let Some(end) = after_start.find("]]") {
                let link_content = &after_start[..end];
                // Extract the note name (before any | alias)
                let note_name = link_content
                    .split('|')
                    .next()
                    .unwrap_or(link_content)
                    .trim();
                if !note_name.is_empty() {
                    links.push(note_name.to_string());
                }
                remaining = &after_start[end + 2..];
            } else {
                break;
            }
        }
    }

    links
}

/// Compute SHA-256 hash of content.
fn compute_hash(content: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(content.as_bytes());
    hex::encode(hasher.finalize())
}

/// Convert an Obsidian Markdown file into a RawEvent.
fn to_raw_event(
    relative_path: &str,
    content: &str,
    hash: &str,
    wikilinks: &[String],
) -> RawEvent {
    let mut tags = std::collections::HashMap::new();
    tags.insert("platform".to_string(), "obsidian".to_string());
    tags.insert("file_path".to_string(), relative_path.to_string());
    tags.insert("content_hash".to_string(), hash.to_string());
    tags.insert("format".to_string(), "markdown".to_string());

    if !wikilinks.is_empty() {
        tags.insert("wikilinks".to_string(), wikilinks.join(","));
    }

    // Extract title from first heading or filename
    let title = extract_markdown_title(content)
        .or_else(|| {
            Path::new(relative_path)
                .file_stem()
                .map(|s| s.to_string_lossy().to_string())
        });
    if let Some(t) = title {
        tags.insert("title".to_string(), t);
    }

    RawEvent {
        id: Uuid::new_v4().to_string(),
        source: format!("connector:obsidian:{}", relative_path),
        event_type: "document".to_string(),
        timestamp_ms: chrono::Utc::now().timestamp_millis(),
        payload: content.as_bytes().to_vec(),
        tags,
    }
}

/// Extract the first Markdown heading as a title.
fn extract_markdown_title(content: &str) -> Option<String> {
    for line in content.lines() {
        let trimmed = line.trim();
        if trimmed.starts_with("# ") {
            return Some(trimmed[2..].trim().to_string());
        }
    }
    None
}
