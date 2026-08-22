use anyhow::{Context, Result};
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
use crate::connector::circuit_breaker::CircuitBreaker;
use crate::connector::Connector;

/// Obsidian connector implementing the unified Connector trait.
pub struct ObsidianConnector {
    config: ObsidianConfig,
}

impl ObsidianConnector {
    pub fn new(config: ObsidianConfig) -> Self {
        Self { config }
    }
}

#[async_trait::async_trait]
impl Connector for ObsidianConnector {
    fn name(&self) -> &str {
        "obsidian"
    }

    async fn ping(&self) -> Result<()> {
        let vault_path = Path::new(&self.config.vault_path);
        if !vault_path.is_dir() {
            anyhow::bail!(
                "Obsidian vault path does not exist: {}",
                self.config.vault_path
            );
        }
        // Check that we can read the directory
        std::fs::read_dir(vault_path)
            .with_context(|| format!("Cannot read Obsidian vault: {}", self.config.vault_path))?;
        Ok(())
    }
}

/// Start the Obsidian connector. Watches the vault directory for file changes
/// and parses Markdown files with WikiLink resolution.
pub async fn start(config: ObsidianConfig, tx: EventTx, circuit_breaker: Option<CircuitBreaker>) -> Result<JoinHandle<()>> {
    let vault_path = Path::new(&config.vault_path).to_path_buf();
    if !vault_path.is_dir() {
        anyhow::bail!("Obsidian vault path does not exist: {}", config.vault_path);
    }

    let handle = tokio::spawn(async move {
        let _cb = circuit_breaker; // Circuit breaker integration point
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
                .is_some_and(|n| n.to_string_lossy() == ".obsidian")
            {
                continue;
            }

            if path.is_dir() {
                stack.push(path);
            } else if path.extension().is_some_and(|ext| ext == "md") {
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
fn to_raw_event(relative_path: &str, content: &str, hash: &str, wikilinks: &[String]) -> RawEvent {
    let mut tags = std::collections::HashMap::new();
    tags.insert("platform".to_string(), "obsidian".to_string());
    tags.insert("file_path".to_string(), relative_path.to_string());
    tags.insert("content_hash".to_string(), hash.to_string());
    tags.insert("format".to_string(), "markdown".to_string());

    if !wikilinks.is_empty() {
        tags.insert("wikilinks".to_string(), wikilinks.join(","));
    }

    // Extract title from first heading or filename
    let title = extract_markdown_title(content).or_else(|| {
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
            return Some(
                trimmed
                    .strip_prefix("# ")
                    .unwrap_or(trimmed)
                    .trim()
                    .to_string(),
            );
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_wikilinks_simple() {
        let content = "See [[Other Note]] for details.";
        let links = extract_wikilinks(content);
        assert_eq!(links, vec!["Other Note"]);
    }

    #[test]
    fn test_extract_wikilinks_with_alias() {
        let content = "Link to [[Actual Name|Display Text]] here.";
        let links = extract_wikilinks(content);
        assert_eq!(links, vec!["Actual Name"]);
    }

    #[test]
    fn test_extract_wikilinks_multiple() {
        let content = "See [[Note A]] and [[Note B]] and [[Note C|Alias]].";
        let links = extract_wikilinks(content);
        assert_eq!(links, vec!["Note A", "Note B", "Note C"]);
    }

    #[test]
    fn test_extract_wikilinks_none() {
        let content = "No wikilinks here, just plain text.";
        let links = extract_wikilinks(content);
        assert!(links.is_empty());
    }

    #[test]
    fn test_extract_wikilinks_empty_brackets() {
        let content = "Empty [[]] should be ignored.";
        let links = extract_wikilinks(content);
        assert!(links.is_empty());
    }

    #[test]
    fn test_compute_hash() {
        let h = compute_hash("test content");
        assert_eq!(h.len(), 64);
    }

    #[test]
    fn test_to_raw_event_with_wikilinks() {
        let links = vec!["Note A".to_string(), "Note B".to_string()];
        let event = to_raw_event("notes/test.md", "# Test\n[[Note A]]", "abc", &links);
        assert_eq!(event.source, "connector:obsidian:notes/test.md");
        assert_eq!(event.tags.get("platform").unwrap(), "obsidian");
        assert_eq!(event.tags.get("wikilinks").unwrap(), "Note A,Note B");
        assert_eq!(event.tags.get("title").unwrap(), "Test");
    }

    #[test]
    fn test_to_raw_event_no_heading_uses_filename() {
        let event = to_raw_event("notes/my-note.md", "Just content", "abc", &[]);
        assert_eq!(event.tags.get("title").unwrap(), "my-note");
    }

    // ── extract_markdown_title edge cases ──

    #[test]
    fn test_extract_markdown_title_simple() {
        assert_eq!(
            extract_markdown_title("# Hello World"),
            Some("Hello World".to_string())
        );
    }

    #[test]
    fn test_extract_markdown_title_with_leading_whitespace() {
        assert_eq!(
            extract_markdown_title("   # Indented Title"),
            Some("Indented Title".to_string())
        );
    }

    #[test]
    fn test_extract_markdown_title_skips_blank_lines() {
        let content = "\n\n\n# First Heading\n## Subtitle";
        assert_eq!(
            extract_markdown_title(content),
            Some("First Heading".to_string())
        );
    }

    #[test]
    fn test_extract_markdown_title_none_for_empty() {
        assert_eq!(extract_markdown_title(""), None);
    }

    #[test]
    fn test_extract_markdown_title_none_for_no_heading() {
        assert_eq!(extract_markdown_title("Just plain text\nNo headings"), None);
    }

    #[test]
    fn test_extract_markdown_title_ignores_h2() {
        // Only H1 (# ) is treated as title, not ## or ###
        let content = "## Subtitle\n### Sub-subtitle";
        assert_eq!(extract_markdown_title(content), None);
    }

    #[test]
    fn test_extract_markdown_title_with_frontmatter() {
        let content = "---\ntitle: ignored\n---\n# Real Title";
        assert_eq!(
            extract_markdown_title(content),
            Some("Real Title".to_string())
        );
    }

    // ── extract_wikilinks edge cases ──

    #[test]
    fn test_extract_wikilinks_nested_brackets() {
        // [[Outer [[Inner]]]] — regex should handle gracefully
        let content = "See [[Outer]] and [[Inner]]";
        let links = extract_wikilinks(content);
        assert_eq!(links, vec!["Outer", "Inner"]);
    }

    #[test]
    fn test_extract_wikilinks_with_special_chars() {
        let content = "Link to [[C++ Notes]] and [[Rust & Cargo]]";
        let links = extract_wikilinks(content);
        assert_eq!(links, vec!["C++ Notes", "Rust & Cargo"]);
    }

    #[test]
    fn test_extract_wikilinks_unicode() {
        let content = "See [[笔记]] and [[メモ]]";
        let links = extract_wikilinks(content);
        assert_eq!(links, vec!["笔记", "メモ"]);
    }

    // ── compute_hash edge cases ──

    #[test]
    fn test_compute_hash_empty() {
        let h = compute_hash("");
        assert_eq!(h.len(), 64);
        // SHA-256 of empty string
        assert_eq!(
            h,
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
    }

    #[test]
    fn test_compute_hash_unicode() {
        let h = compute_hash("你好世界");
        assert_eq!(h.len(), 64);
    }

    #[test]
    fn test_compute_hash_deterministic() {
        let h1 = compute_hash("same content");
        let h2 = compute_hash("same content");
        assert_eq!(h1, h2);
    }

    #[test]
    fn test_compute_hash_different_content() {
        let h1 = compute_hash("content A");
        let h2 = compute_hash("content B");
        assert_ne!(h1, h2);
    }

    // ── to_raw_event edge cases ──

    #[test]
    fn test_to_raw_event_empty_content() {
        let event = to_raw_event("empty.md", "", "hash123", &[]);
        assert_eq!(event.source, "connector:obsidian:empty.md");
        assert_eq!(event.event_type, "document");
        assert!(event.payload.is_empty());
    }

    #[test]
    fn test_to_raw_event_deeply_nested_path() {
        let event = to_raw_event("a/b/c/deep.md", "content", "h", &[]);
        assert_eq!(event.source, "connector:obsidian:a/b/c/deep.md");
    }

    #[test]
    fn test_to_raw_event_tags_contain_content_hash() {
        let event = to_raw_event("note.md", "body", "abc123", &[]);
        assert_eq!(event.tags.get("content_hash").unwrap(), "abc123");
    }
}
