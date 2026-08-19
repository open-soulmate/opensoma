use anyhow::{Context, Result};
use sha2::{Digest, Sha256};
use tokio::task::JoinHandle;
use tokio::time::Duration;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

use crate::collector::{EventTx, RawEvent};
use crate::config::GitConfig;
use crate::connector::Connector;

/// Git connector implementing the unified Connector trait.
pub struct GitConnector {
    config: GitConfig,
}

impl GitConnector {
    pub fn new(config: GitConfig) -> Self {
        Self { config }
    }
}

#[async_trait::async_trait]
impl Connector for GitConnector {
    fn name(&self) -> &str {
        "git"
    }

    async fn ping(&self) -> Result<()> {
        // Verify the local repo exists and has a .git directory
        let git_dir = std::path::Path::new(&self.config.local_path).join(".git");
        if !git_dir.exists() {
            anyhow::bail!(
                "Git repo not found at {} (no .git directory)",
                self.config.local_path
            );
        }
        // Verify git is available
        let output = std::process::Command::new("git")
            .args(["--version"])
            .output()
            .context("git not found in PATH")?;
        if !output.status.success() {
            anyhow::bail!("git --version failed");
        }
        Ok(())
    }
}

/// Start the Git connector. Clones (or pulls) a repo periodically, parses
/// Markdown files, and forwards new/changed content into the collector pipeline.
pub async fn start(config: GitConfig, tx: EventTx) -> Result<JoinHandle<()>> {
    let local_path = config.local_path.clone();

    // Ensure the local path parent exists
    if let Some(parent) = std::path::Path::new(&local_path).parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("Failed to create parent dir for git repo: {:?}", parent))?;
    }

    // Initial clone if the directory doesn't exist or is empty
    let repo_exists = std::path::Path::new(&local_path).join(".git").exists();
    if !repo_exists {
        info!("Cloning git repo {} into {}", config.repo_url, local_path);
        git_clone(&config.repo_url, &config.branch, &local_path)?;
    }

    let handle = tokio::spawn(async move {
        let mut poll_interval =
            tokio::time::interval(Duration::from_secs(config.poll_interval_secs));
        poll_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        info!(
            "Git connector started — repo={}, branch={}, polling every {}s",
            config.repo_url, config.branch, config.poll_interval_secs
        );

        // Track file hashes to detect changes
        let mut known_hashes: std::collections::HashMap<String, String> =
            std::collections::HashMap::new();

        // Initial scan
        if let Err(e) = scan_and_forward(&config, &tx, &mut known_hashes).await {
            warn!("Initial git scan failed: {}", e);
        }

        loop {
            poll_interval.tick().await;

            // Pull latest changes
            if let Err(e) = git_pull(&config.local_path, &config.branch) {
                warn!("git pull failed: {}", e);
                continue;
            }

            // Scan for new/changed files
            if let Err(e) = scan_and_forward(&config, &tx, &mut known_hashes).await {
                warn!("Git scan failed: {}", e);
            }
        }
    });

    Ok(handle)
}

/// Run `git clone`.
fn git_clone(url: &str, branch: &str, local_path: &str) -> Result<()> {
    let output = std::process::Command::new("git")
        .args([
            "clone",
            "--branch",
            branch,
            "--single-branch",
            url,
            local_path,
        ])
        .output()
        .context("Failed to execute git clone")?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        anyhow::bail!("git clone failed: {}", stderr);
    }

    Ok(())
}

/// Run `git pull` in the local repo directory.
fn git_pull(local_path: &str, branch: &str) -> Result<()> {
    let output = std::process::Command::new("git")
        .args(["pull", "origin", branch])
        .current_dir(local_path)
        .output()
        .context("Failed to execute git pull")?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        anyhow::bail!("git pull failed: {}", stderr);
    }

    Ok(())
}

/// Scan the repo for Markdown files, detect new/changed ones, and forward events.
async fn scan_and_forward(
    config: &GitConfig,
    tx: &EventTx,
    known_hashes: &mut std::collections::HashMap<String, String>,
) -> Result<()> {
    let repo_path = std::path::Path::new(&config.local_path);
    let md_files = find_markdown_files(repo_path)?;

    for file_path in md_files {
        let relative = file_path
            .strip_prefix(repo_path)
            .unwrap_or(&file_path)
            .to_string_lossy()
            .to_string();

        let content = match std::fs::read_to_string(&file_path) {
            Ok(c) => c,
            Err(e) => {
                warn!("Failed to read {}: {}", relative, e);
                continue;
            }
        };

        let hash = compute_hash(&content);

        // Skip if unchanged
        if known_hashes.get(&relative) == Some(&hash) {
            continue;
        }

        known_hashes.insert(relative.clone(), hash.clone());

        let raw_event = to_raw_event(&relative, &content, &hash);
        match tx.try_send(raw_event) {
            Ok(()) => {
                debug!("Forwarded git file: {}", relative);
            }
            Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                warn!("Event channel full, dropping git file: {}", relative);
            }
            Err(e) => {
                error!("Failed to send git event: {}", e);
            }
        }
    }

    Ok(())
}

/// Recursively find all `.md` files under a directory.
fn find_markdown_files(dir: &std::path::Path) -> Result<Vec<std::path::PathBuf>> {
    let mut results = Vec::new();

    if !dir.is_dir() {
        return Ok(results);
    }

    for entry in walkdir_or_fallback(dir)? {
        let path = entry;
        if path.extension().is_some_and(|ext| ext == "md") {
            results.push(path);
        }
    }

    Ok(results)
}

/// Simple recursive directory walk (no external dependency).
fn walkdir_or_fallback(dir: &std::path::Path) -> Result<Vec<std::path::PathBuf>> {
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

            // Skip hidden directories (like .git)
            if path
                .file_name()
                .is_some_and(|n| n.to_string_lossy().starts_with('.'))
            {
                continue;
            }

            if path.is_dir() {
                stack.push(path);
            } else {
                results.push(path);
            }
        }
    }

    Ok(results)
}

/// Compute SHA-256 hash of content.
fn compute_hash(content: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(content.as_bytes());
    hex::encode(hasher.finalize())
}

/// Convert a git Markdown file into a RawEvent.
fn to_raw_event(relative_path: &str, content: &str, hash: &str) -> RawEvent {
    let mut tags = std::collections::HashMap::new();
    tags.insert("platform".to_string(), "git".to_string());
    tags.insert("file_path".to_string(), relative_path.to_string());
    tags.insert("content_hash".to_string(), hash.to_string());
    tags.insert("format".to_string(), "markdown".to_string());

    // Extract title from first heading if present
    if let Some(title) = extract_markdown_title(content) {
        tags.insert("title".to_string(), title);
    }

    RawEvent {
        id: Uuid::new_v4().to_string(),
        source: format!("connector:git:{}", relative_path),
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
            return Some(trimmed.strip_prefix("# ").unwrap_or(trimmed).trim().to_string());
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compute_hash_deterministic() {
        let h1 = compute_hash("hello world");
        let h2 = compute_hash("hello world");
        assert_eq!(h1, h2);
        assert_eq!(h1.len(), 64); // SHA-256 hex = 64 chars
    }

    #[test]
    fn test_compute_hash_different_input() {
        let h1 = compute_hash("hello");
        let h2 = compute_hash("world");
        assert_ne!(h1, h2);
    }

    #[test]
    fn test_extract_markdown_title() {
        assert_eq!(
            extract_markdown_title("# My Title\nSome content"),
            Some("My Title".to_string())
        );
        assert_eq!(
            extract_markdown_title("No heading here"),
            None
        );
        assert_eq!(
            extract_markdown_title("## Not H1\n# Real H1"),
            Some("Real H1".to_string())
        );
    }

    #[test]
    fn test_to_raw_event_structure() {
        let event = to_raw_event("docs/readme.md", "# Hello\nWorld", "abc123");
        assert_eq!(event.event_type, "document");
        assert_eq!(event.source, "connector:git:docs/readme.md");
        assert_eq!(event.tags.get("platform").unwrap(), "git");
        assert_eq!(event.tags.get("file_path").unwrap(), "docs/readme.md");
        assert_eq!(event.tags.get("content_hash").unwrap(), "abc123");
        assert_eq!(event.tags.get("title").unwrap(), "Hello");
    }

    #[test]
    fn test_find_markdown_files_empty_dir() {
        let dir = std::env::temp_dir().join("opensoma_test_empty_md");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let result = find_markdown_files(&dir).unwrap();
        assert!(result.is_empty());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_find_markdown_files_with_files() {
        let dir = std::env::temp_dir().join("opensoma_test_md_files");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("readme.md"), "# Test").unwrap();
        std::fs::write(dir.join("notes.txt"), "not markdown").unwrap();
        std::fs::write(dir.join("guide.md"), "# Guide").unwrap();
        // .md in subdirectory
        let subdir = dir.join("subdir");
        std::fs::create_dir_all(&subdir).unwrap();
        std::fs::write(subdir.join("nested.md"), "# Nested").unwrap();

        let mut result = find_markdown_files(&dir).unwrap();
        result.sort();
        assert_eq!(result.len(), 3);
        assert!(result.iter().any(|p| p.ends_with("readme.md")));
        assert!(result.iter().any(|p| p.ends_with("guide.md")));
        assert!(result.iter().any(|p| p.ends_with("nested.md")));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_find_markdown_files_skips_hidden_dirs() {
        let dir = std::env::temp_dir().join("opensoma_test_md_hidden");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("visible.md"), "# Visible").unwrap();
        let hidden = dir.join(".hidden");
        std::fs::create_dir_all(&hidden).unwrap();
        std::fs::write(hidden.join("secret.md"), "# Secret").unwrap();

        let result = find_markdown_files(&dir).unwrap();
        assert_eq!(result.len(), 1);
        assert!(result[0].ends_with("visible.md"));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_find_markdown_files_nonexistent_dir() {
        let dir = std::path::Path::new("/nonexistent/path/that/does/not/exist");
        let result = find_markdown_files(dir).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn test_walkdir_or_fallback_skips_hidden() {
        let dir = std::env::temp_dir().join("opensoma_test_walkdir");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(dir.join(".hidden/sub")).unwrap();
        std::fs::create_dir_all(dir.join("visible")).unwrap();
        std::fs::write(dir.join("file.txt"), "content").unwrap();
        std::fs::write(dir.join(".hidden/secret.txt"), "secret").unwrap();

        let result = walkdir_or_fallback(&dir).unwrap();
        assert!(result.iter().any(|p| p.ends_with("file.txt")));
        assert!(!result.iter().any(|p| p.to_string_lossy().contains(".hidden")));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_extract_markdown_title_no_heading() {
        assert_eq!(extract_markdown_title(""), None);
        assert_eq!(extract_markdown_title("Just text\nNo heading"), None);
        assert_eq!(extract_markdown_title("## Sub heading only"), None);
    }
}
