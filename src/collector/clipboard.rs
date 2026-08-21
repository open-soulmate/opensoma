use anyhow::Result;
use chrono::Utc;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::time::Duration;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

use super::{EventTx, RawEvent};

/// Clipboard monitor that polls the system clipboard at a fixed interval
/// and emits events when content changes.
///
/// On Linux, this uses `xclip` or `wl-paste` as a fallback.
/// Clipboard monitoring is inherently platform-specific; this module
/// handles the Linux/X11/Wayland case.
pub async fn start_clipboard_monitor(interval_ms: u64, tx: EventTx) -> Result<()> {
    info!("Starting clipboard monitor (interval={}ms)", interval_ms);

    let mut prev_hash: u64 = 0;
    let mut ticker = tokio::time::interval(Duration::from_millis(interval_ms));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    // Detect clipboard tool on first tick
    let clipboard_cmd = detect_clipboard_tool().await;

    loop {
        ticker.tick().await;

        let content = match read_clipboard(&clipboard_cmd).await {
            Some(c) => c,
            None => continue,
        };

        if content.is_empty() {
            continue;
        }

        let hash = hash_content(&content);
        if hash == prev_hash {
            continue;
        }

        prev_hash = hash;

        let mut tags = std::collections::HashMap::new();
        tags.insert("content_hash".to_string(), format!("{:016x}", hash));
        tags.insert("content_length".to_string(), content.len().to_string());

        // Classify content type by sniffing
        let content_type = sniff_content_type(&content);
        tags.insert("content_type".to_string(), content_type.to_string());

        // Don't store raw clipboard content in tags for privacy
        // Store only first 100 chars as preview
        let preview: String = content.chars().take(100).collect();
        tags.insert("preview".to_string(), preview);

        let event = RawEvent {
            id: Uuid::new_v4().to_string(),
            source: "clipboard".to_string(),
            event_type: "clipboard_change".to_string(),
            timestamp_ms: Utc::now().timestamp_millis(),
            payload: content.into_bytes(),
            tags,
        };

        debug!("Clipboard content changed (hash={:016x})", hash);
        match tx.try_send(event) {
            Ok(()) => {}
            Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                warn!("Event channel full, dropping clipboard event");
            }
            Err(e) => {
                error!("Failed to send clipboard event: {}", e);
            }
        }
    }
}

/// Detect the available clipboard tool.
async fn detect_clipboard_tool() -> ClipboardTool {
    // Try wl-paste first (Wayland)
    if let Ok(output) = tokio::process::Command::new("wl-paste")
        .arg("--no-newline")
        .output()
        .await
    {
        if output.status.success() {
            info!("Clipboard tool: wl-paste (Wayland)");
            return ClipboardTool::WlPaste;
        }
    }

    // Try xclip (X11)
    if let Ok(output) = tokio::process::Command::new("xclip")
        .args(["-selection", "clipboard", "-o"])
        .output()
        .await
    {
        if output.status.success() {
            info!("Clipboard tool: xclip (X11)");
            return ClipboardTool::XClip;
        }
    }

    // Try xsel (X11 alternative)
    if let Ok(output) = tokio::process::Command::new("xsel")
        .args(["--clipboard", "--output"])
        .output()
        .await
    {
        if output.status.success() {
            info!("Clipboard tool: xsel (X11)");
            return ClipboardTool::XSel;
        }
    }

    warn!("No clipboard tool found (tried: wl-paste, xclip, xsel). Clipboard monitor disabled.");
    ClipboardTool::None
}

/// Read clipboard content using the detected tool.
async fn read_clipboard(tool: &ClipboardTool) -> Option<String> {
    let (cmd, args) = match tool {
        ClipboardTool::WlPaste => ("wl-paste", vec!["--no-newline"]),
        ClipboardTool::XClip => ("xclip", vec!["-selection", "clipboard", "-o"]),
        ClipboardTool::XSel => ("xsel", vec!["--clipboard", "--output"]),
        ClipboardTool::None => return None,
    };

    match tokio::process::Command::new(cmd).args(&args).output().await {
        Ok(output) if output.status.success() => String::from_utf8(output.stdout).ok(),
        Ok(output) => {
            debug!(
                "Clipboard read failed (exit={}): {}",
                output.status,
                String::from_utf8_lossy(&output.stderr)
            );
            None
        }
        Err(e) => {
            debug!("Clipboard read error: {}", e);
            None
        }
    }
}

/// Hash clipboard content for change detection.
fn hash_content(content: &str) -> u64 {
    let mut hasher = DefaultHasher::new();
    content.hash(&mut hasher);
    hasher.finish()
}

/// Sniff content type from clipboard text.
fn sniff_content_type(content: &str) -> &'static str {
    let trimmed = content.trim();

    if trimmed.starts_with("http://") || trimmed.starts_with("https://") {
        return "url";
    }
    if (trimmed.starts_with('{') || trimmed.starts_with('['))
        && serde_json::from_str::<serde_json::Value>(trimmed).is_ok()
    {
        return "json";
    }
    if trimmed.contains('@')
        && trimmed.contains('.')
        && !trimmed.contains(' ')
        && trimmed.len() < 254
    {
        return "email";
    }
    if trimmed.starts_with("-----BEGIN") {
        return "pem";
    }
    // Check if it looks like a file path
    if trimmed.starts_with('/') || (trimmed.len() > 2 && trimmed.as_bytes()[1] == b':') {
        return "filepath";
    }
    if trimmed.contains('\n') {
        return "multiline_text";
    }

    "text"
}

#[derive(Debug)]
enum ClipboardTool {
    WlPaste,
    XClip,
    XSel,
    None,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hash_content() {
        let h1 = hash_content("hello");
        let h2 = hash_content("hello");
        let h3 = hash_content("world");
        assert_eq!(h1, h2);
        assert_ne!(h1, h3);
    }

    #[test]
    fn test_sniff_content_type() {
        assert_eq!(sniff_content_type("https://example.com"), "url");
        assert_eq!(sniff_content_type("{\"key\": \"value\"}"), "json");
        assert_eq!(sniff_content_type("user@example.com"), "email");
        assert_eq!(sniff_content_type("hello world"), "text");
        assert_eq!(sniff_content_type("/home/user/file.txt"), "filepath");
        assert_eq!(sniff_content_type("line1\nline2"), "multiline_text");
    }

    #[test]
    fn test_sniff_content_type_pem() {
        assert_eq!(
            sniff_content_type("-----BEGIN CERTIFICATE-----\nMIIE..."),
            "pem"
        );
    }

    #[test]
    fn test_sniff_content_type_json_array() {
        assert_eq!(sniff_content_type("[1, 2, 3]"), "json");
    }

    #[test]
    fn test_sniff_content_type_invalid_json() {
        assert_eq!(sniff_content_type("{not valid json}"), "text");
    }

    #[test]
    fn test_sniff_content_type_email_with_spaces() {
        // Should NOT match email if it contains spaces
        assert_eq!(sniff_content_type("send to user@example.com now"), "text");
    }

    #[test]
    fn test_sniff_content_type_windows_path() {
        assert_eq!(sniff_content_type("C:\\Users\\test"), "filepath");
    }

    #[test]
    fn test_hash_content_empty() {
        let h = hash_content("");
        // Should not panic, just produce a hash
        assert_eq!(h, hash_content(""));
    }

    #[test]
    fn test_hash_content_unicode() {
        let h1 = hash_content("你好世界");
        let h2 = hash_content("你好世界");
        let h3 = hash_content("hello world");
        assert_eq!(h1, h2);
        assert_ne!(h1, h3);
    }

    #[test]
    fn test_sniff_content_type_ip_address_is_text() {
        // IP addresses are plain text in current implementation
        assert_eq!(sniff_content_type("192.168.1.1"), "text");
        assert_eq!(sniff_content_type("10.0.0.1"), "text");
    }

    #[test]
    fn test_sniff_content_type_long_base64_is_text() {
        // Current implementation does not detect base64 encoding
        let b64 = "SGVsbG8gV29ybGQhIFRoaXMgaXMgYSBsb25nIGJhc2U2NCBlbmNvZGVkIHN0cmluZyB0aGF0IGlzIGxvbmdlciB0aGFuIDUwIGNoYXJhY3RlcnM=";
        assert_eq!(sniff_content_type(b64), "text");
    }

    #[test]
    fn test_sniff_content_type_multiline_json() {
        let json = r#"{
            "key": "value",
            "nested": {
                "array": [1, 2, 3]
            }
        }"#;
        // Multiline JSON starts with '{' and is valid JSON, so it's detected as json
        assert_eq!(sniff_content_type(json), "json");
    }

    #[test]
    fn test_sniff_content_type_ssh_key_is_text() {
        // SSH keys are detected as plain text (contain spaces)
        assert_eq!(
            sniff_content_type("ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABgQC7FBmMSVTj user@host"),
            "text"
        );
    }

    #[test]
    fn test_sniff_content_type_env_var_is_text() {
        // Env vars are detected as plain text in current implementation
        assert_eq!(sniff_content_type("DATABASE_URL=postgres://localhost/db"), "text");
        assert_eq!(sniff_content_type("API_KEY=sk-1234567890"), "text");
    }

    #[test]
    fn test_hash_content_whitespace_sensitivity() {
        let h1 = hash_content("hello world");
        let h2 = hash_content("hello  world");
        assert_ne!(h1, h2); // Different whitespace = different hash
    }

    #[test]
    fn test_hash_content_long_text() {
        let long_text = "x".repeat(100_000);
        let h1 = hash_content(&long_text);
        let h2 = hash_content(&long_text);
        assert_eq!(h1, h2);
    }
}
