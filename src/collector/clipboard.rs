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

    // URL
    if trimmed.starts_with("http://") || trimmed.starts_with("https://") {
        return "url";
    }

    // JSON (must be checked before YAML since valid JSON is not YAML)
    if (trimmed.starts_with('{') || trimmed.starts_with('['))
        && serde_json::from_str::<serde_json::Value>(trimmed).is_ok()
    {
        return "json";
    }

    // YAML (starts with document separator or has key: value patterns)
    if trimmed.starts_with("---\n") || trimmed.starts_with("---\r\n") || trimmed.starts_with("%YAML") {
        return "yaml";
    }
    if trimmed.contains('\n') && looks_like_yaml(trimmed) {
        return "yaml";
    }

    // TOML (has [section] headers with key = value)
    if trimmed.contains('\n') && looks_like_toml(trimmed) {
        return "toml";
    }

    // CSV (comma/tab separated with consistent column counts)
    if trimmed.contains('\n') && looks_like_csv(trimmed) {
        return "csv";
    }

    // Markdown (starts with heading or has common markdown patterns)
    if trimmed.starts_with('#') && trimmed.contains('\n') {
        return "markdown";
    }
    if trimmed.contains('\n') && looks_like_markdown(trimmed) {
        return "markdown";
    }

    // SQL
    if looks_like_sql(trimmed) {
        return "sql";
    }

    // Email address
    if trimmed.contains('@')
        && trimmed.contains('.')
        && !trimmed.contains(' ')
        && trimmed.len() < 254
    {
        return "email";
    }

    // PEM certificate/key
    if trimmed.starts_with("-----BEGIN") {
        return "pem";
    }

    // Base64 (long string of valid base64 characters, no spaces)
    if trimmed.len() > 64 && looks_like_base64(trimmed) {
        return "base64";
    }

    // File path
    if trimmed.starts_with('/') || (trimmed.len() > 2 && trimmed.as_bytes()[1] == b':') {
        return "filepath";
    }

    // Multiline text (fallback for multi-line content)
    if trimmed.contains('\n') {
        return "multiline_text";
    }

    "text"
}

/// Check if content looks like YAML (key: value pairs, list items).
fn looks_like_yaml(content: &str) -> bool {
    let lines: Vec<&str> = content.lines().take(20).collect();
    if lines.len() < 3 {
        return false;
    }
    let kv_lines = lines
        .iter()
        .filter(|l| {
            let l = l.trim();
            // key: value pattern (but not URL-like or email-like)
            l.contains(": ")
                && !l.starts_with("//")
                && !l.starts_with('#')
                && !l.starts_with("http")
        })
        .count();
    let list_lines = lines
        .iter()
        .filter(|l| {
            let l = l.trim();
            l.starts_with("- ")
        })
        .count();
    // At least 40% of lines should be YAML-like (minimum 2 matches)
    (kv_lines + list_lines) >= std::cmp::max(2, lines.len() * 2 / 5)
}

/// Check if content looks like TOML ([section] headers, key = value).
fn looks_like_toml(content: &str) -> bool {
    let lines: Vec<&str> = content.lines().take(20).collect();
    if lines.len() < 2 {
        return false;
    }
    let section_lines = lines
        .iter()
        .filter(|l| {
            let l = l.trim();
            l.starts_with('[') && l.ends_with(']') && l.len() > 2
        })
        .count();
    let kv_lines = lines
        .iter()
        .filter(|l| {
            let l = l.trim();
            l.contains(" = ") && !l.starts_with('#')
        })
        .count();
    section_lines > 0 && kv_lines > 0
}

/// Check if content looks like CSV (consistent column counts across lines).
fn looks_like_csv(content: &str) -> bool {
    let lines: Vec<&str> = content.lines().take(10).collect();
    if lines.len() < 3 {
        return false;
    }
    // Detect delimiter (comma or tab)
    let comma_count = lines[0].matches(',').count();
    let tab_count = lines[0].matches('\t').count();
    if comma_count == 0 && tab_count == 0 {
        return false;
    }
    let delimiter = if comma_count >= tab_count { ',' } else { '\t' };
    let expected_cols = lines[0].split(delimiter).count();
    if expected_cols < 2 {
        return false;
    }
    // Check that most lines have the same column count
    let consistent = lines
        .iter()
        .skip(1)
        .filter(|l| l.split(delimiter).count() == expected_cols)
        .count();
    consistent >= lines.len() * 3 / 5
}

/// Check if content looks like Markdown.
fn looks_like_markdown(content: &str) -> bool {
    let lines: Vec<&str> = content.lines().take(20).collect();
    let md_indicators = lines
        .iter()
        .filter(|l| {
            let l = l.trim();
            l.starts_with("# ")
                || l.starts_with("## ")
                || l.starts_with("- [ ] ")
                || l.starts_with("- [x] ")
                || l.starts_with("> ")
                || l.starts_with("```")
                || l.contains("](http")
                || l.starts_with("---")
                || l.starts_with("***")
        })
        .count();
    md_indicators >= 2
}

/// Check if content looks like SQL.
fn looks_like_sql(content: &str) -> bool {
    let upper = content.to_uppercase();
    let trimmed = upper.trim();
    trimmed.starts_with("SELECT ")
        || trimmed.starts_with("INSERT ")
        || trimmed.starts_with("UPDATE ")
        || trimmed.starts_with("DELETE ")
        || trimmed.starts_with("CREATE ")
        || trimmed.starts_with("ALTER ")
        || trimmed.starts_with("DROP ")
        || trimmed.starts_with("WITH ")
}

/// Check if content looks like base64 encoded data.
fn looks_like_base64(content: &str) -> bool {
    let trimmed = content.trim();
    // Base64 uses A-Z, a-z, 0-9, +, /, = for padding
    // Must have no spaces, no newlines in the core data
    let single_line = trimmed.replace(['\n', '\r'], "");
    if single_line.contains(' ') {
        return false;
    }
    let valid_chars = single_line
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '+' || c == '/' || c == '=');
    if !valid_chars {
        return false;
    }
    // Must be reasonably long and have valid base64 padding
    let len = single_line.len();
    len >= 64 && (len % 4 == 0 || single_line.ends_with('='))
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
    fn test_sniff_content_type_base64_detected() {
        // Now we detect base64 encoding
        let b64 = "SGVsbG8gV29ybGQhIFRoaXMgaXMgYSBsb25nIGJhc2U2NCBlbmNvZGVkIHN0cmluZyB0aGF0IGlzIGxvbmdlciB0aGFuIDUwIGNoYXJhY3RlcnM=";
        assert_eq!(sniff_content_type(b64), "base64");
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

    #[test]
    fn test_sniff_content_type_yaml_document_separator() {
        assert_eq!(
            sniff_content_type("---\nname: test\nversion: 1.0"),
            "yaml"
        );
    }

    #[test]
    fn test_sniff_content_type_yaml_key_value() {
        let yaml = "name: OpenSoma\nversion: 1.0.0\nauthor: test\ndescription: A tool";
        assert_eq!(sniff_content_type(yaml), "yaml");
    }

    #[test]
    fn test_sniff_content_type_yaml_list() {
        let yaml = "items:\n  - first\n  - second\n  - third\n  - fourth";
        assert_eq!(sniff_content_type(yaml), "yaml");
    }

    #[test]
    fn test_sniff_content_type_toml() {
        let toml = "[package]\nname = \"opensoma\"\nversion = \"0.1.0\"\n\n[dependencies]\ntokio = \"1\"";
        assert_eq!(sniff_content_type(toml), "toml");
    }

    #[test]
    fn test_sniff_content_type_csv() {
        let csv = "name,age,city\nAlice,30,NYC\nBob,25,LA\nCharlie,35,Chicago";
        assert_eq!(sniff_content_type(csv), "csv");
    }

    #[test]
    fn test_sniff_content_type_csv_tab_delimited() {
        let csv = "name\tage\tcity\nAlice\t30\tNYC\nBob\t25\tLA\nCharlie\t35\tChicago";
        assert_eq!(sniff_content_type(csv), "csv");
    }

    #[test]
    fn test_sniff_content_type_markdown_heading() {
        let md = "# My Document\n\nSome content here.\n\n## Section 2\n\nMore content.";
        assert_eq!(sniff_content_type(md), "markdown");
    }

    #[test]
    fn test_sniff_content_type_markdown_blockquote() {
        let md = "> This is a quote\n> with multiple lines\n\nAnd some regular text.";
        assert_eq!(sniff_content_type(md), "markdown");
    }

    #[test]
    fn test_sniff_content_type_sql_select() {
        assert_eq!(
            sniff_content_type("SELECT * FROM users WHERE id = 1"),
            "sql"
        );
    }

    #[test]
    fn test_sniff_content_type_sql_create() {
        let sql = "CREATE TABLE users (\n  id INTEGER PRIMARY KEY,\n  name TEXT NOT NULL\n);";
        assert_eq!(sniff_content_type(sql), "sql");
    }

    #[test]
    fn test_sniff_content_type_sql_insert() {
        assert_eq!(
            sniff_content_type("INSERT INTO users (name, age) VALUES ('Alice', 30)"),
            "sql"
        );
    }

    #[test]
    fn test_sniff_content_type_base64_no_padding() {
        // Short base64 without padding should still be text
        assert_eq!(sniff_content_type("SGVsbG8gV29ybGQ"), "text");
    }

    #[test]
    fn test_sniff_content_type_base64_with_spaces() {
        // Base64 with spaces should not be detected as base64
        let b64 = "SGVsbG8g V29ybGQh IFRoaXMg aXMgYSB sb25nIGJ hc2U2NCB lbmNvZGV kIHN0cml uZyB0aGF";
        assert_eq!(sniff_content_type(b64), "text");
    }

    #[test]
    fn test_sniff_content_type_yaml_not_json() {
        // JSON should still be detected as json, not yaml
        assert_eq!(sniff_content_type("{\"key\": \"value\"}"), "json");
    }

    #[test]
    fn test_sniff_content_type_toml_not_yaml() {
        // TOML with sections should be detected as toml, not yaml
        let toml = "[section]\nkey = \"value\"\nother = 42";
        assert_eq!(sniff_content_type(toml), "toml");
    }

    #[test]
    fn test_sniff_content_type_csv_not_enough_lines() {
        // Less than 3 lines should not be detected as CSV
        assert_eq!(sniff_content_type("a,b,c\n1,2,3"), "multiline_text");
    }

    #[test]
    fn test_sniff_content_type_markdown_single_heading() {
        // Single heading without newline should be text
        assert_eq!(sniff_content_type("# Just a heading"), "text");
    }

    #[test]
    fn test_sniff_content_type_sql_case_insensitive() {
        assert_eq!(
            sniff_content_type("select * from users"),
            "sql"
        );
    }
}
