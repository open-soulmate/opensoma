/// Build and version information embedded at compile time.
///
/// This module provides a central source of truth for version info
/// used by the CLI, status server, and heartbeat metadata.
/// Package version from Cargo.toml (e.g., "0.1.0")
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

/// Git commit short hash (e.g., "f4d411d")
pub const GIT_HASH: &str = env!("GIT_HASH");

/// Git branch name (e.g., "main")
pub const GIT_BRANCH: &str = env!("GIT_BRANCH");

/// Build timestamp in UTC (e.g., "2026-08-19T12:34:56Z")
pub const BUILD_TIMESTAMP: &str = env!("BUILD_TIMESTAMP");

/// Full version string: "opensoma v0.1.0 (abc1234 main 2026-08-19T12:34:56Z)"
pub fn version_string() -> String {
    format!(
        "opensoma v{} ({} {} {})",
        VERSION, GIT_HASH, GIT_BRANCH, BUILD_TIMESTAMP
    )
}

/// Compact version: "0.1.0 (abc1234)"
pub fn short_version() -> String {
    format!("{} ({})", VERSION, GIT_HASH)
}

/// Version info as a JSON value for API responses.
pub fn version_json() -> serde_json::Value {
    serde_json::json!({
        "version": VERSION,
        "git_hash": GIT_HASH,
        "git_branch": GIT_BRANCH,
        "build_timestamp": BUILD_TIMESTAMP,
        "rust_version": env!("CARGO_PKG_RUST_VERSION"),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_version_not_empty() {
        assert!(!VERSION.is_empty());
    }

    #[test]
    fn test_git_hash_format() {
        // Git short hash is 7 hex chars, or "unknown"
        if GIT_HASH != "unknown" {
            assert_eq!(GIT_HASH.len(), 7, "Expected 7-char git hash");
            assert!(
                GIT_HASH.chars().all(|c| c.is_ascii_hexdigit()),
                "Expected hex chars"
            );
        }
    }

    #[test]
    fn test_version_string_format() {
        let vs = version_string();
        assert!(vs.starts_with("opensoma v"), "Should start with 'opensoma v'");
        assert!(vs.contains(VERSION));
    }

    #[test]
    fn test_short_version_format() {
        let sv = short_version();
        assert!(sv.starts_with(VERSION));
        assert!(sv.contains('('));
        assert!(sv.contains(')'));
    }

    #[test]
    fn test_version_json_fields() {
        let json = version_json();
        assert!(json.get("version").is_some());
        assert!(json.get("git_hash").is_some());
        assert!(json.get("git_branch").is_some());
        assert!(json.get("build_timestamp").is_some());
        assert_eq!(json["version"], VERSION);
    }
}
