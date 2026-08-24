/// GitHub API connector — syncs issues, pull requests, and releases
/// from one or more repositories into the Soul knowledge base.
///
/// Uses the GitHub REST API v3 (no external crate dependencies beyond reqwest).
use anyhow::{Context, Result};
use serde::Deserialize;
use tokio::task::JoinHandle;
use tokio::time::Duration;
use tracing::{debug, error, info, warn};

use crate::collector::{EventTx, RawEvent};
use crate::config::GitHubConfig;
use crate::connector::circuit_breaker::CircuitBreaker;
use crate::connector::Connector;

/// Rate-limit state tracked between API calls.
/// GitHub allows 60 requests/hour (unauthenticated) or 5000/hour (authenticated).
/// When remaining hits 0, we sleep until the reset timestamp.
struct RateLimitState {
    remaining: u32,
    reset_at: u64, // Unix timestamp in seconds
}

impl RateLimitState {
    fn new() -> Self {
        Self {
            remaining: u32::MAX,
            reset_at: 0,
        }
    }

    /// Update rate limit state from response headers.
    fn update_from_headers(&mut self, headers: &reqwest::header::HeaderMap) {
        if let Some(remaining) = headers.get("x-ratelimit-remaining") {
            if let Ok(val) = remaining.to_str().unwrap_or("").parse::<u32>() {
                self.remaining = val;
            }
        }
        if let Some(reset) = headers.get("x-ratelimit-reset") {
            if let Ok(val) = reset.to_str().unwrap_or("").parse::<u64>() {
                self.reset_at = val;
            }
        }
    }

    /// Wait if we've exhausted the rate limit. Returns Ok(()) once it's safe to proceed.
    async fn wait_if_needed(&self) {
        if self.remaining == 0 && self.reset_at > 0 {
            let now = chrono::Utc::now().timestamp() as u64;
            if self.reset_at > now {
                let wait_secs = self.reset_at - now + 1; // +1s buffer
                warn!(
                    "GitHub rate limit exhausted — sleeping {}s until reset",
                    wait_secs
                );
                tokio::time::sleep(Duration::from_secs(wait_secs)).await;
            }
        }
    }
}

/// Make a GitHub API GET request with rate-limit awareness and one retry on 403/429.
async fn github_get(
    client: &reqwest::Client,
    url: &str,
    rate_limit: &mut RateLimitState,
) -> Result<reqwest::Response> {
    // Wait if we know the rate limit is exhausted
    rate_limit.wait_if_needed().await;

    let resp = client
        .get(url)
        .send()
        .await
        .with_context(|| format!("GitHub API request failed: {}", url))?;

    // Update rate limit state from response headers
    rate_limit.update_from_headers(resp.headers());

    let status = resp.status();

    // Handle rate-limited responses (403 with rate limit or 429)
    if status == reqwest::StatusCode::FORBIDDEN || status == reqwest::StatusCode::TOO_MANY_REQUESTS {
        // Check for Retry-After header first
        let retry_after = resp
            .headers()
            .get("retry-after")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.parse::<u64>().ok());

        let wait_duration = if let Some(seconds) = retry_after {
            Duration::from_secs(seconds)
        } else if rate_limit.reset_at > 0 {
            let now = chrono::Utc::now().timestamp() as u64;
            let wait = rate_limit.reset_at.saturating_sub(now) + 1;
            Duration::from_secs(wait.max(5)) // At least 5s
        } else {
            Duration::from_secs(60) // Default 60s backoff
        };

        warn!(
            "GitHub rate limited ({}), retrying in {}s: {}",
            status,
            wait_duration.as_secs(),
            url
        );
        tokio::time::sleep(wait_duration).await;

        // Retry once after waiting
        let resp2 = client
            .get(url)
            .send()
            .await
            .with_context(|| format!("GitHub API retry failed: {}", url))?;
        rate_limit.update_from_headers(resp2.headers());

        if !resp2.status().is_success() {
            let s = resp2.status();
            let body = resp2.text().await.unwrap_or_default();
            anyhow::bail!("GitHub API error {} after retry: {}", s, body);
        }
        return Ok(resp2);
    }

    if !status.is_success() {
        let body = resp.text().await.unwrap_or_default();
        anyhow::bail!("GitHub API error {}: {}", status, body);
    }

    Ok(resp)
}

/// GitHub connector implementing the unified Connector trait.
pub struct GitHubConnector {
    config: GitHubConfig,
}

impl GitHubConnector {
    pub fn new(config: GitHubConfig) -> Self {
        Self { config }
    }
}

#[async_trait::async_trait]
impl Connector for GitHubConnector {
    fn name(&self) -> &str {
        "github"
    }

    async fn ping(&self) -> Result<()> {
        let client = build_client(&self.config);
        // Check rate limit endpoint (no auth required, lightweight)
        let resp = client
            .get("https://api.github.com/rate_limit")
            .send()
            .await
            .context("GitHub API unreachable")?;
        if !resp.status().is_success() {
            anyhow::bail!("GitHub API returned {}", resp.status());
        }
        Ok(())
    }
}

#[derive(Debug, Deserialize)]
struct GitHubIssue {
    number: u64,
    title: String,
    body: Option<String>,
    state: String,
    html_url: String,
    user: Option<GitHubUser>,
    labels: Vec<GitHubLabel>,
    created_at: String,
    updated_at: String,
    pull_request: Option<serde_json::Value>,
}

#[derive(Debug, Deserialize)]
struct GitHubUser {
    login: String,
}

#[derive(Debug, Deserialize)]
struct GitHubLabel {
    name: String,
}

#[derive(Debug, Deserialize)]
struct GitHubRelease {
    tag_name: String,
    name: Option<String>,
    body: Option<String>,
    html_url: String,
    published_at: String,
    author: Option<GitHubUser>,
}

/// A recent commit from the GitHub commits API.
#[derive(Debug, Deserialize)]
struct GitHubCommit {
    sha: String,
    commit: GitHubCommitDetail,
    html_url: String,
    author: Option<GitHubUser>,
}

#[derive(Debug, Deserialize)]
struct GitHubCommitDetail {
    message: String,
    author: GitHubCommitAuthor,
}

#[derive(Debug, Deserialize)]
struct GitHubCommitAuthor {
    name: String,
    email: String,
    date: String,
}

/// A review comment on a pull request (inline code comment).
#[derive(Debug, Deserialize)]
struct GitHubReviewComment {
    id: u64,
    body: String,
    path: String,
    #[serde(default)]
    line: Option<u64>,
    html_url: String,
    user: Option<GitHubUser>,
    created_at: String,
    updated_at: String,
    #[serde(default)]
    pull_request_url: Option<String>,
}

/// Start the GitHub connector. Polls the GitHub API for issues, PRs, and releases.
pub async fn start(config: GitHubConfig, tx: EventTx, circuit_breaker: Option<CircuitBreaker>) -> Result<JoinHandle<()>> {
    let handle = tokio::spawn(async move {
        let poll_interval = Duration::from_secs(config.poll_interval_secs);
        let client = build_client(&config);

        info!(
            "GitHub connector started — repos={}, polling every {}s, circuit_breaker={}",
            config.repos.join(", "),
            config.poll_interval_secs,
            circuit_breaker.is_some()
        );

        let mut seen_issues: std::collections::HashSet<String> = std::collections::HashSet::new();
        let mut seen_releases: std::collections::HashSet<String> = std::collections::HashSet::new();
        let mut seen_commits: std::collections::HashSet<String> = std::collections::HashSet::new();
        let mut seen_review_comments: std::collections::HashSet<String> =
            std::collections::HashSet::new();
        let mut rate_limit = RateLimitState::new();

        // Initial fetch (skip if circuit breaker is open)
        if let Some(ref cb) = circuit_breaker {
            if cb.allow_request().await.is_err() {
                warn!("GitHub circuit breaker open — skipping initial fetch");
            } else {
                let mut any_failed = false;
                for repo in &config.repos {
                    if let Err(e) = fetch_issues(&client, &config, repo, &tx, &mut seen_issues, &mut rate_limit).await {
                        warn!("Initial GitHub issues fetch failed for {}: {}", repo, e);
                        any_failed = true;
                    }
                    if config.include_releases {
                        if let Err(e) = fetch_releases(&client, repo, &tx, &mut seen_releases, &mut rate_limit).await {
                            warn!("Initial GitHub releases fetch failed for {}: {}", repo, e);
                            any_failed = true;
                        }
                    }
                    if let Err(e) = fetch_commits(&client, repo, &tx, &mut seen_commits, &mut rate_limit).await {
                        warn!("Initial GitHub commits fetch failed for {}: {}", repo, e);
                        any_failed = true;
                    }
                    if config.include_review_comments {
                        if let Err(e) =
                            fetch_review_comments(&client, repo, &tx, &mut seen_review_comments, &mut rate_limit).await
                        {
                            warn!(
                                "Initial GitHub review comments fetch failed for {}: {}",
                                repo, e
                            );
                            any_failed = true;
                        }
                    }
                }
                if any_failed { cb.record_failure().await; } else { cb.record_success().await; }
            }
        } else {
            // No circuit breaker — fetch unconditionally
            for repo in &config.repos {
                if let Err(e) = fetch_issues(&client, &config, repo, &tx, &mut seen_issues, &mut rate_limit).await {
                    warn!("Initial GitHub issues fetch failed for {}: {}", repo, e);
                }
                if config.include_releases {
                    if let Err(e) = fetch_releases(&client, repo, &tx, &mut seen_releases, &mut rate_limit).await {
                        warn!("Initial GitHub releases fetch failed for {}: {}", repo, e);
                    }
                }
                if let Err(e) = fetch_commits(&client, repo, &tx, &mut seen_commits, &mut rate_limit).await {
                    warn!("Initial GitHub commits fetch failed for {}: {}", repo, e);
                }
                if config.include_review_comments {
                    if let Err(e) =
                        fetch_review_comments(&client, repo, &tx, &mut seen_review_comments, &mut rate_limit).await
                    {
                        warn!(
                            "Initial GitHub review comments fetch failed for {}: {}",
                            repo, e
                        );
                    }
                }
            }
        }

        loop {
            tokio::time::sleep(poll_interval).await;

            // Circuit breaker check
            if let Some(ref cb) = circuit_breaker {
                if cb.allow_request().await.is_err() {
                    debug!("GitHub circuit breaker open — skipping poll cycle");
                    continue;
                }
            }

            let mut any_failed = false;
            for repo in &config.repos {
                if let Err(e) = fetch_issues(&client, &config, repo, &tx, &mut seen_issues, &mut rate_limit).await {
                    warn!("GitHub issues fetch failed for {}: {}", repo, e);
                    any_failed = true;
                }
                if config.include_releases {
                    if let Err(e) = fetch_releases(&client, repo, &tx, &mut seen_releases, &mut rate_limit).await {
                        warn!("GitHub releases fetch failed for {}: {}", repo, e);
                        any_failed = true;
                    }
                }
                if let Err(e) = fetch_commits(&client, repo, &tx, &mut seen_commits, &mut rate_limit).await {
                    warn!("GitHub commits fetch failed for {}: {}", repo, e);
                    any_failed = true;
                }
                if config.include_review_comments {
                    if let Err(e) =
                        fetch_review_comments(&client, repo, &tx, &mut seen_review_comments, &mut rate_limit).await
                    {
                        warn!("GitHub review comments fetch failed for {}: {}", repo, e);
                        any_failed = true;
                    }
                }

                // Evict old seen records to prevent unbounded growth
                for seen in [
                    &mut seen_issues,
                    &mut seen_releases,
                    &mut seen_commits,
                    &mut seen_review_comments,
                ] {
                    if seen.len() > 5000 {
                        let excess = seen.len() - 2500;
                        let to_remove: Vec<String> = seen.iter().take(excess).cloned().collect();
                        for id in to_remove {
                            seen.remove(&id);
                        }
                    }
                }
            }

            // Record circuit breaker result
            if let Some(ref cb) = circuit_breaker {
                if any_failed { cb.record_failure().await; } else { cb.record_success().await; }
            }
        }
    });

    Ok(handle)
}

fn build_client(config: &GitHubConfig) -> reqwest::Client {
    let mut headers = reqwest::header::HeaderMap::new();
    headers.insert(
        reqwest::header::ACCEPT,
        "application/vnd.github.v3+json".parse().unwrap(),
    );
    headers.insert(
        reqwest::header::USER_AGENT,
        "OpenSoma-GitHub-Connector/0.1.0".parse().unwrap(),
    );

    if let Some(ref token) = config.token {
        if !token.is_empty() {
            headers.insert(
                reqwest::header::AUTHORIZATION,
                format!("Bearer {}", token).parse().unwrap(),
            );
        }
    }

    reqwest::Client::builder()
        .default_headers(headers)
        .timeout(Duration::from_secs(30))
        .build()
        .expect("Failed to build HTTP client")
}

async fn fetch_issues(
    client: &reqwest::Client,
    config: &GitHubConfig,
    repo: &str,
    tx: &EventTx,
    seen: &mut std::collections::HashSet<String>,
    rate_limit: &mut RateLimitState,
) -> Result<()> {
    let state = if config.include_closed { "all" } else { "open" };
    let per_page = config.max_items_per_fetch.min(100);
    let url = format!(
        "https://api.github.com/repos/{}/issues?state={}&per_page={}&sort=updated&direction=desc",
        repo, state, per_page
    );

    let resp = github_get(client, &url, rate_limit).await?;
    let issues: Vec<GitHubIssue> = resp.json().await.context("Failed to parse GitHub issues")?;
    let mut new_count = 0u32;

    for issue in &issues {
        let item_id = format!("{}#{}", repo, issue.number);

        if seen.contains(&item_id) {
            continue;
        }

        let is_pr = issue.pull_request.is_some();

        if is_pr && !config.include_prs {
            continue;
        }
        if !is_pr && !config.include_issues {
            continue;
        }

        let item_type = if is_pr { "pull_request" } else { "issue" };
        let labels: Vec<String> = issue.labels.iter().map(|l| l.name.clone()).collect();
        let author = issue
            .user
            .as_ref()
            .map(|u| u.login.clone())
            .unwrap_or_default();
        let body_text = issue.body.as_deref().unwrap_or("");

        let content = format!(
            "# {} #{}: {}\n\nAuthor: {}\nState: {}\nLabels: {}\nURL: {}\nCreated: {}\nUpdated: {}\n\n{}",
            item_type, issue.number, issue.title,
            author, issue.state, labels.join(", "), issue.html_url,
            issue.created_at, issue.updated_at, body_text,
        );

        let mut tags = std::collections::HashMap::new();
        tags.insert("repo".to_string(), repo.to_string());
        tags.insert("type".to_string(), item_type.to_string());
        tags.insert("author".to_string(), author);
        tags.insert("state".to_string(), issue.state.clone());
        tags.insert("url".to_string(), issue.html_url.clone());

        let event = RawEvent {
            id: uuid::Uuid::new_v4().to_string(),
            source: format!("github:{}", repo),
            event_type: format!("github.{}", item_type),
            timestamp_ms: chrono::Utc::now().timestamp_millis(),
            payload: content.into_bytes(),
            tags,
        };

        if tx.send(event).await.is_err() {
            error!("Failed to forward GitHub event — channel closed");
            break;
        }

        seen.insert(item_id);
        new_count += 1;
        debug!("Forwarded GitHub {} #{}", item_type, issue.number);
    }

    info!(
        "GitHub issues sync: {} total from {} ({} new)",
        issues.len(),
        repo,
        new_count,
    );

    Ok(())
}

async fn fetch_releases(
    client: &reqwest::Client,
    repo: &str,
    tx: &EventTx,
    seen: &mut std::collections::HashSet<String>,
    rate_limit: &mut RateLimitState,
) -> Result<()> {
    let url = format!("https://api.github.com/repos/{}/releases?per_page=10", repo);

    let resp = github_get(client, &url, rate_limit).await?;
    let releases: Vec<GitHubRelease> = resp
        .json()
        .await
        .context("Failed to parse GitHub releases")?;

    for release in &releases {
        let item_id = format!("{}:release:{}", repo, release.tag_name);

        if seen.contains(&item_id) {
            continue;
        }

        let author = release
            .author
            .as_ref()
            .map(|u| u.login.clone())
            .unwrap_or_default();
        let body_text = release.body.as_deref().unwrap_or("");
        let name = release.name.as_deref().unwrap_or(&release.tag_name);

        let content = format!(
            "# Release: {} ({})\n\nRepository: {}\nAuthor: {}\nURL: {}\nPublished: {}\n\n{}",
            name, release.tag_name, repo, author, release.html_url, release.published_at, body_text,
        );

        let mut tags = std::collections::HashMap::new();
        tags.insert("repo".to_string(), repo.to_string());
        tags.insert("type".to_string(), "release".to_string());
        tags.insert("tag".to_string(), release.tag_name.clone());
        tags.insert("author".to_string(), author);
        tags.insert("url".to_string(), release.html_url.clone());

        let event = RawEvent {
            id: uuid::Uuid::new_v4().to_string(),
            source: format!("github:{}", repo),
            event_type: "github.release".to_string(),
            timestamp_ms: chrono::Utc::now().timestamp_millis(),
            payload: content.into_bytes(),
            tags,
        };

        if tx.send(event).await.is_err() {
            error!("Failed to forward GitHub release event — channel closed");
            break;
        }

        seen.insert(item_id);
        debug!("Forwarded GitHub release {}", release.tag_name);
    }

    Ok(())
}

/// Fetch recent commits from a GitHub repository.
/// Polls the /repos/{owner}/{repo}/commits endpoint.
async fn fetch_commits(
    client: &reqwest::Client,
    repo: &str,
    tx: &EventTx,
    seen: &mut std::collections::HashSet<String>,
    rate_limit: &mut RateLimitState,
) -> Result<()> {
    let url = format!("https://api.github.com/repos/{}/commits?per_page=20", repo);

    let resp = github_get(client, &url, rate_limit).await?;
    let commits: Vec<GitHubCommit> = resp
        .json()
        .await
        .context("Failed to parse GitHub commits")?;
    let mut new_count = 0u32;

    for commit in &commits {
        let item_id = format!("{}:commit:{}", repo, commit.sha);

        if seen.contains(&item_id) {
            continue;
        }

        let author_name = commit
            .author
            .as_ref()
            .map(|u| u.login.clone())
            .unwrap_or_else(|| commit.commit.author.name.clone());

        // Truncate commit message to first line
        let first_line = commit
            .commit
            .message
            .lines()
            .next()
            .unwrap_or("")
            .to_string();

        let content = format!(
            "# Commit: {}\n\nRepository: {}\nAuthor: {} <{}>\nDate: {}\nSHA: {}\nURL: {}\n\n{}",
            first_line,
            repo,
            author_name,
            commit.commit.author.email,
            commit.commit.author.date,
            &commit.sha[..12],
            commit.html_url,
            commit.commit.message,
        );

        let mut tags = std::collections::HashMap::new();
        tags.insert("repo".to_string(), repo.to_string());
        tags.insert("type".to_string(), "commit".to_string());
        tags.insert("sha".to_string(), commit.sha.clone());
        tags.insert("author".to_string(), author_name);
        tags.insert("url".to_string(), commit.html_url.clone());

        let event = RawEvent {
            id: uuid::Uuid::new_v4().to_string(),
            source: format!("github:{}", repo),
            event_type: "github.commit".to_string(),
            timestamp_ms: chrono::Utc::now().timestamp_millis(),
            payload: content.into_bytes(),
            tags,
        };

        if tx.send(event).await.is_err() {
            error!("Failed to forward GitHub commit — channel closed");
            break;
        }

        seen.insert(item_id);
        new_count += 1;
        debug!("Forwarded GitHub commit {}", &commit.sha[..12]);
    }

    if new_count > 0 {
        info!(
            "GitHub commits sync: {} total from {} ({} new)",
            commits.len(),
            repo,
            new_count,
        );
    }

    Ok(())
}

/// Fetch recent review comments from a GitHub repository.
/// Polls the /repos/{owner}/{repo}/pulls/comments endpoint (PR review comments).
async fn fetch_review_comments(
    client: &reqwest::Client,
    repo: &str,
    tx: &EventTx,
    seen: &mut std::collections::HashSet<String>,
    rate_limit: &mut RateLimitState,
) -> Result<()> {
    let url = format!(
        "https://api.github.com/repos/{}/pulls/comments?per_page=30&sort=created&direction=desc",
        repo
    );

    let resp = github_get(client, &url, rate_limit).await?;
    let comments: Vec<GitHubReviewComment> = resp
        .json()
        .await
        .context("Failed to parse GitHub review comments")?;
    let mut new_count = 0u32;

    for comment in &comments {
        let item_id = format!("{}:review_comment:{}", repo, comment.id);

        if seen.contains(&item_id) {
            continue;
        }

        let author = comment
            .user
            .as_ref()
            .map(|u| u.login.clone())
            .unwrap_or_default();
        let line_info = comment.line.map(|l| format!(":{}", l)).unwrap_or_default();

        let content = format!(
            "# PR Review Comment on {}{}\n\nRepository: {}\nAuthor: {}\nURL: {}\nCreated: {}\nUpdated: {}\n\n{}",
            comment.path, line_info, repo, author, comment.html_url,
            comment.created_at, comment.updated_at, comment.body,
        );

        let mut tags = std::collections::HashMap::new();
        tags.insert("repo".to_string(), repo.to_string());
        tags.insert("type".to_string(), "review_comment".to_string());
        tags.insert("author".to_string(), author);
        tags.insert("file".to_string(), comment.path.clone());
        tags.insert("url".to_string(), comment.html_url.clone());
        if let Some(ref pr_url) = comment.pull_request_url {
            tags.insert("pr_url".to_string(), pr_url.clone());
        }

        let event = RawEvent {
            id: uuid::Uuid::new_v4().to_string(),
            source: format!("github:{}", repo),
            event_type: "github.review_comment".to_string(),
            timestamp_ms: chrono::Utc::now().timestamp_millis(),
            payload: content.into_bytes(),
            tags,
        };

        if tx.send(event).await.is_err() {
            error!("Failed to forward GitHub review comment — channel closed");
            break;
        }

        seen.insert(item_id);
        new_count += 1;
        debug!(
            "Forwarded GitHub review comment {} on {}",
            comment.id, comment.path
        );
    }

    if new_count > 0 {
        info!(
            "GitHub review comments sync: {} total from {} ({} new)",
            comments.len(),
            repo,
            new_count,
        );
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::GitHubConfig;

    fn default_config() -> GitHubConfig {
        GitHubConfig {
            enabled: true,
            token: None,
            repos: vec!["owner/repo".to_string()],
            poll_interval_secs: 300,
            include_issues: true,
            include_prs: true,
            include_releases: true,
            include_closed: false,
            include_review_comments: false,
            max_items_per_fetch: 30,
        }
    }

    #[test]
    fn test_build_client_no_token() {
        let config = default_config();
        // Should not panic — verifies the builder succeeds
        let _client = build_client(&config);
    }

    #[test]
    fn test_build_client_with_token() {
        let mut config = default_config();
        config.token = Some("ghp_test123".to_string());
        // Should not panic — verifies token header parse succeeds
        let _client = build_client(&config);
    }

    #[test]
    fn test_build_client_empty_token() {
        let mut config = default_config();
        config.token = Some("".to_string());
        // Empty token should still build fine (token is skipped in build_client)
        let _client = build_client(&config);
    }

    #[test]
    fn test_github_issue_deserialization() {
        let json = serde_json::json!({
            "number": 42,
            "title": "Test Issue",
            "body": "Issue body text",
            "state": "open",
            "html_url": "https://github.com/owner/repo/issues/42",
            "user": {"login": "testuser"},
            "labels": [{"name": "bug"}, {"name": "priority:high"}],
            "created_at": "2026-01-01T00:00:00Z",
            "updated_at": "2026-01-02T00:00:00Z",
            "pull_request": null
        });

        let issue: GitHubIssue = serde_json::from_value(json).unwrap();
        assert_eq!(issue.number, 42);
        assert_eq!(issue.title, "Test Issue");
        assert_eq!(issue.body.unwrap(), "Issue body text");
        assert_eq!(issue.state, "open");
        assert_eq!(issue.user.unwrap().login, "testuser");
        assert_eq!(issue.labels.len(), 2);
        assert_eq!(issue.labels[0].name, "bug");
        assert!(issue.pull_request.is_none());
    }

    #[test]
    fn test_github_pr_deserialization() {
        let json = serde_json::json!({
            "number": 10,
            "title": "Fix bug",
            "body": null,
            "state": "open",
            "html_url": "https://github.com/owner/repo/pull/10",
            "user": {"login": "dev"},
            "labels": [],
            "created_at": "2026-01-01T00:00:00Z",
            "updated_at": "2026-01-01T00:00:00Z",
            "pull_request": {"url": "https://api.github.com/repos/owner/repo/pulls/10"}
        });

        let issue: GitHubIssue = serde_json::from_value(json).unwrap();
        assert!(issue.pull_request.is_some());
        assert_eq!(issue.number, 10);
        assert!(issue.body.is_none());
    }

    #[test]
    fn test_github_release_deserialization() {
        let json = serde_json::json!({
            "tag_name": "v1.0.0",
            "name": "Version 1.0.0",
            "body": "Release notes here",
            "html_url": "https://github.com/owner/repo/releases/tag/v1.0.0",
            "published_at": "2026-01-15T12:00:00Z",
            "author": {"login": "maintainer"}
        });

        let release: GitHubRelease = serde_json::from_value(json).unwrap();
        assert_eq!(release.tag_name, "v1.0.0");
        assert_eq!(release.name.unwrap(), "Version 1.0.0");
        assert_eq!(release.body.unwrap(), "Release notes here");
        assert_eq!(release.author.unwrap().login, "maintainer");
    }

    #[test]
    fn test_github_release_minimal() {
        let json = serde_json::json!({
            "tag_name": "v0.1.0",
            "name": null,
            "body": null,
            "html_url": "https://github.com/owner/repo/releases/tag/v0.1.0",
            "published_at": "2026-01-01T00:00:00Z",
            "author": null
        });

        let release: GitHubRelease = serde_json::from_value(json).unwrap();
        assert_eq!(release.tag_name, "v0.1.0");
        assert!(release.name.is_none());
        assert!(release.body.is_none());
        assert!(release.author.is_none());
    }

    #[test]
    fn test_connector_name() {
        let config = default_config();
        let connector = GitHubConnector::new(config);
        assert_eq!(connector.name(), "github");
    }

    #[test]
    fn test_issue_content_format() {
        // Verify the content string format for issues
        let json = serde_json::json!({
            "number": 1,
            "title": "First issue",
            "body": "Hello world",
            "state": "open",
            "html_url": "https://github.com/o/r/issues/1",
            "user": {"login": "alice"},
            "labels": [{"name": "bug"}],
            "created_at": "2026-01-01T00:00:00Z",
            "updated_at": "2026-01-02T00:00:00Z",
            "pull_request": null
        });

        let issue: GitHubIssue = serde_json::from_value(json).unwrap();
        let is_pr = issue.pull_request.is_some();
        let item_type = if is_pr { "pull_request" } else { "issue" };
        let labels: Vec<String> = issue.labels.iter().map(|l| l.name.clone()).collect();
        let author = issue
            .user
            .as_ref()
            .map(|u| u.login.clone())
            .unwrap_or_default();
        let body_text = issue.body.as_deref().unwrap_or("");

        let content = format!(
            "# {} #{}: {}\n\nAuthor: {}\nState: {}\nLabels: {}\nURL: {}\nCreated: {}\nUpdated: {}\n\n{}",
            item_type, issue.number, issue.title,
            author, issue.state, labels.join(", "), issue.html_url,
            issue.created_at, issue.updated_at, body_text,
        );

        assert!(content.contains("# issue #1: First issue"));
        assert!(content.contains("Author: alice"));
        assert!(content.contains("Labels: bug"));
        assert!(content.contains("Hello world"));
    }

    #[test]
    fn test_issue_filtering_pr_only() {
        // When include_prs=false and include_issues=true, PRs should be skipped
        let json = serde_json::json!({
            "number": 5,
            "title": "A PR",
            "body": null,
            "state": "open",
            "html_url": "https://github.com/o/r/pull/5",
            "user": {"login": "dev"},
            "labels": [],
            "created_at": "2026-01-01T00:00:00Z",
            "updated_at": "2026-01-01T00:00:00Z",
            "pull_request": {"url": "https://api.github.com/repos/o/r/pulls/5"}
        });

        let issue: GitHubIssue = serde_json::from_value(json).unwrap();
        let is_pr = issue.pull_request.is_some();

        // With include_prs=false, this should be skipped
        let include_prs = false;
        let include_issues = true;
        let should_skip = (is_pr && !include_prs) || (!is_pr && !include_issues);
        assert!(should_skip);
    }

    #[test]
    fn test_github_commit_deserialization() {
        let json = serde_json::json!({
            "sha": "abc123def456789",
            "commit": {
                "message": "feat: add new feature\n\nDetailed description here",
                "author": {
                    "name": "Test Author",
                    "email": "test@example.com",
                    "date": "2026-08-19T10:30:00Z"
                }
            },
            "html_url": "https://github.com/owner/repo/commit/abc123def456789",
            "author": {"login": "testuser"}
        });

        let commit: GitHubCommit = serde_json::from_value(json).unwrap();
        assert_eq!(commit.sha, "abc123def456789");
        assert_eq!(
            commit.commit.message,
            "feat: add new feature\n\nDetailed description here"
        );
        assert_eq!(commit.commit.author.name, "Test Author");
        assert_eq!(commit.commit.author.email, "test@example.com");
        assert_eq!(commit.author.unwrap().login, "testuser");

        // Verify first line extraction
        let first_line = commit.commit.message.lines().next().unwrap();
        assert_eq!(first_line, "feat: add new feature");
    }

    #[test]
    fn test_github_commit_minimal() {
        let json = serde_json::json!({
            "sha": "deadbeef1234",
            "commit": {
                "message": "fix: bug fix",
                "author": {
                    "name": "Anonymous",
                    "email": "anon@example.com",
                    "date": "2026-01-01T00:00:00Z"
                }
            },
            "html_url": "https://github.com/o/r/commit/deadbeef1234",
            "author": null
        });

        let commit: GitHubCommit = serde_json::from_value(json).unwrap();
        assert_eq!(commit.sha, "deadbeef1234");
        assert!(commit.author.is_none());
        assert_eq!(commit.commit.author.name, "Anonymous");
    }

    #[test]
    fn test_github_review_comment_deserialization() {
        let json = serde_json::json!({
            "id": 101,
            "body": "This looks good, but consider adding error handling here.",
            "path": "src/main.rs",
            "line": 42,
            "html_url": "https://github.com/owner/repo/pull/5#discussion_r101",
            "user": {"login": "reviewer1"},
            "created_at": "2026-08-20T10:00:00Z",
            "updated_at": "2026-08-20T11:00:00Z",
            "pull_request_url": "https://api.github.com/repos/owner/repo/pulls/5"
        });

        let comment: GitHubReviewComment = serde_json::from_value(json).unwrap();
        assert_eq!(comment.id, 101);
        assert_eq!(comment.path, "src/main.rs");
        assert_eq!(comment.line, Some(42));
        assert_eq!(comment.user.unwrap().login, "reviewer1");
        assert!(comment.body.contains("error handling"));
        assert_eq!(
            comment.pull_request_url.unwrap(),
            "https://api.github.com/repos/owner/repo/pulls/5"
        );
    }

    #[test]
    fn test_github_review_comment_minimal() {
        let json = serde_json::json!({
            "id": 202,
            "body": "LGTM",
            "path": "lib.rs",
            "html_url": "https://github.com/o/r/pull/1#discussion_r202",
            "created_at": "2026-08-21T00:00:00Z",
            "updated_at": "2026-08-21T00:00:00Z"
        });

        let comment: GitHubReviewComment = serde_json::from_value(json).unwrap();
        assert_eq!(comment.id, 202);
        assert!(comment.line.is_none());
        assert!(comment.user.is_none());
        assert!(comment.pull_request_url.is_none());
    }

    #[test]
    fn test_review_comment_event_tags_format() {
        let json = serde_json::json!({
            "id": 303,
            "body": "Nice refactor!",
            "path": "src/connector/github.rs",
            "line": 100,
            "html_url": "https://github.com/o/r/pull/3#discussion_r303",
            "user": {"login": "alice"},
            "created_at": "2026-08-21T12:00:00Z",
            "updated_at": "2026-08-21T12:30:00Z",
            "pull_request_url": "https://api.github.com/repos/o/r/pulls/3"
        });

        let comment: GitHubReviewComment = serde_json::from_value(json).unwrap();
        let mut tags = std::collections::HashMap::new();
        tags.insert("repo".to_string(), "o/r".to_string());
        tags.insert("type".to_string(), "review_comment".to_string());
        tags.insert(
            "author".to_string(),
            comment.user.as_ref().unwrap().login.clone(),
        );
        tags.insert("file".to_string(), comment.path.clone());
        tags.insert("url".to_string(), comment.html_url.clone());

        assert_eq!(tags["type"], "review_comment");
        assert_eq!(tags["author"], "alice");
        assert_eq!(tags["file"], "src/connector/github.rs");
    }

    #[test]
    fn test_review_comment_content_format() {
        let json = serde_json::json!({
            "id": 404,
            "body": "Consider using a match here",
            "path": "src/lib.rs",
            "line": 55,
            "html_url": "https://github.com/o/r/pull/7#discussion_r404",
            "user": {"login": "bob"},
            "created_at": "2026-08-21T15:00:00Z",
            "updated_at": "2026-08-21T15:00:00Z",
            "pull_request_url": "https://api.github.com/repos/o/r/pulls/7"
        });

        let comment: GitHubReviewComment = serde_json::from_value(json).unwrap();
        let line_info = comment.line.map(|l| format!(":{}", l)).unwrap_or_default();
        let content = format!(
            "# PR Review Comment on {}{}\n\nRepository: {}\nAuthor: {}\nURL: {}\nCreated: {}\nUpdated: {}\n\n{}",
            comment.path, line_info, "o/r",
            comment.user.as_ref().map(|u| u.login.clone()).unwrap_or_default(),
            comment.html_url, comment.created_at, comment.updated_at, comment.body,
        );

        assert!(content.contains("PR Review Comment on src/lib.rs:55"));
        assert!(content.contains("Author: bob"));
        assert!(content.contains("Consider using a match"));
    }

    #[test]
    fn test_review_comment_dedup_id_format() {
        let repo = "owner/repo";
        let comment_id = 12345u64;
        let item_id = format!("{}:review_comment:{}", repo, comment_id);
        assert_eq!(item_id, "owner/repo:review_comment:12345");
    }

    #[test]
    fn test_github_config_review_comments_default_false() {
        let toml = r#"
            enabled = true
            repos = ["test/repo"]
        "#;
        let config: GitHubConfig = toml::from_str(toml).unwrap();
        assert!(!config.include_review_comments);
    }

    #[test]
    fn test_github_config_review_comments_enabled() {
        let toml = r#"
            enabled = true
            repos = ["test/repo"]
            include_review_comments = true
        "#;
        let config: GitHubConfig = toml::from_str(toml).unwrap();
        assert!(config.include_review_comments);
    }

    #[test]
    fn test_rate_limit_state_new() {
        let rl = RateLimitState::new();
        assert_eq!(rl.remaining, u32::MAX);
        assert_eq!(rl.reset_at, 0);
    }

    #[test]
    fn test_rate_limit_state_update_from_headers() {
        let mut rl = RateLimitState::new();
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert("x-ratelimit-remaining", "4500".parse().unwrap());
        headers.insert("x-ratelimit-reset", "1700000000".parse().unwrap());

        rl.update_from_headers(&headers);
        assert_eq!(rl.remaining, 4500);
        assert_eq!(rl.reset_at, 1700000000);
    }

    #[test]
    fn test_rate_limit_state_update_ignores_invalid_headers() {
        let mut rl = RateLimitState::new();
        rl.remaining = 100;
        rl.reset_at = 999;

        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert("x-ratelimit-remaining", "not-a-number".parse().unwrap());
        headers.insert("x-ratelimit-reset", "also-bad".parse().unwrap());

        rl.update_from_headers(&headers);
        // Should keep old values since new ones are invalid
        assert_eq!(rl.remaining, 100);
        assert_eq!(rl.reset_at, 999);
    }

    #[test]
    fn test_rate_limit_state_update_partial_headers() {
        let mut rl = RateLimitState::new();

        // Only remaining header, no reset
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert("x-ratelimit-remaining", "10".parse().unwrap());

        rl.update_from_headers(&headers);
        assert_eq!(rl.remaining, 10);
        assert_eq!(rl.reset_at, 0); // unchanged
    }
}
