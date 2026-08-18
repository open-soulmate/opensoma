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

/// Start the GitHub connector. Polls the GitHub API for issues, PRs, and releases.
pub async fn start(config: GitHubConfig, tx: EventTx) -> Result<JoinHandle<()>> {
    let handle = tokio::spawn(async move {
        let poll_interval = Duration::from_secs(config.poll_interval_secs);
        let client = build_client(&config);

        info!(
            "GitHub connector started — repos={}, polling every {}s",
            config.repos.join(", "),
            config.poll_interval_secs
        );

        let mut seen_issues: std::collections::HashSet<String> = std::collections::HashSet::new();
        let mut seen_releases: std::collections::HashSet<String> = std::collections::HashSet::new();

        // Initial fetch
        for repo in &config.repos {
            if let Err(e) = fetch_issues(&client, &config, repo, &tx, &mut seen_issues).await {
                warn!("Initial GitHub issues fetch failed for {}: {}", repo, e);
            }
            if config.include_releases {
                if let Err(e) = fetch_releases(&client, repo, &tx, &mut seen_releases).await {
                    warn!("Initial GitHub releases fetch failed for {}: {}", repo, e);
                }
            }
        }

        loop {
            tokio::time::sleep(poll_interval).await;

            for repo in &config.repos {
                if let Err(e) = fetch_issues(&client, &config, repo, &tx, &mut seen_issues).await {
                    warn!("GitHub issues fetch failed for {}: {}", repo, e);
                }
                if config.include_releases {
                    if let Err(e) = fetch_releases(&client, repo, &tx, &mut seen_releases).await {
                        warn!("GitHub releases fetch failed for {}: {}", repo, e);
                    }
                }
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
) -> Result<()> {
    let state = if config.include_closed { "all" } else { "open" };
    let per_page = config.max_items_per_fetch.min(100);
    let url = format!(
        "https://api.github.com/repos/{}/issues?state={}&per_page={}&sort=updated&direction=desc",
        repo, state, per_page
    );

    let resp = client
        .get(&url)
        .send()
        .await
        .context("Failed to fetch GitHub issues")?;

    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        anyhow::bail!("GitHub API error {}: {}", status, body);
    }

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
) -> Result<()> {
    let url = format!("https://api.github.com/repos/{}/releases?per_page=10", repo);

    let resp = client
        .get(&url)
        .send()
        .await
        .context("Failed to fetch GitHub releases")?;

    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        anyhow::bail!("GitHub releases API error {}: {}", status, body);
    }

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
