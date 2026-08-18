use anyhow::Result;
use reqwest::Client;
use tokio::task::JoinHandle;
use tokio::time::Duration;
use tracing::{debug, error, info, warn};

use crate::collector::{EventTx, RawEvent};
use crate::config::RssConfig;

/// Start the RSS connector. Polls configured RSS/Atom feeds at regular intervals
/// and forwards new entries into the collector pipeline.
pub async fn start(config: RssConfig, tx: EventTx) -> Result<JoinHandle<()>> {
    let http_client = Client::builder()
        .timeout(Duration::from_secs(30))
        .user_agent("OpenSoma/0.1 RSS Connector")
        .build()?;

    let poll_secs = config.poll_interval_secs;
    let feeds = config.feeds.clone();

    info!(
        "RSS connector starting — {} feeds, poll every {}s",
        feeds.len(),
        poll_secs
    );

    let handle = tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(poll_secs));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        // Track seen entry GUIDs/links to avoid duplicates across polls
        let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();

        // Initial poll immediately
        for feed in &feeds {
            if let Err(e) = poll_feed(&http_client, feed, &tx, &mut seen).await {
                error!("RSS initial poll failed for '{}': {}", feed.name, e);
            }
        }

        loop {
            interval.tick().await;
            for feed in &feeds {
                if let Err(e) = poll_feed(&http_client, feed, &tx, &mut seen).await {
                    warn!("RSS poll failed for '{}': {}", feed.name, e);
                }
            }
        }
    });

    Ok(handle)
}

/// Poll a single RSS feed and forward new entries as RawEvents.
async fn poll_feed(
    client: &Client,
    feed: &crate::config::RssFeedConfig,
    tx: &EventTx,
    seen: &mut std::collections::HashSet<String>,
) -> Result<()> {
    debug!("Polling RSS feed: {} ({})", feed.name, feed.url);

    let response = client.get(&feed.url).send().await?;
    if !response.status().is_success() {
        anyhow::bail!("HTTP {} for feed '{}'", response.status(), feed.name);
    }

    let body = response.text().await?;
    let entries = parse_rss_entries(&body);

    let mut new_count = 0u32;
    for entry in entries {
        // Use guid or link as dedup key
        let dedup_key = entry.guid.clone().unwrap_or_else(|| {
            entry
                .link
                .clone()
                .unwrap_or_else(|| format!("{}:{}", feed.name, entry.title))
        });

        if seen.contains(&dedup_key) {
            continue;
        }
        seen.insert(dedup_key.clone());

        // Build payload as JSON
        let payload_json = serde_json::json!({
            "feed_name": feed.name,
            "feed_url": feed.url,
            "title": entry.title,
            "link": entry.link,
            "description": entry.description,
            "pub_date": entry.pub_date,
            "guid": entry.guid,
        });

        let event = RawEvent {
            id: uuid::Uuid::new_v4().to_string(),
            source: format!("connector:rss:{}", feed.name),
            event_type: "rss_entry".to_string(),
            timestamp_ms: chrono::Utc::now().timestamp_millis(),
            payload: payload_json.to_string().into_bytes(),
            tags: {
                let mut tags = std::collections::HashMap::new();
                tags.insert("connector".to_string(), "rss".to_string());
                tags.insert("feed".to_string(), feed.name.clone());
                tags
            },
        };

        if tx.send(event).await.is_err() {
            error!("RSS collector channel closed");
            break;
        }
        new_count += 1;
    }

    if new_count > 0 {
        info!("RSS feed '{}': {} new entries", feed.name, new_count);
    } else {
        debug!("RSS feed '{}': no new entries", feed.name);
    }

    Ok(())
}

/// Minimal RSS/Atom entry parsed from XML.
struct RssEntry {
    title: String,
    link: Option<String>,
    description: Option<String>,
    pub_date: Option<String>,
    guid: Option<String>,
}

/// Parse RSS 2.0 or Atom XML into entries. Uses simple string parsing to avoid
/// heavy XML dependencies — RSS/Atom structures are regular enough for this.
fn parse_rss_entries(xml: &str) -> Vec<RssEntry> {
    let mut entries = Vec::new();

    // Try RSS 2.0 <item> elements first
    let items: Vec<&str> = xml.split("<item").skip(1).collect();
    if !items.is_empty() {
        for item_xml in items {
            let end = item_xml.find("</item>").unwrap_or(item_xml.len());
            let fragment = &item_xml[..end];
            entries.push(RssEntry {
                title: extract_tag(fragment, "title").unwrap_or_else(|| "Untitled".to_string()),
                link: extract_tag(fragment, "link"),
                description: extract_tag(fragment, "description"),
                pub_date: extract_tag(fragment, "pubDate"),
                guid: extract_tag(fragment, "guid"),
            });
        }
        return entries;
    }

    // Try Atom <entry> elements
    let atom_entries: Vec<&str> = xml.split("<entry").skip(1).collect();
    for entry_xml in atom_entries {
        let end = entry_xml.find("</entry>").unwrap_or(entry_xml.len());
        let fragment = &entry_xml[..end];
        // Atom uses <link href="..."/> instead of <link>...</link>
        let link = extract_attr(fragment, "link", "href").or_else(|| extract_tag(fragment, "link"));
        entries.push(RssEntry {
            title: extract_tag(fragment, "title").unwrap_or_else(|| "Untitled".to_string()),
            link,
            description: extract_tag(fragment, "summary")
                .or_else(|| extract_tag(fragment, "content")),
            pub_date: extract_tag(fragment, "published")
                .or_else(|| extract_tag(fragment, "updated")),
            guid: extract_tag(fragment, "id"),
        });
    }

    entries
}

/// Extract text content between `<tag>` and `</tag>`.
fn extract_tag(xml: &str, tag: &str) -> Option<String> {
    let open = format!("<{}", tag);
    let start_idx = xml.find(&open)?;
    // Skip to end of opening tag (handle attributes)
    let after_open = &xml[start_idx..];
    let tag_end = after_open.find('>')? + 1;
    let content_start = start_idx + tag_end;

    // Check for self-closing tag
    if after_open[..tag_end].contains("/>") {
        return None;
    }

    let close = format!("</{}>", tag);
    let content_end = xml[content_start..].find(&close)? + content_start;
    let raw = &xml[content_start..content_end];
    Some(decode_html_entities(raw.trim()))
}

/// Extract an attribute value from a tag, e.g. `<link href="..."/>`.
fn extract_attr(xml: &str, tag: &str, attr: &str) -> Option<String> {
    let open = format!("<{}", tag);
    let start_idx = xml.find(&open)?;
    let after_open = &xml[start_idx..];
    let tag_end = after_open.find('>')?;
    let tag_content = &after_open[..tag_end];

    let attr_eq = format!("{}=", attr);
    let attr_idx = tag_content.find(&attr_eq)?;
    let after_attr = tag_content[attr_idx + attr_eq.len()..].trim_start();

    // Handle both "value" and 'value'
    let quote = after_attr.as_bytes().first()?;
    if *quote != b'"' && *quote != b'\'' {
        return None;
    }
    let end_quote = after_attr[1..].find(*quote as char)?;
    Some(after_attr[1..1 + end_quote].to_string())
}

/// Decode common HTML entities.
fn decode_html_entities(s: &str) -> String {
    s.replace("&amp;", "&")
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", "\"")
        .replace("&apos;", "'")
        .replace("&#39;", "'")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_rss_2() {
        let xml = r#"<?xml version="1.0"?>
        <rss version="2.0"><channel>
            <title>Test Feed</title>
            <item>
                <title>Hello World</title>
                <link>https://example.com/1</link>
                <description>First post</description>
                <pubDate>Sat, 15 Aug 2026 00:00:00 GMT</pubDate>
                <guid>post-1</guid>
            </item>
            <item>
                <title>Second Post</title>
                <link>https://example.com/2</link>
            </item>
        </channel></rss>"#;

        let entries = parse_rss_entries(xml);
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].title, "Hello World");
        assert_eq!(entries[0].link.as_deref(), Some("https://example.com/1"));
        assert_eq!(entries[0].guid.as_deref(), Some("post-1"));
        assert_eq!(entries[1].title, "Second Post");
    }

    #[test]
    fn test_parse_atom() {
        let xml = r#"<?xml version="1.0"?>
        <feed xmlns="http://www.w3.org/2005/Atom">
            <title>Atom Feed</title>
            <entry>
                <title>Atom Entry</title>
                <link href="https://example.com/atom1"/>
                <id>atom-1</id>
                <summary>Summary text</summary>
                <updated>2026-08-15T00:00:00Z</updated>
            </entry>
        </feed>"#;

        let entries = parse_rss_entries(xml);
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].title, "Atom Entry");
        assert_eq!(
            entries[0].link.as_deref(),
            Some("https://example.com/atom1")
        );
        assert_eq!(entries[0].guid.as_deref(), Some("atom-1"));
    }

    #[test]
    fn test_decode_entities() {
        assert_eq!(decode_html_entities("a &amp; b"), "a & b");
        assert_eq!(decode_html_entities("&lt;html&gt;"), "<html>");
    }
}
