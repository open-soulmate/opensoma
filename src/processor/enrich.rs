use serde::{Deserialize, Serialize};

use crate::collector::RawEvent;

/// Enrichment data extracted from an event.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Enrichment {
    /// Extracted entities (URLs, emails, IPs, file paths, etc.)
    pub entities: Vec<Entity>,
    /// Keywords extracted from content.
    pub keywords: Vec<String>,
    /// Brief summary of the content (first N chars or first sentence).
    pub summary: String,
    /// Detected language (if text content).
    pub language: Option<String>,
    /// Word count of the payload.
    pub word_count: usize,
}

/// An extracted entity with its type.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Entity {
    pub entity_type: EntityType,
    pub value: String,
    /// Position in the original text (start offset).
    pub offset: Option<usize>,
}

/// Entity types that can be extracted.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum EntityType {
    Url,
    Email,
    IpAddress,
    FilePath,
    PhoneNumber,
    DateTime,
    Hash, // MD5/SHA hashes
    Port,
    Domain,
    MacAddress,
    Uuid,
    AwsArn,
    CreditCard,
    Mention,    // @username (Telegram, Slack, etc.)
    Hashtag,    // #topic (Telegram, Slack, Twitter)
    BotCommand, // /command (Telegram bot commands)
}

/// Enrich a raw event with extracted entities, keywords, and summary.
pub fn enrich_event(event: &RawEvent) -> Enrichment {
    let payload_str = String::from_utf8_lossy(&event.payload);

    let entities = extract_entities(&payload_str);
    let keywords = extract_keywords(&payload_str);
    let summary = generate_summary(&payload_str, 200);
    let language = detect_language(&payload_str);
    let word_count = count_words(&payload_str);

    Enrichment {
        entities,
        keywords,
        summary,
        language,
        word_count,
    }
}

/// Apply enrichment data to event tags.
pub fn apply_enrichment(event: &mut RawEvent, enrichment: &Enrichment) {
    // Add entity tags
    for entity in &enrichment.entities {
        let key = format!("entity_{:?}", entity.entity_type).to_lowercase();
        event
            .tags
            .entry(key)
            .or_insert_with(|| entity.value.clone());
    }

    // Add keywords
    if !enrichment.keywords.is_empty() {
        event
            .tags
            .insert("keywords".to_string(), enrichment.keywords.join(","));
    }

    // Add summary (truncate to 500 chars for tags)
    if !enrichment.summary.is_empty() {
        let truncated: String = enrichment.summary.chars().take(500).collect();
        event.tags.insert("summary".to_string(), truncated);
    }

    // Add language
    if let Some(ref lang) = enrichment.language {
        event.tags.insert("language".to_string(), lang.clone());
    }

    event
        .tags
        .insert("word_count".to_string(), enrichment.word_count.to_string());
}

/// Extract entities from text content using regex patterns.
fn extract_entities(text: &str) -> Vec<Entity> {
    let mut entities = Vec::new();

    // URLs
    for m in regex_find(text, r"https?://[^\s<>\])}]+") {
        entities.push(Entity {
            entity_type: EntityType::Url,
            value: m.0.to_string(),
            offset: Some(m.1),
        });
    }

    // Email addresses
    for m in regex_find(text, r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}") {
        entities.push(Entity {
            entity_type: EntityType::Email,
            value: m.0.to_string(),
            offset: Some(m.1),
        });
    }

    // IPv4 addresses
    for m in regex_find(text, r"\b(?:\d{1,3}\.){3}\d{1,3}\b") {
        // Basic validation — each octet should be 0-255
        let parts: Vec<&str> = m.0.split('.').collect();
        if parts
            .iter()
            .all(|p| p.parse::<u16>().map(|n| n <= 255).unwrap_or(false))
        {
            entities.push(Entity {
                entity_type: EntityType::IpAddress,
                value: m.0.to_string(),
                offset: Some(m.1),
            });
        }
    }

    // File paths (Unix-style)
    for m in regex_find(text, r"(?:/[\w.-]+){2,}") {
        entities.push(Entity {
            entity_type: EntityType::FilePath,
            value: m.0.to_string(),
            offset: Some(m.1),
        });
    }

    // SHA-256 / MD5 hashes
    for m in regex_find(text, r"\b[a-fA-F0-9]{32}\b|\b[a-fA-F0-9]{64}\b") {
        entities.push(Entity {
            entity_type: EntityType::Hash,
            value: m.0.to_string(),
            offset: Some(m.1),
        });
    }

    // Ports (host:port pattern like localhost:8080 or 192.168.1.1:443)
    for m in regex_find(
        text,
        r"(?:localhost|\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}):(\d{1,5})\b",
    ) {
        if let Some(port_str) = m.0.rsplit(':').next() {
            if let Ok(port) = port_str.parse::<u16>() {
                if port > 0 {
                    entities.push(Entity {
                        entity_type: EntityType::Port,
                        value: port_str.to_string(),
                        offset: Some(m.1 + m.0.len() - port_str.len()),
                    });
                }
            }
        }
    }

    // Phone numbers (international and local formats)
    for m in regex_find(
        text,
        r"\+?\d{1,3}[-.\s]?\(?\d{1,4}\)?[-.\s]?\d{1,4}[-.\s]?\d{1,9}",
    ) {
        let digits: String = m.0.chars().filter(|c| c.is_ascii_digit()).collect();
        // Must have at least 7 digits to be a plausible phone number
        if digits.len() >= 7 && digits.len() <= 15 {
            // Avoid colliding with IP addresses (already extracted)
            let is_ip = entities
                .iter()
                .any(|e| e.entity_type == EntityType::IpAddress && e.value == m.0.trim());
            if !is_ip {
                entities.push(Entity {
                    entity_type: EntityType::PhoneNumber,
                    value: m.0.to_string(),
                    offset: Some(m.1),
                });
            }
        }
    }

    // DateTime patterns (ISO 8601 and common formats)
    for m in regex_find(
        text,
        r"\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}(?::\d{2})?(?:\.\d+)?(?:Z|[+-]\d{2}:?\d{2})?",
    ) {
        entities.push(Entity {
            entity_type: EntityType::DateTime,
            value: m.0.to_string(),
            offset: Some(m.1),
        });
    }
    // Also match date-only and time-only patterns
    for m in regex_find(text, r"\b\d{4}/\d{2}/\d{2}\b|\b\d{2}:\d{2}:\d{2}\b") {
        // Skip if already captured by ISO pattern
        let already = entities
            .iter()
            .any(|e| e.entity_type == EntityType::DateTime && e.value.contains(m.0));
        if !already {
            entities.push(Entity {
                entity_type: EntityType::DateTime,
                value: m.0.to_string(),
                offset: Some(m.1),
            });
        }
    }

    // Domains (simple pattern)
    for m in regex_find(
        text,
        r"\b[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?\.(?:com|org|net|io|dev|cn|ru|uk|de|fr)\b",
    ) {
        // Skip if it's already captured as part of a URL or email
        let is_sub = entities.iter().any(|e| e.value.contains(m.0));
        if !is_sub {
            entities.push(Entity {
                entity_type: EntityType::Domain,
                value: m.0.to_string(),
                offset: Some(m.1),
            });
        }
    }

    // MAC addresses (aa:bb:cc:dd:ee:ff or aa-bb-cc-dd-ee-ff)
    for m in regex_find(text, r"(?i)\b(?:[0-9a-f]{2}[:\-]){5}[0-9a-f]{2}\b") {
        entities.push(Entity {
            entity_type: EntityType::MacAddress,
            value: m.0.to_string(),
            offset: Some(m.1),
        });
    }

    // UUIDs (v1-v5)
    for m in regex_find(
        text,
        r"(?i)\b[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\b",
    ) {
        entities.push(Entity {
            entity_type: EntityType::Uuid,
            value: m.0.to_string(),
            offset: Some(m.1),
        });
    }

    // AWS ARNs (supports both full ARNs and S3-style without account ID)
    for m in regex_find(
        text,
        r"arn:aws:[a-z0-9-]+:[a-z0-9-]*:(?:\d{12})?:?[a-zA-Z0-9/_.:-]+",
    ) {
        entities.push(Entity {
            entity_type: EntityType::AwsArn,
            value: m.0.to_string(),
            offset: Some(m.1),
        });
    }

    // Credit card numbers (13-19 digits, common patterns)
    for m in regex_find(
        text,
        r"\b(?:4\d{3}[\s-]?\d{4}[\s-]?\d{4}[\s-]?\d{4}|5[1-5]\d{2}[\s-]?\d{4}[\s-]?\d{4}[\s-]?\d{4}|3[47]\d{2}[\s-]?\d{6}[\s-]?\d{5})\b",
    ) {
        // Luhn check for basic validation
        let digits: String = m.0.chars().filter(|c| c.is_ascii_digit()).collect();
        if luhn_check(&digits) {
            entities.push(Entity {
                entity_type: EntityType::CreditCard,
                value: m.0.to_string(),
                offset: Some(m.1),
            });
        }
    }

    // @mentions (Telegram, Slack, Discord, Twitter — @username with alphanumeric/underscore)
    // Filter out email-like patterns (preceded by word char)
    for m in regex_find(text, r"@[a-zA-Z_][a-zA-Z0-9_]{0,63}\b") {
        // Skip if preceded by a word character (likely part of email)
        if m.1 > 0 {
            let prev_byte = text.as_bytes()[m.1 - 1];
            if prev_byte.is_ascii_alphanumeric() || prev_byte == b'.' {
                continue;
            }
        }
        entities.push(Entity {
            entity_type: EntityType::Mention,
            value: m.0.to_string(),
            offset: Some(m.1),
        });
    }

    // #hashtags (word-boundary delimited, supports Unicode letters)
    for m in regex_find(text, r"#[\p{L}\p{N}_]{1,128}\b") {
        // Skip if preceded by a word character
        if m.1 > 0 {
            let prev_byte = text.as_bytes()[m.1 - 1];
            if prev_byte.is_ascii_alphanumeric() || prev_byte == b'_' {
                continue;
            }
        }
        entities.push(Entity {
            entity_type: EntityType::Hashtag,
            value: m.0.to_string(),
            offset: Some(m.1),
        });
    }

    // /bot_commands (Telegram-style: /command or /command@botname)
    for m in regex_find(
        text,
        r"/[a-zA-Z_][a-zA-Z0-9_]{0,63}(?:@[a-zA-Z_][a-zA-Z0-9_]{0,63})?\b",
    ) {
        // Skip if preceded by a non-whitespace character (likely URL path)
        if m.1 > 0 {
            let prev_byte = text.as_bytes()[m.1 - 1];
            if !prev_byte.is_ascii_whitespace() {
                continue;
            }
        }
        entities.push(Entity {
            entity_type: EntityType::BotCommand,
            value: m.0.to_string(),
            offset: Some(m.1),
        });
    }

    entities
}

/// Luhn algorithm for credit card validation.
fn luhn_check(number: &str) -> bool {
    if number.len() < 13 || number.len() > 19 {
        return false;
    }
    let mut sum = 0;
    let mut alternate = false;
    for ch in number.chars().rev() {
        if let Some(digit) = ch.to_digit(10) {
            let mut n = digit;
            if alternate {
                n *= 2;
                if n > 9 {
                    n -= 9;
                }
            }
            sum += n;
            alternate = !alternate;
        } else {
            return false;
        }
    }
    sum % 10 == 0
}

/// Simple regex finder that returns (match_text, offset) pairs.
fn regex_find<'a>(text: &'a str, pattern: &str) -> Vec<(&'a str, usize)> {
    let re = match regex::Regex::new(pattern) {
        Ok(r) => r,
        Err(_) => return Vec::new(),
    };
    re.find_iter(text)
        .map(|m| (&text[m.start()..m.end()], m.start()))
        .collect()
}

/// Extract keywords from text by finding frequent, meaningful words.
fn extract_keywords(text: &str) -> Vec<String> {
    use std::collections::HashMap;

    // Common stop words to filter out
    let stop_words: &[&str] = &[
        "the", "a", "an", "is", "are", "was", "were", "be", "been", "being", "have", "has", "had",
        "do", "does", "did", "will", "would", "could", "should", "may", "might", "shall", "can",
        "need", "dare", "ought", "used", "to", "of", "in", "for", "on", "with", "at", "by", "from",
        "as", "into", "through", "during", "before", "after", "above", "below", "between", "out",
        "off", "over", "under", "again", "further", "then", "once", "and", "but", "or", "nor",
        "not", "so", "yet", "both", "either", "neither", "each", "every", "all", "any", "few",
        "more", "most", "other", "some", "such", "no", "only", "own", "same", "than", "too",
        "very", "just", "because", "if", "when", "where", "how", "what", "which", "who", "whom",
        "this", "that", "these", "those", "i", "me", "my", "we", "our", "you", "your", "he", "him",
        "his", "she", "her", "it", "its", "they", "them", "their",
    ];

    let mut word_counts: HashMap<String, usize> = HashMap::new();

    for word in text.split(|c: char| !c.is_alphanumeric()) {
        let lower = word.to_lowercase();
        if lower.len() < 3 || stop_words.contains(&lower.as_str()) {
            continue;
        }
        *word_counts.entry(lower).or_insert(0) += 1;
    }

    let mut words: Vec<(String, usize)> = word_counts.into_iter().collect();
    words.sort_by_key(|a| std::cmp::Reverse(a.1));

    // Return top 10 keywords that appear more than once
    words
        .into_iter()
        .filter(|(_, count)| *count > 1)
        .take(10)
        .map(|(word, _)| word)
        .collect()
}

/// Generate a brief summary of the text.
fn generate_summary(text: &str, max_chars: usize) -> String {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return String::new();
    }

    // Try to get the first sentence
    if let Some(end) = trimmed.find(['.', '!', '?']) {
        let sentence = &trimmed[..=end];
        if sentence.len() <= max_chars {
            return sentence.to_string();
        }
    }

    // Fall back to first N characters
    let summary: String = trimmed.chars().take(max_chars).collect();
    if summary.len() < trimmed.len() {
        format!("{}…", summary)
    } else {
        summary
    }
}

/// Simple language detection based on character ranges.
fn detect_language(text: &str) -> Option<String> {
    if text.is_empty() {
        return None;
    }

    let mut ascii = 0u32;
    let mut cjk = 0u32;
    let mut cyrillic = 0u32;
    let mut arabic = 0u32;
    let total = text.chars().count() as f32;

    for ch in text.chars() {
        let cp = ch as u32;
        if cp < 128 {
            ascii += 1;
        } else if (0x4E00..=0x9FFF).contains(&cp) || (0x3400..=0x4DBF).contains(&cp) {
            cjk += 1;
        } else if (0x0400..=0x04FF).contains(&cp) {
            cyrillic += 1;
        } else if (0x0600..=0x06FF).contains(&cp) {
            arabic += 1;
        }
    }

    let threshold = total * 0.3;
    if cjk as f32 > threshold {
        return Some("zh".to_string());
    }
    if cyrillic as f32 > threshold {
        return Some("ru".to_string());
    }
    if arabic as f32 > threshold {
        return Some("ar".to_string());
    }
    if ascii as f32 > total * 0.8 {
        return Some("en".to_string());
    }

    None
}

/// Count words in text.
fn count_words(text: &str) -> usize {
    text.split_whitespace().count()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn test_enrich_basic() {
        let event = RawEvent {
            id: "test".into(),
            source: "file:test.txt".into(),
            event_type: "file_change".into(),
            timestamp_ms: 1000,
            payload: b"This is a test document. It contains some keywords. Test test test."
                .to_vec(),
            tags: HashMap::new(),
        };
        let enrichment = enrich_event(&event);
        assert!(enrichment.keywords.contains(&"test".to_string()));
        assert!(enrichment.summary.contains("test document"));
        assert_eq!(enrichment.language, Some("en".to_string()));
    }

    #[test]
    fn test_extract_keywords_frequency() {
        let text = "rust rust rust python java rust python";
        let keywords = extract_keywords(text);
        // "rust" appears 4 times, "python" appears 2 times
        assert!(keywords.contains(&"rust".to_string()));
        assert!(keywords.contains(&"python".to_string()));
    }

    #[test]
    fn test_generate_summary_short() {
        let summary = generate_summary("Hello world.", 200);
        assert_eq!(summary, "Hello world.");
    }

    #[test]
    fn test_generate_summary_long() {
        let long_text = "A".repeat(500);
        let summary = generate_summary(&long_text, 200);
        assert!(summary.ends_with('…'));
        // 200 chars of 'A' + '…' (3 bytes UTF-8) = 203 bytes max
        assert!(summary.len() <= 203);
    }

    #[test]
    fn test_detect_language_english() {
        assert_eq!(
            detect_language("Hello world this is English text"),
            Some("en".to_string())
        );
    }

    #[test]
    fn test_detect_language_chinese() {
        assert_eq!(
            detect_language("这是一个中文测试文档包含多个汉字"),
            Some("zh".to_string())
        );
    }

    #[test]
    fn test_detect_language_empty() {
        assert_eq!(detect_language(""), None);
    }

    #[test]
    fn test_count_words() {
        assert_eq!(count_words("one two three four"), 4);
        assert_eq!(count_words(""), 0);
        assert_eq!(count_words("  spaced  out  "), 2);
    }

    #[test]
    fn test_extract_entities_url() {
        let entities = extract_entities("Visit https://example.com/path?q=1 and http://test.org");
        let urls: Vec<_> = entities
            .iter()
            .filter(|e| e.entity_type == EntityType::Url)
            .collect();
        assert_eq!(urls.len(), 2);
        assert!(urls[0].value.contains("example.com"));
    }

    #[test]
    fn test_extract_entities_email() {
        let entities = extract_entities("Contact user@example.com or admin@test.org for help");
        let emails: Vec<_> = entities
            .iter()
            .filter(|e| e.entity_type == EntityType::Email)
            .collect();
        assert_eq!(emails.len(), 2);
    }

    #[test]
    fn test_extract_entities_ip_valid() {
        let entities = extract_entities("Connected to 192.168.1.1 and 10.0.0.1");
        let ips: Vec<_> = entities
            .iter()
            .filter(|e| e.entity_type == EntityType::IpAddress)
            .collect();
        assert_eq!(ips.len(), 2);
    }

    #[test]
    fn test_extract_entities_ip_invalid() {
        let entities = extract_entities("Not an IP: 999.999.999.999");
        let ips: Vec<_> = entities
            .iter()
            .filter(|e| e.entity_type == EntityType::IpAddress)
            .collect();
        assert_eq!(ips.len(), 0); // 999 > 255
    }

    #[test]
    fn test_extract_entities_file_path() {
        let entities = extract_entities("Config at /etc/nginx/nginx.conf and /var/log/syslog");
        let paths: Vec<_> = entities
            .iter()
            .filter(|e| e.entity_type == EntityType::FilePath)
            .collect();
        assert!(!paths.is_empty());
    }

    #[test]
    fn test_extract_entities_hash_md5() {
        let md5 = "d41d8cd98f00b204e9800998ecf8427e";
        let entities = extract_entities(&format!("Hash: {}", md5));
        let hashes: Vec<_> = entities
            .iter()
            .filter(|e| e.entity_type == EntityType::Hash)
            .collect();
        assert_eq!(hashes.len(), 1);
        assert_eq!(hashes[0].value, md5);
    }

    #[test]
    fn test_extract_entities_empty_text() {
        let entities = extract_entities("");
        assert!(entities.is_empty());
    }

    #[test]
    fn test_apply_enrichment() {
        let mut event = RawEvent {
            id: "test".into(),
            source: "test".into(),
            event_type: "test".into(),
            timestamp_ms: 1000,
            payload: b"Visit https://example.com from 192.168.1.1".to_vec(),
            tags: HashMap::new(),
        };
        let enrichment = enrich_event(&event);
        apply_enrichment(&mut event, &enrichment);

        assert!(event.tags.contains_key("word_count"));
        assert!(event.tags.contains_key("language"));
    }

    #[test]
    fn test_generate_summary_no_sentence_end() {
        let summary = generate_summary("no ending sentence here", 200);
        assert_eq!(summary, "no ending sentence here");
    }

    #[test]
    fn test_detect_language_russian() {
        assert_eq!(
            detect_language("Это русский текст для тестирования"),
            Some("ru".to_string())
        );
    }

    #[test]
    fn test_extract_entities_port_localhost() {
        let entities = extract_entities("Server running on localhost:8080");
        let ports: Vec<_> = entities
            .iter()
            .filter(|e| e.entity_type == EntityType::Port)
            .collect();
        assert_eq!(ports.len(), 1);
        assert_eq!(ports[0].value, "8080");
    }

    #[test]
    fn test_extract_entities_port_ip() {
        let entities = extract_entities("Connect to 192.168.1.1:443 for HTTPS");
        let ports: Vec<_> = entities
            .iter()
            .filter(|e| e.entity_type == EntityType::Port)
            .collect();
        assert_eq!(ports.len(), 1);
        assert_eq!(ports[0].value, "443");
    }

    #[test]
    fn test_extract_entities_port_multiple() {
        let entities = extract_entities("Ports: localhost:3000, localhost:8090, localhost:5432");
        let ports: Vec<_> = entities
            .iter()
            .filter(|e| e.entity_type == EntityType::Port)
            .collect();
        assert_eq!(ports.len(), 3);
    }

    #[test]
    fn test_extract_entities_phone_international() {
        let entities = extract_entities("Call +1-202-555-0147 for support");
        let phones: Vec<_> = entities
            .iter()
            .filter(|e| e.entity_type == EntityType::PhoneNumber)
            .collect();
        assert_eq!(phones.len(), 1);
        assert!(phones[0].value.contains("202"));
    }

    #[test]
    fn test_extract_entities_phone_local() {
        let entities = extract_entities("Phone: 021-6543-2100 ext 123");
        let phones: Vec<_> = entities
            .iter()
            .filter(|e| e.entity_type == EntityType::PhoneNumber)
            .collect();
        assert!(!phones.is_empty());
    }

    #[test]
    fn test_extract_entities_phone_too_short() {
        // Only 3 digits — should NOT be detected as a phone number
        let entities = extract_entities("Call 123 for info");
        let phones: Vec<_> = entities
            .iter()
            .filter(|e| e.entity_type == EntityType::PhoneNumber)
            .collect();
        assert!(phones.is_empty());
    }

    #[test]
    fn test_extract_entities_datetime_iso() {
        let entities = extract_entities("Created at 2024-01-15T10:30:00Z");
        let dates: Vec<_> = entities
            .iter()
            .filter(|e| e.entity_type == EntityType::DateTime)
            .collect();
        assert_eq!(dates.len(), 1);
        assert!(dates[0].value.contains("2024-01-15"));
    }

    #[test]
    fn test_extract_entities_datetime_iso_with_offset() {
        let entities = extract_entities("Meeting: 2024-03-20 14:00:00+08:00");
        let dates: Vec<_> = entities
            .iter()
            .filter(|e| e.entity_type == EntityType::DateTime)
            .collect();
        assert_eq!(dates.len(), 1);
    }

    #[test]
    fn test_extract_entities_datetime_slash_format() {
        let entities = extract_entities("Deadline: 2024/12/31");
        let dates: Vec<_> = entities
            .iter()
            .filter(|e| e.entity_type == EntityType::DateTime)
            .collect();
        assert_eq!(dates.len(), 1);
        assert!(dates[0].value.contains("2024/12/31"));
    }

    #[test]
    fn test_extract_entities_time_only() {
        let entities = extract_entities("Daily standup at 09:30:00");
        let dates: Vec<_> = entities
            .iter()
            .filter(|e| e.entity_type == EntityType::DateTime)
            .collect();
        assert_eq!(dates.len(), 1);
        assert_eq!(dates[0].value, "09:30:00");
    }

    #[test]
    fn test_extract_all_entity_types() {
        let text = "Contact user@example.com at https://example.com from 192.168.1.1:443. \
                     Call +1-555-123-4567. Meeting 2024-06-15T09:00:00Z. \
                     File /tmp/data.csv. Hash d41d8cd98f00b204e9800998ecf8427e.";
        let entities = extract_entities(text);

        let types: std::collections::HashSet<_> = entities.iter().map(|e| &e.entity_type).collect();
        // Should have at least: Url, Email, IpAddress, Port, PhoneNumber, DateTime, FilePath, Hash
        assert!(types.contains(&EntityType::Url));
        assert!(types.contains(&EntityType::Email));
        assert!(types.contains(&EntityType::IpAddress));
        assert!(types.contains(&EntityType::Port));
        assert!(types.contains(&EntityType::PhoneNumber));
        assert!(types.contains(&EntityType::DateTime));
        assert!(types.contains(&EntityType::FilePath));
        assert!(types.contains(&EntityType::Hash));
    }

    // ── MAC address extraction ────────────────────────────────────

    #[test]
    fn test_extract_entities_mac_colon() {
        let entities = extract_entities("Device MAC: 00:1A:2B:3C:4D:5E connected");
        let macs: Vec<_> = entities
            .iter()
            .filter(|e| e.entity_type == EntityType::MacAddress)
            .collect();
        assert_eq!(macs.len(), 1);
        assert_eq!(macs[0].value.to_lowercase(), "00:1a:2b:3c:4d:5e");
    }

    #[test]
    fn test_extract_entities_mac_dash() {
        let entities = extract_entities("NIC: AA-BB-CC-DD-EE-FF is up");
        let macs: Vec<_> = entities
            .iter()
            .filter(|e| e.entity_type == EntityType::MacAddress)
            .collect();
        assert_eq!(macs.len(), 1);
    }

    #[test]
    fn test_extract_entities_mac_lowercase() {
        let entities = extract_entities("eth0: aa:bb:cc:dd:ee:ff");
        let macs: Vec<_> = entities
            .iter()
            .filter(|e| e.entity_type == EntityType::MacAddress)
            .collect();
        assert_eq!(macs.len(), 1);
    }

    // ── UUID extraction ───────────────────────────────────────────

    #[test]
    fn test_extract_entities_uuid_v4() {
        let entities = extract_entities("ID: 550e8400-e29b-41d4-a716-446655440000");
        let uuids: Vec<_> = entities
            .iter()
            .filter(|e| e.entity_type == EntityType::Uuid)
            .collect();
        assert_eq!(uuids.len(), 1);
        assert_eq!(uuids[0].value, "550e8400-e29b-41d4-a716-446655440000");
    }

    #[test]
    fn test_extract_entities_uuid_v1() {
        let entities = extract_entities("UUID: 6ba7b810-9dad-11d1-80b4-00c04fd430c8");
        let uuids: Vec<_> = entities
            .iter()
            .filter(|e| e.entity_type == EntityType::Uuid)
            .collect();
        assert_eq!(uuids.len(), 1);
    }

    #[test]
    fn test_extract_entities_uuid_uppercase() {
        let entities = extract_entities("Ref: 550E8400-E29B-41D4-A716-446655440000");
        let uuids: Vec<_> = entities
            .iter()
            .filter(|e| e.entity_type == EntityType::Uuid)
            .collect();
        assert_eq!(uuids.len(), 1);
    }

    // ── AWS ARN extraction ────────────────────────────────────────

    #[test]
    fn test_extract_entities_aws_arn() {
        let entities = extract_entities("Resource: arn:aws:s3:::my-bucket/path/to/object");
        let arns: Vec<_> = entities
            .iter()
            .filter(|e| e.entity_type == EntityType::AwsArn)
            .collect();
        assert_eq!(arns.len(), 1);
        assert!(arns[0].value.contains("s3"));
    }

    #[test]
    fn test_extract_entities_aws_arn_lambda() {
        let entities = extract_entities(
            "Function: arn:aws:lambda:us-east-1:123456789012:function:my-function",
        );
        let arns: Vec<_> = entities
            .iter()
            .filter(|e| e.entity_type == EntityType::AwsArn)
            .collect();
        assert_eq!(arns.len(), 1);
        assert!(arns[0].value.contains("lambda"));
    }

    // ── Credit card extraction ────────────────────────────────────

    #[test]
    fn test_extract_entities_visa() {
        // Visa test number (passes Luhn)
        let entities = extract_entities("Card: 4111 1111 1111 1111");
        let cards: Vec<_> = entities
            .iter()
            .filter(|e| e.entity_type == EntityType::CreditCard)
            .collect();
        assert_eq!(cards.len(), 1);
    }

    #[test]
    fn test_extract_entities_mastercard() {
        // Mastercard test number (passes Luhn)
        let entities = extract_entities("Card: 5500-0000-0000-0004");
        let cards: Vec<_> = entities
            .iter()
            .filter(|e| e.entity_type == EntityType::CreditCard)
            .collect();
        assert_eq!(cards.len(), 1);
    }

    #[test]
    fn test_extract_entities_credit_card_invalid_luhn() {
        // Invalid card number (fails Luhn)
        let entities = extract_entities("Card: 4111 1111 1111 1112");
        let cards: Vec<_> = entities
            .iter()
            .filter(|e| e.entity_type == EntityType::CreditCard)
            .collect();
        assert_eq!(cards.len(), 0);
    }

    // ── Luhn algorithm tests ──────────────────────────────────────

    #[test]
    fn test_luhn_valid_visa() {
        assert!(luhn_check("4111111111111111"));
    }

    #[test]
    fn test_luhn_valid_mastercard() {
        assert!(luhn_check("5500000000000004"));
    }

    #[test]
    fn test_luhn_invalid() {
        assert!(!luhn_check("4111111111111112"));
    }

    #[test]
    fn test_luhn_too_short() {
        assert!(!luhn_check("4111111"));
    }

    #[test]
    fn test_luhn_empty() {
        assert!(!luhn_check(""));
    }

    // ── @mentions ─────────────────────────────────────────────────

    #[test]
    fn test_extract_mention_simple() {
        let entities = extract_entities("Hello @alice how are you?");
        let mentions: Vec<_> = entities
            .iter()
            .filter(|e| e.entity_type == EntityType::Mention)
            .collect();
        assert_eq!(mentions.len(), 1);
        assert_eq!(mentions[0].value, "@alice");
    }

    #[test]
    fn test_extract_mention_with_underscore() {
        let entities = extract_entities("cc @user_name_123");
        let mentions: Vec<_> = entities
            .iter()
            .filter(|e| e.entity_type == EntityType::Mention)
            .collect();
        assert_eq!(mentions.len(), 1);
        assert_eq!(mentions[0].value, "@user_name_123");
    }

    #[test]
    fn test_extract_multiple_mentions() {
        let entities = extract_entities("@alice @bob @charlie discussed this");
        let mentions: Vec<_> = entities
            .iter()
            .filter(|e| e.entity_type == EntityType::Mention)
            .collect();
        assert_eq!(mentions.len(), 3);
    }

    #[test]
    fn test_no_mention_in_email() {
        // Email addresses contain @ but should not be extracted as mentions
        let entities = extract_entities("Send to user@example.com");
        let mentions: Vec<_> = entities
            .iter()
            .filter(|e| e.entity_type == EntityType::Mention)
            .collect();
        assert_eq!(mentions.len(), 0);
    }

    // ── #hashtags ─────────────────────────────────────────────────

    #[test]
    fn test_extract_hashtag_simple() {
        let entities = extract_entities("Check out #rust lang today");
        let hashtags: Vec<_> = entities
            .iter()
            .filter(|e| e.entity_type == EntityType::Hashtag)
            .collect();
        assert_eq!(hashtags.len(), 1);
        assert_eq!(hashtags[0].value, "#rust");
    }

    #[test]
    fn test_extract_hashtag_with_numbers() {
        let entities = extract_entities("Join #dev2026 conference");
        let hashtags: Vec<_> = entities
            .iter()
            .filter(|e| e.entity_type == EntityType::Hashtag)
            .collect();
        assert_eq!(hashtags.len(), 1);
        assert_eq!(hashtags[0].value, "#dev2026");
    }

    #[test]
    fn test_extract_hashtag_unicode() {
        let entities = extract_entities("发布 #技术分享 话题");
        let hashtags: Vec<_> = entities
            .iter()
            .filter(|e| e.entity_type == EntityType::Hashtag)
            .collect();
        assert_eq!(hashtags.len(), 1);
        assert_eq!(hashtags[0].value, "#技术分享");
    }

    // ── /bot_commands ─────────────────────────────────────────────

    #[test]
    fn test_extract_bot_command_simple() {
        let entities = extract_entities("/start hello");
        let commands: Vec<_> = entities
            .iter()
            .filter(|e| e.entity_type == EntityType::BotCommand)
            .collect();
        assert_eq!(commands.len(), 1);
        assert_eq!(commands[0].value, "/start");
    }

    #[test]
    fn test_extract_bot_command_with_bot_name() {
        let entities = extract_entities("/help@mybot do something");
        let commands: Vec<_> = entities
            .iter()
            .filter(|e| e.entity_type == EntityType::BotCommand)
            .collect();
        assert_eq!(commands.len(), 1);
        assert_eq!(commands[0].value, "/help@mybot");
    }

    #[test]
    fn test_extract_multiple_commands() {
        let entities = extract_entities("/start /help /settings");
        let commands: Vec<_> = entities
            .iter()
            .filter(|e| e.entity_type == EntityType::BotCommand)
            .collect();
        assert_eq!(commands.len(), 3);
    }

    #[test]
    fn test_no_command_in_url_path() {
        // URL paths contain / but should not be extracted as commands
        let entities = extract_entities("Visit https://example.com/api/test");
        let commands: Vec<_> = entities
            .iter()
            .filter(|e| e.entity_type == EntityType::BotCommand)
            .collect();
        assert_eq!(commands.len(), 0);
    }

    // ── Mixed IM content ─────────────────────────────────────────

    #[test]
    fn test_extract_telegram_message_entities() {
        let text = "Hey @alice check #projectX and run /status@workbot — see https://example.com";
        let entities = extract_entities(text);

        let mentions: Vec<_> = entities
            .iter()
            .filter(|e| e.entity_type == EntityType::Mention)
            .collect();
        let hashtags: Vec<_> = entities
            .iter()
            .filter(|e| e.entity_type == EntityType::Hashtag)
            .collect();
        let commands: Vec<_> = entities
            .iter()
            .filter(|e| e.entity_type == EntityType::BotCommand)
            .collect();
        let urls: Vec<_> = entities
            .iter()
            .filter(|e| e.entity_type == EntityType::Url)
            .collect();

        assert_eq!(mentions.len(), 1);
        assert_eq!(hashtags.len(), 1);
        assert_eq!(commands.len(), 1);
        assert_eq!(urls.len(), 1);
    }
}
