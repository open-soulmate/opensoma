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

    entities
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
}
