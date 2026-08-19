#![allow(dead_code)]
pub mod asr;
pub mod image;
pub mod ocr;
pub mod video;

use serde::{Deserialize, Serialize};

/// Supported media types for multimodal parsing.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum MediaType {
    Image,
    Audio,
    Video,
    Pdf,
}

/// Unified parsing result from any sense module.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SenseResult {
    pub media_type: MediaType,
    pub extracted_text: String,
    pub metadata: serde_json::Value,
}

/// Trait that all sense parsers implement.
#[async_trait::async_trait]
pub trait SensePlugin: Send + Sync {
    /// Parse raw media bytes and return a unified result.
    async fn parse(&self, data: &[u8]) -> anyhow::Result<SenseResult>;

    /// Human-readable name of this parser.
    fn name(&self) -> &str;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_media_type_serialization_roundtrip() {
        let types = vec![
            MediaType::Image,
            MediaType::Audio,
            MediaType::Video,
            MediaType::Pdf,
        ];
        for mt in types {
            let json = serde_json::to_string(&mt).unwrap();
            let deserialized: MediaType = serde_json::from_str(&json).unwrap();
            assert_eq!(mt, deserialized);
        }
    }

    #[test]
    fn test_media_type_json_values() {
        assert_eq!(
            serde_json::to_string(&MediaType::Image).unwrap(),
            "\"image\""
        );
        assert_eq!(
            serde_json::to_string(&MediaType::Audio).unwrap(),
            "\"audio\""
        );
        assert_eq!(
            serde_json::to_string(&MediaType::Video).unwrap(),
            "\"video\""
        );
        assert_eq!(serde_json::to_string(&MediaType::Pdf).unwrap(), "\"pdf\"");
    }

    #[test]
    fn test_media_type_from_json() {
        let img: MediaType = serde_json::from_str("\"image\"").unwrap();
        assert_eq!(img, MediaType::Image);
        let vid: MediaType = serde_json::from_str("\"video\"").unwrap();
        assert_eq!(vid, MediaType::Video);
    }

    #[test]
    fn test_sense_result_serialization() {
        let result = SenseResult {
            media_type: MediaType::Image,
            extracted_text: "Hello world".to_string(),
            metadata: serde_json::json!({"engine": "ocr", "bytes": 1024}),
        };
        let json = serde_json::to_string(&result).unwrap();
        assert!(json.contains("\"extracted_text\":\"Hello world\""));
        assert!(json.contains("\"image\""));
        assert!(json.contains("\"bytes\":1024"));

        let deserialized: SenseResult = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.extracted_text, "Hello world");
        assert_eq!(deserialized.media_type, MediaType::Image);
    }

    #[test]
    fn test_sense_result_empty_text() {
        let result = SenseResult {
            media_type: MediaType::Audio,
            extracted_text: String::new(),
            metadata: serde_json::json!(null),
        };
        let json = serde_json::to_string(&result).unwrap();
        let deserialized: SenseResult = serde_json::from_str(&json).unwrap();
        assert!(deserialized.extracted_text.is_empty());
        assert_eq!(deserialized.media_type, MediaType::Audio);
    }

    #[test]
    fn test_sense_result_complex_metadata() {
        let result = SenseResult {
            media_type: MediaType::Video,
            extracted_text: "frame text".to_string(),
            metadata: serde_json::json!({
                "frame_count": 30,
                "timeline": ["[0s] text1", "[5s] text2"],
                "nested": {"key": "value"}
            }),
        };
        let json = serde_json::to_string(&result).unwrap();
        let deserialized: SenseResult = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.metadata["frame_count"], 30);
        assert_eq!(deserialized.metadata["timeline"][0], "[0s] text1");
        assert_eq!(deserialized.metadata["nested"]["key"], "value");
    }
}
