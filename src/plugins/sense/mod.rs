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
