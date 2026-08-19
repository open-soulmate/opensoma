use super::{MediaType, SensePlugin, SenseResult};
use anyhow::{Context, Result};
use serde::Deserialize;
use tracing::debug;

/// Configuration for image understanding via multimodal LLM.
#[derive(Debug, Clone, Deserialize)]
pub struct ImageConfig {
    /// Model identifier (e.g. "gpt-4o", "claude-sonnet-4-20250514").
    pub model: String,
    /// API endpoint (e.g. "https://api.openai.com/v1/chat/completions").
    pub api_url: String,
    #[serde(default)]
    pub api_key: Option<String>,
}

/// Image understanding plugin — calls a multimodal LLM to describe/OCR an image.
pub struct ImagePlugin {
    config: ImageConfig,
    client: reqwest::Client,
}

impl ImagePlugin {
    pub fn new(config: ImageConfig) -> Self {
        Self {
            config,
            client: reqwest::Client::new(),
        }
    }
}

#[derive(Deserialize)]
struct ChatResponse {
    choices: Vec<ChatChoice>,
}

#[derive(Deserialize)]
struct ChatChoice {
    message: ChatMessage,
}

#[derive(Deserialize)]
struct ChatMessage {
    content: String,
}

#[async_trait::async_trait]
impl SensePlugin for ImagePlugin {
    async fn parse(&self, data: &[u8]) -> Result<SenseResult> {
        debug!("Image understanding parsing {} bytes", data.len());

        let b64 = base64_encode(data);
        let data_url = format!("data:image/png;base64,{b64}");

        let body = serde_json::json!({
            "model": self.config.model,
            "messages": [{
                "role": "user",
                "content": [
                    {
                        "type": "text",
                        "text": "Analyze this image in detail. Provide:\n1. A concise description of the image content\n2. Any visible text (OCR)\n3. Key information or objects of interest\n\nFormat your response as structured text with these three sections."
                    },
                    {
                        "type": "image_url",
                        "image_url": { "url": data_url }
                    }
                ]
            }],
            "max_tokens": 1024
        });

        let mut req = self.client.post(&self.config.api_url).json(&body);

        if let Some(ref key) = self.config.api_key {
            req = req.bearer_auth(key);
        }

        let resp = req.send().await.context("Image LLM API request failed")?;
        let chat: ChatResponse = resp
            .json()
            .await
            .context("Failed to parse Image LLM response")?;

        let text = chat
            .choices
            .first()
            .map(|c| c.message.content.clone())
            .unwrap_or_default();

        Ok(SenseResult {
            media_type: MediaType::Image,
            extracted_text: text,
            metadata: serde_json::json!({
                "model": self.config.model,
                "bytes": data.len(),
            }),
        })
    }

    fn name(&self) -> &str {
        "image"
    }
}

fn base64_encode(data: &[u8]) -> String {
    use base64::Engine;
    base64::engine::general_purpose::STANDARD.encode(data)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_image_config_deserialize() {
        let json = r#"{"model": "gpt-4o", "api_url": "https://api.openai.com/v1/chat/completions", "api_key": "sk-123"}"#;
        let config: ImageConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.model, "gpt-4o");
        assert_eq!(config.api_url, "https://api.openai.com/v1/chat/completions");
        assert_eq!(config.api_key.as_deref(), Some("sk-123"));
    }

    #[test]
    fn test_image_config_no_api_key() {
        let json = r#"{"model": "claude-sonnet-4-20250514", "api_url": "http://localhost:8080/v1"}"#;
        let config: ImageConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.model, "claude-sonnet-4-20250514");
        assert!(config.api_key.is_none());
    }

    #[test]
    fn test_image_plugin_name() {
        let config = ImageConfig {
            model: "gpt-4o".to_string(),
            api_url: "http://localhost".to_string(),
            api_key: None,
        };
        let plugin = ImagePlugin::new(config);
        assert_eq!(plugin.name(), "image");
    }

    #[test]
    fn test_chat_response_deserialize() {
        let json = r#"{"choices": [{"message": {"content": "A red car on the road"}}]}"#;
        let resp: ChatResponse = serde_json::from_str(json).unwrap();
        assert_eq!(resp.choices[0].message.content, "A red car on the road");
    }

    #[test]
    fn test_chat_response_empty_choices() {
        let json = r#"{"choices": []}"#;
        let resp: ChatResponse = serde_json::from_str(json).unwrap();
        assert!(resp.choices.is_empty());
    }
}
