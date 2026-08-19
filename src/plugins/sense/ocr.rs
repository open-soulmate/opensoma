use super::{MediaType, SensePlugin, SenseResult};
use anyhow::{Context, Result};
use serde::Deserialize;
use tracing::debug;

/// OCR engine selection.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OcrEngine {
    /// Local Tesseract via CLI.
    Tesseract,
    /// Remote OCR HTTP API.
    Api,
}

/// Configuration for OCR parsing.
#[derive(Debug, Clone, Deserialize)]
pub struct OcrConfig {
    pub engine: OcrEngine,
    #[serde(default)]
    pub api_url: Option<String>,
    #[serde(default)]
    pub api_key: Option<String>,
    /// Tesseract language flag, e.g. "chi_sim+eng".
    #[serde(default = "default_tesseract_lang")]
    pub tesseract_lang: String,
}

fn default_tesseract_lang() -> String {
    "chi_sim+eng".into()
}

/// OCR image text recognition plugin.
pub struct OcrPlugin {
    config: OcrConfig,
    client: reqwest::Client,
}

impl OcrPlugin {
    pub fn new(config: OcrConfig) -> Self {
        Self {
            config,
            client: reqwest::Client::new(),
        }
    }

    async fn run_tesseract(&self, data: &[u8]) -> Result<String> {
        let tmp = tempfile::NamedTempFile::new()?;
        std::fs::write(tmp.path(), data)?;

        let output = tokio::process::Command::new("tesseract")
            .arg(tmp.path())
            .arg("stdout")
            .arg("-l")
            .arg(&self.config.tesseract_lang)
            .arg("--psm")
            .arg("6")
            .output()
            .await
            .context("Failed to run tesseract — is it installed?")?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            anyhow::bail!("tesseract failed: {}", stderr);
        }

        Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
    }

    async fn run_api(&self, data: &[u8]) -> Result<String> {
        let api_url = self
            .config
            .api_url
            .as_deref()
            .context("OCR API URL not configured")?;

        let b64 = base64_encode(data);

        let mut req = self
            .client
            .post(api_url)
            .json(&serde_json::json!({ "image_base64": b64 }));

        if let Some(ref key) = self.config.api_key {
            req = req.bearer_auth(key);
        }

        let resp = req.send().await.context("OCR API request failed")?;
        let body: OcrApiResponse = resp
            .json()
            .await
            .context("Failed to parse OCR API response")?;

        Ok(body.text)
    }
}

#[derive(Deserialize)]
struct OcrApiResponse {
    text: String,
}

#[async_trait::async_trait]
impl SensePlugin for OcrPlugin {
    async fn parse(&self, data: &[u8]) -> Result<SenseResult> {
        debug!("OCR parsing {} bytes", data.len());

        let text = match self.config.engine {
            OcrEngine::Tesseract => self.run_tesseract(data).await?,
            OcrEngine::Api => self.run_api(data).await?,
        };

        Ok(SenseResult {
            media_type: MediaType::Image,
            extracted_text: text,
            metadata: serde_json::json!({
                "engine": match self.config.engine {
                    OcrEngine::Tesseract => "tesseract",
                    OcrEngine::Api => "api",
                },
                "bytes": data.len(),
            }),
        })
    }

    fn name(&self) -> &str {
        "ocr"
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
    fn test_ocr_config_deserialize_tesseract() {
        let json = r#"{"engine": "tesseract", "tesseract_lang": "eng"}"#;
        let config: OcrConfig = serde_json::from_str(json).unwrap();
        assert!(matches!(config.engine, OcrEngine::Tesseract));
        assert_eq!(config.tesseract_lang, "eng");
        assert!(config.api_url.is_none());
        assert!(config.api_key.is_none());
    }

    #[test]
    fn test_ocr_config_deserialize_api() {
        let json = r#"{"engine": "api", "api_url": "http://ocr.local/parse", "api_key": "secret123"}"#;
        let config: OcrConfig = serde_json::from_str(json).unwrap();
        assert!(matches!(config.engine, OcrEngine::Api));
        assert_eq!(config.api_url.as_deref(), Some("http://ocr.local/parse"));
        assert_eq!(config.api_key.as_deref(), Some("secret123"));
    }

    #[test]
    fn test_ocr_config_default_lang() {
        let json = r#"{"engine": "tesseract"}"#;
        let config: OcrConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.tesseract_lang, "chi_sim+eng");
    }

    #[test]
    fn test_ocr_plugin_name() {
        let config = OcrConfig {
            engine: OcrEngine::Tesseract,
            api_url: None,
            api_key: None,
            tesseract_lang: "eng".to_string(),
        };
        let plugin = OcrPlugin::new(config);
        assert_eq!(plugin.name(), "ocr");
    }

    #[test]
    fn test_base64_encode() {
        let data = b"hello world";
        let encoded = base64_encode(data);
        assert_eq!(encoded, "aGVsbG8gd29ybGQ=");
    }

    #[test]
    fn test_base64_encode_empty() {
        let encoded = base64_encode(b"");
        assert_eq!(encoded, "");
    }
}
