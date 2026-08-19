use super::{MediaType, SensePlugin, SenseResult};
use anyhow::{Context, Result};
use serde::Deserialize;
use tracing::debug;

/// ASR engine selection.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AsrEngine {
    /// Local Whisper CLI.
    Whisper,
    /// Remote Whisper-compatible HTTP API.
    Api,
}

/// Configuration for ASR parsing.
#[derive(Debug, Clone, Deserialize)]
pub struct AsrConfig {
    pub engine: AsrEngine,
    #[serde(default)]
    pub api_url: Option<String>,
    #[serde(default)]
    pub api_key: Option<String>,
    /// Whisper model size for local mode (e.g. "base", "small", "medium", "large").
    #[serde(default = "default_whisper_model")]
    pub whisper_model: String,
    /// Max segment duration in seconds before splitting.
    #[serde(default = "default_segment_secs")]
    pub segment_duration_secs: u64,
}

fn default_whisper_model() -> String {
    "base".into()
}

fn default_segment_secs() -> u64 {
    600
}

/// ASR speech-to-text plugin.
pub struct AsrPlugin {
    config: AsrConfig,
    client: reqwest::Client,
}

impl AsrPlugin {
    pub fn new(config: AsrConfig) -> Self {
        Self {
            config,
            client: reqwest::Client::new(),
        }
    }

    async fn run_whisper_local(&self, data: &[u8]) -> Result<String> {
        let tmp_in = tempfile::NamedTempFile::new()?;
        std::fs::write(tmp_in.path(), data)?;

        let output = tokio::process::Command::new("whisper")
            .arg(tmp_in.path())
            .arg("--model")
            .arg(&self.config.whisper_model)
            .arg("--output_format")
            .arg("txt")
            .arg("--output_dir")
            .arg("/tmp")
            .output()
            .await
            .context("Failed to run whisper — is it installed?")?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            anyhow::bail!("whisper failed: {}", stderr);
        }

        // Whisper writes <filename>.txt alongside the input
        let txt_path = tmp_in.path().with_extension("txt");
        // The actual output path whisper uses: stem from the temp file
        let stem = tmp_in
            .path()
            .file_stem()
            .unwrap_or_default()
            .to_string_lossy();
        let whisper_out = std::path::Path::new("/tmp").join(format!("{stem}.txt"));

        let text = std::fs::read_to_string(&whisper_out)
            .or_else(|_| std::fs::read_to_string(&txt_path))
            .unwrap_or_default();
        let _ = std::fs::remove_file(&whisper_out);

        Ok(text.trim().to_string())
    }

    async fn run_api(&self, data: &[u8]) -> Result<String> {
        let api_url = self
            .config
            .api_url
            .as_deref()
            .context("ASR API URL not configured")?;

        let form = reqwest::multipart::Form::new()
            .part(
                "file",
                reqwest::multipart::Part::bytes(data.to_vec())
                    .file_name("audio.wav")
                    .mime_str("audio/wav")?,
            )
            .text("model", "whisper-1");

        let mut req = self.client.post(api_url).multipart(form);

        if let Some(ref key) = self.config.api_key {
            req = req.bearer_auth(key);
        }

        let resp = req.send().await.context("ASR API request failed")?;
        let body: AsrApiResponse = resp
            .json()
            .await
            .context("Failed to parse ASR API response")?;

        Ok(body.text)
    }
}

#[derive(Deserialize)]
struct AsrApiResponse {
    text: String,
}

#[async_trait::async_trait]
impl SensePlugin for AsrPlugin {
    async fn parse(&self, data: &[u8]) -> Result<SenseResult> {
        debug!("ASR parsing {} bytes", data.len());

        let text = match self.config.engine {
            AsrEngine::Whisper => self.run_whisper_local(data).await?,
            AsrEngine::Api => self.run_api(data).await?,
        };

        Ok(SenseResult {
            media_type: MediaType::Audio,
            extracted_text: text,
            metadata: serde_json::json!({
                "engine": match self.config.engine {
                    AsrEngine::Whisper => "whisper",
                    AsrEngine::Api => "api",
                },
                "bytes": data.len(),
            }),
        })
    }

    fn name(&self) -> &str {
        "asr"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_asr_config_deserialize_whisper() {
        let json =
            r#"{"engine": "whisper", "whisper_model": "small", "segment_duration_secs": 300}"#;
        let config: AsrConfig = serde_json::from_str(json).unwrap();
        assert!(matches!(config.engine, AsrEngine::Whisper));
        assert_eq!(config.whisper_model, "small");
        assert_eq!(config.segment_duration_secs, 300);
        assert!(config.api_url.is_none());
    }

    #[test]
    fn test_asr_config_deserialize_api() {
        let json = r#"{"engine": "api", "api_url": "http://whisper.local/transcribe", "api_key": "key123"}"#;
        let config: AsrConfig = serde_json::from_str(json).unwrap();
        assert!(matches!(config.engine, AsrEngine::Api));
        assert_eq!(
            config.api_url.as_deref(),
            Some("http://whisper.local/transcribe")
        );
        assert_eq!(config.api_key.as_deref(), Some("key123"));
    }

    #[test]
    fn test_asr_config_defaults() {
        let json = r#"{"engine": "whisper"}"#;
        let config: AsrConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.whisper_model, "base");
        assert_eq!(config.segment_duration_secs, 600);
    }

    #[test]
    fn test_asr_plugin_name() {
        let config = AsrConfig {
            engine: AsrEngine::Whisper,
            api_url: None,
            api_key: None,
            whisper_model: "base".to_string(),
            segment_duration_secs: 600,
        };
        let plugin = AsrPlugin::new(config);
        assert_eq!(plugin.name(), "asr");
    }

    #[test]
    fn test_asr_engine_deserialize() {
        let whisper: AsrEngine = serde_json::from_str("\"whisper\"").unwrap();
        assert!(matches!(whisper, AsrEngine::Whisper));

        let api: AsrEngine = serde_json::from_str("\"api\"").unwrap();
        assert!(matches!(api, AsrEngine::Api));
    }
}
