use super::{MediaType, SensePlugin, SenseResult};
use anyhow::{Context, Result};
use serde::Deserialize;
use tracing::{debug, info};

/// Configuration for video frame extraction and analysis.
#[derive(Debug, Clone, Deserialize)]
pub struct VideoConfig {
    /// Seconds between extracted frames (default 5).
    #[serde(default = "default_frame_interval")]
    pub frame_interval_sec: u64,
    /// Maximum number of frames to extract (default 60).
    #[serde(default = "default_max_frames")]
    pub max_frames: usize,
    /// Which sense plugin to run on each frame: "ocr" or "image".
    #[serde(default = "default_frame_analyzer")]
    pub frame_analyzer: String,
}

fn default_frame_interval() -> u64 {
    5
}
fn default_max_frames() -> usize {
    60
}
fn default_frame_analyzer() -> String {
    "ocr".into()
}

/// Video parsing plugin — extracts frames via ffmpeg then runs OCR/image analysis on each.
pub struct VideoPlugin {
    config: VideoConfig,
}

impl VideoPlugin {
    pub fn new(config: VideoConfig) -> Self {
        Self { config }
    }

    /// Extract frames from video bytes using ffmpeg, returning (frame_index, png_bytes) pairs.
    async fn extract_frames(&self, data: &[u8]) -> Result<Vec<(usize, Vec<u8>)>> {
        let tmp_in = tempfile::NamedTempFile::new()?;
        std::fs::write(tmp_in.path(), data)?;

        let tmp_dir = tempfile::tempdir()?;
        let out_pattern = tmp_dir.path().join("frame_%04d.png");

        let status = tokio::process::Command::new("ffmpeg")
            .args([
                "-i",
                &tmp_in.path().to_string_lossy(),
                "-vf",
                &format!("fps=1/{}", self.config.frame_interval_sec),
                "-frames:v",
                &self.config.max_frames.to_string(),
                "-y",
                &out_pattern.to_string_lossy(),
            ])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()
            .await
            .context("Failed to run ffmpeg — is it installed?")?;

        if !status.success() {
            anyhow::bail!("ffmpeg exited with status: {}", status);
        }

        let mut frames = Vec::new();
        let mut entries: Vec<_> = std::fs::read_dir(tmp_dir.path())?
            .filter_map(|e| e.ok())
            .filter(|e| {
                e.path()
                    .extension()
                    .map(|ext| ext == "png")
                    .unwrap_or(false)
            })
            .collect();
        entries.sort_by_key(|e| e.path());

        for (i, entry) in entries.into_iter().enumerate() {
            let bytes = std::fs::read(entry.path())?;
            frames.push((i, bytes));
        }

        info!("Extracted {} frames from video", frames.len());
        Ok(frames)
    }
}

#[async_trait::async_trait]
impl SensePlugin for VideoPlugin {
    async fn parse(&self, data: &[u8]) -> Result<SenseResult> {
        debug!("Video parsing {} bytes", data.len());

        let frames = self.extract_frames(data).await?;
        let frame_count = frames.len();
        let mut timeline = Vec::new();

        // Analyze each frame — inline OCR via tesseract for simplicity.
        // For production, delegate to OcrPlugin or ImagePlugin.
        for (i, frame_bytes) in &frames {
            let timestamp_secs = (*i as u64) * self.config.frame_interval_sec;
            match extract_text_from_frame(&self.config.frame_analyzer, frame_bytes).await {
                Ok(text) if !text.is_empty() => {
                    timeline.push(format!("[{timestamp_secs}s] {text}"));
                }
                Ok(_) => {
                    debug!("Frame {i} at {timestamp_secs}s: no text detected");
                }
                Err(e) => {
                    debug!("Frame {i} at {timestamp_secs}s analysis failed: {e}");
                }
            }
        }

        let extracted_text = timeline.join("\n");

        Ok(SenseResult {
            media_type: MediaType::Video,
            extracted_text,
            metadata: serde_json::json!({
                "frame_count": frame_count,
                "frame_interval_sec": self.config.frame_interval_sec,
                "max_frames": self.config.max_frames,
                "analyzer": self.config.frame_analyzer,
            }),
        })
    }

    fn name(&self) -> &str {
        "video"
    }
}

/// Run text extraction on a single frame.
async fn extract_text_from_frame(analyzer: &str, frame_bytes: &[u8]) -> Result<String> {
    match analyzer {
        "ocr" => run_tesseract(frame_bytes).await,
        _ => anyhow::bail!("Unknown frame analyzer: {analyzer}"),
    }
}

async fn run_tesseract(data: &[u8]) -> Result<String> {
    let tmp = tempfile::NamedTempFile::with_suffix(".png")?;
    std::fs::write(tmp.path(), data)?;

    let output = tokio::process::Command::new("tesseract")
        .arg(tmp.path())
        .arg("stdout")
        .arg("-l")
        .arg("chi_sim+eng")
        .arg("--psm")
        .arg("6")
        .output()
        .await
        .context("Failed to run tesseract")?;

    if !output.status.success() {
        return Ok(String::new());
    }

    Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_video_config_defaults() {
        let json = r#"{}"#;
        let config: VideoConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.frame_interval_sec, 5);
        assert_eq!(config.max_frames, 60);
        assert_eq!(config.frame_analyzer, "ocr");
    }

    #[test]
    fn test_video_config_custom() {
        let json = r#"{"frame_interval_sec": 10, "max_frames": 120, "frame_analyzer": "image"}"#;
        let config: VideoConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.frame_interval_sec, 10);
        assert_eq!(config.max_frames, 120);
        assert_eq!(config.frame_analyzer, "image");
    }

    #[test]
    fn test_video_plugin_name() {
        let config = VideoConfig {
            frame_interval_sec: 5,
            max_frames: 60,
            frame_analyzer: "ocr".to_string(),
        };
        let plugin = VideoPlugin::new(config);
        assert_eq!(plugin.name(), "video");
    }

    #[test]
    fn test_video_config_partial() {
        let json = r#"{"max_frames": 30}"#;
        let config: VideoConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.frame_interval_sec, 5); // default
        assert_eq!(config.max_frames, 30); // custom
        assert_eq!(config.frame_analyzer, "ocr"); // default
    }

    #[test]
    fn test_default_functions() {
        assert_eq!(default_frame_interval(), 5);
        assert_eq!(default_max_frames(), 60);
        assert_eq!(default_frame_analyzer(), "ocr");
    }
}
