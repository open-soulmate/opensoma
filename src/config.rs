use anyhow::{Context, Result};
use serde::Deserialize;
use std::path::Path;
use std::sync::Arc;
use tokio::sync::watch;
use tracing::{info, warn};

#[derive(Debug, Clone, Deserialize)]
pub struct AppConfig {
    pub daemon: DaemonConfig,
    pub soul: SoulConfig,
    pub collector: CollectorConfig,
    pub connector: ConnectorConfig,
    pub processor: ProcessorConfig,
    pub sync: SyncConfig,
}

#[derive(Debug, Clone, Deserialize)]
pub struct DaemonConfig {
    pub node_id: String,
    #[serde(default = "default_log_level")]
    pub log_level: String,
    #[serde(default = "default_data_dir")]
    pub data_dir: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct SoulConfig {
    pub endpoint: String,
    #[serde(default = "default_heartbeat_interval")]
    pub heartbeat_interval: u64,
    #[serde(default = "default_connect_timeout")]
    pub connect_timeout: u64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct CollectorConfig {
    pub watch_dirs: Vec<String>,
    #[serde(default = "default_include")]
    pub include: Vec<String>,
    #[serde(default)]
    pub exclude: Vec<String>,
    #[serde(default = "default_debounce_ms")]
    pub debounce_ms: u64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ConnectorConfig {
    #[serde(default)]
    pub feishu: Option<FeishuConfig>,
    #[serde(default)]
    pub dingtalk: Option<DingtalkConfig>,
    #[serde(default)]
    pub wecom: Option<WecomConfig>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct FeishuConfig {
    pub enabled: bool,
    pub app_id: String,
    pub app_secret: String,
    #[serde(default = "default_feishu_webhook")]
    pub webhook_path: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct DingtalkConfig {
    pub enabled: bool,
    pub app_key: String,
    pub app_secret: String,
    pub robot_webhook: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct WecomConfig {
    pub enabled: bool,
    pub corp_id: String,
    pub agent_id: String,
    pub secret: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ProcessorConfig {
    #[serde(default = "default_true")]
    pub normalize_timestamps: bool,
    #[serde(default = "default_max_event_size")]
    pub max_event_size: usize,
    #[serde(default = "default_dedup_window")]
    pub dedup_window_secs: u64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct SyncConfig {
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
    #[serde(default = "default_upload_interval")]
    pub upload_interval: u64,
    #[serde(default = "default_max_retries")]
    pub max_retries: u32,
    #[serde(default = "default_retry_backoff")]
    pub retry_backoff_ms: u64,
    #[serde(default = "default_cache_size")]
    pub cache_size_mb: u64,
}

impl AppConfig {
    pub fn load(path: &str) -> Result<Self> {
        let content = std::fs::read_to_string(path)
            .with_context(|| format!("Failed to read config file: {}", path))?;
        let config: AppConfig =
            toml::from_str(&content).with_context(|| "Failed to parse config TOML")?;
        Ok(config)
    }
}

/// Start a file watcher on the config path. Returns a handle and a receiver
/// that emits the new AppConfig whenever the file changes.
pub fn watch_config(path: &str) -> Result<(tokio::task::JoinHandle<()>, watch::Receiver<Arc<AppConfig>>)> {
    let initial = AppConfig::load(path)?;
    let (tx, rx) = watch::channel(Arc::new(initial));
    let path_buf = Path::new(path).to_path_buf();

    let handle = tokio::spawn(async move {
        use notify::{RecommendedWatcher, RecursiveMode, Watcher};
        use std::sync::mpsc;
        use std::time::Duration;

        let (notify_tx, notify_rx) = mpsc::channel();

        let mut watcher = match RecommendedWatcher::new(
            notify_tx,
            notify::Config::default().with_poll_interval(Duration::from_secs(2)),
        ) {
            Ok(w) => w,
            Err(e) => {
                warn!("Failed to create config watcher: {}", e);
                return;
            }
        };

        if let Err(e) = watcher.watch(path_buf.parent().unwrap_or(Path::new(".")), RecursiveMode::NonRecursive) {
            warn!("Failed to watch config directory: {}", e);
            return;
        }

        info!("Config hot-reload watcher started for: {}", path_buf.display());

        loop {
            match notify_rx.recv() {
                Ok(Ok(events)) => {
                    for event in events {
                        if event.paths.iter().any(|p| p == &path_buf) {
                            match AppConfig::load(path) {
                                Ok(new_config) => {
                                    info!("Config reloaded successfully.");
                                    let _ = tx.send(Arc::new(new_config));
                                }
                                Err(e) => {
                                    warn!("Config reload failed (keeping previous): {}", e);
                                }
                            }
                        }
                    }
                }
                Ok(Err(e)) => {
                    warn!("Config watcher error: {}", e);
                }
                Err(e) => {
                    warn!("Config watcher channel error: {}", e);
                    break;
                }
            }
        }
    });

    Ok((handle, rx))
}

// --- Default value functions ---

fn default_log_level() -> String {
    "info".into()
}
fn default_data_dir() -> String {
    "/var/lib/opensoma".into()
}
fn default_heartbeat_interval() -> u64 {
    30
}
fn default_connect_timeout() -> u64 {
    10
}
fn default_include() -> Vec<String> {
    vec!["*.json".into(), "*.csv".into(), "*.txt".into()]
}
fn default_debounce_ms() -> u64 {
    500
}
fn default_feishu_webhook() -> String {
    "/api/feishu/webhook".into()
}
fn default_true() -> bool {
    true
}
fn default_max_event_size() -> usize {
    1_048_576
}
fn default_dedup_window() -> u64 {
    300
}
fn default_batch_size() -> usize {
    50
}
fn default_upload_interval() -> u64 {
    10
}
fn default_max_retries() -> u32 {
    5
}
fn default_retry_backoff() -> u64 {
    1000
}
fn default_cache_size() -> u64 {
    512
}
