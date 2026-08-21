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
    /// Sense plugin configuration for multimodal content parsing.
    #[serde(default)]
    pub sense: SenseConfig,
}

#[derive(Debug, Clone, Deserialize)]
pub struct DaemonConfig {
    pub node_id: String,
    #[serde(default = "default_log_level")]
    pub log_level: String,
    #[serde(default = "default_data_dir")]
    pub data_dir: String,
    #[serde(default = "default_status_port")]
    pub status_port: u16,
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
    /// Poll interval for process monitor in milliseconds (default 5000)
    #[serde(default = "default_process_interval_ms")]
    pub process_interval_ms: u64,
    /// Poll interval for network monitor in milliseconds (default 10000)
    #[serde(default = "default_network_interval_ms")]
    pub network_interval_ms: u64,
    /// Poll interval for clipboard monitor in milliseconds (default 2000)
    #[serde(default = "default_clipboard_interval_ms")]
    pub clipboard_interval_ms: u64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ConnectorConfig {
    #[serde(default)]
    pub feishu: Option<FeishuConfig>,
    #[serde(default)]
    pub dingtalk: Option<DingtalkConfig>,
    #[serde(default)]
    pub wecom: Option<WecomConfig>,
    #[serde(default)]
    pub rss: Option<RssConfig>,
    #[serde(default)]
    pub email: Option<EmailConfig>,
    #[serde(default)]
    pub notion: Option<NotionConfig>,
    #[serde(default)]
    pub git: Option<GitConfig>,
    #[serde(default)]
    pub obsidian: Option<ObsidianConfig>,
    #[serde(default)]
    pub webhook: Option<WebhookConfig>,
    #[serde(default)]
    pub github: Option<GitHubConfig>,
    #[serde(default)]
    pub slack: Option<SlackConfig>,
    #[serde(default)]
    pub telegram: Option<TelegramConfig>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct FeishuConfig {
    pub enabled: bool,
    pub app_id: String,
    pub app_secret: String,
    #[serde(default = "default_feishu_webhook")]
    pub webhook_path: String,
    /// Folder token to poll for documents (optional)
    #[serde(default)]
    pub folder_token: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct DingtalkConfig {
    pub enabled: bool,
    pub app_key: String,
    pub app_secret: String,
    pub agent_id: String,
    #[serde(default)]
    pub robot_webhook: String,
    /// Polling interval in seconds for approval and message data (default 60)
    #[serde(default = "default_poll_interval")]
    pub poll_interval_secs: u64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct WecomConfig {
    pub enabled: bool,
    pub corp_id: String,
    pub agent_id: String,
    pub secret: String,
    /// Polling interval in seconds (default 60)
    #[serde(default = "default_poll_interval")]
    pub poll_interval_secs: u64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct RssConfig {
    pub enabled: bool,
    pub feeds: Vec<RssFeedConfig>,
    /// Polling interval in seconds (default 300)
    #[serde(default = "default_rss_interval")]
    pub poll_interval_secs: u64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct RssFeedConfig {
    pub name: String,
    pub url: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct EmailConfig {
    pub enabled: bool,
    pub accounts: Vec<EmailAccountConfig>,
    /// Polling interval in seconds (default 120)
    #[serde(default = "default_email_interval")]
    pub poll_interval_secs: u64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct EmailAccountConfig {
    pub name: String,
    pub imap_server: String,
    #[serde(default = "default_imap_port")]
    pub imap_port: u16,
    pub username: String,
    pub password: String,
    #[serde(default = "default_inbox_folder")]
    pub folder: String,
    #[serde(default)]
    pub tls: bool,
}

#[derive(Debug, Clone, Deserialize)]
pub struct NotionConfig {
    pub enabled: bool,
    pub integration_token: String,
    pub database_id: String,
    /// Polling interval in seconds (default 120)
    #[serde(default = "default_notion_interval")]
    pub poll_interval_secs: u64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct GitConfig {
    pub enabled: bool,
    pub repo_url: String,
    #[serde(default = "default_git_branch")]
    pub branch: String,
    pub local_path: String,
    /// Polling interval in seconds (default 300)
    #[serde(default = "default_git_interval")]
    pub poll_interval_secs: u64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ObsidianConfig {
    pub enabled: bool,
    pub vault_path: String,
    /// Debounce duration in milliseconds (default 500)
    #[serde(default = "default_obsidian_debounce")]
    pub debounce_ms: u64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct WebhookConfig {
    pub enabled: bool,
    /// Listen address, e.g. "0.0.0.0:9800"
    #[serde(default = "default_webhook_listen")]
    pub listen: String,
    /// Shared secret for HMAC signature verification
    #[serde(default)]
    pub secret: Option<String>,
    /// Allowed origin prefixes for validation (e.g. `<https://open.feishu.cn>`)
    #[serde(default)]
    pub allowed_origins: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct GitHubConfig {
    pub enabled: bool,
    /// GitHub personal access token (optional, increases rate limits)
    #[serde(default)]
    pub token: Option<String>,
    /// List of "owner/repo" to sync
    pub repos: Vec<String>,
    /// Polling interval in seconds (default 300)
    #[serde(default = "default_github_interval")]
    pub poll_interval_secs: u64,
    /// Include issues (default true)
    #[serde(default = "default_true")]
    pub include_issues: bool,
    /// Include pull requests (default true)
    #[serde(default = "default_true")]
    pub include_prs: bool,
    /// Include releases (default true)
    #[serde(default = "default_true")]
    pub include_releases: bool,
    /// Include closed items (default false)
    #[serde(default)]
    pub include_closed: bool,
    /// Include PR review comments (default false)
    #[serde(default)]
    pub include_review_comments: bool,
    /// Max items per API fetch (default 30)
    #[serde(default = "default_github_max_items")]
    pub max_items_per_fetch: usize,
}

#[derive(Debug, Clone, Deserialize)]
pub struct SlackConfig {
    pub enabled: bool,
    /// Slack Bot User OAuth Token (xoxb-...)
    pub bot_token: String,
    /// List of channel IDs to monitor (e.g. ["C01ABC123", "C02DEF456"])
    #[serde(default)]
    pub channels: Vec<String>,
    /// Polling interval in seconds (default 60)
    #[serde(default = "default_poll_interval")]
    pub poll_interval_secs: u64,
    /// Include thread replies (default true)
    #[serde(default = "default_true")]
    pub include_threads: bool,
}

#[derive(Debug, Clone, Deserialize)]
pub struct TelegramConfig {
    pub enabled: bool,
    /// Telegram Bot API token (from @BotFather)
    pub bot_token: String,
    /// List of chat IDs to monitor (None = all chats)
    #[serde(default)]
    pub allowed_chats: Option<Vec<i64>>,
    /// Include edited messages (default true)
    #[serde(default = "default_true")]
    pub include_edited: bool,
    /// Polling interval in seconds (default 30)
    #[serde(default = "default_telegram_interval")]
    pub poll_interval_secs: u64,
}

/// Sense plugin configuration for multimodal content parsing.
#[derive(Debug, Clone, Deserialize, Default)]
pub struct SenseConfig {
    /// Enable automatic sense parsing for media files.
    #[serde(default)]
    pub enabled: bool,
    /// ASR (speech-to-text) configuration.
    #[serde(default)]
    pub asr: Option<AsrSenseConfig>,
    /// OCR (image text recognition) configuration.
    #[serde(default)]
    pub ocr: Option<OcrSenseConfig>,
    /// Image understanding via multimodal LLM.
    #[serde(default)]
    pub image: Option<ImageSenseConfig>,
    /// Video frame extraction and analysis.
    #[serde(default)]
    pub video: Option<VideoSenseConfig>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct AsrSenseConfig {
    /// "whisper" for local, "api" for remote.
    #[serde(default = "default_asr_engine")]
    pub engine: String,
    #[serde(default)]
    pub api_url: Option<String>,
    #[serde(default)]
    pub api_key: Option<String>,
    #[serde(default = "default_whisper_model")]
    pub whisper_model: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct OcrSenseConfig {
    /// "tesseract" for local, "api" for remote.
    #[serde(default = "default_ocr_engine")]
    pub engine: String,
    #[serde(default)]
    pub api_url: Option<String>,
    #[serde(default)]
    pub api_key: Option<String>,
    #[serde(default = "default_tesseract_lang")]
    pub tesseract_lang: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ImageSenseConfig {
    pub model: String,
    pub api_url: String,
    #[serde(default)]
    pub api_key: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct VideoSenseConfig {
    #[serde(default = "default_frame_interval")]
    pub frame_interval_sec: u64,
    #[serde(default = "default_max_frames")]
    pub max_frames: usize,
    #[serde(default = "default_frame_analyzer")]
    pub frame_analyzer: String,
}

fn default_asr_engine() -> String {
    "whisper".into()
}
fn default_ocr_engine() -> String {
    "tesseract".into()
}
fn default_whisper_model() -> String {
    "base".into()
}
fn default_tesseract_lang() -> String {
    "chi_sim+eng".into()
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

#[derive(Debug, Clone, Deserialize)]
pub struct ProcessorConfig {
    #[serde(default = "default_true")]
    pub normalize_timestamps: bool,
    #[serde(default = "default_max_event_size")]
    pub max_event_size: usize,
    #[serde(default = "default_dedup_window")]
    pub dedup_window_secs: u64,
    /// Enable content classification (source category, content type, urgency).
    #[serde(default = "default_true")]
    pub enable_classify: bool,
    /// Enable content enrichment (entity extraction, keywords, summary).
    #[serde(default = "default_true")]
    pub enable_enrich: bool,
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
    /// Conflict resolution strategy when local and server events diverge.
    #[serde(default = "default_conflict_strategy")]
    pub conflict_strategy: String,
    /// Enable real-time event streaming alongside batch upload.
    /// When true, each event is also sent immediately via the stream endpoint.
    #[serde(default)]
    pub enable_streaming: bool,
}

fn default_conflict_strategy() -> String {
    "newest_wins".to_string()
}
#[allow(dead_code)]
fn default_streaming() -> bool {
    false
}

impl AppConfig {
    pub fn load(path: &str) -> Result<Self> {
        let content = std::fs::read_to_string(path)
            .with_context(|| format!("Failed to read config file: {}", path))?;
        let mut config: AppConfig =
            toml::from_str(&content).with_context(|| "Failed to parse config TOML")?;
        config.apply_env_overrides();
        Ok(config)
    }

    /// Validate configuration and return a list of warnings (non-fatal) or errors.
    /// Returns Ok(warnings) if config is valid, Err(message) if fatally invalid.
    pub fn validate(&self) -> Result<Vec<String>> {
        let mut warnings = Vec::new();

        // Daemon checks
        if self.daemon.node_id.is_empty() {
            anyhow::bail!("daemon.node_id must not be empty");
        }
        if self.daemon.status_port == 0 {
            warnings.push("daemon.status_port is 0 — status server will be disabled".into());
        }

        // Soul endpoint check
        if self.soul.endpoint.is_empty() {
            anyhow::bail!("soul.endpoint must not be empty");
        }
        if !self.soul.endpoint.starts_with("http") {
            warnings.push(format!(
                "soul.endpoint '{}' does not start with http — may fail to connect",
                self.soul.endpoint
            ));
        }

        // Collector checks
        if self.collector.watch_dirs.is_empty() {
            warnings.push(
                "collector.watch_dirs is empty — file collector will have nothing to watch".into(),
            );
        }
        for dir in &self.collector.watch_dirs {
            if !std::path::Path::new(dir).exists() {
                warnings.push(format!("collector.watch_dirs: '{}' does not exist", dir));
            }
        }

        // Connector credential checks
        if let Some(ref fc) = self.connector.feishu {
            if fc.enabled && (fc.app_id.is_empty() || fc.app_secret.is_empty()) {
                anyhow::bail!("feishu connector is enabled but app_id or app_secret is empty");
            }
        }
        if let Some(ref dc) = self.connector.dingtalk {
            if dc.enabled && (dc.app_key.is_empty() || dc.app_secret.is_empty()) {
                anyhow::bail!("dingtalk connector is enabled but app_key or app_secret is empty");
            }
        }
        if let Some(ref wc) = self.connector.wecom {
            if wc.enabled && (wc.corp_id.is_empty() || wc.secret.is_empty()) {
                anyhow::bail!("wecom connector is enabled but corp_id or secret is empty");
            }
        }
        if let Some(ref ec) = self.connector.email {
            if ec.enabled && ec.accounts.is_empty() {
                anyhow::bail!("email connector is enabled but no accounts configured");
            }
            for (i, account) in ec.accounts.iter().enumerate() {
                if account.username.is_empty() || account.password.is_empty() {
                    warnings.push(format!(
                        "email account[{}] '{}' has empty credentials",
                        i, account.name
                    ));
                }
            }
        }
        if let Some(ref nc) = self.connector.notion {
            if nc.enabled && nc.integration_token.is_empty() {
                anyhow::bail!("notion connector is enabled but integration_token is empty");
            }
        }
        if let Some(ref gc) = self.connector.github {
            if gc.enabled && gc.repos.is_empty() {
                anyhow::bail!("github connector is enabled but no repos configured");
            }
            if gc.enabled && gc.token.is_none() {
                warnings.push("github connector has no token — rate limits will be lower".into());
            }
        }
        if let Some(ref rc) = self.connector.rss {
            if rc.enabled && rc.feeds.is_empty() {
                anyhow::bail!("rss connector is enabled but no feeds configured");
            }
        }

        // Git connector checks
        if let Some(ref gc) = self.connector.git {
            if gc.enabled && gc.repo_url.is_empty() {
                anyhow::bail!("git connector is enabled but repo_url is empty");
            }
            if gc.enabled && gc.local_path.is_empty() {
                anyhow::bail!("git connector is enabled but local_path is empty");
            }
        }

        // Obsidian connector checks
        if let Some(ref oc) = self.connector.obsidian {
            if oc.enabled && oc.vault_path.is_empty() {
                anyhow::bail!("obsidian connector is enabled but vault_path is empty");
            }
            if oc.enabled && !std::path::Path::new(&oc.vault_path).exists() {
                warnings.push(format!(
                    "obsidian vault_path '{}' does not exist",
                    oc.vault_path
                ));
            }
        }

        // Webhook connector checks
        if let Some(ref wc) = self.connector.webhook {
            if wc.enabled && wc.listen.is_empty() {
                anyhow::bail!("webhook connector is enabled but listen address is empty");
            }
        }

        // Slack connector checks
        if let Some(ref sc) = self.connector.slack {
            if sc.enabled && sc.bot_token.is_empty() {
                anyhow::bail!("slack connector is enabled but bot_token is empty");
            }
            if sc.enabled && !sc.bot_token.starts_with("xoxb-") {
                warnings
                    .push("slack bot_token does not start with 'xoxb-' — may be invalid".into());
            }
            if sc.enabled && sc.channels.is_empty() {
                warnings.push("slack connector has no channels configured — will auto-discover joined channels".into());
            }
        }

        // Telegram connector checks
        if let Some(ref tc) = self.connector.telegram {
            if tc.enabled && tc.bot_token.is_empty() {
                anyhow::bail!("telegram connector is enabled but bot_token is empty");
            }
            if tc.enabled && !tc.bot_token.contains(':') {
                warnings.push("telegram bot_token does not contain ':' separator — may be invalid".into());
            }
        }

        // Streaming check
        if self.sync.enable_streaming && self.sync.batch_size == 0 {
            warnings
                .push("sync.enable_streaming=true but batch_size=0 — streaming may help".into());
        }

        // Sync checks
        if self.sync.batch_size == 0 {
            warnings.push("sync.batch_size is 0 — events will never be uploaded".into());
        }
        if self.sync.max_retries > 10 {
            warnings.push(format!(
                "sync.max_retries is {} — this may cause very long delays on failure",
                self.sync.max_retries
            ));
        }

        // Processor checks
        if self.processor.max_event_size < 1024 {
            warnings.push(format!(
                "processor.max_event_size is only {} bytes — most events will be dropped",
                self.processor.max_event_size
            ));
        }

        Ok(warnings)
    }

    /// Override config values from environment variables.
    /// Convention: `OPENSOMA_<SECTION>_<FIELD>` in uppercase.
    /// Examples:
    ///   OPENSOMA_DAEMON_NODE_ID
    ///   OPENSOMA_SOUL_ENDPOINT
    ///   OPENSOMA_CONNECTOR_DINGTALK_APP_KEY
    ///   OPENSOMA_CONNECTOR_WECOM_SECRET
    ///   OPENSOMA_CONNECTOR_EMAIL_ACCOUNTS (JSON array)
    fn apply_env_overrides(&mut self) {
        if let Ok(v) = std::env::var("OPENSOMA_DAEMON_NODE_ID") {
            self.daemon.node_id = v;
        }
        if let Ok(v) = std::env::var("OPENSOMA_DAEMON_LOG_LEVEL") {
            self.daemon.log_level = v;
        }
        if let Ok(v) = std::env::var("OPENSOMA_DAEMON_DATA_DIR") {
            self.daemon.data_dir = v;
        }
        if let Ok(v) = std::env::var("OPENSOMA_SOUL_ENDPOINT") {
            self.soul.endpoint = v;
        }
        if let Ok(v) = std::env::var("OPENSOMA_SOUL_HEARTBEAT_INTERVAL") {
            if let Ok(n) = v.parse() {
                self.soul.heartbeat_interval = n;
            }
        }

        // Feishu overrides
        if let Some(ref mut fc) = self.connector.feishu {
            if let Ok(v) = std::env::var("OPENSOMA_CONNECTOR_FEISHU_APP_ID") {
                fc.app_id = v;
            }
            if let Ok(v) = std::env::var("OPENSOMA_CONNECTOR_FEISHU_APP_SECRET") {
                fc.app_secret = v;
            }
        }

        // DingTalk overrides
        if let Some(ref mut dc) = self.connector.dingtalk {
            if let Ok(v) = std::env::var("OPENSOMA_CONNECTOR_DINGTALK_APP_KEY") {
                dc.app_key = v;
            }
            if let Ok(v) = std::env::var("OPENSOMA_CONNECTOR_DINGTALK_APP_SECRET") {
                dc.app_secret = v;
            }
            if let Ok(v) = std::env::var("OPENSOMA_CONNECTOR_DINGTALK_AGENT_ID") {
                dc.agent_id = v;
            }
        }

        // WeCom overrides
        if let Some(ref mut wc) = self.connector.wecom {
            if let Ok(v) = std::env::var("OPENSOMA_CONNECTOR_WECOM_CORP_ID") {
                wc.corp_id = v;
            }
            if let Ok(v) = std::env::var("OPENSOMA_CONNECTOR_WECOM_SECRET") {
                wc.secret = v;
            }
            if let Ok(v) = std::env::var("OPENSOMA_CONNECTOR_WECOM_AGENT_ID") {
                wc.agent_id = v;
            }
        }

        // GitHub overrides
        if let Some(ref mut gc) = self.connector.github {
            if let Ok(v) = std::env::var("OPENSOMA_CONNECTOR_GITHUB_TOKEN") {
                gc.token = Some(v);
            }
            if let Ok(v) = std::env::var("OPENSOMA_CONNECTOR_GITHUB_REPOS") {
                gc.repos = v
                    .split(',')
                    .map(|s| s.trim().to_string())
                    .filter(|s| !s.is_empty())
                    .collect();
            }
        }

        // RSS overrides
        if let Some(ref mut rc) = self.connector.rss {
            if let Ok(v) = std::env::var("OPENSOMA_CONNECTOR_RSS_FEEDS") {
                if let Ok(feeds) = serde_json::from_str::<Vec<RssFeedConfig>>(&v) {
                    rc.feeds = feeds;
                }
            }
        }

        // Email overrides
        if let Some(ref mut ec) = self.connector.email {
            if let Ok(v) = std::env::var("OPENSOMA_CONNECTOR_EMAIL_ACCOUNTS") {
                if let Ok(accounts) = serde_json::from_str::<Vec<EmailAccountConfig>>(&v) {
                    ec.accounts = accounts;
                }
            }
        }

        // Notion overrides
        if let Some(ref mut nc) = self.connector.notion {
            if let Ok(v) = std::env::var("OPENSOMA_CONNECTOR_NOTION_TOKEN") {
                nc.integration_token = v;
            }
            if let Ok(v) = std::env::var("OPENSOMA_CONNECTOR_NOTION_DATABASE_ID") {
                nc.database_id = v;
            }
        }

        // Git overrides
        if let Some(ref mut gc) = self.connector.git {
            if let Ok(v) = std::env::var("OPENSOMA_CONNECTOR_GIT_REPO_URL") {
                gc.repo_url = v;
            }
            if let Ok(v) = std::env::var("OPENSOMA_CONNECTOR_GIT_LOCAL_PATH") {
                gc.local_path = v;
            }
            if let Ok(v) = std::env::var("OPENSOMA_CONNECTOR_GIT_BRANCH") {
                gc.branch = v;
            }
        }

        // Obsidian overrides
        if let Some(ref mut oc) = self.connector.obsidian {
            if let Ok(v) = std::env::var("OPENSOMA_CONNECTOR_OBSIDIAN_VAULT_PATH") {
                oc.vault_path = v;
            }
        }

        // Webhook overrides
        if let Some(ref mut wc) = self.connector.webhook {
            if let Ok(v) = std::env::var("OPENSOMA_CONNECTOR_WEBHOOK_LISTEN") {
                wc.listen = v;
            }
            if let Ok(v) = std::env::var("OPENSOMA_CONNECTOR_WEBHOOK_SECRET") {
                wc.secret = Some(v);
            }
        }

        // Slack overrides
        if let Some(ref mut sc) = self.connector.slack {
            if let Ok(v) = std::env::var("OPENSOMA_CONNECTOR_SLACK_BOT_TOKEN") {
                sc.bot_token = v;
            }
            if let Ok(v) = std::env::var("OPENSOMA_CONNECTOR_SLACK_CHANNELS") {
                sc.channels = v
                    .split(',')
                    .map(|s| s.trim().to_string())
                    .filter(|s| !s.is_empty())
                    .collect();
            }
            if let Ok(v) = std::env::var("OPENSOMA_CONNECTOR_SLACK_POLL_INTERVAL") {
                if let Ok(n) = v.parse() {
                    sc.poll_interval_secs = n;
                }
            }
        }

        // Telegram overrides
        if let Some(ref mut tc) = self.connector.telegram {
            if let Ok(v) = std::env::var("OPENSOMA_CONNECTOR_TELEGRAM_BOT_TOKEN") {
                tc.bot_token = v;
            }
            if let Ok(v) = std::env::var("OPENSOMA_CONNECTOR_TELEGRAM_ALLOWED_CHATS") {
                tc.allowed_chats = Some(
                    v.split(',')
                        .filter_map(|s| s.trim().parse::<i64>().ok())
                        .collect(),
                );
            }
            if let Ok(v) = std::env::var("OPENSOMA_CONNECTOR_TELEGRAM_POLL_INTERVAL") {
                if let Ok(n) = v.parse() {
                    tc.poll_interval_secs = n;
                }
            }
        }

        // Streaming override
        if let Ok(v) = std::env::var("OPENSOMA_SYNC_ENABLE_STREAMING") {
            self.sync.enable_streaming = v == "true" || v == "1";
        }
    }
}

/// Start a file watcher on the config path. Returns a handle and a receiver
/// that emits the new AppConfig whenever the file changes.
pub fn watch_config(
    path: &str,
) -> Result<(tokio::task::JoinHandle<()>, watch::Receiver<Arc<AppConfig>>)> {
    let initial = AppConfig::load(path)?;
    let (tx, rx) = watch::channel(Arc::new(initial));
    let path_buf = Path::new(path).to_path_buf();
    let path_owned = path.to_string();

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

        if let Err(e) = watcher.watch(
            path_buf.parent().unwrap_or(Path::new(".")),
            RecursiveMode::NonRecursive,
        ) {
            warn!("Failed to watch config directory: {}", e);
            return;
        }

        info!(
            "Config hot-reload watcher started for: {}",
            path_buf.display()
        );

        loop {
            match notify_rx.recv() {
                Ok(Ok(event)) => {
                    if event.paths.iter().any(|p| p == &path_buf) {
                        match AppConfig::load(&path_owned) {
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
fn default_status_port() -> u16 {
    8091
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
fn default_process_interval_ms() -> u64 {
    5000
}
fn default_network_interval_ms() -> u64 {
    10000
}
fn default_clipboard_interval_ms() -> u64 {
    2000
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
fn default_poll_interval() -> u64 {
    60
}
fn default_rss_interval() -> u64 {
    300
}
fn default_email_interval() -> u64 {
    120
}
fn default_telegram_interval() -> u64 {
    30
}
fn default_imap_port() -> u16 {
    993
}
fn default_inbox_folder() -> String {
    "INBOX".into()
}
fn default_notion_interval() -> u64 {
    120
}
fn default_git_branch() -> String {
    "main".into()
}
fn default_git_interval() -> u64 {
    300
}
fn default_obsidian_debounce() -> u64 {
    500
}
fn default_webhook_listen() -> String {
    "0.0.0.0:9800".to_string()
}

fn default_github_interval() -> u64 {
    300
}

fn default_github_max_items() -> usize {
    30
}

#[cfg(test)]
mod tests {
    use super::*;

    fn minimal_config_toml() -> String {
        r#"
[daemon]
node_id = "test-node"

[soul]
endpoint = "http://localhost:8090"

[collector]
watch_dirs = []

[connector]

[processor]

[sync]
"#
        .to_string()
    }

    #[test]
    fn test_load_minimal_config() {
        let config: AppConfig = toml::from_str(&minimal_config_toml()).unwrap();
        assert_eq!(config.daemon.node_id, "test-node");
        assert_eq!(config.soul.endpoint, "http://localhost:8090");
    }

    #[test]
    fn test_validate_minimal_config() {
        let config: AppConfig = toml::from_str(&minimal_config_toml()).unwrap();
        let warnings = config.validate().unwrap();
        // Should have at least a warning about empty watch_dirs
        assert!(warnings.iter().any(|w| w.contains("watch_dirs")));
    }

    #[test]
    fn test_validate_empty_node_id() {
        let toml = r#"
[daemon]
node_id = ""

[soul]
endpoint = "http://localhost:8090"

[collector]
watch_dirs = []

[connector]

[processor]

[sync]
"#;
        let config: AppConfig = toml::from_str(toml).unwrap();
        let result = config.validate();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("node_id"));
    }

    #[test]
    fn test_validate_feishu_missing_credentials() {
        let toml = r#"
[daemon]
node_id = "test"

[soul]
endpoint = "http://localhost:8090"

[collector]
watch_dirs = []

[connector]

[processor]

[sync]

[connector.feishu]
enabled = true
app_id = ""
app_secret = ""
"#;
        let config: AppConfig = toml::from_str(toml).unwrap();
        let result = config.validate();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("feishu"));
    }

    #[test]
    fn test_validate_github_no_token_warning() {
        let toml = r#"
[daemon]
node_id = "test"

[soul]
endpoint = "http://localhost:8090"

[collector]
watch_dirs = []

[processor]

[sync]

[connector.github]
enabled = true
repos = ["owner/repo"]
"#;
        let config: AppConfig = toml::from_str(toml).unwrap();
        let warnings = config.validate().unwrap();
        assert!(warnings
            .iter()
            .any(|w| w.contains("token") && w.contains("rate")));
    }

    #[test]
    fn test_validate_zero_batch_size_warning() {
        let toml = r#"
[daemon]
node_id = "test"

[soul]
endpoint = "http://localhost:8090"

[collector]
watch_dirs = []

[connector]

[processor]

[sync]
batch_size = 0
"#;
        let config: AppConfig = toml::from_str(toml).unwrap();
        let warnings = config.validate().unwrap();
        assert!(warnings.iter().any(|w| w.contains("batch_size")));
    }

    #[test]
    fn test_validate_non_http_endpoint_warning() {
        let toml = r#"
[daemon]
node_id = "test"

[soul]
endpoint = "localhost:8090"

[collector]
watch_dirs = []

[connector]

[processor]

[sync]
"#;
        let config: AppConfig = toml::from_str(toml).unwrap();
        let warnings = config.validate().unwrap();
        assert!(warnings.iter().any(|w| w.contains("http")));
    }

    #[test]
    fn test_env_override_node_id() {
        std::env::set_var("OPENSOMA_DAEMON_NODE_ID", "env-node");
        let mut config: AppConfig = toml::from_str(&minimal_config_toml()).unwrap();
        config.apply_env_overrides();
        assert_eq!(config.daemon.node_id, "env-node");
        std::env::remove_var("OPENSOMA_DAEMON_NODE_ID");
    }

    #[test]
    fn test_default_values() {
        assert_eq!(default_status_port(), 8091);
        assert_eq!(default_heartbeat_interval(), 30);
        assert_eq!(default_connect_timeout(), 10);
        assert_eq!(default_batch_size(), 50);
        assert_eq!(default_upload_interval(), 10);
        assert_eq!(default_max_retries(), 5);
        assert_eq!(default_retry_backoff(), 1000);
        assert_eq!(default_cache_size(), 512);
        assert_eq!(default_max_event_size(), 1_048_576);
        assert_eq!(default_dedup_window(), 300);
        assert_eq!(default_imap_port(), 993);
        assert_eq!(default_inbox_folder(), "INBOX");
        assert!(default_true());
    }

    #[test]
    fn test_validate_git_missing_repo_url() {
        let toml = r#"
[daemon]
node_id = "test"

[soul]
endpoint = "http://localhost:8090"

[collector]
watch_dirs = []

[connector]
[processor]
[sync]

[connector.git]
enabled = true
repo_url = ""
local_path = "/tmp/repo"
"#;
        let config: AppConfig = toml::from_str(toml).unwrap();
        let result = config.validate();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("repo_url"));
    }

    #[test]
    fn test_validate_obsidian_missing_vault() {
        let toml = r#"
[daemon]
node_id = "test"

[soul]
endpoint = "http://localhost:8090"

[collector]
watch_dirs = []

[connector]
[processor]
[sync]

[connector.obsidian]
enabled = true
vault_path = ""
"#;
        let config: AppConfig = toml::from_str(toml).unwrap();
        let result = config.validate();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("vault_path"));
    }

    #[test]
    fn test_validate_webhook_empty_listen() {
        let toml = r#"
[daemon]
node_id = "test"

[soul]
endpoint = "http://localhost:8090"

[collector]
watch_dirs = []

[connector]
[processor]
[sync]

[connector.webhook]
enabled = true
listen = ""
"#;
        let config: AppConfig = toml::from_str(toml).unwrap();
        let result = config.validate();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("listen"));
    }

    #[test]
    fn test_env_override_github_token() {
        std::env::set_var("OPENSOMA_CONNECTOR_GITHUB_TOKEN", "ghp_test123");
        let toml = r#"
[daemon]
node_id = "test"

[soul]
endpoint = "http://localhost:8090"

[collector]
watch_dirs = []

[processor]
[sync]

[connector.github]
enabled = true
repos = ["owner/repo"]
"#;
        let mut config: AppConfig = toml::from_str(toml).unwrap();
        config.apply_env_overrides();
        assert_eq!(
            config.connector.github.unwrap().token.unwrap(),
            "ghp_test123"
        );
        std::env::remove_var("OPENSOMA_CONNECTOR_GITHUB_TOKEN");
    }

    #[test]
    fn test_env_override_notion() {
        std::env::set_var("OPENSOMA_CONNECTOR_NOTION_TOKEN", "secret_test");
        std::env::set_var("OPENSOMA_CONNECTOR_NOTION_DATABASE_ID", "db123");
        let toml = r#"
[daemon]
node_id = "test"

[soul]
endpoint = "http://localhost:8090"

[collector]
watch_dirs = []

[processor]
[sync]

[connector.notion]
enabled = true
integration_token = ""
database_id = ""
"#;
        let mut config: AppConfig = toml::from_str(toml).unwrap();
        config.apply_env_overrides();
        let nc = config.connector.notion.unwrap();
        assert_eq!(nc.integration_token, "secret_test");
        assert_eq!(nc.database_id, "db123");
        std::env::remove_var("OPENSOMA_CONNECTOR_NOTION_TOKEN");
        std::env::remove_var("OPENSOMA_CONNECTOR_NOTION_DATABASE_ID");
    }

    #[test]
    fn test_env_override_git() {
        std::env::set_var(
            "OPENSOMA_CONNECTOR_GIT_REPO_URL",
            "https://github.com/test/repo.git",
        );
        std::env::set_var("OPENSOMA_CONNECTOR_GIT_BRANCH", "develop");
        let toml = r#"
[daemon]
node_id = "test"

[soul]
endpoint = "http://localhost:8090"

[collector]
watch_dirs = []

[processor]
[sync]

[connector.git]
enabled = true
repo_url = ""
local_path = "/tmp/repo"
"#;
        let mut config: AppConfig = toml::from_str(toml).unwrap();
        config.apply_env_overrides();
        let gc = config.connector.git.unwrap();
        assert_eq!(gc.repo_url, "https://github.com/test/repo.git");
        assert_eq!(gc.branch, "develop");
        std::env::remove_var("OPENSOMA_CONNECTOR_GIT_REPO_URL");
        std::env::remove_var("OPENSOMA_CONNECTOR_GIT_BRANCH");
    }

    #[test]
    fn test_env_override_obsidian() {
        std::env::set_var("OPENSOMA_CONNECTOR_OBSIDIAN_VAULT_PATH", "/home/user/vault");
        let toml = r#"
[daemon]
node_id = "test"

[soul]
endpoint = "http://localhost:8090"

[collector]
watch_dirs = []

[processor]
[sync]

[connector.obsidian]
enabled = true
vault_path = ""
"#;
        let mut config: AppConfig = toml::from_str(toml).unwrap();
        config.apply_env_overrides();
        assert_eq!(
            config.connector.obsidian.unwrap().vault_path,
            "/home/user/vault"
        );
        std::env::remove_var("OPENSOMA_CONNECTOR_OBSIDIAN_VAULT_PATH");
    }

    #[test]
    fn test_sense_config_deserialize_full() {
        let toml = r#"
[daemon]
node_id = "test"

[soul]
endpoint = "http://localhost:8090"

[collector]
watch_dirs = []

[connector]

[processor]
[sync]

[sense]
enabled = true

[sense.asr]
engine = "whisper"
whisper_model = "small"

[sense.ocr]
engine = "tesseract"
tesseract_lang = "eng"

[sense.image]
model = "gpt-4o"
api_url = "https://api.openai.com/v1/chat/completions"

[sense.video]
frame_interval_sec = 10
max_frames = 30
frame_analyzer = "ocr"
"#;
        let config: AppConfig = toml::from_str(toml).unwrap();
        assert!(config.sense.enabled);
        assert!(config.sense.asr.is_some());
        assert!(config.sense.ocr.is_some());
        assert!(config.sense.image.is_some());
        assert!(config.sense.video.is_some());

        let asr = config.sense.asr.as_ref().unwrap();
        assert_eq!(asr.engine, "whisper");
        assert_eq!(asr.whisper_model, "small");

        let ocr = config.sense.ocr.as_ref().unwrap();
        assert_eq!(ocr.engine, "tesseract");
        assert_eq!(ocr.tesseract_lang, "eng");

        let image = config.sense.image.as_ref().unwrap();
        assert_eq!(image.model, "gpt-4o");

        let video = config.sense.video.as_ref().unwrap();
        assert_eq!(video.frame_interval_sec, 10);
        assert_eq!(video.max_frames, 30);
        assert_eq!(video.frame_analyzer, "ocr");
    }

    #[test]
    fn test_sense_config_defaults() {
        let toml = r#"
[daemon]
node_id = "test"

[soul]
endpoint = "http://localhost:8090"

[collector]
watch_dirs = []

[connector]

[processor]
[sync]
"#;
        let config: AppConfig = toml::from_str(toml).unwrap();
        assert!(!config.sense.enabled);
        assert!(config.sense.asr.is_none());
        assert!(config.sense.ocr.is_none());
        assert!(config.sense.image.is_none());
        assert!(config.sense.video.is_none());
    }

    #[test]
    fn test_slack_connector_config() {
        let toml = r#"
[daemon]
node_id = "test"

[soul]
endpoint = "http://localhost:8090"

[collector]
watch_dirs = []

[processor]
[sync]

[connector.slack]
enabled = true
bot_token = "xoxb-test"
channels = ["C01ABC", "C02DEF"]
poll_interval_secs = 30
include_threads = true
"#;
        let config: AppConfig = toml::from_str(toml).unwrap();
        let slack = config.connector.slack.unwrap();
        assert!(slack.enabled);
        assert_eq!(slack.bot_token, "xoxb-test");
        assert_eq!(slack.channels.len(), 2);
        assert_eq!(slack.poll_interval_secs, 30);
        assert!(slack.include_threads);
    }

    #[test]
    fn test_telegram_connector_config() {
        let toml = r#"
[daemon]
node_id = "test"

[soul]
endpoint = "http://localhost:8090"

[collector]
watch_dirs = []

[processor]
[sync]

[connector.telegram]
enabled = true
bot_token = "123456:ABC-DEF1234ghIkl-zyx57W2v1u123ew11"
allowed_chats = [123456789, -100987654]
poll_interval_secs = 15
include_edited = false
"#;
        let config: AppConfig = toml::from_str(toml).unwrap();
        let telegram = config.connector.telegram.unwrap();
        assert!(telegram.enabled);
        assert_eq!(telegram.bot_token, "123456:ABC-DEF1234ghIkl-zyx57W2v1u123ew11");
        assert_eq!(telegram.allowed_chats.unwrap(), vec![123456789, -100987654]);
        assert_eq!(telegram.poll_interval_secs, 15);
        assert!(!telegram.include_edited);
    }

    #[test]
    fn test_telegram_connector_validation_empty_token() {
        let toml = r#"
[daemon]
node_id = "test"

[soul]
endpoint = "http://localhost:8090"

[collector]
watch_dirs = []

[processor]
[sync]

[connector.telegram]
enabled = true
bot_token = ""
"#;
        let config: AppConfig = toml::from_str(toml).unwrap();
        let result = config.validate();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("bot_token is empty"));
    }

    #[test]
    fn test_telegram_connector_defaults() {
        let toml = r#"
[daemon]
node_id = "test"

[soul]
endpoint = "http://localhost:8090"

[collector]
watch_dirs = []

[processor]
[sync]

[connector.telegram]
enabled = true
bot_token = "123456:ABC"
"#;
        let config: AppConfig = toml::from_str(toml).unwrap();
        let telegram = config.connector.telegram.unwrap();
        assert!(telegram.include_edited); // default true
        assert_eq!(telegram.poll_interval_secs, 30); // default
        assert!(telegram.allowed_chats.is_none()); // default None
    }

    #[test]
    fn test_sync_config_all_fields() {
        let toml = r#"
[daemon]
node_id = "test"

[soul]
endpoint = "http://localhost:8090"

[collector]
watch_dirs = []

[connector]

[processor]

[sync]
batch_size = 100
upload_interval = 30
max_retries = 10
retry_backoff_ms = 2000
cache_size_mb = 1024
"#;
        let config: AppConfig = toml::from_str(toml).unwrap();
        assert_eq!(config.sync.batch_size, 100);
        assert_eq!(config.sync.upload_interval, 30);
        assert_eq!(config.sync.max_retries, 10);
        assert_eq!(config.sync.retry_backoff_ms, 2000);
        assert_eq!(config.sync.cache_size_mb, 1024);
    }

    #[test]
    fn test_github_config_with_review_comments() {
        let toml = r#"
[daemon]
node_id = "test"

[soul]
endpoint = "http://localhost:8090"

[collector]
watch_dirs = []

[connector.github]
enabled = true
repos = ["owner/repo"]
include_review_comments = true
include_releases = false

[processor]

[sync]
"#;
        let config: AppConfig = toml::from_str(toml).unwrap();
        let github = config.connector.github.unwrap();
        assert!(github.enabled);
        assert!(github.include_review_comments);
        assert!(!github.include_releases);
        assert_eq!(github.repos, vec!["owner/repo"]);
    }

    #[test]
    fn test_github_config_review_comments_default() {
        let toml = r#"
[daemon]
node_id = "test"

[soul]
endpoint = "http://localhost:8090"

[collector]
watch_dirs = []

[connector.github]
enabled = true
repos = ["org/project"]

[processor]

[sync]
"#;
        let config: AppConfig = toml::from_str(toml).unwrap();
        let github = config.connector.github.unwrap();
        assert!(!github.include_review_comments); // default false
        assert!(github.include_releases); // default true
        assert!(github.include_issues); // default true
        assert!(github.include_prs); // default true
    }
}
