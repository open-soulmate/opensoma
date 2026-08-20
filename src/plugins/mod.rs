//! Plugin registry — dynamic plugin lifecycle management for OpenSoma.
//!
//! Plugins are modular components that extend OpenSoma's capabilities
//! (e.g. media parsers, format converters, enrichment services).
//! The registry handles registration, discovery, lifecycle (init/start/stop),
//! health monitoring, and configuration.

pub mod sense;

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

// ─── Plugin Types ────────────────────────────────────────────────────────

/// Category of a plugin for classification and routing.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum PluginCategory {
    /// Multimodal media parsing (OCR, ASR, image, video).
    Sense,
    /// Data enrichment (NER, summarization, embedding).
    Enrich,
    /// Data transformation / format conversion.
    Transform,
    /// External service integration.
    Connector,
    /// Storage backend.
    Storage,
    /// Custom / unclassified.
    Custom,
}

/// Lifecycle state of a plugin.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PluginState {
    /// Registered but not yet initialized.
    Registered,
    /// Initialization in progress.
    Initializing,
    /// Ready and running.
    Active,
    /// Temporarily paused (e.g. dependency unavailable).
    Paused,
    /// Encountered a non-fatal error; will auto-retry.
    Degraded,
    /// Fatal error; manual intervention required.
    Error,
    /// Gracefully shut down.
    Stopped,
}

/// Metadata describing a plugin's identity and capabilities.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PluginInfo {
    /// Unique plugin identifier (e.g. "sense.ocr", "enrich.ner").
    pub id: String,
    /// Human-readable display name.
    pub name: String,
    /// Short description.
    pub description: String,
    /// SemVer version string.
    pub version: String,
    /// Plugin category.
    pub category: PluginCategory,
    /// MIME types or file extensions this plugin handles (for sense plugins).
    #[serde(default)]
    pub supported_types: Vec<String>,
    /// Plugin author / maintainer.
    #[serde(default)]
    pub author: String,
    /// Priority for conflict resolution (lower = higher priority).
    #[serde(default = "default_priority")]
    pub priority: u32,
}

fn default_priority() -> u32 {
    100
}

/// Health snapshot for a single plugin.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PluginHealth {
    pub plugin_id: String,
    pub state: PluginState,
    pub uptime_secs: u64,
    pub requests_handled: u64,
    pub errors: u64,
    pub last_error: Option<String>,
    pub last_active_secs_ago: u64,
}

// ─── Plugin Trait ─────────────────────────────────────────────────────────

/// Core trait that every OpenSoma plugin must implement.
#[async_trait::async_trait]
pub trait Plugin: Send + Sync {
    /// Return static metadata about this plugin.
    fn info(&self) -> PluginInfo;

    /// Initialize the plugin (allocate resources, verify dependencies).
    async fn init(&mut self) -> Result<()> {
        Ok(())
    }

    /// Start the plugin's main loop / accept work.
    async fn start(&mut self) -> Result<()> {
        Ok(())
    }

    /// Gracefully stop the plugin.
    async fn stop(&mut self) -> Result<()> {
        Ok(())
    }

    /// Handle a dynamic request (e.g. from the HTTP API or nerve bus).
    /// Returns a JSON response.
    async fn handle_request(&self, _method: &str, _payload: &[u8]) -> Result<serde_json::Value> {
        anyhow::bail!("plugin does not handle dynamic requests")
    }
}

// ─── Sense Plugin Adapter ────────────────────────────────────────────────

/// Wraps a `sense::SensePlugin` so it can live in the `PluginRegistry`.
pub struct SensePluginAdapter {
    inner: Box<dyn sense::SensePlugin>,
    info_data: PluginInfo,
}

impl SensePluginAdapter {
    pub fn new(inner: Box<dyn sense::SensePlugin>, category_suffix: &str, mime_types: Vec<String>) -> Self {
        let id = format!("sense.{}", category_suffix);
        let name = inner.name().to_string();
        Self {
            inner,
            info_data: PluginInfo {
                id,
                name,
                description: format!("Sense plugin for {}", category_suffix),
                version: env!("CARGO_PKG_VERSION").to_string(),
                category: PluginCategory::Sense,
                supported_types: mime_types,
                author: "opensoma".to_string(),
                priority: 50,
            },
        }
    }
}

#[async_trait::async_trait]
impl Plugin for SensePluginAdapter {
    fn info(&self) -> PluginInfo {
        self.info_data.clone()
    }

    async fn handle_request(&self, _method: &str, payload: &[u8]) -> Result<serde_json::Value> {
        let result = self.inner.parse(payload).await?;
        Ok(serde_json::to_value(&result)?)
    }
}

// ─── Plugin Registry ─────────────────────────────────────────────────────

/// Internal entry wrapping a registered plugin with runtime state.
struct PluginEntry {
    info: PluginInfo,
    state: PluginState,
    /// The plugin instance (behind RwLock for interior mutability).
    plugin: Arc<RwLock<dyn Plugin>>,
    /// Timestamp when the plugin was registered.
    registered_at: std::time::Instant,
    /// Timestamp of last successful activity.
    last_active: std::time::Instant,
    /// Total requests handled.
    requests: u64,
    /// Total errors.
    errors: u64,
    /// Last error message (if any).
    last_error: Option<String>,
}

/// Central registry for all OpenSoma plugins.
///
/// Thread-safe; shared across the daemon via `Arc<PluginRegistry>`.
pub struct PluginRegistry {
    entries: RwLock<HashMap<String, PluginEntry>>,
}

impl PluginRegistry {
    /// Create an empty registry.
    pub fn new() -> Self {
        Self {
            entries: RwLock::new(HashMap::new()),
        }
    }

    /// Register a plugin. Overwrites any existing plugin with the same ID.
    pub async fn register(&self, plugin: impl Plugin + 'static) -> Result<()> {
        let info = plugin.info();
        let id = info.id.clone();

        info!(
            "Registering plugin: {} ({}) v{} [{}]",
            id, info.name, info.version, info.description
        );

        let entry = PluginEntry {
            info,
            state: PluginState::Registered,
            plugin: Arc::new(RwLock::new(plugin)),
            registered_at: std::time::Instant::now(),
            last_active: std::time::Instant::now(),
            requests: 0,
            errors: 0,
            last_error: None,
        };

        self.entries.write().await.insert(id.clone(), entry);
        debug!("Plugin '{}' registered successfully", id);
        Ok(())
    }

    /// Initialize, then start a plugin by ID.
    pub async fn activate(&self, id: &str) -> Result<()> {
        let mut entries = self.entries.write().await;
        let entry = entries
            .get_mut(id)
            .with_context(|| format!("Plugin '{}' not found", id))?;

        if entry.state == PluginState::Active {
            debug!("Plugin '{}' is already active", id);
            return Ok(());
        }

        entry.state = PluginState::Initializing;
        let plugin = entry.plugin.clone();
        drop(entries);

        // Init
        {
            let mut p = plugin.write().await;
            if let Err(e) = p.init().await {
                let mut entries = self.entries.write().await;
                if let Some(entry) = entries.get_mut(id) {
                    entry.state = PluginState::Error;
                    entry.last_error = Some(format!("init failed: {:#}", e));
                    entry.errors += 1;
                }
                tracing::error!("Plugin '{}' init failed: {:#}", id, e);
                return Err(e).context(format!("Plugin '{}' init failed", id));
            }
        }

        // Start
        {
            let mut p = plugin.write().await;
            if let Err(e) = p.start().await {
                let mut entries = self.entries.write().await;
                if let Some(entry) = entries.get_mut(id) {
                    entry.state = PluginState::Error;
                    entry.last_error = Some(format!("start failed: {:#}", e));
                    entry.errors += 1;
                }
                tracing::error!("Plugin '{}' start failed: {:#}", id, e);
                return Err(e).context(format!("Plugin '{}' start failed", id));
            }
        }

        let mut entries = self.entries.write().await;
        if let Some(entry) = entries.get_mut(id) {
            entry.state = PluginState::Active;
            entry.last_active = std::time::Instant::now();
        }
        info!("Plugin '{}' activated", id);
        Ok(())
    }

    /// Stop a plugin by ID.
    pub async fn deactivate(&self, id: &str) -> Result<()> {
        let entries = self.entries.read().await;
        let entry = entries
            .get(id)
            .with_context(|| format!("Plugin '{}' not found", id))?;

        if entry.state != PluginState::Active {
            debug!("Plugin '{}' is not active (state: {:?})", id, entry.state);
            return Ok(());
        }

        let plugin = entry.plugin.clone();
        drop(entries);

        {
            let mut p = plugin.write().await;
            if let Err(e) = p.stop().await {
                warn!("Plugin '{}' stop error (non-fatal): {:#}", id, e);
            }
        }

        let mut entries = self.entries.write().await;
        if let Some(entry) = entries.get_mut(id) {
            entry.state = PluginState::Stopped;
        }
        info!("Plugin '{}' deactivated", id);
        Ok(())
    }

    /// Activate all registered plugins.
    pub async fn activate_all(&self) -> Vec<(String, Result<()>)> {
        let ids: Vec<String> = self.entries.read().await.keys().cloned().collect();
        let mut results = Vec::new();
        for id in ids {
            let result = self.activate(&id).await;
            results.push((id, result));
        }
        results
    }

    /// Get info for all registered plugins.
    pub async fn list(&self) -> Vec<PluginInfo> {
        self.entries
            .read()
            .await
            .values()
            .map(|e| e.info.clone())
            .collect()
    }

    /// Get health for all plugins.
    pub async fn health_all(&self) -> Vec<PluginHealth> {
        let entries = self.entries.read().await;
        let mut healths = Vec::new();

        for entry in entries.values() {
            let uptime = entry.registered_at.elapsed().as_secs();
            healths.push(PluginHealth {
                plugin_id: entry.info.id.clone(),
                state: entry.state.clone(),
                uptime_secs: uptime,
                requests_handled: entry.requests,
                errors: entry.errors,
                last_error: entry.last_error.clone(),
                last_active_secs_ago: entry.last_active.elapsed().as_secs(),
            });
        }

        healths
    }

    /// Get health for a single plugin.
    pub async fn health(&self, id: &str) -> Result<PluginHealth> {
        let entries = self.entries.read().await;
        let entry = entries
            .get(id)
            .with_context(|| format!("Plugin '{}' not found", id))?;

        let uptime = entry.registered_at.elapsed().as_secs();
        Ok(PluginHealth {
            plugin_id: entry.info.id.clone(),
            state: entry.state.clone(),
            uptime_secs: uptime,
            requests_handled: entry.requests,
            errors: entry.errors,
            last_error: entry.last_error.clone(),
            last_active_secs_ago: entry.last_active.elapsed().as_secs(),
        })
    }

    /// Route a request to a specific plugin by ID.
    pub async fn dispatch(
        &self,
        plugin_id: &str,
        method: &str,
        payload: &[u8],
    ) -> Result<serde_json::Value> {
        let entries = self.entries.read().await;
        let entry = entries
            .get(plugin_id)
            .with_context(|| format!("Plugin '{}' not found", plugin_id))?;

        if entry.state != PluginState::Active {
            anyhow::bail!(
                "Plugin '{}' is not active (state: {:?})",
                plugin_id,
                entry.state
            );
        }

        let plugin = entry.plugin.clone();
        drop(entries);

        let result = plugin.read().await.handle_request(method, payload).await;

        // Update stats
        let mut entries = self.entries.write().await;
        if let Some(entry) = entries.get_mut(plugin_id) {
            entry.requests += 1;
            entry.last_active = std::time::Instant::now();
            if result.is_err() {
                entry.errors += 1;
                entry.last_error = result.as_ref().err().map(|e| format!("{:#}", e));
            }
        }

        result
    }

    /// Find plugins that handle a given MIME type or file extension.
    pub async fn find_by_type(&self, mime_or_ext: &str) -> Vec<PluginInfo> {
        let entries = self.entries.read().await;
        let needle = mime_or_ext.to_lowercase();
        entries
            .values()
            .filter(|e| {
                e.state == PluginState::Active
                    && e.info
                        .supported_types
                        .iter()
                        .any(|t| t.to_lowercase() == needle || needle.ends_with(&t.to_lowercase()))
            })
            .map(|e| e.info.clone())
            .collect()
    }

    /// Get total number of registered plugins.
    pub async fn count(&self) -> usize {
        self.entries.read().await.len()
    }

    /// Get number of active plugins.
    pub async fn active_count(&self) -> usize {
        self.entries
            .read()
            .await
            .values()
            .filter(|e| e.state == PluginState::Active)
            .count()
    }
}

impl Default for PluginRegistry {
    fn default() -> Self {
        Self::new()
    }
}

// ─── Convenience: build registry with all built-in sense plugins ──────────

/// Create a `PluginRegistry` pre-loaded with all built-in sense plugins.
pub fn build_default_registry() -> PluginRegistry {
    PluginRegistry::new()
}

/// Register the built-in sense plugins based on configuration.
pub async fn register_sense_plugins(
    registry: &PluginRegistry,
    sense_config: &crate::config::SenseConfig,
) -> Result<()> {
    if !sense_config.enabled {
        debug!("Sense plugins disabled in config");
        return Ok(());
    }

    // Register OCR plugin
    if let Some(ref ocr_cfg) = sense_config.ocr {
        let engine = match ocr_cfg.engine.as_str() {
            "api" => sense::ocr::OcrEngine::Api,
            _ => sense::ocr::OcrEngine::Tesseract,
        };
        let ocr_config = sense::ocr::OcrConfig {
            engine,
            api_url: ocr_cfg.api_url.clone(),
            api_key: ocr_cfg.api_key.clone(),
            tesseract_lang: ocr_cfg.tesseract_lang.clone(),
        };
        let plugin = sense::ocr::OcrPlugin::new(ocr_config);
        let adapter = SensePluginAdapter::new(
            Box::new(plugin),
            "ocr",
            vec![
                "image/png".into(),
                "image/jpeg".into(),
                "image/tiff".into(),
                "image/bmp".into(),
                "image/gif".into(),
                "application/pdf".into(),
            ],
        );
        registry.register(adapter).await?;
        registry.activate("sense.ocr").await?;
    }

    // Register ASR plugin
    if let Some(ref asr_cfg) = sense_config.asr {
        let engine = match asr_cfg.engine.as_str() {
            "api" => sense::asr::AsrEngine::Api,
            _ => sense::asr::AsrEngine::Whisper,
        };
        let asr_config = sense::asr::AsrConfig {
            engine,
            api_url: asr_cfg.api_url.clone(),
            api_key: asr_cfg.api_key.clone(),
            whisper_model: asr_cfg.whisper_model.clone(),
            segment_duration_secs: 600, // default
        };
        let plugin = sense::asr::AsrPlugin::new(asr_config);
        let adapter = SensePluginAdapter::new(
            Box::new(plugin),
            "asr",
            vec![
                "audio/wav".into(),
                "audio/mp3".into(),
                "audio/ogg".into(),
                "audio/flac".into(),
                "audio/m4a".into(),
            ],
        );
        registry.register(adapter).await?;
        registry.activate("sense.asr").await?;
    }

    info!(
        "Sense plugins registered: {} active",
        registry.active_count().await
    );
    Ok(())
}

// ─── Tests ────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    /// A minimal test plugin.
    struct MockPlugin {
        id: String,
        init_called: Arc<std::sync::atomic::AtomicBool>,
        start_called: Arc<std::sync::atomic::AtomicBool>,
        stop_called: Arc<std::sync::atomic::AtomicBool>,
    }

    impl MockPlugin {
        fn new(id: &str) -> Self {
            Self {
                id: id.to_string(),
                init_called: Arc::new(std::sync::atomic::AtomicBool::new(false)),
                start_called: Arc::new(std::sync::atomic::AtomicBool::new(false)),
                stop_called: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            }
        }
    }

    #[async_trait::async_trait]
    impl Plugin for MockPlugin {
        fn info(&self) -> PluginInfo {
            PluginInfo {
                id: self.id.clone(),
                name: format!("Mock {}", self.id),
                description: "Test plugin".into(),
                version: "0.1.0".into(),
                category: PluginCategory::Custom,
                supported_types: vec!["text/plain".into()],
                author: "test".into(),
                priority: 50,
            }
        }

        async fn init(&mut self) -> Result<()> {
            self.init_called
                .store(true, std::sync::atomic::Ordering::SeqCst);
            Ok(())
        }

        async fn start(&mut self) -> Result<()> {
            self.start_called
                .store(true, std::sync::atomic::Ordering::SeqCst);
            Ok(())
        }

        async fn stop(&mut self) -> Result<()> {
            self.stop_called
                .store(true, std::sync::atomic::Ordering::SeqCst);
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_plugin_registry_register_and_list() {
        let registry = PluginRegistry::new();
        registry
            .register(MockPlugin::new("test.1"))
            .await
            .unwrap();
        registry
            .register(MockPlugin::new("test.2"))
            .await
            .unwrap();

        let list = registry.list().await;
        assert_eq!(list.len(), 2);
        let ids: Vec<&str> = list.iter().map(|i| i.id.as_str()).collect();
        assert!(ids.contains(&"test.1"));
        assert!(ids.contains(&"test.2"));
    }

    #[tokio::test]
    async fn test_plugin_registry_activate_lifecycle() {
        let registry = PluginRegistry::new();
        let plugin = MockPlugin::new("lifecycle.test");
        let init_flag = plugin.init_called.clone();
        let start_flag = plugin.start_called.clone();
        let stop_flag = plugin.stop_called.clone();

        registry.register(plugin).await.unwrap();

        // Not yet active
        assert_eq!(registry.active_count().await, 0);

        // Activate
        registry.activate("lifecycle.test").await.unwrap();
        assert!(init_flag.load(std::sync::atomic::Ordering::SeqCst));
        assert!(start_flag.load(std::sync::atomic::Ordering::SeqCst));
        assert_eq!(registry.active_count().await, 1);

        // Deactivate
        registry.deactivate("lifecycle.test").await.unwrap();
        assert!(stop_flag.load(std::sync::atomic::Ordering::SeqCst));
        assert_eq!(registry.active_count().await, 0);
    }

    #[tokio::test]
    async fn test_plugin_registry_health() {
        let registry = PluginRegistry::new();
        registry.register(MockPlugin::new("health.test")).await.unwrap();
        registry.activate("health.test").await.unwrap();

        let health = registry.health("health.test").await.unwrap();
        assert_eq!(health.plugin_id, "health.test");
        assert_eq!(health.state, PluginState::Active);
    }

    #[tokio::test]
    async fn test_plugin_registry_find_by_type() {
        let registry = PluginRegistry::new();
        registry.register(MockPlugin::new("finder.test")).await.unwrap();
        registry.activate("finder.test").await.unwrap();

        let found = registry.find_by_type("text/plain").await;
        assert_eq!(found.len(), 1);
        assert_eq!(found[0].id, "finder.test");

        let not_found = registry.find_by_type("application/pdf").await;
        assert!(not_found.is_empty());
    }

    #[tokio::test]
    async fn test_plugin_registry_activate_nonexistent() {
        let registry = PluginRegistry::new();
        let result = registry.activate("nonexistent").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_plugin_registry_activate_all() {
        let registry = PluginRegistry::new();
        registry.register(MockPlugin::new("a")).await.unwrap();
        registry.register(MockPlugin::new("b")).await.unwrap();

        let results = registry.activate_all().await;
        assert_eq!(results.len(), 2);
        for (_, result) in results {
            result.unwrap();
        }
        assert_eq!(registry.active_count().await, 2);
    }

    #[tokio::test]
    async fn test_plugin_registry_dispatch() {
        let registry = PluginRegistry::new();
        // MockPlugin doesn't implement handle_request, so dispatch returns error
        registry.register(MockPlugin::new("dispatch.test")).await.unwrap();
        registry.activate("dispatch.test").await.unwrap();

        // handle_request returns an error by default
        let result = registry.dispatch("dispatch.test", "test", b"{}").await;
        assert!(result.is_err());

        // Verify error count was incremented
        let health = registry.health("dispatch.test").await.unwrap();
        assert_eq!(health.errors, 1);
        assert_eq!(health.requests_handled, 1);
    }

    #[tokio::test]
    async fn test_plugin_registry_dispatch_inactive() {
        let registry = PluginRegistry::new();
        registry.register(MockPlugin::new("inactive.test")).await.unwrap();

        // Not activated yet — should fail
        let result = registry.dispatch("inactive.test", "test", b"{}").await;
        assert!(result.is_err());
    }

    #[test]
    fn test_plugin_category_serialization() {
        let cat = PluginCategory::Sense;
        let json = serde_json::to_string(&cat).unwrap();
        assert_eq!(json, "\"sense\"");

        let deserialized: PluginCategory = serde_json::from_str("\"enrich\"").unwrap();
        assert_eq!(deserialized, PluginCategory::Enrich);
    }

    #[test]
    fn test_plugin_state_serialization() {
        let state = PluginState::Active;
        let json = serde_json::to_string(&state).unwrap();
        assert_eq!(json, "\"active\"");
    }

    #[test]
    fn test_plugin_info_serialization_roundtrip() {
        let info = PluginInfo {
            id: "test.roundtrip".into(),
            name: "Roundtrip Test".into(),
            description: "Tests serialization".into(),
            version: "1.2.3".into(),
            category: PluginCategory::Transform,
            supported_types: vec!["text/csv".into(), "text/tsv".into()],
            author: "tester".into(),
            priority: 42,
        };
        let json = serde_json::to_string(&info).unwrap();
        let deserialized: PluginInfo = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.id, "test.roundtrip");
        assert_eq!(deserialized.version, "1.2.3");
        assert_eq!(deserialized.supported_types, vec!["text/csv", "text/tsv"]);
        assert_eq!(deserialized.priority, 42);
    }

    #[test]
    fn test_build_default_registry() {
        let registry = build_default_registry();
        // Registry starts empty; sense plugins added via register_sense_plugins
        assert_eq!(registry.entries.blocking_read().len(), 0);
    }
}
