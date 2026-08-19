#![allow(dead_code)]

use anyhow::Result;
use opensoma::{collector, config, connector, grpc, heartbeat, processor, status_server, sync};
use tracing::info;
use tracing_subscriber::{fmt, EnvFilter};

#[tokio::main]
async fn main() -> Result<()> {
    // Parse CLI args
    let config_path = parse_config_path();

    // Initialize logging
    init_logging();

    info!("OpenSoma starting — Deploy Everywhere, Collect Everything.");
    info!("Loading config from: {}", config_path);

    // Load configuration
    let config = config::AppConfig::load(&config_path)?;

    // Initialize config hot-reload watcher
    let (_config_handle, _config_rx) = config::watch_config(&config_path)?;

    // Initialize local cache (sled)
    let cache = sync::cache::Cache::open(&config.daemon.data_dir)?;

    // Build HTTP client (replaces gRPC)
    let grpc_client = grpc::client::SoulClient::new(&config.soul).await?;

    // Register this node with Soul's Nerve bus
    if let Err(e) = grpc_client
        .register_node(&config.daemon.node_id, "soma")
        .await
    {
        tracing::warn!("Node registration failed (will retry via heartbeat): {}", e);
    }

    // Channel wiring: collector → processor → sync
    // Channel 1: raw events from collectors/connectors
    let (raw_tx, raw_rx) = tokio::sync::mpsc::channel(1024);
    // Channel 2: processed events to sync engine
    let (processed_tx, processed_rx) = tokio::sync::mpsc::channel(1024);

    // Start heartbeat
    let heartbeat_handle = heartbeat::start(
        config.daemon.node_id.clone(),
        config.soul.heartbeat_interval,
        grpc_client.clone(),
    );

    // Start collectors (file watcher)
    let collector_handle = collector::start_all(&config.collector, raw_tx.clone()).await?;

    // Start connectors (feishu, dingtalk, wecom)
    let connector_handle = connector::start_all(&config.connector, raw_tx.clone()).await?;

    // Start processor pipeline: raw_rx → normalize → classify → enrich → dedup → processed_tx
    let processor_handle = if config.sense.enabled {
        processor::start_pipeline_with_sense(raw_rx, processed_tx, &config.processor, &config.sense)
    } else {
        processor::start_pipeline(raw_rx, processed_tx, &config.processor)
    };

    // Shared cache stats for status server
    let cache_stats = std::sync::Arc::new(tokio::sync::RwLock::new(
        status_server::CacheStatsSnapshot::default(),
    ));

    // Start sync engine: processed_rx → cache → upload to Soul
    let cache_clone = cache.clone();
    let sync_handle = sync::start_engine_with_rx(
        &config.sync,
        cache,
        grpc_client.clone(),
        processed_rx,
        cache_stats.clone(),
    );

    // Start HTTP status server for monitoring
    let status_state = status_server::StatusServerState {
        node_id: config.daemon.node_id.clone(),
        start_time: std::time::Instant::now(),
        events_collected: std::sync::Arc::new(tokio::sync::RwLock::new(0)),
        events_synced: std::sync::Arc::new(tokio::sync::RwLock::new(0)),
        connectors_active: std::sync::Arc::new(tokio::sync::RwLock::new(Vec::new())),
        last_error: std::sync::Arc::new(tokio::sync::RwLock::new(None)),
        connector_enabled: std::sync::Arc::new(tokio::sync::RwLock::new(
            std::collections::HashMap::new(),
        )),
        connector_event_counts: std::sync::Arc::new(tokio::sync::RwLock::new(
            std::collections::HashMap::new(),
        )),
        cache_stats: cache_stats.clone(),
        cache: Some(cache_clone),
    };
    let status_handle =
        status_server::start_status_server(config.daemon.status_port, status_state).await;

    info!("All subsystems initialized. Daemon is running.");

    // Wait for shutdown signal
    let shutdown = wait_for_signal();
    tokio::select! {
        _ = shutdown => {
            info!("Shutdown signal received, initiating graceful stop...");
        }
    }

    // Graceful shutdown — abort all tasks
    heartbeat_handle.abort();
    collector_handle.abort();
    connector_handle.abort();
    processor_handle.abort();
    sync_handle.abort();
    status_handle.abort();

    info!("OpenSoma stopped.");
    Ok(())
}

/// Parse config path from CLI args, default to "config.toml"
fn parse_config_path() -> String {
    let args: Vec<String> = std::env::args().collect();

    // Handle --version / -V
    if args.iter().any(|a| a == "--version" || a == "-V") {
        println!("{}", opensoma::build_info::version_string());
        std::process::exit(0);
    }

    // Handle --help / -h
    if args.iter().any(|a| a == "--help" || a == "-h") {
        println!("OpenSoma — Deploy Everywhere, Collect Everything");
        println!("{}", opensoma::build_info::version_string());
        println!();
        println!("USAGE:");
        println!("    opensoma [OPTIONS]");
        println!();
        println!("OPTIONS:");
        println!("    -c, --config <PATH>    Path to config.toml [default: config.toml]");
        println!("    --validate             Validate config.toml and exit (dry-run)");
        println!("    --init                 Generate a default config.toml and exit");
        println!("    -V, --version          Print version information");
        println!("    -h, --help             Print this help message");
        std::process::exit(0);
    }

    // Handle --init
    if args.iter().any(|a| a == "--init") {
        let mut config_path = "config.toml".to_string();
        for i in 0..args.len() {
            if (args[i] == "--config" || args[i] == "-c") && i + 1 < args.len() {
                config_path = args[i + 1].clone();
            }
        }
        std::process::exit(run_init_with_path(&config_path));
    }

    // Handle --validate
    if args.iter().any(|a| a == "--validate") {
        // Extract config path inline to avoid recursion
        let mut config_path = "config.toml".to_string();
        for i in 0..args.len() {
            if (args[i] == "--config" || args[i] == "-c") && i + 1 < args.len() {
                config_path = args[i + 1].clone();
            }
        }
        std::process::exit(run_validate_with_path(&config_path));
    }

    for i in 0..args.len() {
        if (args[i] == "--config" || args[i] == "-c") && i + 1 < args.len() {
            return args[i + 1].clone();
        }
    }
    "config.toml".to_string()
}

/// Initialize tracing subscriber with env filter
fn init_logging() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

    fmt()
        .with_env_filter(filter)
        .with_target(true)
        .with_thread_ids(true)
        .with_file(true)
        .with_line_number(true)
        .init();
}

/// Wait for SIGINT or SIGTERM
async fn wait_for_signal() {
    let ctrl_c = tokio::signal::ctrl_c();

    #[cfg(unix)]
    {
        let mut sigterm =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()).unwrap();
        tokio::select! {
            _ = ctrl_c => {}
            _ = sigterm.recv() => {}
        }
    }

    #[cfg(not(unix))]
    {
        ctrl_c.await.ok();
    }
}

/// Validate config and print results. Returns exit code (0=ok, 1=error).
fn run_validate_with_path(config_path: &str) -> i32 {
    println!("Validating config: {}", config_path);

    let config = match config::AppConfig::load(config_path) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("❌ FATAL: Failed to load config: {:#}", e);
            return 1;
        }
    };

    println!("✅ Config parsed successfully.");

    match config.validate() {
        Ok(warnings) => {
            if warnings.is_empty() {
                println!("✅ All checks passed — no warnings.");
            } else {
                println!("⚠️  {} warning(s):", warnings.len());
                for w in &warnings {
                    println!("   • {}", w);
                }
            }
            // Print summary
            let mut connector_count = 0;
            let mut enabled_count = 0;
            macro_rules! check_connector {
                ($field:expr) => {
                    if let Some(ref c) = $field {
                        connector_count += 1;
                        if c.enabled {
                            enabled_count += 1;
                        }
                    }
                };
            }
            check_connector!(config.connector.feishu);
            check_connector!(config.connector.dingtalk);
            check_connector!(config.connector.wecom);
            check_connector!(config.connector.rss);
            check_connector!(config.connector.email);
            check_connector!(config.connector.notion);
            check_connector!(config.connector.git);
            check_connector!(config.connector.obsidian);
            check_connector!(config.connector.webhook);
            check_connector!(config.connector.github);
            check_connector!(config.connector.slack);

            println!();
            println!("Summary:");
            println!("  Node ID:     {}", config.daemon.node_id);
            println!("  Soul:        {}", config.soul.endpoint);
            println!(
                "  Connectors:  {}/{} enabled",
                enabled_count, connector_count
            );
            println!("  Watch dirs:  {}", config.collector.watch_dirs.len());
            println!("  Status port: {}", config.daemon.status_port);
            println!("  Batch size:  {}", config.sync.batch_size);
            0
        }
        Err(e) => {
            eprintln!("❌ Validation failed: {:#}", e);
            1
        }
    }
}

/// Generate a default config.toml at the given path. Returns exit code.
fn run_init_with_path(config_path: &str) -> i32 {
    if std::path::Path::new(config_path).exists() {
        eprintln!(
            "⚠️  File '{}' already exists. Remove it first or choose a different path.",
            config_path
        );
        return 1;
    }

    let default_config = r#"# OpenSoma — Deploy Everywhere, Collect Everything
# Configuration file generated by `opensoma --init`

[daemon]
node_id = "soma-node-1"
# log_level = "info"
# data_dir = "/var/lib/opensoma"
# status_port = 8091

[soul]
endpoint = "http://localhost:8090"
# heartbeat_interval = 30
# connect_timeout = 10

[collector]
watch_dirs = []
# include = ["*.json", "*.csv", "*.txt"]
# exclude = []
# debounce_ms = 500
# process_interval_ms = 5000
# network_interval_ms = 10000
# clipboard_interval_ms = 2000

[connector]
# Uncomment and configure the connectors you need:

# [connector.feishu]
# enabled = true
# app_id = "cli_xxxxx"
# app_secret = "xxxxx"

# [connector.dingtalk]
# enabled = true
# app_key = "xxxxx"
# app_secret = "xxxxx"
# agent_id = "xxxxx"

# [connector.wecom]
# enabled = true
# corp_id = "xxxxx"
# agent_id = "xxxxx"
# secret = "xxxxx"

# [connector.github]
# enabled = true
# token = "ghp_xxxxx"
# repos = ["owner/repo"]
# poll_interval_secs = 300

# [connector.rss]
# enabled = true
# feeds = [{name = "Hacker News", url = "https://news.ycombinator.com/rss"}]
# poll_interval_secs = 300

# [connector.email]
# enabled = true
# accounts = [{name = "work", imap_server = "imap.gmail.com", username = "user@gmail.com", password = "app-password"}]
# poll_interval_secs = 120

# [connector.notion]
# enabled = true
# integration_token = "secret_xxxxx"
# database_id = "xxxxx"

# [connector.git]
# enabled = true
# repo_url = "https://github.com/user/repo.git"
# local_path = "/tmp/repo"
# poll_interval_secs = 300

# [connector.obsidian]
# enabled = true
# vault_path = "/path/to/vault"

# [connector.webhook]
# enabled = true
# listen = "0.0.0.0:9800"
# secret = "hmac-secret"

# [connector.slack]
# enabled = true
# bot_token = "«redacted:xox…»"
# channels = ["C01ABC123"]
# poll_interval_secs = 60
# include_threads = true

[processor]
# normalize_timestamps = true
# max_event_size = 1048576
# dedup_window_secs = 300
# enable_classify = true
# enable_enrich = true

[sync]
# batch_size = 50
# upload_interval = 10
# max_retries = 5
# retry_backoff_ms = 1000
# cache_size_mb = 512

[sense]
# enabled = false
# [sense.asr]
# engine = "whisper"
# whisper_model = "base"
# [sense.ocr]
# engine = "tesseract"
# tesseract_lang = "chi_sim+eng"
"#;

    match std::fs::write(config_path, default_config) {
        Ok(_) => {
            println!("✅ Default config written to '{}'", config_path);
            println!(
                "   Edit the file to configure your node, then run: opensoma -c {}",
                config_path
            );
            0
        }
        Err(e) => {
            eprintln!("❌ Failed to write config: {}", e);
            1
        }
    }
}
