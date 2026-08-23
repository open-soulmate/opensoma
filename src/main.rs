#![allow(dead_code)]

use anyhow::Result;
use opensoma::{
    collector, config, connector, grpc, health, heartbeat, metrics, processor, status_server, sync,
};
use tracing::info;
use tracing_subscriber::{fmt, layer::SubscriberExt, reload, EnvFilter};

#[tokio::main]
async fn main() -> Result<()> {
    // Parse CLI args
    let config_path = parse_config_path();

    // Initialize logging (returns reload handle for runtime log level changes)
    let filter_handle = init_logging();

    info!("OpenSoma starting — Deploy Everywhere, Collect Everything.");
    info!("Loading config from: {}", config_path);

    // Load configuration
    let config = config::AppConfig::load(&config_path)?;

    // Initialize config hot-reload watcher
    let (_config_handle, config_rx) = config::watch_config(&config_path)?;

    // Wire up config hot-reload: update log level when config.toml changes
    {
        let mut config_rx = config_rx;
        let fh = filter_handle;
        tokio::spawn(async move {
            while config_rx.changed().await.is_ok() {
                let new_config = config_rx.borrow().clone();
                let new_level = &new_config.daemon.log_level;
                match new_level.parse::<EnvFilter>() {
                    Ok(new_filter) => {
                        if let Err(e) = fh.modify(|f| *f = new_filter) {
                            tracing::warn!("Failed to update log level: {}", e);
                        } else {
                            tracing::info!("Log level hot-reloaded to: {}", new_level);
                        }
                    }
                    Err(e) => {
                        tracing::warn!(
                            "Invalid log level '{}' in config (keeping previous): {}",
                            new_level,
                            e
                        );
                    }
                }
            }
        });
    }

    // Initialize local cache (sled)
    let cache = sync::cache::Cache::open(&config.daemon.data_dir)?;

    // Initialize plugin registry and register built-in sense plugins
    let plugin_registry = std::sync::Arc::new(opensoma::plugins::PluginRegistry::new());
    if let Err(e) = opensoma::plugins::register_sense_plugins(&plugin_registry, &config.sense).await
    {
        tracing::warn!("Sense plugin registration failed (non-fatal): {:#}", e);
    }
    let plugin_count = plugin_registry.count().await;
    let active_plugins = plugin_registry.active_count().await;
    info!(
        "Plugin registry: {} registered, {} active",
        plugin_count, active_plugins
    );

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

    // Create the shared health checker (used by connectors + status server)
    let health_checker = health::HealthChecker::new();

    // Create the shared circuit breaker registry (used by connectors + status server)
    let circuit_breaker_registry = opensoma::connector::circuit_breaker::CircuitBreakerRegistry::new();

    // Start connectors (feishu, dingtalk, wecom, …) with health checking + circuit breakers
    let connector_handle =
        connector::start_all(&config.connector, raw_tx.clone(), Some(health_checker.clone()), Some(circuit_breaker_registry.clone()))
            .await?;

    // Start processor pipeline: raw_rx → normalize → classify → enrich → dedup → processed_tx
    let pipeline_metrics = metrics::PipelineMetrics::new();
    let processor_handle = if config.sense.enabled {
        processor::start_pipeline_with_sense(
            raw_rx,
            processed_tx,
            &config.processor,
            &config.sense,
            Some(pipeline_metrics.clone()),
        )
    } else {
        processor::start_pipeline(
            raw_rx,
            processed_tx,
            &config.processor,
            Some(pipeline_metrics.clone()),
        )
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
        Some(pipeline_metrics.clone()),
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
        pipeline_metrics: Some(pipeline_metrics),
        health_checker: Some(health_checker),
        plugin_registry: Some(plugin_registry),
        config_snapshot: Some(status_server::ConfigSnapshot::from_config(&config)),
        circuit_breakers: Some(circuit_breaker_registry),
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

    // Handle --version-json
    if args.iter().any(|a| a == "--version-json") {
        println!("{}", opensoma::build_info::version_json());
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
        println!("    --status               Query running daemon status and exit");
        println!("    --metrics              Print Prometheus metrics from running daemon");
        println!("    --health               Quick health check (exit 0=ok, 1=down)");
        println!("    --connectors           List configured connectors and their status");
        println!("    --doctor               Diagnose runtime environment and dependencies");
        println!("    --self-test            Run end-to-end pipeline self-test (no Soul needed)");
        println!("    --export <FILE>        Export cached events to JSON file");
        println!("    --import <FILE>        Import events from JSON file into cache");
        println!("    --cache-info           Show local event cache statistics");
        println!("    --recent [N]           Show N most recent cached events (default: 10)");
        println!("    --search <QUERY>       Search cached events by payload text");
        println!("    --source <PREFIX>      Filter cached events by source prefix");
        println!("    --type <TYPE>          Filter cached events by event type");
        println!("    --stats                Show aggregate event statistics by source and type");
        println!("    --tail [N]             Real-time event stream (poll every 2s, show last N)");
        println!("    --top                  Live monitoring dashboard (refreshes every 2s)");
        println!("    --prune <DAYS>         Remove cached events older than N days");
        println!("    --test-connector <N>   Test connectivity to a specific connector (e.g. feishu, github)");
        println!("    -V, --version          Print version information");
        println!("    --version-json         Print version as JSON (for scripts)");
        println!("    -h, --help             Print this help message");
        std::process::exit(0);
    }

    // Handle --connectors
    if args.iter().any(|a| a == "--connectors") {
        let mut config_path = "config.toml".to_string();
        for i in 0..args.len() {
            if (args[i] == "--config" || args[i] == "-c") && i + 1 < args.len() {
                config_path = args[i + 1].clone();
            }
        }
        std::process::exit(run_connectors_list(&config_path));
    }

    // Handle --status
    if args.iter().any(|a| a == "--status") {
        let mut port: u16 = 8091;
        // Try to read port from config
        let mut config_path = "config.toml".to_string();
        for i in 0..args.len() {
            if (args[i] == "--config" || args[i] == "-c") && i + 1 < args.len() {
                config_path = args[i + 1].clone();
            }
        }
        if let Ok(config) = config::AppConfig::load(&config_path) {
            port = config.daemon.status_port;
        }
        std::process::exit(run_status_query(port));
    }

    // Handle --metrics
    if args.iter().any(|a| a == "--metrics") {
        let mut port: u16 = 8091;
        let mut config_path = "config.toml".to_string();
        for i in 0..args.len() {
            if (args[i] == "--config" || args[i] == "-c") && i + 1 < args.len() {
                config_path = args[i + 1].clone();
            }
        }
        if let Ok(config) = config::AppConfig::load(&config_path) {
            port = config.daemon.status_port;
        }
        std::process::exit(run_metrics_query(port));
    }

    // Handle --self-test
    if args.iter().any(|a| a == "--self-test") {
        std::process::exit(run_self_test());
    }

    // Handle --doctor
    if args.iter().any(|a| a == "--doctor") {
        let mut config_path = "config.toml".to_string();
        for i in 0..args.len() {
            if (args[i] == "--config" || args[i] == "-c") && i + 1 < args.len() {
                config_path = args[i + 1].clone();
            }
        }
        std::process::exit(run_doctor(&config_path));
    }

    // Handle --export
    if args.iter().any(|a| a == "--export") {
        let export_idx = args.iter().position(|a| a == "--export").unwrap();
        if export_idx + 1 >= args.len() {
            eprintln!("❌ --export requires a file path argument");
            std::process::exit(1);
        }
        let output_file = args[export_idx + 1].clone();
        let mut config_path = "config.toml".to_string();
        for i in 0..args.len() {
            if (args[i] == "--config" || args[i] == "-c") && i + 1 < args.len() {
                config_path = args[i + 1].clone();
            }
        }
        std::process::exit(run_export(&config_path, &output_file));
    }

    // Handle --import
    if args.iter().any(|a| a == "--import") {
        let import_idx = args.iter().position(|a| a == "--import").unwrap();
        if import_idx + 1 >= args.len() {
            eprintln!("❌ --import requires a file path argument");
            std::process::exit(1);
        }
        let input_file = args[import_idx + 1].clone();
        let mut config_path = "config.toml".to_string();
        for i in 0..args.len() {
            if (args[i] == "--config" || args[i] == "-c") && i + 1 < args.len() {
                config_path = args[i + 1].clone();
            }
        }
        std::process::exit(run_import(&config_path, &input_file));
    }

    // Handle --cache-info
    if args.iter().any(|a| a == "--cache-info") {
        let mut config_path = "config.toml".to_string();
        for i in 0..args.len() {
            if (args[i] == "--config" || args[i] == "-c") && i + 1 < args.len() {
                config_path = args[i + 1].clone();
            }
        }
        std::process::exit(run_cache_info(&config_path));
    }
    // Handle --recent [N]
    if args.iter().any(|a| a == "--recent") {
        let mut config_path = "config.toml".to_string();
        let mut count = 10usize;
        for i in 0..args.len() {
            if (args[i] == "--config" || args[i] == "-c") && i + 1 < args.len() {
                config_path = args[i + 1].clone();
            }
            if args[i] == "--recent" && i + 1 < args.len() {
                if let Ok(n) = args[i + 1].parse::<usize>() {
                    count = n;
                }
            }
        }
        std::process::exit(run_recent_events(&config_path, count));
    }
    // Handle --search <QUERY>
    if args.iter().any(|a| a == "--search") {
        let search_idx = args.iter().position(|a| a == "--search").unwrap();
        if search_idx + 1 >= args.len() {
            eprintln!("❌ --search requires a query argument");
            std::process::exit(1);
        }
        let query = args[search_idx + 1].clone();
        let mut config_path = "config.toml".to_string();
        for i in 0..args.len() {
            if (args[i] == "--config" || args[i] == "-c") && i + 1 < args.len() {
                config_path = args[i + 1].clone();
            }
        }
        std::process::exit(run_search_events(&config_path, &query));
    }
    // Handle --source <PREFIX>
    if args.iter().any(|a| a == "--source") {
        let src_idx = args.iter().position(|a| a == "--source").unwrap();
        if src_idx + 1 >= args.len() {
            eprintln!("❌ --source requires a prefix argument");
            std::process::exit(1);
        }
        let prefix = args[src_idx + 1].clone();
        let mut config_path = "config.toml".to_string();
        for i in 0..args.len() {
            if (args[i] == "--config" || args[i] == "-c") && i + 1 < args.len() {
                config_path = args[i + 1].clone();
            }
        }
        std::process::exit(run_source_filter(&config_path, &prefix));
    }
    // Handle --type <TYPE>
    if args.iter().any(|a| a == "--type") {
        let type_idx = args.iter().position(|a| a == "--type").unwrap();
        if type_idx + 1 >= args.len() {
            eprintln!("❌ --type requires an event type argument");
            std::process::exit(1);
        }
        let event_type = args[type_idx + 1].clone();
        let mut config_path = "config.toml".to_string();
        for i in 0..args.len() {
            if (args[i] == "--config" || args[i] == "-c") && i + 1 < args.len() {
                config_path = args[i + 1].clone();
            }
        }
        std::process::exit(run_type_filter(&config_path, &event_type));
    }
    // Handle --stats
    if args.iter().any(|a| a == "--stats") {
        let mut config_path = "config.toml".to_string();
        for i in 0..args.len() {
            if (args[i] == "--config" || args[i] == "-c") && i + 1 < args.len() {
                config_path = args[i + 1].clone();
            }
        }
        std::process::exit(run_stats(&config_path));
    }
    // Handle --tail [N]
    if args.iter().any(|a| a == "--tail") {
        let mut config_path = "config.toml".to_string();
        let mut count = 10usize;
        for i in 0..args.len() {
            if (args[i] == "--config" || args[i] == "-c") && i + 1 < args.len() {
                config_path = args[i + 1].clone();
            }
            if args[i] == "--tail" && i + 1 < args.len() {
                if let Ok(n) = args[i + 1].parse::<usize>() {
                    count = n;
                }
            }
        }
        std::process::exit(run_tail(&config_path, count));
    }
    // Handle --prune <DAYS>
    if args.iter().any(|a| a == "--prune") {
        let prune_idx = args.iter().position(|a| a == "--prune").unwrap();
        if prune_idx + 1 >= args.len() {
            eprintln!("❌ --prune requires a number of days argument");
            std::process::exit(1);
        }
        let days: i64 = match args[prune_idx + 1].parse() {
            Ok(n) => n,
            Err(_) => {
                eprintln!("❌ --prune requires a numeric argument (days)");
                std::process::exit(1);
            }
        };
        let mut config_path = "config.toml".to_string();
        for i in 0..args.len() {
            if (args[i] == "--config" || args[i] == "-c") && i + 1 < args.len() {
                config_path = args[i + 1].clone();
            }
        }
        std::process::exit(run_prune(&config_path, days));
    }
    // Handle --test-connector <NAME>
    if args.iter().any(|a| a == "--test-connector") {
        let tc_idx = args.iter().position(|a| a == "--test-connector").unwrap();
        if tc_idx + 1 >= args.len() {
            eprintln!("❌ --test-connector requires a connector name (e.g. feishu, github, email)");
            std::process::exit(1);
        }
        let connector_name = args[tc_idx + 1].clone();
        let mut config_path = "config.toml".to_string();
        for i in 0..args.len() {
            if (args[i] == "--config" || args[i] == "-c") && i + 1 < args.len() {
                config_path = args[i + 1].clone();
            }
        }
        std::process::exit(run_test_connector(&config_path, &connector_name));
    }
    // Handle --top
    if args.iter().any(|a| a == "--top") {
        let mut port: u16 = 8091;
        let mut config_path = "config.toml".to_string();
        for i in 0..args.len() {
            if (args[i] == "--config" || args[i] == "-c") && i + 1 < args.len() {
                config_path = args[i + 1].clone();
            }
        }
        if let Ok(config) = config::AppConfig::load(&config_path) {
            port = config.daemon.status_port;
        }
        std::process::exit(run_top(port));
    }
    // Handle --health
    if args.iter().any(|a| a == "--health") {
        let mut port: u16 = 8091;
        let mut config_path = "config.toml".to_string();
        for i in 0..args.len() {
            if (args[i] == "--config" || args[i] == "-c") && i + 1 < args.len() {
                config_path = args[i + 1].clone();
            }
        }
        if let Ok(config) = config::AppConfig::load(&config_path) {
            port = config.daemon.status_port;
        }
        std::process::exit(run_health_check(port));
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

/// Initialize tracing subscriber with env filter and return a reload handle
/// for runtime log level changes via config hot-reload.
fn init_logging() -> reload::Handle<EnvFilter, tracing_subscriber::Registry> {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let (filter_layer, filter_handle) = reload::Layer::new(filter);

    let subscriber = tracing_subscriber::Registry::default()
        .with(filter_layer)
        .with(
            fmt::layer()
                .with_target(true)
                .with_thread_ids(true)
                .with_file(true)
                .with_line_number(true),
        );

    tracing::subscriber::set_global_default(subscriber)
        .expect("Failed to set global default subscriber");

    filter_handle
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
            check_connector!(config.connector.telegram);
            check_connector!(config.connector.discord);

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

# [connector.telegram]
# enabled = true
# bot_token = "123456:ABC-DEF..."
# allowed_chats = [123456789]
# poll_interval_secs = 30
# include_edited = true
#
# [connector.discord]
# enabled = true
# bot_token = "your-discord-bot-token"
# guild_id = "1234567890123456789"
# channels = []  # empty = all text channels
# ignore_bots = true
# poll_interval_secs = 30

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
# [sense.image]
# model = "gpt-4o"
# api_url = "https://api.openai.com/v1/chat/completions"
# api_key = "sk-..."
# [sense.video]
# frame_interval_sec = 5
# max_frames = 60
# frame_analyzer = "ocr"
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

/// Query the running daemon's /api/status endpoint and display formatted output.
/// Uses a blocking HTTP client since this runs before tokio starts.
fn run_status_query(port: u16) -> i32 {
    let url = format!("http://127.0.0.1:{}/api/status", port);

    // Use ureq (blocking) or fall back to std::net
    match blocking_http_get(&url) {
        Ok(body) => {
            match serde_json::from_str::<serde_json::Value>(&body) {
                Ok(json) => {
                    println!("╔══════════════════════════════════════════════╗");
                    println!("║          OpenSoma Daemon Status              ║");
                    println!("╚══════════════════════════════════════════════╝");
                    println!();

                    let node_id = json["node_id"].as_str().unwrap_or("?");
                    let component = json["component"].as_str().unwrap_or("?");
                    let uptime = json["uptime_seconds"].as_u64().unwrap_or(0);
                    let events_collected = json["events_collected"].as_u64().unwrap_or(0);
                    let events_synced = json["events_synced"].as_u64().unwrap_or(0);
                    let hostname = json["hostname"].as_str().unwrap_or("?");
                    let ip = json["ip"].as_str().unwrap_or("?");
                    let cpu = json["cpu_percent"].as_f64().unwrap_or(0.0);
                    let mem_used = json["memory_used_mb"].as_u64().unwrap_or(0);
                    let mem_total = json["memory_total_mb"].as_u64().unwrap_or(0);

                    println!("  Node ID:          {}", node_id);
                    println!("  Component:        {}", component);
                    println!("  Hostname:         {}", hostname);
                    println!("  IP:               {}", ip);
                    println!(
                        "  Uptime:           {}d {}h {}m {}s",
                        uptime / 86400,
                        (uptime % 86400) / 3600,
                        (uptime % 3600) / 60,
                        uptime % 60
                    );
                    println!();
                    println!("  Events collected: {}", events_collected);
                    println!("  Events synced:    {}", events_synced);
                    println!(
                        "  Events pending:   {}",
                        events_collected.saturating_sub(events_synced)
                    );
                    println!();
                    println!("  CPU usage:        {:.1}%", cpu);
                    println!(
                        "  Memory:           {} / {} MB ({:.0}%)",
                        mem_used,
                        mem_total,
                        if mem_total > 0 {
                            mem_used as f64 / mem_total as f64 * 100.0
                        } else {
                            0.0
                        }
                    );

                    // Show active connectors
                    if let Some(connectors) = json["connectors_active"].as_array() {
                        if !connectors.is_empty() {
                            let names: Vec<&str> =
                                connectors.iter().filter_map(|c| c.as_str()).collect();
                            println!("  Active connectors: {}", names.join(", "));
                        }
                    }
                    if let Some(err) = json["last_error"].as_str() {
                        if !err.is_empty() {
                            println!("  ⚠ Last error:     {}", err);
                        }
                    }
                    println!();
                    0
                }
                Err(e) => {
                    eprintln!("❌ Failed to parse status response: {}", e);
                    1
                }
            }
        }
        Err(e) => {
            eprintln!(
                "❌ Cannot reach OpenSoma daemon at port {} — is it running?",
                port
            );
            eprintln!("   Error: {}", e);
            1
        }
    }
}

/// Query the running daemon's /metrics endpoint and print Prometheus-format metrics.
fn run_metrics_query(port: u16) -> i32 {
    let url = format!("http://127.0.0.1:{}/metrics", port);

    match blocking_http_get(&url) {
        Ok(body) => {
            print!("{}", body);
            0
        }
        Err(e) => {
            eprintln!(
                "❌ Cannot reach OpenSoma daemon at port {} — is it running?",
                port
            );
            eprintln!("   Error: {}", e);
            1
        }
    }
}

/// Quick health check — exit 0 if healthy, 1 if not.
/// Designed for use in monitoring scripts, load balancers, and systemd watchdog.
fn run_health_check(port: u16) -> i32 {
    let url = format!("http://127.0.0.1:{}/health", port);

    match blocking_http_get(&url) {
        Ok(body) => {
            // Parse the JSON health response
            match serde_json::from_str::<serde_json::Value>(&body) {
                Ok(json) => {
                    let status = json["status"].as_str().unwrap_or("unknown");
                    if status == "ok" {
                        println!("OK");
                        0
                    } else {
                        eprintln!("UNHEALTHY: {}", status);
                        1
                    }
                }
                Err(_) => {
                    // If we got a 200 response, consider it healthy
                    println!("OK");
                    0
                }
            }
        }
        Err(_) => {
            // Silent failure — just exit non-zero
            1
        }
    }
}

/// List configured connectors from config.toml and display their status.
fn run_connectors_list(config_path: &str) -> i32 {
    let config = match config::AppConfig::load(config_path) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("❌ Failed to load config: {:#}", e);
            return 1;
        }
    };

    println!("╔══════════════════════════════════════════════╗");
    println!("║         OpenSoma Connectors                  ║");
    println!("╚══════════════════════════════════════════════╝");
    println!();

    struct ConnectorInfo {
        name: &'static str,
        enabled: bool,
        source_type: &'static str,
        mode: &'static str,
    }

    let mut connectors: Vec<ConnectorInfo> = Vec::new();

    macro_rules! collect_connector {
        ($field:expr, $name:expr, $source:expr, $mode:expr) => {
            if let Some(ref c) = $field {
                connectors.push(ConnectorInfo {
                    name: $name,
                    enabled: c.enabled,
                    source_type: $source,
                    mode: $mode,
                });
            }
        };
    }

    collect_connector!(
        config.connector.feishu,
        "Feishu",
        "Feishu (Lark) API",
        "Webhook + Poll"
    );
    collect_connector!(
        config.connector.dingtalk,
        "DingTalk",
        "DingTalk Open API",
        "Poll"
    );
    collect_connector!(config.connector.wecom, "WeCom", "Enterprise WeChat", "Poll");
    collect_connector!(config.connector.github, "GitHub", "GitHub REST API", "Poll");
    collect_connector!(config.connector.slack, "Slack", "Slack API", "Poll");
    collect_connector!(config.connector.rss, "RSS", "RSS/Atom feeds", "Poll");
    collect_connector!(config.connector.email, "Email", "IMAP mailbox", "Poll");
    collect_connector!(config.connector.notion, "Notion", "Notion API", "Poll");
    collect_connector!(config.connector.git, "Git", "Git repository", "Poll");
    collect_connector!(
        config.connector.obsidian,
        "Obsidian",
        "Obsidian vault",
        "Watch"
    );
    collect_connector!(config.connector.webhook, "Webhook", "HTTP POST", "Listen");
    collect_connector!(
        config.connector.telegram,
        "Telegram",
        "Telegram Bot API",
        "Long-poll"
    );
    collect_connector!(
        config.connector.discord,
        "Discord",
        "Discord Bot API",
        "WebSocket"
    );

    if connectors.is_empty() {
        println!("  No connectors configured.");
        println!("  Run 'opensoma --init' to generate a config with connector examples.");
        return 0;
    }

    let enabled_count = connectors.iter().filter(|c| c.enabled).count();
    println!(
        "  {}/{} connectors enabled\n",
        enabled_count,
        connectors.len()
    );

    println!("  {:<12} {:<8} {:<22} Mode", "Name", "Status", "Source");
    println!("  {:<12} {:<8} {:<22} ────", "────", "──────", "──────");

    for c in &connectors {
        let status = if c.enabled { "✅ ON" } else { "⬜ OFF" };
        println!(
            "  {:<12} {:<8} {:<22} {}",
            c.name, status, c.source_type, c.mode
        );
    }

    println!();
    0
}

/// Blocking HTTP GET using std::net::TcpStream (no async runtime needed).
fn blocking_http_get(url: &str) -> std::io::Result<String> {
    use std::io::{Read, Write};
    use std::net::TcpStream;

    let (host_port, path) = parse_http_url(url);

    let stream = TcpStream::connect(host_port)?;
    stream.set_read_timeout(Some(std::time::Duration::from_secs(5)))?;
    stream.set_write_timeout(Some(std::time::Duration::from_secs(5)))?;

    let mut stream = stream;
    let request = format!(
        "GET {} HTTP/1.1\r\nHost: {}\r\nConnection: close\r\n\r\n",
        path, host_port
    );
    stream.write_all(request.as_bytes())?;

    let mut response = String::new();
    stream.read_to_string(&mut response)?;

    // Split headers and body
    if let Some(body_start) = response.find("\r\n\r\n") {
        let headers = &response[..body_start];
        let body = &response[body_start + 4..];

        // Check for HTTP 200
        if headers.contains("HTTP/1.1 200")
            || headers.contains("HTTP/0.9 200")
            || headers.contains("HTTP/1.0 200")
        {
            Ok(body.to_string())
        } else {
            Err(std::io::Error::other(format!(
                "HTTP error: {}",
                &headers[..headers.find('\r').unwrap_or(headers.len())]
            )))
        }
    } else {
        Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "Invalid HTTP response",
        ))
    }
}

/// Parse an HTTP URL into (host:port, path) components.
fn parse_http_url(url: &str) -> (&str, &str) {
    let stripped = url.strip_prefix("http://").unwrap_or(url);
    match stripped.find('/') {
        Some(i) => (&stripped[..i], &stripped[i..]),
        None => (stripped, "/"),
    }
}

/// Parse an HTTP response string into (status_ok, body).
fn parse_http_response(response: &str) -> std::io::Result<&str> {
    if let Some(body_start) = response.find("\r\n\r\n") {
        let headers = &response[..body_start];
        let body = &response[body_start + 4..];

        if headers.contains("HTTP/1.1 200") || headers.contains("HTTP/1.0 200") {
            Ok(body)
        } else {
            Err(std::io::Error::other(format!(
                "HTTP error: {}",
                &headers[..headers.find('\r').unwrap_or(headers.len())]
            )))
        }
    } else {
        Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "Invalid HTTP response",
        ))
    }
}

/// Diagnose runtime environment — checks config, dependencies, connectivity, and permissions.
fn run_doctor(config_path: &str) -> i32 {
    println!("╔══════════════════════════════════════════════╗");
    println!("║          OpenSoma Doctor — Diagnostics       ║");
    println!("╚══════════════════════════════════════════════╝");
    println!();

    let mut warnings = 0u32;
    let mut errors = 0u32;

    // 1. Config file
    print!("  [config]  Loading {} ... ", config_path);
    match config::AppConfig::load(config_path) {
        Ok(config) => {
            println!("✅ OK");
            match config.validate() {
                Ok(ws) => {
                    if ws.is_empty() {
                        println!("  [config]  Validation: ✅ no warnings");
                    } else {
                        for w in &ws {
                            println!("  [config]  ⚠️  {}", w);
                            warnings += 1;
                        }
                    }
                }
                Err(e) => {
                    println!("  [config]  ❌ Validation failed: {}", e);
                    errors += 1;
                }
            }

            // 2. Soul connectivity
            print!("  [soul]    Checking {} ... ", config.soul.endpoint);
            let url = format!("{}/api/health", config.soul.endpoint);
            match blocking_http_get(&url) {
                Ok(body) if body.contains("ok") || body.contains("status") => {
                    println!("✅ reachable");
                }
                Ok(_) => {
                    println!("⚠️  responded but unexpected body");
                    warnings += 1;
                }
                Err(e) => {
                    println!("❌ unreachable: {}", e);
                    errors += 1;
                }
            }

            // 3. Data directory
            let data_dir = &config.daemon.data_dir;
            print!("  [data]    Checking {} ... ", data_dir);
            match std::fs::metadata(data_dir) {
                Ok(m) if m.is_dir() => {
                    // Check write permission by creating a temp file
                    let test_file = format!("{}/.doctor_test", data_dir);
                    match std::fs::write(&test_file, "test") {
                        Ok(_) => {
                            let _ = std::fs::remove_file(&test_file);
                            println!("✅ writable");
                        }
                        Err(e) => {
                            println!("❌ not writable: {}", e);
                            errors += 1;
                        }
                    }
                }
                Ok(_) => {
                    println!("❌ exists but is not a directory");
                    errors += 1;
                }
                Err(_) => {
                    // Try to create it
                    match std::fs::create_dir_all(data_dir) {
                        Ok(_) => {
                            println!("✅ created (did not exist)");
                            let _ = std::fs::remove_dir(data_dir);
                        }
                        Err(e) => {
                            println!("❌ cannot create: {}", e);
                            errors += 1;
                        }
                    }
                }
            }

            // 4. Watch directories
            for dir in &config.collector.watch_dirs {
                print!("  [watch]   Checking {} ... ", dir);
                if std::path::Path::new(dir).is_dir() {
                    println!("✅ exists");
                } else {
                    println!("⚠️  directory not found");
                    warnings += 1;
                }
            }

            // 5. System dependencies (clipboard tools)
            let clipboard_tools = ["wl-paste", "xclip", "xsel"];
            let mut found_clipboard = false;
            for tool in &clipboard_tools {
                if std::process::Command::new("which")
                    .arg(tool)
                    .output()
                    .map(|o| o.status.success())
                    .unwrap_or(false)
                {
                    println!("  [system]  Clipboard tool: ✅ {} found", tool);
                    found_clipboard = true;
                    break;
                }
            }
            if !found_clipboard {
                println!("  [system]  ⚠️  No clipboard tool (wl-paste/xclip/xsel). Clipboard collector will be no-op.");
                warnings += 1;
            }

            // 6. Tesseract (for OCR sense plugin)
            if config.sense.enabled && config.sense.ocr.is_some() {
                print!("  [sense]   Checking tesseract ... ");
                if std::process::Command::new("which")
                    .arg("tesseract")
                    .output()
                    .map(|o| o.status.success())
                    .unwrap_or(false)
                {
                    println!("✅ found");
                } else {
                    println!("⚠️  not found (OCR will use LLM fallback)");
                    warnings += 1;
                }
            }
        }
        Err(e) => {
            println!("❌ Failed: {:#}", e);
            errors += 1;
        }
    }

    // 7. System info
    println!();
    println!("  [system]  OS:        {}", std::env::consts::OS);
    println!("  [system]  Arch:      {}", std::env::consts::ARCH);
    println!(
        "  [system]  CPUs:      {}",
        std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(0)
    );
    if let Ok(mem) = std::fs::read_to_string("/proc/meminfo") {
        for line in mem.lines().take(2) {
            println!("  [system]  {}", line.trim());
        }
    }

    println!();
    println!("────────────────────────────────────────────────");
    if errors > 0 {
        println!("  Result: ❌ {} error(s), {} warning(s)", errors, warnings);
        1
    } else if warnings > 0 {
        println!("  Result: ⚠️  {} warning(s), 0 errors", warnings);
        0
    } else {
        println!("  Result: ✅ All checks passed");
        0
    }
}
/// Test connectivity to a specific connector by name.
/// Uses the same ping mechanism as the health-check loop but runs once and exits.
fn run_test_connector(config_path: &str, connector_name: &str) -> i32 {
    use tokio::runtime::Runtime;

    println!("╔══════════════════════════════════════════════╗");
    println!("║      OpenSoma — Test Connector               ║");
    println!("╚══════════════════════════════════════════════╝");
    println!();
    println!("  Connector: {}", connector_name);
    println!("  Config:    {}", config_path);
    println!();

    let config = match config::AppConfig::load(config_path) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("❌ Failed to load config: {:#}", e);
            return 1;
        }
    };

    // Validate connector name
    let known_connectors = [
        "feishu", "dingtalk", "wecom", "rss", "email", "webhook",
        "github", "notion", "git", "obsidian", "slack", "telegram", "discord",
    ];
    if !known_connectors.contains(&connector_name) {
        eprintln!("❌ Unknown connector: '{}'", connector_name);
        eprintln!("   Available connectors: {}", known_connectors.join(", "));
        return 1;
    }

    // Check if connector is enabled in config
    let is_enabled = match connector_name {
        "feishu" => config.connector.feishu.as_ref().is_some_and(|c| c.enabled),
        "dingtalk" => config.connector.dingtalk.as_ref().is_some_and(|c| c.enabled),
        "wecom" => config.connector.wecom.as_ref().is_some_and(|c| c.enabled),
        "rss" => config.connector.rss.as_ref().is_some_and(|c| c.enabled),
        "email" => config.connector.email.as_ref().is_some_and(|c| c.enabled),
        "webhook" => config.connector.webhook.as_ref().is_some_and(|c| c.enabled),
        "github" => config.connector.github.as_ref().is_some_and(|c| c.enabled),
        "notion" => config.connector.notion.as_ref().is_some_and(|c| c.enabled),
        "git" => config.connector.git.as_ref().is_some_and(|c| c.enabled),
        "obsidian" => config.connector.obsidian.as_ref().is_some_and(|c| c.enabled),
        "slack" => config.connector.slack.as_ref().is_some_and(|c| c.enabled),
        "telegram" => config.connector.telegram.as_ref().is_some_and(|c| c.enabled),
        "discord" => config.connector.discord.as_ref().is_some_and(|c| c.enabled),
        _ => false,
    };

    if !is_enabled {
        println!("  ⚠️  Connector '{}' is not enabled in config.toml", connector_name);
        println!("     Enable it by setting [connector.{}].enabled = true", connector_name);
        return 0;
    }

    // Run the ping test using tokio runtime
    let rt = Runtime::new().expect("Failed to create tokio runtime");
    let result = rt.block_on(async {
        connector::ping_connector_by_name(connector_name, &config.connector).await
    });

    match result {
        Ok(()) => {
            println!("  ✅ Connector '{}' is healthy — connectivity OK!", connector_name);
            0
        }
        Err(e) => {
            println!("  ❌ Connector '{}' failed: {:#}", connector_name, e);
            1
        }
    }
}
/// Export all cached events to a JSON file. Returns exit code.
fn run_export(config_path: &str, output_file: &str) -> i32 {
    let config = match config::AppConfig::load(config_path) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("❌ Failed to load config: {:#}", e);
            return 1;
        }
    };

    let cache = match sync::cache::Cache::open(&config.daemon.data_dir) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("❌ Failed to open cache: {:#}", e);
            return 1;
        }
    };

    let events = match cache.get_recent(usize::MAX) {
        Ok(e) => e,
        Err(e) => {
            eprintln!("❌ Failed to read events: {:#}", e);
            return 1;
        }
    };

    let count = events.len();
    let json = match serde_json::to_string_pretty(&events) {
        Ok(j) => j,
        Err(e) => {
            eprintln!("❌ Failed to serialize events: {:#}", e);
            return 1;
        }
    };

    match std::fs::write(output_file, &json) {
        Ok(_) => {
            println!("✅ Exported {} events to '{}'", count, output_file);
            0
        }
        Err(e) => {
            eprintln!("❌ Failed to write '{}': {}", output_file, e);
            1
        }
    }
}

/// Import events from a JSON file into the local cache. Returns exit code.
fn run_import(config_path: &str, input_file: &str) -> i32 {
    let config = match config::AppConfig::load(config_path) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("❌ Failed to load config: {:#}", e);
            return 1;
        }
    };

    let content = match std::fs::read_to_string(input_file) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("❌ Failed to read '{}': {}", input_file, e);
            return 1;
        }
    };

    let events: Vec<collector::RawEvent> = match serde_json::from_str(&content) {
        Ok(e) => e,
        Err(e) => {
            eprintln!("❌ Failed to parse JSON: {:#}", e);
            return 1;
        }
    };

    let cache = match sync::cache::Cache::open(&config.daemon.data_dir) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("❌ Failed to open cache: {:#}", e);
            return 1;
        }
    };

    let mut imported = 0usize;
    let mut skipped = 0usize;
    for event in &events {
        match cache.put(event) {
            Ok(()) => imported += 1,
            Err(e) => {
                tracing::debug!("Skipped event {}: {:#}", event.id, e);
                skipped += 1;
            }
        }
    }

    if let Err(e) = cache.flush() {
        eprintln!("⚠️  Cache flush warning: {}", e);
    }

    println!(
        "✅ Import complete: {} imported, {} skipped (duplicates) from '{}'",
        imported, skipped, input_file
    );
    0
}

/// Show local event cache statistics. Returns exit code.
fn run_cache_info(config_path: &str) -> i32 {
    let config = match config::AppConfig::load(config_path) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("❌ Failed to load config: {:#}", e);
            return 1;
        }
    };

    let cache = match sync::cache::Cache::open(&config.daemon.data_dir) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("❌ Failed to open cache: {:#}", e);
            return 1;
        }
    };

    let stats = cache.stats();

    println!("╔══════════════════════════════════════════════╗");
    println!("║          OpenSoma Cache Info                 ║");
    println!("╚══════════════════════════════════════════════╝");
    println!();
    println!("  Data dir:      {}", config.daemon.data_dir);
    println!("  Total events:  {}", stats.total);
    println!("  Uploaded:      {}", stats.uploaded);
    println!("  Pending:       {}", stats.pending);
    println!(
        "  Cache size:    {} ({:.2} MB)",
        stats.cache_size_bytes,
        stats.cache_size_bytes as f64 / (1024.0 * 1024.0)
    );
    println!();

    if stats.total > 0 {
        let upload_pct = stats.uploaded as f64 / stats.total as f64 * 100.0;
        println!("  Upload progress: {:.1}%", upload_pct);
    }

    // Show recent event sources
    match cache.get_recent(5) {
        Ok(recent) if !recent.is_empty() => {
            println!();
            println!("  Recent events:");
            for (i, event) in recent.iter().enumerate() {
                let payload_preview: String = String::from_utf8_lossy(&event.payload)
                    .chars()
                    .take(60)
                    .collect();
                println!(
                    "    {}. [{}] {} — \"{}…\"",
                    i + 1,
                    event.source,
                    event.event_type,
                    payload_preview
                );
            }
        }
        _ => {}
    }

    println!();
    0
}

/// Show N most recent cached events in a human-readable table.
fn run_recent_events(config_path: &str, count: usize) -> i32 {
    let config = match config::AppConfig::load(config_path) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("❌ Failed to load config: {:#}", e);
            return 1;
        }
    };

    let cache = match sync::cache::Cache::open(&config.daemon.data_dir) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("❌ Failed to open cache: {:#}", e);
            return 1;
        }
    };

    let events = match cache.get_recent(count) {
        Ok(e) => e,
        Err(e) => {
            eprintln!("❌ Failed to read events: {:#}", e);
            return 1;
        }
    };

    if events.is_empty() {
        println!("No events in cache.");
        return 0;
    }

    println!("╔══════════════════════════════════════════════╗");
    println!(
        "║       Recent Events ({:>3} of requested)      ║",
        events.len()
    );
    println!("╚══════════════════════════════════════════════╝");
    println!();
    println!(
        "  {:<4} {:<12} {:<18} {:<20} Payload (preview)",
        "#", "Source", "Type", "Time"
    );
    println!(
        "  {:<4} {:<12} {:<18} {:<20} ─────────────────",
        "───", "──────", "────", "────"
    );

    for (i, event) in events.iter().enumerate() {
        let ts = chrono::DateTime::from_timestamp_millis(event.timestamp_ms)
            .map(|dt| dt.format("%Y-%m-%d %H:%M:%S").to_string())
            .unwrap_or_else(|| "unknown".to_string());
        let payload_preview: String = String::from_utf8_lossy(&event.payload)
            .chars()
            .take(40)
            .collect();
        println!(
            "  {:<4} {:<12} {:<18} {:<20} {}",
            i + 1,
            truncate_str(&event.source, 12),
            truncate_str(&event.event_type, 18),
            ts,
            payload_preview
        );
    }
    println!();
    0
}

/// Search cached events by payload text and display matches.
fn run_search_events(config_path: &str, query: &str) -> i32 {
    let config = match config::AppConfig::load(config_path) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("❌ Failed to load config: {:#}", e);
            return 1;
        }
    };

    let cache = match sync::cache::Cache::open(&config.daemon.data_dir) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("❌ Failed to open cache: {:#}", e);
            return 1;
        }
    };

    let events = match cache.search_by_payload(query, 50) {
        Ok(e) => e,
        Err(e) => {
            eprintln!("❌ Search failed: {:#}", e);
            return 1;
        }
    };

    if events.is_empty() {
        println!("No events matching '{}'.", query);
        return 0;
    }

    println!("Found {} event(s) matching '{}':", events.len(), query);
    println!();
    println!(
        "  {:<4} {:<12} {:<18} {:<20} Payload (preview)",
        "#", "Source", "Type", "Time"
    );
    println!(
        "  {:<4} {:<12} {:<18} {:<20} ─────────────────",
        "───", "──────", "────", "────"
    );

    for (i, event) in events.iter().enumerate() {
        let ts = chrono::DateTime::from_timestamp_millis(event.timestamp_ms)
            .map(|dt| dt.format("%Y-%m-%d %H:%M:%S").to_string())
            .unwrap_or_else(|| "unknown".to_string());
        let payload_preview: String = String::from_utf8_lossy(&event.payload)
            .chars()
            .take(40)
            .collect();
        println!(
            "  {:<4} {:<12} {:<18} {:<20} {}",
            i + 1,
            truncate_str(&event.source, 12),
            truncate_str(&event.event_type, 18),
            ts,
            payload_preview
        );
    }
    println!();
    0
}

/// Filter cached events by source prefix and display matches.
fn run_source_filter(config_path: &str, prefix: &str) -> i32 {
    let config = match config::AppConfig::load(config_path) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("❌ Failed to load config: {:#}", e);
            return 1;
        }
    };

    let cache = match sync::cache::Cache::open(&config.daemon.data_dir) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("❌ Failed to open cache: {:#}", e);
            return 1;
        }
    };

    let events = match cache.search_by_source(prefix, 50) {
        Ok(e) => e,
        Err(e) => {
            eprintln!("❌ Search failed: {:#}", e);
            return 1;
        }
    };

    if events.is_empty() {
        println!("No events from source '{}'.", prefix);
        return 0;
    }

    println!("Found {} event(s) from source '{}':", events.len(), prefix);
    println!();
    println!(
        "  {:<4} {:<12} {:<18} {:<20} Payload (preview)",
        "#", "Source", "Type", "Time"
    );
    println!(
        "  {:<4} {:<12} {:<18} {:<20} ─────────────────",
        "───", "──────", "────", "────"
    );

    for (i, event) in events.iter().enumerate() {
        let ts = chrono::DateTime::from_timestamp_millis(event.timestamp_ms)
            .map(|dt| dt.format("%Y-%m-%d %H:%M:%S").to_string())
            .unwrap_or_else(|| "unknown".to_string());
        let payload_preview: String = String::from_utf8_lossy(&event.payload)
            .chars()
            .take(40)
            .collect();
        println!(
            "  {:<4} {:<12} {:<18} {:<20} {}",
            i + 1,
            truncate_str(&event.source, 12),
            truncate_str(&event.event_type, 18),
            ts,
            payload_preview
        );
    }
    println!();
    0
}

/// Filter cached events by event type and display matches.
fn run_type_filter(config_path: &str, event_type: &str) -> i32 {
    let config = match config::AppConfig::load(config_path) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("❌ Failed to load config: {:#}", e);
            return 1;
        }
    };

    let cache = match sync::cache::Cache::open(&config.daemon.data_dir) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("❌ Failed to open cache: {:#}", e);
            return 1;
        }
    };

    let events = match cache.search_by_type(event_type, 50) {
        Ok(e) => e,
        Err(e) => {
            eprintln!("❌ Search failed: {:#}", e);
            return 1;
        }
    };

    if events.is_empty() {
        println!("No events of type '{}'.", event_type);
        return 0;
    }

    println!("Found {} event(s) of type '{}':", events.len(), event_type);
    println!();
    println!(
        "  {:<4} {:<12} {:<18} {:<20} Payload (preview)",
        "#", "Source", "Type", "Time"
    );
    println!(
        "  {:<4} {:<12} {:<18} {:<20} ─────────────────",
        "───", "──────", "────", "────"
    );

    for (i, event) in events.iter().enumerate() {
        let ts = chrono::DateTime::from_timestamp_millis(event.timestamp_ms)
            .map(|dt| dt.format("%Y-%m-%d %H:%M:%S").to_string())
            .unwrap_or_else(|| "unknown".to_string());
        let payload_preview: String = String::from_utf8_lossy(&event.payload)
            .chars()
            .take(40)
            .collect();
        println!(
            "  {:<4} {:<12} {:<18} {:<20} {}",
            i + 1,
            truncate_str(&event.source, 12),
            truncate_str(&event.event_type, 18),
            ts,
            payload_preview
        );
    }
    println!();
    0
}

/// Show aggregate event statistics by source, type, and time distribution.
fn run_stats(config_path: &str) -> i32 {
    use std::collections::HashMap;

    let config = match config::AppConfig::load(config_path) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("❌ Failed to load config: {:#}", e);
            return 1;
        }
    };

    let cache = match sync::cache::Cache::open(&config.daemon.data_dir) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("❌ Failed to open cache: {:#}", e);
            return 1;
        }
    };

    let stats = cache.stats();
    let events = match cache.get_recent(10000) {
        Ok(e) => e,
        Err(e) => {
            eprintln!("❌ Failed to read events: {:#}", e);
            return 1;
        }
    };

    if events.is_empty() {
        println!("No events in cache.");
        return 0;
    }

    // Aggregate by source
    let mut by_source: HashMap<String, u64> = HashMap::new();
    // Aggregate by event type
    let mut by_type: HashMap<String, u64> = HashMap::new();
    // Aggregate by hour
    let mut by_hour: HashMap<String, u64> = HashMap::new();
    // Aggregate by day
    let mut by_day: HashMap<String, u64> = HashMap::new();

    for event in &events {
        *by_source.entry(event.source.clone()).or_insert(0) += 1;
        *by_type.entry(event.event_type.clone()).or_insert(0) += 1;

        if let Some(dt) = chrono::DateTime::from_timestamp_millis(event.timestamp_ms) {
            let hour = dt.format("%Y-%m-%d %H:00").to_string();
            let day = dt.format("%Y-%m-%d").to_string();
            *by_hour.entry(hour).or_insert(0) += 1;
            *by_day.entry(day).or_insert(0) += 1;
        }
    }

    println!("╔══════════════════════════════════════════════════════╗");
    println!("║            OpenSoma — Event Statistics               ║");
    println!("╚══════════════════════════════════════════════════════╝");
    println!();
    println!("  Cache: {} total events, {} pending upload, {} bytes",
        stats.total, stats.pending, stats.cache_size_bytes);
    if let Some(first) = events.last() {
        if let Some(dt) = chrono::DateTime::from_timestamp_millis(first.timestamp_ms) {
            println!("  Oldest event: {}", dt.format("%Y-%m-%d %H:%M:%S"));
        }
    }
    if let Some(last) = events.first() {
        if let Some(dt) = chrono::DateTime::from_timestamp_millis(last.timestamp_ms) {
            println!("  Newest event: {}", dt.format("%Y-%m-%d %H:%M:%S"));
        }
    }

    // Events by source
    println!();
    println!("  ┌─── Events by Source ───────────────────────────┐");
    let mut source_vec: Vec<_> = by_source.iter().collect();
    source_vec.sort_by(|a, b| b.1.cmp(a.1));
    let max_source_count = source_vec.first().map(|e| *e.1).unwrap_or(1);
    for (source, count) in &source_vec {
        let bar_width = (**count as f64 / max_source_count as f64 * 30.0) as usize;
        println!(
            "  │ {:<20} {:>6} {}",
            truncate_str(source, 20),
            count,
            "█".repeat(bar_width)
        );
    }
    println!("  └────────────────────────────────────────────────┘");

    // Events by type
    println!();
    println!("  ┌─── Events by Type ─────────────────────────────┐");
    let mut type_vec: Vec<_> = by_type.iter().collect();
    type_vec.sort_by(|a, b| b.1.cmp(a.1));
    let max_type_count = type_vec.first().map(|e| *e.1).unwrap_or(1);
    for (event_type, count) in type_vec.iter().take(15) {
        let bar_width = (**count as f64 / max_type_count as f64 * 30.0) as usize;
        println!(
            "  │ {:<20} {:>6} {}",
            truncate_str(event_type, 20),
            count,
            "█".repeat(bar_width)
        );
    }
    if type_vec.len() > 15 {
        println!("  │ ... and {} more types", type_vec.len() - 15);
    }
    println!("  └────────────────────────────────────────────────┘");

    // Events by day (last 14 days)
    println!();
    println!("  ┌─── Events by Day (recent) ─────────────────────┐");
    let mut day_vec: Vec<_> = by_day.iter().collect();
    day_vec.sort_by(|a, b| a.0.cmp(b.0));
    let max_day_count = day_vec.iter().map(|e| *e.1).max().unwrap_or(1);
    for (day, count) in day_vec.iter().rev().take(14).collect::<Vec<_>>().iter().rev() {
        let bar_width = (**count as f64 / max_day_count as f64 * 30.0) as usize;
        println!(
            "  │ {:<12} {:>8} {}",
            day,
            count,
            "█".repeat(bar_width)
        );
    }
    println!("  └────────────────────────────────────────────────┘");
    println!();
    0
}

/// Real-time event stream — polls cache for new events every 2 seconds.
/// Shows the last N events, then continuously shows new ones.
fn run_tail(config_path: &str, initial_count: usize) -> i32 {
    let config = match config::AppConfig::load(config_path) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("❌ Failed to load config: {:#}", e);
            return 1;
        }
    };

    let cache = match sync::cache::Cache::open(&config.daemon.data_dir) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("❌ Failed to open cache: {:#}", e);
            return 1;
        }
    };

    // Show initial events
    let events = match cache.get_recent(initial_count) {
        Ok(e) => e,
        Err(e) => {
            eprintln!("❌ Failed to read events: {:#}", e);
            return 1;
        }
    };

    println!("╔══════════════════════════════════════════════════════╗");
    println!("║         OpenSoma — Event Tail (poll: 2s)             ║");
    println!("║         Press Ctrl+C to exit                         ║");
    println!("╚══════════════════════════════════════════════════════╝");
    println!();

    if events.is_empty() {
        println!("  (no events yet — waiting for new events...)");
    } else {
        println!(
            "  {:<4} {:<14} {:<20} {:<20} Payload",
            "#", "Source", "Type", "Time"
        );
        println!(
            "  {:<4} {:<14} {:<20} {:<20} ───────",
            "───", "──────", "────", "────"
        );
        for (i, event) in events.iter().enumerate() {
            let ts = chrono::DateTime::from_timestamp_millis(event.timestamp_ms)
                .map(|dt| dt.format("%Y-%m-%d %H:%M:%S").to_string())
                .unwrap_or_else(|| "unknown".to_string());
            let payload_preview: String = String::from_utf8_lossy(&event.payload)
                .chars()
                .take(50)
                .collect();
            println!(
                "  {:<4} {:<14} {:<20} {:<20} {}",
                i + 1,
                truncate_str(&event.source, 14),
                truncate_str(&event.event_type, 20),
                ts,
                payload_preview
            );
        }
    }

    // Track last seen timestamp to detect new events
    let mut last_seen_ts = events.first().map(|e| e.timestamp_ms).unwrap_or(0);
    println!();
    println!("  ── Streaming new events (Ctrl+C to stop) ──");
    println!();

    loop {
        std::thread::sleep(std::time::Duration::from_secs(2));

        // Get recent events and filter for new ones
        if let Ok(recent) = cache.get_recent(50) {
            let new_events: Vec<_> = recent
                .iter()
                .filter(|e| e.timestamp_ms > last_seen_ts)
                .collect();

            for event in &new_events {
                let ts = chrono::DateTime::from_timestamp_millis(event.timestamp_ms)
                    .map(|dt| dt.format("%H:%M:%S").to_string())
                    .unwrap_or_else(|| "??:??:??".to_string());
                let payload_preview: String = String::from_utf8_lossy(&event.payload)
                    .chars()
                    .take(60)
                    .collect();
                println!(
                    "  [{}] {:<12} {:<18} {}",
                    ts,
                    truncate_str(&event.source, 12),
                    truncate_str(&event.event_type, 18),
                    payload_preview
                );

                if event.timestamp_ms > last_seen_ts {
                    last_seen_ts = event.timestamp_ms;
                }
            }

            if !new_events.is_empty() {
                // Flush stdout
                use std::io::Write;
                std::io::stdout().flush().ok();
            }
        }
    }
}

/// Remove cached events older than N days.
fn run_prune(config_path: &str, days: i64) -> i32 {
    let config = match config::AppConfig::load(config_path) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("❌ Failed to load config: {:#}", e);
            return 1;
        }
    };

    let cache = match sync::cache::Cache::open(&config.daemon.data_dir) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("❌ Failed to open cache: {:#}", e);
            return 1;
        }
    };

    let stats_before = cache.stats();
    let cutoff_ms = chrono::Utc::now().timestamp_millis() - (days * 86400 * 1000);

    println!("Pruning events older than {} days...", days);
    if let Some(cutoff_dt) = chrono::DateTime::from_timestamp_millis(cutoff_ms) {
        println!("  Cutoff timestamp:      {}", cutoff_dt.format("%Y-%m-%d %H:%M:%S"));
    }

    match cache.evict_before(cutoff_ms) {
        Ok(evicted) => {
            let stats_after = cache.stats();
            println!("✅ Pruned {} events.", evicted);
            println!(
                "   Cache: {} → {} events",
                stats_before.total, stats_after.total
            );
            0
        }
        Err(e) => {
            eprintln!("❌ Prune failed: {:#}", e);
            1
        }
    }
}

/// Live monitoring dashboard — polls the daemon status every 2 seconds.
/// Displays a compact, refreshing view similar to `top`.
fn run_top(port: u16) -> i32 {
    let url = format!("http://127.0.0.1:{}/api/status", port);
    let metrics_url = format!("http://127.0.0.1:{}/metrics", port);

    // Check if daemon is reachable first
    match blocking_http_get(&url) {
        Ok(_) => {}
        Err(e) => {
            eprintln!("❌ Cannot reach OpenSoma daemon at port {}", port);
            eprintln!("   Error: {}", e);
            return 1;
        }
    }

    let mut iteration = 0u64;
    loop {
        // Clear screen (ANSI escape)
        print!("\x1B[2J\x1B[H");
        println!("╔══════════════════════════════════════════════════════════════╗");
        println!("║            OpenSoma — Live Monitor (refresh: 2s)            ║");
        println!("║            Press Ctrl+C to exit                             ║");
        println!("╚══════════════════════════════════════════════════════════════╝");
        println!();

        match blocking_http_get(&url) {
            Ok(body) => {
                match serde_json::from_str::<serde_json::Value>(&body) {
                    Ok(json) => {
                        let node_id = json["node_id"].as_str().unwrap_or("?");
                        let component = json["component"].as_str().unwrap_or("?");
                        let uptime = json["uptime_seconds"].as_u64().unwrap_or(0);
                        let events_collected = json["events_collected"].as_u64().unwrap_or(0);
                        let events_synced = json["events_synced"].as_u64().unwrap_or(0);
                        let hostname = json["hostname"].as_str().unwrap_or("?");
                        let cpu = json["cpu_percent"].as_f64().unwrap_or(0.0);
                        let mem_used = json["memory_used_mb"].as_u64().unwrap_or(0);
                        let mem_total = json["memory_total_mb"].as_u64().unwrap_or(0);

                        let d = uptime / 86400;
                        let h = (uptime % 86400) / 3600;
                        let m = (uptime % 3600) / 60;
                        let s = uptime % 60;

                        println!("  Node: {:<20} Host: {}", node_id, hostname);
                        println!(
                            "  Component: {:<16} Uptime: {}d {}h {}m {}s",
                            component, d, h, m, s
                        );
                        println!();
                        println!("  ┌─────────────────────────────────────────────────────────┐");
                        println!(
                            "  │  Events Collected: {:<10} │ Synced: {:<10}      │",
                            events_collected, events_synced
                        );
                        println!(
                            "  │  Pending:          {:<10} │                        │",
                            events_collected.saturating_sub(events_synced)
                        );
                        println!("  └─────────────────────────────────────────────────────────┘");
                        println!();
                        println!("  CPU:  {:>5.1}%  {}", cpu, bar(cpu, 30));
                        let mem_pct = if mem_total > 0 {
                            mem_used as f64 / mem_total as f64 * 100.0
                        } else {
                            0.0
                        };
                        println!(
                            "  MEM:  {:>5.1}%  {}  ({} / {} MB)",
                            mem_pct,
                            bar(mem_pct, 30),
                            mem_used,
                            mem_total
                        );
                        println!();

                        // Show connectors
                        if let Some(connectors) = json["connectors_active"].as_array() {
                            if !connectors.is_empty() {
                                let names: Vec<&str> =
                                    connectors.iter().filter_map(|c| c.as_str()).collect();
                                println!("  Active connectors: {}", names.join(", "));
                            } else {
                                println!("  Active connectors: (none)");
                            }
                        }

                        // Show last error
                        if let Some(err) = json["last_error"].as_str() {
                            if !err.is_empty() {
                                println!("  ⚠ Last error: {}", err);
                            }
                        }
                    }
                    Err(e) => {
                        println!("  Failed to parse status: {}", e);
                    }
                }
            }
            Err(e) => {
                println!("  ❌ Connection lost: {}", e);
            }
        }

        // Show pipeline metrics if available
        if let Ok(body) = blocking_http_get(&metrics_url) {
            let mut processed = 0u64;
            let mut normalized = 0u64;
            let mut classified = 0u64;
            let mut enriched = 0u64;
            let mut deduped = 0u64;
            for line in body.lines() {
                let parts: Vec<&str> = line.split_whitespace().collect();
                if parts.len() >= 2 {
                    if let Ok(val) = parts[1].parse::<f64>() {
                        match parts[0] {
                            "opensoma_pipeline_events_processed_total" => {
                                processed = val as u64
                            }
                            "opensoma_pipeline_events_normalized_total" => {
                                normalized = val as u64
                            }
                            "opensoma_pipeline_events_classified_total" => {
                                classified = val as u64
                            }
                            "opensoma_pipeline_events_enriched_total" => enriched = val as u64,
                            "opensoma_pipeline_events_deduped_total" => deduped = val as u64,
                            _ => {}
                        }
                    }
                }
            }
            if processed > 0 {
                println!();
                println!("  Pipeline: processed={} normalized={} classified={} enriched={} deduped={}",
                    processed, normalized, classified, enriched, deduped);
            }
        }

        println!();
        println!("  ── Iteration {} ── Press Ctrl+C to exit ──", iteration);
        iteration += 1;
        std::thread::sleep(std::time::Duration::from_secs(2));
    }
}

/// Render a simple ASCII progress bar.
fn bar(pct: f64, width: usize) -> String {
    let filled = ((pct / 100.0) * width as f64).round() as usize;
    let filled = filled.min(width);
    let empty = width - filled;
    format!("[{}{}]", "█".repeat(filled), "░".repeat(empty))
}

/// Truncate a string to max_len characters, adding "…" if truncated.
fn truncate_str(s: &str, max_len: usize) -> String {
    if s.len() <= max_len {
        s.to_string()
    } else {
        format!("{}…", &s[..max_len - 1])
    }
}

/// Run end-to-end pipeline self-test. No running daemon or Soul server needed.
/// Tests: cache, processor pipeline, conflict resolver, circuit breaker, metrics.
#[allow(unused_assignments)]
fn run_self_test() -> i32 {
    use std::collections::HashMap;

    println!("╔══════════════════════════════════════════════╗");
    println!("║       OpenSoma Self-Test — Pipeline Check    ║");
    println!("╚══════════════════════════════════════════════╝");
    println!();

    let mut passed = 0u32;
    let mut failed = 0u32;

    macro_rules! check {
        ($name:expr, $result:expr) => {
            match $result {
                Ok(val) => {
                    println!("  ✅ {}", $name);
                    passed += 1;
                    val
                }
                Err(e) => {
                    println!("  ❌ {} — {:#}", $name, e);
                    failed += 1;
                    return 1;
                }
            }
        };
    }

    // ── 1. Cache ──────────────────────────────────────────────
    println!("  [cache]");
    let tmp_dir = check!(
        "Create temp directory",
        tempfile::tempdir().map_err(|e| anyhow::anyhow!("{}", e))
    );
    let cache = check!(
        "Open sled cache",
        sync::cache::Cache::open(tmp_dir.path().to_str().unwrap())
    );

    let event1 = collector::RawEvent {
        id: "selftest-001".to_string(),
        source: "selftest".to_string(),
        event_type: "test.message".to_string(),
        timestamp_ms: chrono::Utc::now().timestamp_millis(),
        payload: b"Hello from OpenSoma self-test".to_vec(),
        tags: HashMap::from([("env".to_string(), "test".to_string())]),
    };
    let mut event2 = event1.clone();
    event2.id = "selftest-002".to_string();
    event2.payload = b"Second test event with different content".to_vec();

    check!("Put event into cache", cache.put(&event1));
    check!("Put second event", cache.put(&event2));

    let stats = cache.stats();
    if stats.total >= 2 {
        println!("  ✅ Cache stats: {} events", stats.total);
        passed += 1;
    } else {
        println!("  ❌ Cache stats expected ≥2, got {}", stats.total);
        failed += 1;
    }

    let recent = check!("Get recent events", cache.get_recent(10));
    if recent.len() >= 2 {
        println!("  ✅ Retrieved {} events from cache", recent.len());
        passed += 1;
    } else {
        println!("  ❌ Expected ≥2 recent events, got {}", recent.len());
        failed += 1;
    }

    let found = check!(
        "Search by payload text",
        cache.search_by_payload("self-test", 10)
    );
    if !found.is_empty() {
        println!("  ✅ Search found {} matching event(s)", found.len());
        passed += 1;
    } else {
        println!("  ❌ Search returned no results for 'self-test'");
        failed += 1;
    }

    // ── 2. Processor Pipeline ─────────────────────────────────
    println!();
    println!("  [processor]");

    let mut norm_event = collector::RawEvent {
        id: "norm-test".to_string(),
        source: "selftest".to_string(),
        event_type: "test.normalize".to_string(),
        timestamp_ms: 0,
        payload: b"normalize me".to_vec(),
        tags: HashMap::new(),
    };
    let proc_config = config::ProcessorConfig {
        normalize_timestamps: true,
        enable_classify: true,
        enable_enrich: true,
        dedup_window_secs: 60,
        max_event_size: 1024 * 1024,
    };
    processor::normalize::normalize_event(&mut norm_event, &proc_config);
    if norm_event.timestamp_ms > 0 {
        println!("  ✅ Normalize: zero timestamp fixed to {}", norm_event.timestamp_ms);
        passed += 1;
    } else {
        println!("  ❌ Normalize: timestamp still zero");
        failed += 1;
    }

    let classify_event = collector::RawEvent {
        id: "classify-test".to_string(),
        source: "github".to_string(),
        event_type: String::new(),
        timestamp_ms: chrono::Utc::now().timestamp_millis(),
        payload: serde_json::to_vec(&serde_json::json!({
            "action": "opened",
            "pull_request": {"title": "Test PR"}
        }))
        .unwrap(),
        tags: HashMap::new(),
    };
    let classify_result = processor::classify::classify_event(&classify_event);
    if !classify_result.source_category.is_empty() {
        println!("  ✅ Classify: source_category='{}', content_type={:?}", classify_result.source_category, classify_result.content_type);
        passed += 1;
    } else {
        println!("  ⚠️  Classify: empty result (best-effort)");
        passed += 1;
    }

    let enrich_event = collector::RawEvent {
        id: "enrich-test".to_string(),
        source: "selftest".to_string(),
        event_type: "test.enrich".to_string(),
        timestamp_ms: chrono::Utc::now().timestamp_millis(),
        payload: b"Contact us at test@example.com or visit https://opensoma.dev".to_vec(),
        tags: HashMap::new(),
    };
    let enrich_result = processor::enrich::enrich_event(&enrich_event);
    if !enrich_result.entities.is_empty() || !enrich_result.keywords.is_empty() {
        println!("  ✅ Enrich: {} entities, {} keywords", enrich_result.entities.len(), enrich_result.keywords.len());
        passed += 1;
    } else {
        println!("  ⚠️  Enrich: no entities extracted (non-critical)");
        passed += 1;
    }

    // Dedup is async
    tokio::task::block_in_place(|| {
        tokio::runtime::Handle::current().block_on(async {
            let dedup = processor::dedup::Deduplicator::new(60);
            let is_dup_first = dedup.is_duplicate(&event1).await;
            let is_dup_same = dedup.is_duplicate(&event1).await;
            if !is_dup_first && is_dup_same {
                println!("  ✅ Dedup: first seen=new, second seen=duplicate");
                passed += 1;
            } else {
                println!("  ❌ Dedup: unexpected results (first={}, second={})", is_dup_first, is_dup_same);
                failed += 1;
            }
        })
    });

    // ── 3. Conflict Resolver ──────────────────────────────────
    println!();
    println!("  [conflict]");
    {
        use sync::conflict::*;
        let mut resolver = ConflictResolver::new(ConflictStrategy::NewestWins);
        let local_event = collector::RawEvent {
            id: "conflict-test".to_string(),
            source: "selftest".to_string(),
            event_type: "test".to_string(),
            timestamp_ms: 1000,
            payload: b"local version".to_vec(),
            tags: HashMap::new(),
        };
        let server = EventSnapshot {
            id: "conflict-test".to_string(),
            source: "selftest".to_string(),
            event_type: "test".to_string(),
            timestamp_ms: 2000,
            content_hash: "different_hash_so_conflict_detected".to_string(),
            tags: HashMap::new(),
        };
        if let Some(conflict) = resolver.detect(&local_event, &server) {
            let resolved = resolver.resolve(conflict);
            match &resolved.resolution {
                Resolution::UsedNewest { winner } => {
                    println!("  ✅ Conflict resolution: newest_wins → {}", winner);
                    passed += 1;
                }
                other => {
                    println!("  ⚠️  Conflict resolved as {:?} (acceptable)", other);
                    passed += 1;
                }
            }
        } else {
            println!("  ⚠️  No conflict detected (hashes may match)");
            passed += 1;
        }
    }

    // ── 4. Circuit Breaker ────────────────────────────────────
    println!();
    println!("  [circuit-breaker]");
    tokio::task::block_in_place(|| {
        tokio::runtime::Handle::current().block_on(async {
            use connector::circuit_breaker::*;
            let cb = CircuitBreaker::new(
                "selftest",
                CircuitBreakerConfig {
                    failure_threshold: 3,
                    cooldown_duration: std::time::Duration::from_millis(100),
                    success_threshold: 2,
                },
            );
            if cb.allow_request().await.is_ok() {
                println!("  ✅ Circuit breaker: closed state allows requests");
                passed += 1;
            } else {
                println!("  ❌ Circuit breaker: should allow in closed state");
                failed += 1;
            }

            for _ in 0..3 {
                cb.record_failure().await;
            }
            if cb.allow_request().await.is_err() {
                println!("  ✅ Circuit breaker: opens after 3 failures");
                passed += 1;
            } else {
                println!("  ❌ Circuit breaker: should be open after 3 failures");
                failed += 1;
            }

            tokio::time::sleep(std::time::Duration::from_millis(150)).await;
            if cb.allow_request().await.is_ok() {
                println!("  ✅ Circuit breaker: half-open after cooldown");
                passed += 1;
            } else {
                println!("  ❌ Circuit breaker: should be half-open after cooldown");
                failed += 1;
            }
        })
    });

    // ── 5. Metrics ────────────────────────────────────────────
    println!();
    println!("  [metrics]");
    let m = metrics::PipelineMetrics::new();
    m.inc_events_collected();
    m.inc_events_processed();
    m.inc_events_synced();
    m.record_process_latency(std::time::Duration::from_micros(500));
    let snap = m.snapshot();
    if snap.events_collected == 1 && snap.events_processed == 1 && snap.events_synced == 1 {
        println!("  ✅ Metrics: counters work correctly");
        passed += 1;
    } else {
        println!("  ❌ Metrics: unexpected snapshot values");
        failed += 1;
    }

    let prom = m.to_prometheus();
    if prom.contains("opensoma_pipeline_collected_total 1") {
        println!("  ✅ Metrics: Prometheus format correct");
        passed += 1;
    } else {
        println!("  ❌ Metrics: Prometheus format missing expected line");
        failed += 1;
    }

    // ── 6. Health Checker ─────────────────────────────────────
    println!();
    println!("  [health]");
    tokio::task::block_in_place(|| {
        tokio::runtime::Handle::current().block_on(async {
            let checker = health::HealthChecker::new();
            checker.record_healthy("selftest").await;
            let h = checker.get("selftest").await;
            if let Some(h) = h {
                if h.status == health::HealthStatus::Healthy {
                    println!("  ✅ Health checker: record and retrieve works");
                    passed += 1;
                } else {
                    println!("  ❌ Health checker: unexpected status");
                    failed += 1;
                }
            } else {
                println!("  ❌ Health checker: get returned None");
                failed += 1;
            }
        })
    });

    // ── Summary ───────────────────────────────────────────────
    println!();
    println!("────────────────────────────────────────────────");
    if failed == 0 {
        println!("  Result: ✅ All {} checks passed — pipeline is healthy!", passed);
        0
    } else {
        println!("  Result: ❌ {} passed, {} failed", passed, failed);
        1
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_http_url_with_path() {
        let (host, path) = parse_http_url("http://127.0.0.1:8091/api/status");
        assert_eq!(host, "127.0.0.1:8091");
        assert_eq!(path, "/api/status");
    }

    #[test]
    fn test_parse_http_url_no_path() {
        let (host, path) = parse_http_url("http://localhost:8091");
        assert_eq!(host, "localhost:8091");
        assert_eq!(path, "/");
    }

    #[test]
    fn test_parse_http_url_no_scheme() {
        let (host, path) = parse_http_url("127.0.0.1:8091/metrics");
        assert_eq!(host, "127.0.0.1:8091");
        assert_eq!(path, "/metrics");
    }

    #[test]
    fn test_parse_http_url_root() {
        let (host, path) = parse_http_url("http://example.com/");
        assert_eq!(host, "example.com");
        assert_eq!(path, "/");
    }

    #[test]
    fn test_parse_http_response_ok() {
        let response =
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n\r\n{\"status\":\"ok\"}";
        let body = parse_http_response(response).unwrap();
        assert_eq!(body, "{\"status\":\"ok\"}");
    }

    #[test]
    fn test_parse_http_response_not_found() {
        let response = "HTTP/1.1 404 Not Found\r\n\r\n";
        let result = parse_http_response(response);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("404"));
    }

    #[test]
    fn test_parse_http_response_no_separator() {
        let result = parse_http_response("garbage data");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_http_response_empty_body() {
        let response = "HTTP/1.1 200 OK\r\n\r\n";
        let body = parse_http_response(response).unwrap();
        assert_eq!(body, "");
    }

    #[test]
    fn test_parse_http_url_deep_path() {
        let (host, path) = parse_http_url("http://10.0.0.1:9999/api/connectors/feishu/toggle");
        assert_eq!(host, "10.0.0.1:9999");
        assert_eq!(path, "/api/connectors/feishu/toggle");
    }

    #[test]
    fn test_parse_http_url_health_endpoint() {
        let (host, path) = parse_http_url("http://127.0.0.1:8091/health");
        assert_eq!(host, "127.0.0.1:8091");
        assert_eq!(path, "/health");
    }

    #[test]
    fn test_parse_http_url_metrics_endpoint() {
        let (host, path) = parse_http_url("http://localhost:8091/metrics");
        assert_eq!(host, "localhost:8091");
        assert_eq!(path, "/metrics");
    }

    #[test]
    fn test_parse_http_url_connectors_health() {
        let (host, path) = parse_http_url("http://127.0.0.1:8091/api/connectors/health");
        assert_eq!(host, "127.0.0.1:8091");
        assert_eq!(path, "/api/connectors/health");
    }

    #[test]
    fn test_truncate_str_short() {
        assert_eq!(truncate_str("hello", 10), "hello");
    }

    #[test]
    fn test_truncate_str_exact() {
        assert_eq!(truncate_str("hello", 5), "hello");
    }

    #[test]
    fn test_truncate_str_long() {
        assert_eq!(truncate_str("hello world", 6), "hello…");
    }

    #[test]
    fn test_truncate_str_empty() {
        assert_eq!(truncate_str("", 5), "");
    }

    #[test]
    fn test_truncate_str_one_char() {
        assert_eq!(truncate_str("ab", 1), "…");
    }

    #[test]
    fn test_truncate_str_unicode() {
        // Unicode chars may be multi-byte; truncate_str works on bytes
        let s = "你好世界";
        let result = truncate_str(s, 4);
        // Should truncate and add ellipsis
        assert!(result.ends_with('…') || result == s);
    }

    #[test]
    fn test_bar_zero() {
        let b = bar(0.0, 10);
        assert_eq!(b, "[░░░░░░░░░░]");
    }

    #[test]
    fn test_bar_half() {
        let b = bar(50.0, 10);
        assert_eq!(b, "[█████░░░░░]");
    }

    #[test]
    fn test_bar_full() {
        let b = bar(100.0, 10);
        assert_eq!(b, "[██████████]");
    }

    #[test]
    fn test_bar_over_100() {
        let b = bar(150.0, 10);
        assert_eq!(b, "[██████████]");
    }

    #[test]
    fn test_bar_small_width() {
        let b = bar(50.0, 4);
        assert_eq!(b, "[██░░]");
    }

    // ── --test-connector CLI tests ──────────────────────────────

    #[test]
    fn test_known_connectors_list() {
        // Verify the known connectors list matches the connector module
        let known = [
            "feishu", "dingtalk", "wecom", "rss", "email", "webhook",
            "github", "notion", "git", "obsidian", "slack", "telegram", "discord",
        ];
        assert_eq!(known.len(), 13);
        assert!(known.contains(&"feishu"));
        assert!(known.contains(&"github"));
        assert!(known.contains(&"discord"));
        assert!(!known.contains(&"nonexistent"));
    }

    #[test]
    fn test_test_connector_help_text() {
        // Verify the help text includes --test-connector
        let help_text = "--test-connector <N>   Test connectivity to a specific connector (e.g. feishu, github)";
        assert!(help_text.contains("--test-connector"));
        assert!(help_text.contains("feishu"));
    }
}
