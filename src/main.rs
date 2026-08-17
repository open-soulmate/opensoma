#![allow(dead_code)]
mod config;
mod heartbeat;
mod collector;
mod connector;
mod plugins;
mod processor;
mod sync;
mod grpc;
mod status_server;

use anyhow::Result;
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
    if let Err(e) = grpc_client.register_node(&config.daemon.node_id, "soma").await {
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

    // Start processor pipeline: raw_rx → normalize → dedup → processed_tx
    let processor_handle = processor::start_pipeline(
        raw_rx,
        processed_tx,
        &config.processor,
    );

    // Start sync engine: processed_rx → cache → upload to Soul
    let sync_handle = sync::start_engine_with_rx(
        &config.sync,
        cache,
        grpc_client.clone(),
        processed_rx,
    );

    // Start HTTP status server for monitoring
    let status_state = status_server::StatusServerState {
        node_id: config.daemon.node_id.clone(),
        start_time: std::time::Instant::now(),
        events_collected: std::sync::Arc::new(tokio::sync::RwLock::new(0)),
        events_synced: std::sync::Arc::new(tokio::sync::RwLock::new(0)),
        connectors_active: std::sync::Arc::new(tokio::sync::RwLock::new(Vec::new())),
        last_error: std::sync::Arc::new(tokio::sync::RwLock::new(None)),
    };
    let status_handle = status_server::start_status_server(
        config.daemon.status_port,
        status_state,
    ).await;

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
