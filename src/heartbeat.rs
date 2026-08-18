use serde::Serialize;
use std::time::Duration;
use sysinfo::System;
use tokio::task::JoinHandle;
use tracing::{error, info};

use crate::grpc::client::SoulClient;

/// Heartbeat payload logged locally and sent to Soul.
#[derive(Debug, Serialize)]
pub struct HeartbeatPayload {
    pub node_id: String,
    pub timestamp_ms: i64,
    pub hostname: String,
    pub ip: String,
    pub cpu_usage: f32,
    pub memory_total_mb: u64,
    pub memory_used_mb: u64,
    pub disk_total_mb: u64,
    pub disk_used_mb: u64,
}

/// Start the heartbeat loop. Every `interval` seconds, collects node status
/// (hostname, ip, cpu, memory, disk) and sends a heartbeat to Soul via gRPC.
pub fn start(node_id: String, interval: u64, client: SoulClient) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(Duration::from_secs(interval));
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        let hostname = System::host_name().unwrap_or_else(|| "unknown".to_string());

        let local_ip = local_ip_address::local_ip()
            .map(|ip| ip.to_string())
            .unwrap_or_else(|_| "127.0.0.1".to_string());

        info!(
            "Heartbeat started — interval={}s, node={}, host={}, ip={}",
            interval, node_id, hostname, local_ip
        );

        loop {
            ticker.tick().await;

            let status = collect_node_status(&node_id, &hostname, &local_ip);

            match client.heartbeat(&node_id).await {
                Ok(resp) => {
                    if resp.ok {
                        info!(
                            "Heartbeat OK — cpu={:.1}%, mem={}/{}MB",
                            status.cpu_usage, status.memory_used_mb, status.memory_total_mb
                        );
                    } else {
                        error!("Heartbeat rejected: {}", resp.message);
                    }
                }
                Err(e) => {
                    error!("Heartbeat failed: {}. Will retry next cycle.", e);
                }
            }
        }
    })
}

/// Collect current node status: CPU, memory, disk usage.
pub fn collect_node_status(node_id: &str, hostname: &str, ip: &str) -> HeartbeatPayload {
    let mut sys = System::new_all();
    sys.refresh_all();

    let cpu_usage = sys.global_cpu_usage();
    let memory_total_mb = sys.total_memory() / 1024 / 1024;
    let memory_used_mb = sys.used_memory() / 1024 / 1024;

    let mut disk_total_mb: u64 = 0;
    let mut disk_used_mb: u64 = 0;
    let disks = sysinfo::Disks::new_with_refreshed_list();
    for disk in disks.iter() {
        disk_total_mb += disk.total_space() / 1024 / 1024;
        disk_used_mb += (disk.total_space() - disk.available_space()) / 1024 / 1024;
    }

    HeartbeatPayload {
        node_id: node_id.to_string(),
        timestamp_ms: chrono::Utc::now().timestamp_millis(),
        hostname: hostname.to_string(),
        ip: ip.to_string(),
        cpu_usage,
        memory_total_mb,
        memory_used_mb,
        disk_total_mb,
        disk_used_mb,
    }
}
