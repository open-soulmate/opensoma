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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_collect_node_status() {
        let payload = collect_node_status("test-node", "localhost", "127.0.0.1");
        assert_eq!(payload.node_id, "test-node");
        assert_eq!(payload.hostname, "localhost");
        assert_eq!(payload.ip, "127.0.0.1");
        assert!(payload.memory_total_mb > 0);
        assert!(payload.timestamp_ms > 0);
    }

    #[test]
    fn test_heartbeat_payload_serializes() {
        let payload = collect_node_status("node1", "host1", "10.0.0.1");
        let json = serde_json::to_value(&payload).unwrap();
        assert_eq!(json["node_id"], "node1");
        assert_eq!(json["hostname"], "host1");
        assert_eq!(json["ip"], "10.0.0.1");
        assert!(json["memory_total_mb"].as_u64().unwrap() > 0);
    }

    #[test]
    fn test_collect_node_status_cpu_count() {
        let payload = collect_node_status("node-cpu", "host", "127.0.0.1");
        assert!(payload.cpu_usage >= 0.0, "CPU usage should be non-negative");
    }

    #[test]
    fn test_collect_node_status_disk_usage() {
        let payload = collect_node_status("node-disk", "host", "127.0.0.1");
        // disk_total_mb should be > 0 on any real system
        assert!(payload.disk_total_mb > 0, "Should report disk total");
    }

    #[test]
    fn test_heartbeat_payload_json_structure() {
        let payload = collect_node_status("struct-test", "myhost", "192.168.1.1");
        let json = serde_json::to_value(&payload).unwrap();
        // Verify all expected fields exist
        assert!(json.get("node_id").is_some());
        assert!(json.get("hostname").is_some());
        assert!(json.get("ip").is_some());
        assert!(json.get("cpu_usage").is_some());
        assert!(json.get("memory_total_mb").is_some());
        assert!(json.get("memory_used_mb").is_some());
        assert!(json.get("disk_total_mb").is_some());
        assert!(json.get("disk_used_mb").is_some());
        assert!(json.get("timestamp_ms").is_some());
    }
}
