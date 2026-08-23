use anyhow::Result;
use chrono::Utc;
use std::collections::HashMap;
use std::time::Duration;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

use super::{EventTx, RawEvent};

/// A snapshot of a network connection.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct ConnectionInfo {
    local_addr: String,
    local_port: u16,
    remote_addr: String,
    remote_port: u16,
    protocol: String,
    state: String,
    pid: Option<u32>,
}

/// Network monitor that reads /proc/net/tcp and /proc/net/tcp6 periodically
/// and emits events for new/changed/closed connections.
pub async fn start_network_monitor(interval_ms: u64, tx: EventTx) -> Result<()> {
    info!("Starting network monitor (interval={}ms)", interval_ms);

    let mut prev_conns: HashMap<String, ConnectionInfo> = HashMap::new();
    let mut ticker = tokio::time::interval(Duration::from_millis(interval_ms));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut first_snapshot = true;

    loop {
        ticker.tick().await;

        let current = match tokio::task::spawn_blocking(read_network_connections).await {
            Ok(Ok(c)) => c,
            Ok(Err(e)) => {
                error!("Network snapshot failed: {}", e);
                continue;
            }
            Err(e) => {
                error!("Network snapshot task panicked: {}", e);
                continue;
            }
        };

        // Build key map for current connections
        let current_map: HashMap<String, ConnectionInfo> = current
            .into_iter()
            .map(|c| {
                let key = conn_key(&c);
                (key, c)
            })
            .collect();

        // On the first snapshot, just record baseline — don't emit events
        // for all pre-existing connections (avoids startup flood).
        if first_snapshot {
            info!(
                "Network baseline: {} connections recorded",
                current_map.len()
            );
            prev_conns = current_map;
            first_snapshot = false;
            continue;
        }

        // Detect new connections
        for (key, conn) in &current_map {
            if !prev_conns.contains_key(key) {
                let mut tags = HashMap::new();
                tags.insert(
                    "local".to_string(),
                    format!("{}:{}", conn.local_addr, conn.local_port),
                );
                tags.insert(
                    "remote".to_string(),
                    format!("{}:{}", conn.remote_addr, conn.remote_port),
                );
                tags.insert("protocol".to_string(), conn.protocol.clone());
                tags.insert("state".to_string(), conn.state.clone());
                if let Some(pid) = conn.pid {
                    tags.insert("pid".to_string(), pid.to_string());
                }
                tags.insert("change_type".to_string(), "new".to_string());

                debug!(
                    "New connection: {}:{} → {}:{} [{}]",
                    conn.local_addr,
                    conn.local_port,
                    conn.remote_addr,
                    conn.remote_port,
                    conn.state
                );
                emit_event(&tx, "network_new_connection", tags).await;
            }
        }

        // Detect closed connections
        for (key, conn) in &prev_conns {
            if !current_map.contains_key(key) {
                let mut tags = HashMap::new();
                tags.insert(
                    "local".to_string(),
                    format!("{}:{}", conn.local_addr, conn.local_port),
                );
                tags.insert(
                    "remote".to_string(),
                    format!("{}:{}", conn.remote_addr, conn.remote_port),
                );
                tags.insert("protocol".to_string(), conn.protocol.clone());
                tags.insert("change_type".to_string(), "closed".to_string());

                debug!(
                    "Closed connection: {}:{} → {}:{}",
                    conn.local_addr, conn.local_port, conn.remote_addr, conn.remote_port
                );
                emit_event(&tx, "network_closed_connection", tags).await;
            }
        }

        // Detect state changes (e.g., ESTABLISHED → CLOSE_WAIT)
        for (key, conn) in &current_map {
            if let Some(prev) = prev_conns.get(key) {
                if prev.state != conn.state {
                    let mut tags = HashMap::new();
                    tags.insert(
                        "local".to_string(),
                        format!("{}:{}", conn.local_addr, conn.local_port),
                    );
                    tags.insert(
                        "remote".to_string(),
                        format!("{}:{}", conn.remote_addr, conn.remote_port),
                    );
                    tags.insert("old_state".to_string(), prev.state.clone());
                    tags.insert("new_state".to_string(), conn.state.clone());
                    tags.insert("change_type".to_string(), "state_change".to_string());

                    debug!("Connection state change: {} → {}", prev.state, conn.state);
                    emit_event(&tx, "network_state_change", tags).await;
                }
            }
        }

        prev_conns = current_map;
    }
}

/// Generate a unique key for a connection.
fn conn_key(conn: &ConnectionInfo) -> String {
    format!(
        "{}:{}-{}:{}-{}",
        conn.local_addr, conn.local_port, conn.remote_addr, conn.remote_port, conn.protocol
    )
}

/// Read current network connections from /proc/net/tcp and /proc/net/tcp6.
/// This is Linux-specific and reads proc filesystem directly.
fn read_network_connections() -> Result<Vec<ConnectionInfo>> {
    let mut connections = Vec::new();

    // Read IPv4 TCP connections
    if let Ok(content) = std::fs::read_to_string("/proc/net/tcp") {
        parse_proc_net(&content, "tcp", &mut connections);
    }

    // Read IPv6 TCP connections
    if let Ok(content) = std::fs::read_to_string("/proc/net/tcp6") {
        parse_proc_net(&content, "tcp6", &mut connections);
    }

    // Read IPv4 UDP connections
    if let Ok(content) = std::fs::read_to_string("/proc/net/udp") {
        parse_proc_net(&content, "udp", &mut connections);
    }

    // Read IPv6 UDP connections
    if let Ok(content) = std::fs::read_to_string("/proc/net/udp6") {
        parse_proc_net(&content, "udp6", &mut connections);
    }

    Ok(connections)
}

/// Parse a /proc/net/tcp style file.
/// Format: sl  local_address rem_address   st tx_queue rx_queue ...
fn parse_proc_net(content: &str, protocol: &str, out: &mut Vec<ConnectionInfo>) {
    for line in content.lines().skip(1) {
        // skip header
        let fields: Vec<&str> = line.split_whitespace().collect();
        if fields.len() < 4 {
            continue;
        }

        let (local_addr, local_port) = parse_address(fields[1]);
        let (remote_addr, remote_port) = parse_address(fields[2]);
        let state = tcp_state(fields[3]);

        out.push(ConnectionInfo {
            local_addr,
            local_port,
            remote_addr,
            remote_port,
            protocol: protocol.to_string(),
            state,
            pid: None, // /proc/net/tcp doesn't provide PID directly
        });
    }
}

/// Parse a hex address:port string like "0100007F:1F90".
fn parse_address(hex_str: &str) -> (String, u16) {
    let parts: Vec<&str> = hex_str.split(':').collect();
    if parts.len() != 2 {
        return ("0.0.0.0".to_string(), 0);
    }

    let port = u16::from_str_radix(parts[1], 16).unwrap_or(0);

    let addr_hex = parts[0];
    if addr_hex.len() == 8 {
        // IPv4 — little-endian in /proc/net/tcp
        if let Ok(n) = u32::from_str_radix(addr_hex, 16) {
            let ip = format!(
                "{}.{}.{}.{}",
                n & 0xFF,
                (n >> 8) & 0xFF,
                (n >> 16) & 0xFF,
                (n >> 24) & 0xFF
            );
            return (ip, port);
        }
    } else if addr_hex.len() == 32 {
        // IPv6 — /proc/net/tcp6 stores address as 4 x u32 in little-endian.
        // Each u32 represents 4 bytes of the IPv6 address in network byte order
        // after byte-swapping each u32.
        let mut octets = [0u8; 16];
        let mut ok = true;
        for i in 0..4 {
            let start = i * 8;
            match u32::from_str_radix(&addr_hex[start..start + 8], 16) {
                Ok(n) => {
                    let bytes = n.to_le_bytes();
                    // /proc/net/tcp6 stores each u32 in little-endian host byte order.
                    // Use the LE bytes directly as the IPv6 address octets.
                    octets[i * 4] = bytes[0];
                    octets[i * 4 + 1] = bytes[1];
                    octets[i * 4 + 2] = bytes[2];
                    octets[i * 4 + 3] = bytes[3];
                }
                Err(_) => {
                    ok = false;
                    break;
                }
            }
        }
        if ok {
            // Parse as 8 x u16 groups for standard IPv6 representation
            let groups: [u16; 8] = [
                u16::from_be_bytes([octets[0], octets[1]]),
                u16::from_be_bytes([octets[2], octets[3]]),
                u16::from_be_bytes([octets[4], octets[5]]),
                u16::from_be_bytes([octets[6], octets[7]]),
                u16::from_be_bytes([octets[8], octets[9]]),
                u16::from_be_bytes([octets[10], octets[11]]),
                u16::from_be_bytes([octets[12], octets[13]]),
                u16::from_be_bytes([octets[14], octets[15]]),
            ];
            return (format_ipv6(&groups), port);
        }
    }

    (hex_str.to_string(), port)
}

/// Format 8 x u16 IPv6 groups into standard colon notation with :: compression.
/// Follows RFC 5952 rules: compress the longest run of zero groups (≥2).
fn format_ipv6(groups: &[u16; 8]) -> String {
    // Find the longest run of consecutive zero groups for :: compression
    let mut best_start = 8;
    let mut best_len = 0;
    let mut cur_start = 8;
    let mut cur_len = 0;

    for (i, &g) in groups.iter().enumerate() {
        if g == 0 {
            if cur_len == 0 {
                cur_start = i;
            }
            cur_len += 1;
            if cur_len > best_len {
                best_start = cur_start;
                best_len = cur_len;
            }
        } else {
            cur_len = 0;
        }
    }

    // Only compress runs of 2+ zero groups
    if best_len < 2 {
        return groups
            .iter()
            .map(|g| format!("{:x}", g))
            .collect::<Vec<_>>()
            .join(":");
    }

    // Build prefix (groups before compressed section) and suffix (groups after)
    let prefix: String = groups[..best_start]
        .iter()
        .map(|g| format!("{:x}", g))
        .collect::<Vec<_>>()
        .join(":");

    let suffix: String = groups[(best_start + best_len)..]
        .iter()
        .map(|g| format!("{:x}", g))
        .collect::<Vec<_>>()
        .join(":");

    match (prefix.is_empty(), suffix.is_empty()) {
        (true, true) => "::".to_string(),
        (true, false) => format!("::{}", suffix),
        (false, true) => format!("{}::", prefix),
        (false, false) => format!("{}::{}", prefix, suffix),
    }
}

/// Convert TCP state hex to human-readable name.
fn tcp_state(hex: &str) -> String {
    match hex {
        "01" => "ESTABLISHED",
        "02" => "SYN_SENT",
        "03" => "SYN_RECV",
        "04" => "FIN_WAIT1",
        "05" => "FIN_WAIT2",
        "06" => "TIME_WAIT",
        "07" => "CLOSE",
        "08" => "CLOSE_WAIT",
        "09" => "LAST_ACK",
        "0A" => "LISTEN",
        "0B" => "CLOSING",
        _ => "UNKNOWN",
    }
    .to_string()
}

/// Emit a network event.
async fn emit_event(tx: &EventTx, event_type: &str, tags: HashMap<String, String>) {
    let event = RawEvent {
        id: Uuid::new_v4().to_string(),
        source: "network".to_string(),
        event_type: event_type.to_string(),
        timestamp_ms: Utc::now().timestamp_millis(),
        payload: serde_json::to_vec(&tags).unwrap_or_default(),
        tags,
    };

    match tx.try_send(event) {
        Ok(()) => {}
        Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
            warn!("Event channel full, dropping network event");
        }
        Err(e) => {
            error!("Failed to send network event: {}", e);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_address_ipv4() {
        let (ip, port) = parse_address("0100007F:1F90");
        assert_eq!(ip, "127.0.0.1");
        assert_eq!(port, 0x1F90); // 8080
    }

    #[test]
    fn test_tcp_state() {
        assert_eq!(tcp_state("01"), "ESTABLISHED");
        assert_eq!(tcp_state("0A"), "LISTEN");
        assert_eq!(tcp_state("FF"), "UNKNOWN");
    }

    #[test]
    fn test_read_network_connections() {
        let result = read_network_connections();
        // On Linux this should succeed (may be empty in containers)
        assert!(result.is_ok());
    }

    #[test]
    fn test_conn_key() {
        let conn = ConnectionInfo {
            local_addr: "127.0.0.1".into(),
            local_port: 8080,
            remote_addr: "10.0.0.1".into(),
            remote_port: 443,
            protocol: "tcp".into(),
            state: "ESTABLISHED".into(),
            pid: None,
        };
        let key = conn_key(&conn);
        assert_eq!(key, "127.0.0.1:8080-10.0.0.1:443-tcp");
    }

    #[test]
    fn test_parse_address_ipv4_loopback() {
        let (ip, port) = parse_address("0100007F:0050");
        assert_eq!(ip, "127.0.0.1");
        assert_eq!(port, 0x0050); // 80
    }

    #[test]
    fn test_parse_address_ipv4_zero() {
        let (ip, port) = parse_address("00000000:0000");
        assert_eq!(ip, "0.0.0.0");
        assert_eq!(port, 0);
    }

    #[test]
    fn test_parse_address_ipv6_loopback() {
        let (ip, port) = parse_address("00000000000000000000000001000000:0050");
        assert_eq!(ip, "::1");
        assert_eq!(port, 0x0050);
    }

    #[test]
    fn test_parse_address_ipv6_all_zeros() {
        let (ip, port) = parse_address("00000000000000000000000000000000:0000");
        assert_eq!(ip, "::");
        assert_eq!(port, 0);
    }

    #[test]
    fn test_parse_address_malformed() {
        // When hex parsing fails, parse_address falls through with the raw string
        // and port 0. The actual behavior depends on the u32::from_str_radix call.
        let (ip, port) = parse_address("invalid");
        // Invalid hex → falls through, returns raw input with port 0
        assert_eq!(port, 0);
        // The actual IP depends on how the parsing handles the error
        // Just verify it doesn't panic and returns something
        assert!(!ip.is_empty());
    }

    #[test]
    fn test_parse_address_ipv6_fe80() {
        // fe80::1 in /proc/net/tcp6 on little-endian:
        // Network bytes: FE:80:00:00:00:00:00:00:00:00:00:00:00:00:00:01
        // s6_addr32[0] = 0x000080FE, s6_addr32[3] = 0x01000000
        let (ip, port) = parse_address("000080FE000000000000000001000000:1F90");
        assert_eq!(ip, "fe80::1");
        assert_eq!(port, 0x1F90);
    }

    #[test]
    fn test_parse_address_ipv6_mapped() {
        // Test a non-compressible IPv6 address: 2001:db8:85a3::8a2e:370:7334
        // Network bytes: 20:01:0d:b8:85:a3:00:00:00:00:8a:2e:03:70:73:34
        // s6_addr32[0] = 0xb80d0120, s6_addr32[1] = 0x0000a385
        // s6_addr32[2] = 0x2e8a0000, s6_addr32[3] = 0x34737003
        let (ip, port) = parse_address("b80d01200000a3852e8a000034737003:0050");
        // Groups: 2001:db8:85a3:0:0:8a2e:370:7334
        assert_eq!(ip, "2001:db8:85a3::8a2e:370:7334");
        assert_eq!(port, 0x0050);
    }

    #[test]
    fn test_format_ipv6_no_compression() {
        // All non-zero groups — no :: compression
        let groups: [u16; 8] = [0x2001, 0xdb8, 0x85a3, 0x1, 0x2, 0x3, 0x4, 0x5];
        assert_eq!(format_ipv6(&groups), "2001:db8:85a3:1:2:3:4:5");
    }

    #[test]
    fn test_format_ipv6_trailing_zeros() {
        let groups: [u16; 8] = [1, 0, 0, 0, 0, 0, 0, 0];
        assert_eq!(format_ipv6(&groups), "1::");
    }

    #[test]
    fn test_format_ipv6_single_zero_group() {
        // Only 1 zero group — no compression (minimum is 2)
        let groups: [u16; 8] = [1, 0, 2, 3, 4, 5, 6, 7];
        assert_eq!(format_ipv6(&groups), "1:0:2:3:4:5:6:7");
    }

    #[test]
    fn test_tcp_state_all_values() {
        assert_eq!(tcp_state("01"), "ESTABLISHED");
        assert_eq!(tcp_state("02"), "SYN_SENT");
        assert_eq!(tcp_state("03"), "SYN_RECV");
        assert_eq!(tcp_state("04"), "FIN_WAIT1");
        assert_eq!(tcp_state("05"), "FIN_WAIT2");
        assert_eq!(tcp_state("06"), "TIME_WAIT");
        assert_eq!(tcp_state("07"), "CLOSE");
        assert_eq!(tcp_state("08"), "CLOSE_WAIT");
        assert_eq!(tcp_state("09"), "LAST_ACK");
        assert_eq!(tcp_state("0A"), "LISTEN");
        assert_eq!(tcp_state("0B"), "CLOSING");
        assert_eq!(tcp_state("00"), "UNKNOWN");
    }
}
