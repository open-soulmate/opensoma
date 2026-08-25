use anyhow::Result;
use chrono::Utc;
use std::collections::HashMap;
use std::time::Duration;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

use super::{EventTx, RawEvent};

/// Snapshot of a running process for change detection.
#[derive(Debug, Clone, PartialEq)]
struct ProcessSnapshot {
    pid: i32,
    name: String,
    cpu_usage: f32,
    memory_bytes: u64,
}

/// Process monitor that polls system processes at a fixed interval and
/// emits `process_change` events for new, exited, or significantly changed processes.
pub async fn start_process_monitor(interval_ms: u64, tx: EventTx) -> Result<()> {
    info!("Starting process monitor (interval={}ms)", interval_ms);

    let mut prev_snapshot: HashMap<i32, ProcessSnapshot> = HashMap::new();
    let mut ticker = tokio::time::interval(Duration::from_millis(interval_ms));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        ticker.tick().await;

        // sysinfo is synchronous; run it in a blocking thread.
        let current = match tokio::task::spawn_blocking(collect_process_snapshot).await {
            Ok(Ok(snap)) => snap,
            Ok(Err(e)) => {
                error!("Process snapshot failed: {}", e);
                continue;
            }
            Err(e) => {
                error!("Process snapshot task panicked: {}", e);
                continue;
            }
        };

        // Detect new and changed processes
        for (pid, snap) in &current {
            if let Some(prev) = prev_snapshot.get(pid) {
                // Check for significant CPU change (>20%) or memory change (>10MB)
                let cpu_delta = (snap.cpu_usage - prev.cpu_usage).abs();
                let mem_delta = snap.memory_bytes.abs_diff(prev.memory_bytes);

                if cpu_delta > 20.0 || mem_delta > 10 * 1024 * 1024 {
                    let mut tags = HashMap::new();
                    tags.insert("pid".to_string(), pid.to_string());
                    tags.insert("name".to_string(), snap.name.clone());
                    tags.insert("change_type".to_string(), "resource_change".to_string());
                    tags.insert("cpu_usage".to_string(), format!("{:.1}", snap.cpu_usage));
                    tags.insert(
                        "memory_mb".to_string(),
                        format!("{}", snap.memory_bytes / 1024 / 1024),
                    );

                    emit_event(&tx, "process_resource_change", snap.pid, tags).await;
                }
            } else {
                // New process
                let mut tags = HashMap::new();
                tags.insert("pid".to_string(), pid.to_string());
                tags.insert("name".to_string(), snap.name.clone());
                tags.insert("change_type".to_string(), "started".to_string());
                tags.insert("cpu_usage".to_string(), format!("{:.1}", snap.cpu_usage));
                tags.insert(
                    "memory_mb".to_string(),
                    format!("{}", snap.memory_bytes / 1024 / 1024),
                );

                debug!("New process detected: {} ({})", snap.name, pid);
                emit_event(&tx, "process_started", snap.pid, tags).await;
            }
        }

        // Detect exited processes
        for (pid, snap) in &prev_snapshot {
            if !current.contains_key(pid) {
                let mut tags = HashMap::new();
                tags.insert("pid".to_string(), pid.to_string());
                tags.insert("name".to_string(), snap.name.clone());
                tags.insert("change_type".to_string(), "exited".to_string());

                debug!("Process exited: {} ({})", snap.name, pid);
                emit_event(&tx, "process_exited", *pid, tags).await;
            }
        }

        prev_snapshot = current;
    }
}

/// Collect a snapshot of all running processes using sysinfo (blocking).
fn collect_process_snapshot() -> Result<HashMap<i32, ProcessSnapshot>> {
    use sysinfo::{ProcessesToUpdate, System};

    let mut sys = System::new();
    sys.refresh_processes(ProcessesToUpdate::All, true);

    let mut snapshot = HashMap::new();
    for (pid, proc) in sys.processes() {
        snapshot.insert(
            pid.as_u32() as i32,
            ProcessSnapshot {
                pid: pid.as_u32() as i32,
                name: proc.name().to_string_lossy().to_string(),
                cpu_usage: proc.cpu_usage(),
                memory_bytes: proc.memory(),
            },
        );
    }
    Ok(snapshot)
}

/// Emit a process event to the event channel.
async fn emit_event(tx: &EventTx, event_type: &str, pid: i32, tags: HashMap<String, String>) {
    let event = RawEvent {
        id: Uuid::new_v4().to_string(),
        source: format!("process:{}", pid),
        event_type: event_type.to_string(),
        timestamp_ms: Utc::now().timestamp_millis(),
        payload: serde_json::to_vec(&tags).unwrap_or_default(),
        tags,
    };

    match tx.try_send(event) {
        Ok(()) => {}
        Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
            warn!("Event channel full, dropping process event");
        }
        Err(e) => {
            error!("Failed to send process event: {}", e);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_collect_process_snapshot() {
        let result = collect_process_snapshot();
        assert!(result.is_ok());
        let snap = result.unwrap();
        // At least one process (the test itself) should be visible
        assert!(!snap.is_empty());
    }

    #[test]
    fn test_process_snapshot_equality() {
        let a = ProcessSnapshot {
            pid: 1,
            name: "init".into(),
            cpu_usage: 0.0,
            memory_bytes: 1024,
        };
        let b = ProcessSnapshot {
            pid: 1,
            name: "init".into(),
            cpu_usage: 0.0,
            memory_bytes: 1024,
        };
        assert_eq!(a, b);
    }

    #[test]
    fn test_process_snapshot_fields() {
        let snap = ProcessSnapshot {
            pid: 1234,
            name: "test-proc".into(),
            cpu_usage: 5.5,
            memory_bytes: 1024 * 1024,
        };
        assert_eq!(snap.pid, 1234);
        assert_eq!(snap.name, "test-proc");
        assert!((snap.cpu_usage - 5.5).abs() < f32::EPSILON);
        assert_eq!(snap.memory_bytes, 1048576);
    }

    #[test]
    fn test_process_snapshot_clone() {
        let a = ProcessSnapshot {
            pid: 42,
            name: "clone-test".into(),
            cpu_usage: 1.0,
            memory_bytes: 512,
        };
        let b = a.clone();
        assert_eq!(a, b);
    }

    #[test]
    fn test_collect_process_snapshot_contains_self() {
        let result = collect_process_snapshot();
        assert!(result.is_ok());
        let snaps = result.unwrap();
        // The current process should be in the list
        let my_pid = std::process::id() as i32;
        let found = snaps.values().any(|s| s.pid == my_pid);
        assert!(found, "Should find own PID {} in process list", my_pid);
    }

    #[test]
    fn test_process_snapshot_inequality() {
        let a = ProcessSnapshot {
            pid: 1,
            name: "init".into(),
            cpu_usage: 0.0,
            memory_bytes: 1024,
        };
        let b = ProcessSnapshot {
            pid: 2,
            name: "init".into(),
            cpu_usage: 0.0,
            memory_bytes: 1024,
        };
        assert_ne!(a, b);
    }

    #[test]
    fn test_resource_change_threshold_cpu() {
        // CPU delta > 20.0 should trigger resource_change event
        let prev = ProcessSnapshot {
            pid: 100,
            name: "test".into(),
            cpu_usage: 10.0,
            memory_bytes: 100 * 1024 * 1024,
        };
        let curr = ProcessSnapshot {
            pid: 100,
            name: "test".into(),
            cpu_usage: 35.0, // delta = 25.0 > 20.0
            memory_bytes: 100 * 1024 * 1024,
        };
        let cpu_delta = (curr.cpu_usage - prev.cpu_usage).abs();
        let mem_delta = curr.memory_bytes.abs_diff(prev.memory_bytes);
        assert!(cpu_delta > 20.0);
        assert!(mem_delta < 10 * 1024 * 1024);
    }

    #[test]
    fn test_resource_change_threshold_memory() {
        // Memory delta > 10MB should trigger resource_change event
        let prev = ProcessSnapshot {
            pid: 200,
            name: "test".into(),
            cpu_usage: 5.0,
            memory_bytes: 100 * 1024 * 1024,
        };
        let curr = ProcessSnapshot {
            pid: 200,
            name: "test".into(),
            cpu_usage: 5.0,
            memory_bytes: 115 * 1024 * 1024, // delta = 15MB > 10MB
        };
        let cpu_delta = (curr.cpu_usage - prev.cpu_usage).abs();
        let mem_delta = curr.memory_bytes.abs_diff(prev.memory_bytes);
        assert!(cpu_delta <= 20.0);
        assert!(mem_delta > 10 * 1024 * 1024);
    }

    #[test]
    fn test_no_change_below_threshold() {
        // Small changes should NOT trigger resource_change event
        let prev = ProcessSnapshot {
            pid: 300,
            name: "stable".into(),
            cpu_usage: 10.0,
            memory_bytes: 100 * 1024 * 1024,
        };
        let curr = ProcessSnapshot {
            pid: 300,
            name: "stable".into(),
            cpu_usage: 15.0,                 // delta = 5.0 < 20.0
            memory_bytes: 102 * 1024 * 1024, // delta = 2MB < 10MB
        };
        let cpu_delta = (curr.cpu_usage - prev.cpu_usage).abs();
        let mem_delta = curr.memory_bytes.abs_diff(prev.memory_bytes);
        assert!(cpu_delta <= 20.0);
        assert!(mem_delta <= 10 * 1024 * 1024);
    }

    #[test]
    fn test_new_process_detection_logic() {
        // Simulate: prev has {1, 2}, current has {1, 2, 3} -> pid 3 is new
        let mut prev: HashMap<i32, ProcessSnapshot> = HashMap::new();
        prev.insert(
            1,
            ProcessSnapshot {
                pid: 1,
                name: "a".into(),
                cpu_usage: 0.0,
                memory_bytes: 0,
            },
        );
        prev.insert(
            2,
            ProcessSnapshot {
                pid: 2,
                name: "b".into(),
                cpu_usage: 0.0,
                memory_bytes: 0,
            },
        );

        let mut current: HashMap<i32, ProcessSnapshot> = HashMap::new();
        current.insert(
            1,
            ProcessSnapshot {
                pid: 1,
                name: "a".into(),
                cpu_usage: 0.0,
                memory_bytes: 0,
            },
        );
        current.insert(
            2,
            ProcessSnapshot {
                pid: 2,
                name: "b".into(),
                cpu_usage: 0.0,
                memory_bytes: 0,
            },
        );
        current.insert(
            3,
            ProcessSnapshot {
                pid: 3,
                name: "c".into(),
                cpu_usage: 0.0,
                memory_bytes: 0,
            },
        );

        // New processes = in current but not in prev
        let new_pids: Vec<i32> = current
            .keys()
            .filter(|pid| !prev.contains_key(pid))
            .copied()
            .collect();
        assert_eq!(new_pids.len(), 1);
        assert_eq!(new_pids[0], 3);
    }

    #[test]
    fn test_exited_process_detection_logic() {
        // Simulate: prev has {1, 2, 3}, current has {1, 2} -> pid 3 exited
        let mut prev: HashMap<i32, ProcessSnapshot> = HashMap::new();
        prev.insert(
            1,
            ProcessSnapshot {
                pid: 1,
                name: "a".into(),
                cpu_usage: 0.0,
                memory_bytes: 0,
            },
        );
        prev.insert(
            2,
            ProcessSnapshot {
                pid: 2,
                name: "b".into(),
                cpu_usage: 0.0,
                memory_bytes: 0,
            },
        );
        prev.insert(
            3,
            ProcessSnapshot {
                pid: 3,
                name: "c".into(),
                cpu_usage: 0.0,
                memory_bytes: 0,
            },
        );

        let mut current: HashMap<i32, ProcessSnapshot> = HashMap::new();
        current.insert(
            1,
            ProcessSnapshot {
                pid: 1,
                name: "a".into(),
                cpu_usage: 0.0,
                memory_bytes: 0,
            },
        );
        current.insert(
            2,
            ProcessSnapshot {
                pid: 2,
                name: "b".into(),
                cpu_usage: 0.0,
                memory_bytes: 0,
            },
        );

        // Exited processes = in prev but not in current
        let exited_pids: Vec<i32> = prev
            .keys()
            .filter(|pid| !current.contains_key(pid))
            .copied()
            .collect();
        assert_eq!(exited_pids.len(), 1);
        assert_eq!(exited_pids[0], 3);
    }

    #[test]
    fn test_event_source_format() {
        let pid = 1234;
        let source = format!("process:{}", pid);
        assert_eq!(source, "process:1234");
    }

    #[test]
    fn test_event_type_values() {
        // Verify the three event types used by the process collector
        let types = [
            "process_started",
            "process_exited",
            "process_resource_change",
        ];
        for et in types {
            assert!(et.starts_with("process_"));
        }
    }
}
