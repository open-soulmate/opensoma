use std::time::Duration;
use tokio::task::JoinHandle;
use tracing::{error, info, warn};

use crate::grpc::client::SoulClient;

/// Start the heartbeat loop. Sends a heartbeat to Soul every `interval` seconds.
pub fn start(
    node_id: String,
    interval: u64,
    client: SoulClient,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(Duration::from_secs(interval));
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        info!("Heartbeat started — interval={}s, node={}", interval, node_id);

        loop {
            ticker.tick().await;

            match client.heartbeat(&node_id).await {
                Ok(resp) => {
                    if resp.ok {
                        info!(
                            "Heartbeat acknowledged — server_ts={}",
                            resp.server_timestamp_ms
                        );
                    } else {
                        warn!("Heartbeat rejected: {}", resp.message);
                    }
                }
                Err(e) => {
                    error!("Heartbeat failed: {}. Will retry next cycle.", e);
                }
            }
        }
    })
}
