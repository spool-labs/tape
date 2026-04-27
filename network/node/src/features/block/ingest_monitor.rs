//! Periodic monitor task that recomputes `IngestState` from the atomics
//! the `BlockIngestor` writes to `IngestProgress`. The monitor is the only
//! writer to the `IngestBus` watch channel; the ingestor only writes raw
//! progress signals.
//!
//! Runs as an independent supervised service so a slow / 429ing
//! `get_block` retry trapped inside `BlockIngestor::fetch_parse_and_dispatch`
//! cannot wedge stall detection.

use std::sync::Arc;

use rpc::Rpc;
use store::Store;
use tape_protocol::Api;
use tokio::time::interval;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info};

use crate::context::NodeContext;
use crate::core::error::NodeError;
use crate::core::ingest::{IngestState, MONITOR_TICK};

pub async fn run<Db, Cluster, Blockchain>(
    context: Arc<NodeContext<Db, Cluster, Blockchain>>,
    cancel: CancellationToken,
) -> Result<(), NodeError>
where
    Db: Store + 'static,
    Cluster: Api + 'static,
    Blockchain: Rpc + 'static,
{
    let mut tick = interval(MONITOR_TICK);
    let mut last_logged = describe(&context.ingest.current());

    loop {
        tokio::select! {
            _ = cancel.cancelled() => {
                info!("ingest_monitor: shutting down");
                return Ok(());
            }
            _ = tick.tick() => {
                let next = context.ingest.compute();
                context.ingest.publish(next);

                let label = describe(&next);
                if label != last_logged {
                    debug!(state = label, "ingest_monitor: state changed");
                    last_logged = label;
                }
            }
        }
    }
}

fn describe(state: &IngestState) -> &'static str {
    match state {
        IngestState::Catching { .. } => "catching",
        IngestState::AtTip => "at_tip",
        IngestState::Stalled { .. } => "stalled",
    }
}
