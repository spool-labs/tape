pub mod http;
pub mod board;

mod aggregator;
mod cache;
mod collectors;
mod epoch;
mod peers;

use std::sync::Arc;

use rpc::Rpc;
use store::Store;
use tape_metrics::MetricsRegistry;
use tape_protocol::Api;
use tape_store::ops::MetaOps;
use tokio::sync::mpsc;

use crate::context::NodeContext;
use crate::core::channels::DownstreamSenders;
use crate::features::replay::types::ReplayBatch;

use collectors::{CapacityFn, ChannelCollector, NodeStatusCollector, StoreStatsCollector};

pub use aggregator::PeerAggregator;
pub use cache::{cached_network, cached_board};
pub use peers::lookup as peer_liveness;
pub use epoch::{current_epoch_progress, last_epoch, lifetime, roll_epoch};

/// The role this process stamps on the boards it serves, set once at startup.
static BOARD_KIND: std::sync::OnceLock<tape_observe_api::BoardKind> = std::sync::OnceLock::new();

/// Mark every board this process builds as coming from a read gateway.
pub fn mark_gateway_boards() {
    let _ = BOARD_KIND.set(tape_observe_api::BoardKind::Gateway);
}

/// The role stamped on boards, defaulting to a storage node.
pub(crate) fn board_kind() -> tape_observe_api::BoardKind {
    BOARD_KIND.get().copied().unwrap_or_default()
}

/// Register the context-dependent pull collectors and the per-epoch roller. The
/// metric set and process collector are registered earlier.
pub fn register_core_collectors<Db, Cluster, Blockchain>(
    context: Arc<NodeContext<Db, Cluster, Blockchain>>,
) where
    Db: Store + 'static,
    Cluster: Api + 'static,
    Blockchain: Rpc + 'static,
{
    let registry = MetricsRegistry::init().prometheus_registry();
    let _ = registry.register(Box::new(StoreStatsCollector::new(context.store.clone())));
    epoch::init();
    // Restore the last completed epoch's deltas from before a restart.
    if let Ok(Some(bytes)) = context.store.get_observe_last_epoch() {
        if let Ok(last) = serde_json::from_slice(&bytes) {
            epoch::set_last_epoch(last);
        }
    }
    if let Ok(Some(bytes)) = context.store.get_observe_lifetime() {
        if let Ok(life) = serde_json::from_slice(&bytes) {
            epoch::set_lifetime(life);
        }
    }
    board::init();
    let _ = registry.register(Box::new(NodeStatusCollector::new(context)));
}

/// Register channel-depth gauges for the block/replay pipeline.
pub fn register_block_channels(senders: &DownstreamSenders, store_tx: &mpsc::Sender<ReplayBatch>) {
    let channels = vec![
        ("state", capacity_fn(&senders.state)),
        ("assignment", capacity_fn(&senders.assignment)),
        ("replay", capacity_fn(&senders.replay)),
        ("snapshot", capacity_fn(&senders.snapshot)),
        ("store", capacity_fn(store_tx)),
    ];
    let registry = MetricsRegistry::init().prometheus_registry();
    let _ = registry.register(Box::new(ChannelCollector::new(channels)));
}

fn capacity_fn<T: Send + 'static>(sender: &mpsc::Sender<T>) -> CapacityFn {
    let sender = sender.clone();
    Box::new(move || (sender.capacity(), sender.max_capacity()))
}
