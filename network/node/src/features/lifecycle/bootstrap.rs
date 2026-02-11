//! Metadata sync — resolves track metadata for newly assigned spools.
//!
//! The block processor stores TrackInfo for all RegisterTrack instructions.
//! Any remaining missing metadata is handled on-demand during per-track
//! recovery (fetch_metadata_from_peers in track_synchronizer).
//! This function unblocks the RecoverMetadata state.

use std::sync::Arc;

use store::Store;
use tape_store::ops::MetaOps;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::core::context::NodeContext;
use crate::features::sync::SpoolSyncHandler;

use super::{NodeEvent, evaluate_transition};
use crate::features::recovery::start_spool_recovery;

/// Run metadata sync for newly assigned spools.
///
/// The block processor stores TrackInfo for all RegisterTrack instructions.
/// Any remaining missing metadata is handled on-demand during per-track
/// recovery (fetch_metadata_from_peers in track_synchronizer).
/// This function unblocks the RecoverMetadata state.
pub async fn run_metadata_sync<S: Store + 'static>(
    ctx: Arc<NodeContext<S>>,
    sync_handler: SpoolSyncHandler,
    cancel: CancellationToken,
) {
    info!("metadata sync starting");

    if cancel.is_cancelled() {
        return;
    }

    let current_status = ctx.control_plane.get_node_status();
    let event = NodeEvent::MetadataSyncComplete;
    if let Some(new_status) = evaluate_transition(&current_status, &event) {
        info!(from = ?current_status, to = ?new_status, "metadata sync complete");
        ctx.control_plane.set_node_status(new_status.clone());
        if let Err(e) = ctx.storage.store.set_node_status(new_status) {
            warn!(error = %e, "failed to persist node status");
        }
        // Spawn spool recovery — calls mark_local_sync_complete when done
        let ctx2 = Arc::clone(&ctx);
        let cancel2 = cancel.clone();
        tokio::spawn(async move {
            start_spool_recovery(ctx2, sync_handler, cancel2).await;
        });
    }
}
