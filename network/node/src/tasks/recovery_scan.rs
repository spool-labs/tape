//! RecoveryScan — scan for missing slices in a spool.

use std::sync::Arc;

use rpc::Rpc;
use store::Store;
use tape_core::erasure::spool_in_group;
use tape_store::ops::{SliceOps, SpoolOps, TrackOps};
use tokio_util::sync::CancellationToken;

use crate::core::NodeContext;
use crate::supervisor::TaskOutcome;

const SCAN_BATCH_SIZE: usize = 100;

pub async fn run<S: Store, R: Rpc>(
    context: Arc<NodeContext<S, R>>,
    spool: u16,
    cancel: CancellationToken,
) -> TaskOutcome {
    let mut cursor = None;

    loop {
        if cancel.is_cancelled() {
            return TaskOutcome::Success;
        }

        let tracks = match context.store.iter_tracks_from(cursor, SCAN_BATCH_SIZE) {
            Ok(t) => t,
            Err(e) => return TaskOutcome::Retryable(format!("iter_tracks: {e}")),
        };

        if tracks.is_empty() {
            break;
        }

        for (track_addr, track_info) in &tracks {
            // Check if this spool belongs to this track's spool group
            if !spool_in_group(spool, track_info.spool_group) {
                continue;
            }

            // Check if we have the slice
            let has = match context.store.has_slice(spool, *track_addr) {
                Ok(h) => h,
                Err(e) => {
                    tracing::warn!(?track_addr, spool, "has_slice error: {e}");
                    continue;
                }
            };

            if !has {
                if let Err(e) = context.store.add_pending_recovery(spool, *track_addr) {
                    tracing::warn!(?track_addr, spool, "add_pending_recovery error: {e}");
                }
            }
        }

        cursor = tracks.last().map(|(addr, _)| *addr);
    }

    tracing::info!(spool, "recovery scan complete");
    TaskOutcome::Success
}
