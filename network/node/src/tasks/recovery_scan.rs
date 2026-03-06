//! RecoveryScan — scan for missing slices in a spool.

use std::sync::Arc;

use rpc::Rpc;
use tape_protocol::Api;
use store::Store;
use tape_core::erasure::spool_in_group;
use tape_store::ops::{ObjectInfoOps, SliceOps, SpoolOps, TrackOps};
use tape_store::types::ObjectInfo;
use tokio_util::sync::CancellationToken;

use crate::core::NodeContext;
use crate::TaskOutcome;

const SCAN_BATCH_SIZE: usize = 100;

pub async fn run<Db: Store, Cluster: Api, Blockchain: Rpc>(
    context: Arc<NodeContext<Db, Cluster, Blockchain>>,
    spool: u16,
    cancel: CancellationToken,
) -> TaskOutcome {
    let mut cursor = None;
    let mut any_errors = false;

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
            if !spool_in_group(spool, track_info.spool_group) {
                continue;
            }

            // Only scan certified tracks — uncertified tracks have no on-chain
            // commitment and helpers may not have the slice data.
            let certified = match context.store.get_object_info(*track_addr) {
                Ok(Some(ObjectInfo::Valid { certified_epoch: Some(_), .. })) => true,
                Ok(_) => false,
                Err(e) => {
                    tracing::warn!(?track_addr, spool, "get_object_info error: {e}");
                    any_errors = true;
                    continue;
                }
            };
            if !certified {
                continue;
            }

            let has = match context.store.has_slice(spool, *track_addr) {
                Ok(h) => h,
                Err(e) => {
                    tracing::warn!(?track_addr, spool, "has_slice error: {e}");
                    any_errors = true;
                    continue;
                }
            };

            if !has {
                if let Err(e) = context.store.add_pending_recovery(spool, *track_addr) {
                    tracing::warn!(?track_addr, spool, "add_pending_recovery error: {e}");
                    any_errors = true;
                }
            }
        }

        cursor = tracks.last().map(|(addr, _)| *addr);
    }

    if any_errors {
        return TaskOutcome::Retryable("scan encountered store errors".into());
    }

    if let Err(e) = context.store.set_scan_done(spool) {
        return TaskOutcome::Retryable(format!("set scan_done: {e}"));
    }

    tracing::info!(spool, "recovery scan complete");
    TaskOutcome::Success
}
