//! SpoolRecovery — recover missing slices via Clay repair protocol.

use std::sync::Arc;

use store::Store;
use tape_node_api::RepairRequest;
use tape_node_client::NodeClientBuilder;
use tape_core::erasure::spool_in_group;
use tape_store::ops::{CommitteeOps, MetaOps, SliceOps, SpoolOps, TrackOps};
use tokio_util::sync::CancellationToken;

use crate::core::NodeContext;
use crate::supervisor::TaskOutcome;

const RECOVERY_BATCH_SIZE: usize = 10;

pub async fn run<S: Store>(
    context: Arc<NodeContext<S>>,
    spool: u16,
    cancel: CancellationToken,
) -> TaskOutcome {
    let pending = match context.store.iter_pending_recoveries(spool, RECOVERY_BATCH_SIZE) {
        Ok(p) => p,
        Err(e) => return TaskOutcome::Retryable(format!("iter_pending_recoveries: {e}")),
    };

    if pending.is_empty() {
        return TaskOutcome::Success;
    }

    // Get current committee for finding helper nodes
    let epoch = match context.store.get_current_epoch() {
        Ok(Some(e)) => e,
        Ok(None) => return TaskOutcome::Retryable("no current epoch".into()),
        Err(e) => return TaskOutcome::Retryable(format!("get epoch: {e}")),
    };

    let committee = match context.store.get_committee(epoch) {
        Ok(Some(c)) => c,
        Ok(None) => return TaskOutcome::Retryable("no committee for current epoch".into()),
        Err(e) => return TaskOutcome::Retryable(format!("get committee: {e}")),
    };

    for track_addr in pending {
        if cancel.is_cancelled() {
            return TaskOutcome::Success;
        }

        let track_info = match context.store.get_track(track_addr) {
            Ok(Some(t)) => t,
            Ok(None) => {
                // Track was deleted, remove from pending
                let _ = context.store.remove_pending_recovery(spool, track_addr);
                continue;
            }
            Err(e) => {
                tracing::warn!(?track_addr, "get_track error: {e}");
                continue;
            }
        };

        // Find all helper nodes with a different spool in the same group
        let helpers: Vec<_> = committee.iter().filter(|node| {
            node.spools.iter().any(|&s| {
                s != spool
                    && spool_in_group(s, track_info.spool_group)
            })
        }).collect();

        if helpers.is_empty() {
            tracing::warn!(?track_addr, spool, "no helper found for recovery");
            continue;
        }

        let mut recovered = false;
        for helper in &helpers {
            let helper_spool = match helper.spools.iter().find(|&&s| {
                s != spool && spool_in_group(s, track_info.spool_group)
            }) {
                Some(&s) => s,
                None => continue,
            };

            let addr = match helper.network_address.to_socket_addr() {
                Ok(a) => a,
                Err(e) => {
                    tracing::warn!(?track_addr, "parse helper address: {e}");
                    continue;
                }
            };

            let client = match NodeClientBuilder::new().build(&addr.to_string()) {
                Ok(c) => c,
                Err(e) => {
                    tracing::warn!(?track_addr, "build helper client: {e}");
                    continue;
                }
            };

            let request = RepairRequest {
                lost_slice: spool,
                helper_spool,
                stripes: (0..track_info.stripe_count as u32)
                    .map(|s| tape_node_api::StripeSubChunkRequest {
                        stripe: s,
                        sub_chunks: vec![],
                    })
                    .collect(),
            };

            match client.request_repair(track_addr, &request).await {
                Ok(data) if !data.is_empty() => {
                    if let Err(e) = context.store.put_slice(spool, track_addr, data) {
                        tracing::warn!(?track_addr, "put_slice error: {e}");
                        continue;
                    }
                    let _ = context.store.remove_pending_recovery(spool, track_addr);
                    tracing::debug!(?track_addr, spool, "recovered slice");
                    recovered = true;
                    break;
                }
                Ok(_) => {
                    tracing::debug!(?track_addr, spool, helper = ?helper.network_address, "empty repair response");
                }
                Err(e) => {
                    tracing::debug!(?track_addr, spool, helper = ?helper.network_address, "repair error: {e}");
                }
            }
        }

        if !recovered {
            tracing::debug!(?track_addr, spool, "all helpers exhausted");
        }
    }

    TaskOutcome::Success
}
