//! SpoolRecovery — recover missing slices via Clay repair protocol.

use std::sync::Arc;

use rpc::Rpc;
use store::Store;
use tape_node_api::RepairRequest;
use tape_node_client::{NodeClientBuilder, RetryConfig, with_retry};
use tape_core::erasure::spool_in_group;
use tape_store::ops::{CommitteeOps, MetaOps, SliceOps, SpoolOps, TrackOps};
use tokio_util::sync::CancellationToken;

use crate::core::NodeContext;
use crate::supervisor::TaskOutcome;

const RECOVERY_BATCH_SIZE: usize = 10;

pub async fn run<S: Store, R: Rpc>(
    context: Arc<NodeContext<S, R>>,
    spool: u16,
    cancel: CancellationToken,
) -> TaskOutcome {
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

    let mut any_failed = false;

    loop {
        if cancel.is_cancelled() {
            return TaskOutcome::Success;
        }

        let pending = match context.store.iter_pending_recoveries(spool, RECOVERY_BATCH_SIZE) {
            Ok(p) => p,
            Err(e) => return TaskOutcome::Retryable(format!("iter_pending_recoveries: {e}")),
        };

        if pending.is_empty() {
            break;
        }

        let mut removed_any = false;

        for track_addr in pending {
            if cancel.is_cancelled() {
                return TaskOutcome::Success;
            }

            let track_info = match context.store.get_track(track_addr) {
                Ok(Some(t)) => t,
                Ok(None) => {
                    let _ = context.store.remove_pending_recovery(spool, track_addr);
                    removed_any = true;
                    continue;
                }
                Err(e) => {
                    tracing::warn!(?track_addr, "get_track error: {e}");
                    any_failed = true;
                    continue;
                }
            };

            let helpers: Vec<_> = committee.iter().filter(|node| {
                node.spools.iter().any(|&s| {
                    s != spool
                        && spool_in_group(s, track_info.spool_group)
                })
            }).collect();

            if helpers.is_empty() {
                tracing::warn!(?track_addr, spool, "no helper found for recovery");
                any_failed = true;
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
                    helper_spool,
                    stripes: (0..track_info.stripe_count as u32)
                        .map(|s| tape_node_api::StripeSubChunkRequest {
                            stripe: s,
                            sub_chunks: vec![],
                        })
                        .collect(),
                };

                match with_retry(&RetryConfig::fast(), || client.request_repair(track_addr, &request)).await {
                    Ok(data) if !data.is_empty() => {
                        if let Err(e) = context.store.put_slice(spool, track_addr, data) {
                            tracing::warn!(?track_addr, "put_slice error: {e}");
                            continue;
                        }
                        let _ = context.store.remove_pending_recovery(spool, track_addr);
                        tracing::debug!(?track_addr, spool, "recovered slice");
                        recovered = true;
                        removed_any = true;
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
                any_failed = true;
            }
        }

        // If no items were removed this batch, remaining items all failed.
        // Break to avoid re-processing the same items indefinitely.
        if !removed_any {
            break;
        }
    }

    if any_failed {
        TaskOutcome::Retryable("some tracks could not be recovered".into())
    } else {
        TaskOutcome::Success
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use tape_core::types::EpochNumber;
    use tape_store::ops::MetaOps;
    use tape_store::types::TrackInfo;
    use tokio_util::sync::CancellationToken;

    use crate::test_util::test_context;

    #[tokio::test]
    async fn recovery_empty_queue() {
        let ctx = test_context();
        ctx.store.set_current_epoch(EpochNumber(1)).unwrap();
        ctx.store.put_committee(EpochNumber(1), vec![]).unwrap();

        let cancel = CancellationToken::new();
        let result = run(ctx, 5, cancel).await;
        assert!(matches!(result, TaskOutcome::Success));
    }

    #[tokio::test]
    async fn recovery_partial_failure() {
        let ctx = test_context();
        ctx.store.set_current_epoch(EpochNumber(1)).unwrap();
        // Empty committee → no helpers available
        ctx.store.put_committee(EpochNumber(1), vec![]).unwrap();

        let track = tape_store::types::Pubkey([1u8; 32]);
        ctx.store.put_track(track, TrackInfo {
            tape_address: tape_store::types::Pubkey([0u8; 32]),
            spool_group: 0,
            original_size: 1024,
            stripe_size: 512,
            stripe_count: 2,
            encoding_type: 0,
            encoding_params: 0,
            commitment: vec![],
        }).unwrap();
        ctx.store.add_pending_recovery(5, track).unwrap();

        let cancel = CancellationToken::new();
        let result = run(ctx, 5, cancel).await;
        assert!(matches!(result, TaskOutcome::Retryable(_)));
    }
}
