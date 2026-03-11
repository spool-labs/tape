//! SpoolSync — sync spool data from a peer that previously owned it.

use std::sync::Arc;

use rpc::Rpc;
use tape_protocol::Api;
use tape_protocol::api::ApiError;
use tape_protocol::api::SyncReq;
use tape_retry::Retryable;
use store::Store;
use tape_core::spooler::SpoolIndex;
use tape_core::types::NodeId;
use tape_store::ops::{SliceOps, SpoolOps, TrackOps};
use tape_store::types::Pubkey as StorePubkey;
use tape_store::types::{SpoolState, SpoolStatus};
use tokio_util::sync::CancellationToken;

use crate::core::NodeContext;
use crate::TaskOutcome;
use crate::core::call_peer;
use crate::tasks::spool_support::validate_slice_entry;

const SYNC_BATCH_SIZE: u32 = 100;

enum SyncSource {
    VerifyLocal,
    SyncFrom { node_id: NodeId },
}

pub async fn run<Db: Store, Cluster: Api, Blockchain: Rpc>(
    ctx: Arc<NodeContext<Db, Cluster, Blockchain>>,
    spool: SpoolIndex,
    cancel: CancellationToken,
) -> TaskOutcome {
    // Phase 1: Resolve source.
    let state = match ctx.store.get_spool_state(spool) {
        Ok(Some(s)) => s,
        Ok(None) => return TaskOutcome::Success,
        Err(e) => return TaskOutcome::Retryable(format!("get_spool_state: {e}")),
    };

    if !state.is_syncing() {
        return TaskOutcome::Success;
    }

    let source = match state.prev_owner {
        None => SyncSource::VerifyLocal,
        Some(id) if id == ctx.node_id() => SyncSource::VerifyLocal,
        Some(id) => SyncSource::SyncFrom { node_id: id },
    };

    match source {
        SyncSource::VerifyLocal => {
            return finish_sync(&ctx, spool);
        }
        SyncSource::SyncFrom { node_id: peer } => {
            // Phase 2: Sync loop.
            let mut cursor: Option<[u8; 32]> = match ctx.store.get_spool_sync_cursor(spool) {
                Ok(Some(pubkey)) => Some(pubkey.0),
                Ok(None) => None,
                Err(e) => return TaskOutcome::Retryable(format!("get_spool_sync_cursor: {e}")),
            };

            let mut total_bytes: u64 = 0;

            loop {
                if cancel.is_cancelled() {
                    return TaskOutcome::Success;
                }

                let req = SyncReq {
                    spool_index: spool,
                    cursor,
                    limit: SYNC_BATCH_SIZE,
                };

                let api = ctx.api.clone();
                let res = call_peer(&ctx.peer_manager, peer, Some(&cancel), move || {
                    let api = api.clone();
                    let req = req.clone();
                    async move { api.sync(peer, &req).await }
                })
                .await;

                let res = match res {
                    Ok(res) => res,
                    Err(e) if e.is_retryable() || is_transient_sync_source_error(&e) => {
                        return TaskOutcome::Retryable(format!("sync peer {}: {e}", peer.0));
                    }
                    Err(e) => {
                        return TaskOutcome::Permanent(format!("sync peer {}: {e}", peer.0));
                    }
                };

                if res.entries.is_empty() && res.next_cursor.is_none() {
                    break;
                }

                let mut last_track: Option<StorePubkey> = None;

                for entry in &res.entries {
                    let track_addr = StorePubkey(entry.track_address);

                    // Skip if we already have this slice.
                    match ctx.store.has_slice(spool, track_addr) {
                        Ok(true) => {
                            last_track = Some(track_addr);
                            continue;
                        }
                        Ok(false) => {}
                        Err(e) => {
                            tracing::warn!(?track_addr, spool, "has_slice: {e}");
                            last_track = Some(track_addr);
                            continue;
                        }
                    }

                    // Validate if track metadata exists.
                    match ctx.store.get_track(track_addr) {
                        Ok(Some(track_info)) => {
                            if let Err(reason) =
                                validate_slice_entry(spool, &track_info, &entry.slice_data)
                            {
                                tracing::warn!(?track_addr, spool, "skipping invalid slice: {reason}");
                                last_track = Some(track_addr);
                                continue;
                            }
                        }
                        Ok(None) => {
                            // No track metadata yet — accept the slice on trust.
                        }
                        Err(e) => {
                            tracing::warn!(?track_addr, spool, "get_track: {e}");
                            last_track = Some(track_addr);
                            continue;
                        }
                    }

                    // Persist slice.
                    match ctx.store.put_slice(spool, track_addr, entry.slice_data.clone()) {
                        Ok(()) => {
                            total_bytes += entry.slice_data.len() as u64;
                        }
                        Err(e) => {
                            tracing::warn!(?track_addr, spool, "put_slice: {e}");
                        }
                    }

                    last_track = Some(track_addr);
                }

                // Persist cursor after each batch.
                if let Some(track) = last_track {
                    if let Err(e) = ctx.store.set_spool_sync_cursor(spool, track) {
                        tracing::warn!(spool, "set_spool_sync_cursor: {e}");
                    }
                }

                match res.next_cursor {
                    Some(c) => cursor = Some(c),
                    None => break,
                }
            }

            if total_bytes > 0 {
                ctx.stats.add_sync_received(total_bytes);
            }
        }
    }

    // Phase 3: Finish.
    finish_sync(&ctx, spool)
}

fn is_transient_sync_source_error(err: &ApiError) -> bool {
    matches!(err, ApiError::NotResponsible | ApiError::NotFound)
}

fn finish_sync<Db: Store, Cluster: Api, Blockchain: Rpc>(
    ctx: &Arc<NodeContext<Db, Cluster, Blockchain>>,
    spool: SpoolIndex,
) -> TaskOutcome {
    let state = match ctx.store.get_spool_state(spool) {
        Ok(Some(s)) => s,
        Ok(None) => {
            tracing::debug!(spool, "finish_sync: no spool state, skipping");
            return TaskOutcome::Success;
        }
        Err(e) => return TaskOutcome::Retryable(format!("finish_sync get_spool_state: {e}")),
    };

    if !state.is_syncing() {
        tracing::debug!(spool, status = ?state.status, "finish_sync: not syncing, skipping");
        return TaskOutcome::Success;
    }

    let new_state = SpoolState {
        status: SpoolStatus::ActiveRecover,
        epoch: state.epoch,
        prev_owner: state.prev_owner,
    };

    if let Err(e) = ctx.store.set_spool_state(spool, new_state) {
        return TaskOutcome::Retryable(format!("finish_sync set_spool_state: {e}"));
    }

    let _ = ctx.store.remove_spool_sync_cursor(spool);
    let _ = ctx.store.clear_scan_done(spool);

    tracing::info!(spool, "spool sync complete, transitioning to ActiveRecover");

    TaskOutcome::Success
}

#[cfg(test)]
mod tests {
    use super::*;

    use tape_core::types::EpochNumber;
    use tokio_util::sync::CancellationToken;

    use crate::core::test_utils::test_context;

    #[tokio::test]
    async fn sync_self_owner() {
        let ctx = test_context();
        let node_id = ctx.node_id();

        ctx.store
            .set_spool_state(
                5,
                SpoolState {
                    status: SpoolStatus::ActiveSync,
                    epoch: EpochNumber(3),
                    prev_owner: Some(node_id),
                },
            )
            .unwrap();

        let cancel = CancellationToken::new();
        let result = run(ctx.clone(), 5, cancel).await;
        assert!(matches!(result, TaskOutcome::Success));

        let state = ctx.store.get_spool_state(5).unwrap().unwrap();
        assert_eq!(state.status, SpoolStatus::ActiveRecover);
        assert_eq!(state.epoch, EpochNumber(3));
    }

    #[tokio::test]
    async fn sync_no_prev_owner() {
        let ctx = test_context();

        ctx.store
            .set_spool_state(
                5,
                SpoolState {
                    status: SpoolStatus::ActiveSync,
                    epoch: EpochNumber(3),
                    prev_owner: None,
                },
            )
            .unwrap();

        let cancel = CancellationToken::new();
        let result = run(ctx.clone(), 5, cancel).await;
        assert!(matches!(result, TaskOutcome::Success));

        let state = ctx.store.get_spool_state(5).unwrap().unwrap();
        assert_eq!(state.status, SpoolStatus::ActiveRecover);
    }

    #[tokio::test]
    async fn sync_not_syncing() {
        let ctx = test_context();

        ctx.store
            .set_spool_state(
                5,
                SpoolState {
                    status: SpoolStatus::Active,
                    epoch: EpochNumber(3),
                    prev_owner: None,
                },
            )
            .unwrap();

        let cancel = CancellationToken::new();
        let result = run(ctx.clone(), 5, cancel).await;
        assert!(matches!(result, TaskOutcome::Success));

        // Should remain Active, not transition.
        let state = ctx.store.get_spool_state(5).unwrap().unwrap();
        assert_eq!(state.status, SpoolStatus::Active);
    }

    #[tokio::test]
    async fn sync_no_state() {
        let ctx = test_context();

        let cancel = CancellationToken::new();
        let result = run(ctx, 5, cancel).await;
        assert!(matches!(result, TaskOutcome::Success));
    }

    #[tokio::test]
    async fn sync_unhealthy_peer() {
        let ctx = test_context();
        let peer = NodeId(99);

        ctx.store
            .set_spool_state(
                5,
                SpoolState {
                    status: SpoolStatus::ActiveSync,
                    epoch: EpochNumber(3),
                    prev_owner: Some(peer),
                },
            )
            .unwrap();

        // Mark peer as unhealthy by reporting failures.
        for _ in 0..10 {
            ctx.peer_manager.report_failure(peer);
        }

        let cancel = CancellationToken::new();
        let result = run(ctx.clone(), 5, cancel).await;

        // MemoryApi returns NotFound for unknown nodes → non-retryable after retries exhaust.
        // The exact outcome depends on MemoryApi behavior, but it should not panic.
        assert!(matches!(
            result,
            TaskOutcome::Retryable(_) | TaskOutcome::Permanent(_)
        ));
    }
}
