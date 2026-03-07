//! SpoolSync — sync spool data from a peer that previously owned it.

use std::sync::Arc;
use std::time::Duration;

use rpc::Rpc;
use tape_protocol::Api;
use tape_protocol::api::{SyncReq, SyncRes};
use store::Store;
use tape_core::spooler::SpoolIndex;
use tape_core::types::NodeId;
use tape_store::ops::{SliceOps, SpoolOps, TrackOps};
use tape_store::types::Pubkey as StorePubkey;
use tape_store::types::{SpoolState, SpoolStatus};
use tokio_util::sync::CancellationToken;

use crate::core::{has_missing_slices, validate_slice_entry};
use crate::core::NodeContext;
use crate::TaskOutcome;

const SYNC_BATCH_SIZE: u32 = 100;
const SYNC_FAILURE_THRESHOLD: u32 = 5;

enum SyncSource {
    SkipSync,
    SyncFrom { node_id: NodeId },
}

pub async fn run<Db: Store, Cluster: Api, Blockchain: Rpc>(
    context: Arc<NodeContext<Db, Cluster, Blockchain>>,
    spool: SpoolIndex,
    attempt: u32,
    cancel: CancellationToken,
) -> TaskOutcome {
    let attempt_count = attempt.saturating_add(1);

    let spool_state = match context.store.get_spool_state(spool) {
        Ok(Some(s)) => s,
        Ok(None) => return TaskOutcome::Success,
        Err(e) => return TaskOutcome::Retryable(format!("read spool state: {e}")),
    };

    if !spool_state.is_syncing() {
        return TaskOutcome::Success;
    }

    let sync_source = match resolve_sync_source(&context, spool, &spool_state) {
        Ok(source) => source,
        Err(outcome) => return outcome,
    };

    let mut synced: Vec<StorePubkey> = Vec::new();

    if let SyncSource::SyncFrom { node_id } = sync_source {
        if !context.peer_manager.is_healthy(node_id) {
            return TaskOutcome::Pending(Duration::from_secs(5));
        }

        let mut cursor: Option<[u8; 32]> = match context.store.get_spool_sync_cursor(spool) {
            Ok(cursor) => cursor.map(|p| p.0),
            Err(e) => return TaskOutcome::Retryable(format!("get cursor: {e}")),
        };

        let api = context.peer_manager.api();

        loop {
            if cancel.is_cancelled() {
                return TaskOutcome::Success;
            }

            let req = SyncReq {
                spool_index: spool,
                cursor,
                limit: SYNC_BATCH_SIZE,
            };

            let response: SyncRes = match api.sync(node_id, &req).await {
                Ok(r) => r,
                Err(e) => {
                    return fail_peer(
                        &context, node_id, attempt_count,
                        format!("sync_spool rpc: {e}"),
                    );
                }
            };

            for entry in &response.entries {
                let track_address = StorePubkey::new(entry.track_address);

                let track_info = match context.store.get_track(track_address) {
                    Ok(Some(i)) => i,
                    Ok(None) => {
                        return TaskOutcome::Retryable(format!(
                            "sync missing track metadata: {track_address:?}",
                        ));
                    }
                    Err(e) => return TaskOutcome::Retryable(format!("read track metadata: {e}")),
                };

                if let Err(err) = validate_slice_entry(spool, &track_info, &entry.slice_data) {
                    return fail_peer(
                        &context, node_id, attempt_count,
                        format!("sync validation failed: {err}"),
                    );
                }

                if let Err(e) = context
                    .store
                    .put_slice(spool, track_address, entry.slice_data.clone())
                {
                    return TaskOutcome::Retryable(format!("put_slice: {e}"));
                }

                synced.push(track_address);
            }

            if let Some(last) = response.entries.last() {
                let last_addr = StorePubkey::new(last.track_address);
                if let Err(e) = context.store.set_spool_sync_cursor(spool, last_addr) {
                    return TaskOutcome::Retryable(format!("set cursor: {e}"));
                }
            }
            context.peer_manager.report_success(node_id);

            match response.next_cursor {
                Some(c) => cursor = Some(c),
                None => break,
            }
        }
    }

    // State transition — single site, mirrors spool_recovery.rs pattern.
    let state = match context.store.get_spool_state(spool) {
        Ok(Some(s)) => s,
        Ok(None) => return TaskOutcome::Success,
        Err(e) => return TaskOutcome::Retryable(format!("read spool state: {e}")),
    };

    if !state.is_syncing() {
        return TaskOutcome::Success;
    }

    if !synced.is_empty() {
        let _ = context.store.remove_spool_sync_cursor(spool);
        let new_state = SpoolState { status: SpoolStatus::Active, epoch: state.epoch, prev_owner: None };
        if let Err(e) = context.store.set_spool_state(spool, new_state) {
            return TaskOutcome::Retryable(format!("set spool active: {e}"));
        }
        tracing::info!(spool, synced = synced.len(), "spool sync complete");
    } else {
        match has_missing_slices(&*context.store, spool) {
            Ok(false) => {
                let new_state = SpoolState { status: SpoolStatus::Active, epoch: state.epoch, prev_owner: None };
                if let Err(e) = context.store.set_spool_state(spool, new_state) {
                    return TaskOutcome::Retryable(format!("set spool active: {e}"));
                }
            }
            Ok(true) => {
                tracing::info!(spool, "missing slices detected, transitioning to recovery");
                let new_state = SpoolState { status: SpoolStatus::ActiveRecover, epoch: state.epoch, prev_owner: None };
                if let Err(e) = context.store.set_spool_state(spool, new_state) {
                    return TaskOutcome::Retryable(format!("set spool recovering: {e}"));
                }
            }
            Err(e) => return TaskOutcome::Retryable(format!("scan missing slices: {e}")),
        }
    }

    TaskOutcome::Success
}


fn resolve_sync_source<Db: Store, Cluster: Api, Blockchain: Rpc>(
    context: &Arc<NodeContext<Db, Cluster, Blockchain>>,
    spool: SpoolIndex,
    spool_state: &SpoolState,
) -> Result<SyncSource, TaskOutcome> {
    if spool_state.epoch.is_zero() {
        return Ok(SyncSource::SkipSync);
    }

    let prev_owner_id = match spool_state.prev_owner {
        Some(id) => id,
        None => {
            tracing::info!(spool, "no previous owner, verifying data");
            return Ok(SyncSource::SkipSync);
        }
    };

    if prev_owner_id == context.node_id() {
        tracing::info!(spool, "we owned this spool last epoch, verifying data");
        return Ok(SyncSource::SkipSync);
    }

    Ok(SyncSource::SyncFrom { node_id: prev_owner_id })
}

fn fail_peer<Db: Store, Cluster: Api, Blockchain: Rpc>(
    context: &Arc<NodeContext<Db, Cluster, Blockchain>>,
    node_id: NodeId,
    attempt_count: u32,
    msg: String,
) -> TaskOutcome {
    context.peer_manager.report_failure(node_id);
    if attempt_count >= SYNC_FAILURE_THRESHOLD {
        TaskOutcome::Permanent(format!("{msg} after {attempt_count} attempts"))
    } else {
        TaskOutcome::Retryable(msg)
    }
}
