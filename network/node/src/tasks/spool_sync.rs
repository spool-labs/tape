//! SpoolSync — sync spool data from a peer that previously owned it.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use rpc::Rpc;
use store::Store;
use tape_node_client::{NodeClient, NodeClientBuilder, RetryConfig, with_retry};
use tape_node_api::{SyncSpoolRequest, SyncSpoolResponse};
use tape_core::spooler::SpoolIndex;
use tape_core::types::EpochNumber;
use tape_store::ops::{SliceOps, SpoolOps, TrackOps};
use tape_store::types::Pubkey as StorePubkey;
use tape_store::types::{SpoolState, SpoolStatus};
use tokio_util::sync::CancellationToken;

use crate::core::{has_missing_slices, validate_slice_entry};
use crate::core::{NodeContext, PeerHandle};
use crate::TaskOutcome;

const SYNC_BATCH_SIZE: u32 = 100;
const SYNC_FAILURE_THRESHOLD: u32 = 5;

enum SyncSource {
    SkipSync,
    SyncFrom { client: NodeClient, peer_address: SocketAddr },
}

pub async fn run<S: Store, R: Rpc>(
    context: Arc<NodeContext<S, R>>,
    peer_handle: PeerHandle,
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

    if let SyncSource::SyncFrom { client, peer_address } = sync_source {
        match peer_handle.is_cooling_down(peer_address).await {
            Ok(true) => return TaskOutcome::Pending(Duration::from_secs(5)),
            Ok(false) => {}
            Err(e) => return TaskOutcome::Retryable(format!("peer tracker unavailable: {e}")),
        }

        let mut cursor: Option<[u8; 32]> = match context.store.get_spool_sync_cursor(spool) {
            Ok(cursor) => cursor.map(|p| p.0),
            Err(e) => return TaskOutcome::Retryable(format!("get cursor: {e}")),
        };

        loop {
            if cancel.is_cancelled() {
                return TaskOutcome::Success;
            }

            let request = SyncSpoolRequest {
                spool_index: spool,
                cursor,
                limit: SYNC_BATCH_SIZE,
            };

            let response: SyncSpoolResponse = match with_retry(&RetryConfig::fast(), || client.sync_spool(&request)).await {
                Ok(r) => r,
                Err(e) => {
                    return fail_peer(
                        &peer_handle, peer_address, attempt_count,
                        format!("sync_spool rpc: {e}"),
                    ).await;
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
                        &peer_handle, peer_address, attempt_count,
                        format!("sync validation failed: {err}"),
                    ).await;
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
            if let Err(e) = peer_handle.record_success(peer_address).await {
                tracing::warn!("failed to record peer success for {peer_address}: {e}");
            }

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
        let new_state = SpoolState { status: SpoolStatus::Active, epoch: state.epoch };
        if let Err(e) = context.store.set_spool_state(spool, new_state) {
            return TaskOutcome::Retryable(format!("set spool active: {e}"));
        }
        tracing::info!(spool, synced = synced.len(), "spool sync complete");
    } else {
        match has_missing_slices(&*context.store, spool) {
            Ok(false) => {
                let new_state = SpoolState { status: SpoolStatus::Active, epoch: state.epoch };
                if let Err(e) = context.store.set_spool_state(spool, new_state) {
                    return TaskOutcome::Retryable(format!("set spool active: {e}"));
                }
            }
            Ok(true) => {
                tracing::info!(spool, "missing slices detected, transitioning to recovery");
                let new_state = SpoolState { status: SpoolStatus::ActiveRecover, epoch: state.epoch };
                if let Err(e) = context.store.set_spool_state(spool, new_state) {
                    return TaskOutcome::Retryable(format!("set spool recovering: {e}"));
                }
            }
            Err(e) => return TaskOutcome::Retryable(format!("scan missing slices: {e}")),
        }
    }

    TaskOutcome::Success
}


fn resolve_sync_source<S: Store, R: Rpc>(
    context: &Arc<NodeContext<S, R>>,
    spool: SpoolIndex,
    spool_state: &SpoolState,
) -> Result<SyncSource, TaskOutcome> {
    if spool_state.epoch.is_zero() {
        return Ok(SyncSource::SkipSync);
    }

    let prev_epoch = spool_state.epoch - EpochNumber(1);
    let cs = context.chain_state.load();

    let prev_committee = if prev_epoch == cs.epoch {
        cs.committee.clone()
    } else if !cs.epoch.is_zero() && prev_epoch == cs.epoch - EpochNumber(1) {
        cs.committee_prev.clone()
    } else {
        return Err(TaskOutcome::Retryable("committee for previous epoch not available".into()));
    };

    let prev_owner = match prev_committee.iter().find(|node| node.spools.contains(&spool)) {
        Some(n) => n,
        None => {
            tracing::info!(spool, "no previous owner, verifying data");
            return Ok(SyncSource::SkipSync);
        }
    };

    let our_address: StorePubkey = context.node_address().into();
    if prev_owner.node_address == our_address {
        tracing::info!(spool, "we owned this spool last epoch, verifying data");
        return Ok(SyncSource::SkipSync);
    }

    let peer_address = prev_owner
        .network_address
        .to_socket_addr()
        .map_err(|e| TaskOutcome::Permanent(format!("parse network address: {e}")))?;

    let client = NodeClientBuilder::new()
        .build(&peer_address.to_string())
        .map_err(|e| TaskOutcome::Retryable(format!("build client: {e}")))?;

    Ok(SyncSource::SyncFrom { client, peer_address })
}

async fn fail_peer(
    peer_handle: &PeerHandle,
    peer_address: SocketAddr,
    attempt_count: u32,
    msg: String,
) -> TaskOutcome {
    if let Err(err) = peer_handle.record_failure(peer_address).await {
        tracing::warn!("failed to record peer failure for {peer_address}: {err}");
    }
    if attempt_count >= SYNC_FAILURE_THRESHOLD {
        TaskOutcome::Permanent(format!("{msg} after {attempt_count} attempts"))
    } else {
        TaskOutcome::Retryable(msg)
    }
}

