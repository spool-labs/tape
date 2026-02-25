//! SpoolSync — sync spool data from a peer that previously owned it.

use std::sync::Arc;

use rpc::Rpc;
use store::Store;
use tape_node_client::{NodeClientBuilder, RetryConfig, with_retry};
use tape_node_api::{SyncSpoolRequest, SyncSpoolResponse};
use tape_core::types::EpochNumber;
use tape_store::ops::{SliceOps, SpoolOps, TrackOps};
use tape_store::types::Pubkey as StorePubkey;
use tape_store::types::SpoolStatus;
use tokio_util::sync::CancellationToken;

use std::time::Duration;

use crate::core::validate_slice_entry;
use crate::core::{NodeContext, PeerHandle};
use crate::core::require_epoch;
use crate::TaskOutcome;

const SYNC_BATCH_SIZE: u32 = 100;
const SYNC_FAILURE_THRESHOLD: u32 = 5;

pub async fn run<S: Store, R: Rpc>(
    context: Arc<NodeContext<S, R>>,
    peer_handle: PeerHandle,
    spool: u16,
    attempt: u32,
    cancel: CancellationToken,
) -> TaskOutcome {
    let attempt_count = attempt.saturating_add(1);

    let epoch = match require_epoch(&context.chain_state) {
        Ok(e) => e,
        Err(outcome) => return outcome,
    };

    if epoch.is_zero() {
        return promote_active(&context, spool, false);
    }

    // Look up previous epoch committee to find the peer that owned this spool
    let prev_epoch = epoch - EpochNumber(1);
    let chain_state = context.chain_state.load();
    let prev_committee = match chain_state.committee_for(prev_epoch) {
        Some(c) => c.clone(),
        None => return TaskOutcome::Retryable("no committee for previous epoch".into()),
    };

    // Find the peer that owned this spool in the previous epoch
    let prev_owner = prev_committee
        .iter()
        .find(|node| node.spools.contains(&spool));

    let prev_owner = match prev_owner {
        Some(n) => n,
        None => {
            tracing::info!(spool, "no previous owner, marking active");
            return promote_active(&context, spool, false);
        }
    };

    // We already have the data — no need to HTTP-sync from ourselves.
    let our_address: StorePubkey = context.node_address().into();
    if prev_owner.node_address == our_address {
        tracing::info!(spool, "we owned this spool last epoch, marking active");
        return promote_active(&context, spool, false);
    }

    // Build client for previous owner
    let peer_address = match prev_owner.network_address.to_socket_addr() {
        Ok(a) => a,
        Err(e) => return TaskOutcome::Permanent(format!("parse network address: {e}")),
    };

    match peer_handle.is_cooling_down(peer_address).await {
        Ok(true) => return TaskOutcome::Pending(Duration::from_secs(5)),
        Ok(false) => {}
        Err(e) => return TaskOutcome::Retryable(format!("peer tracker unavailable: {e}")),
    }

    let client = match NodeClientBuilder::new().build(&peer_address.to_string()) {
        Ok(c) => c,
        Err(e) => {
            if reached_limit(attempt_count) {
                return TaskOutcome::Permanent(format!(
                    "build client failed after {} attempts: {e}",
                    attempt_count,
                ));
            }
            return TaskOutcome::Retryable(format!("build client: {e}"));
        }
    };

    // Resume from cursor if we have one
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

        let request_bytes = match wincode::serialize(&request) {
            Ok(b) => b,
            Err(e) => return TaskOutcome::Retryable(format!("serialize request: {e}")),
        };

        let response_bytes = match with_retry(&RetryConfig::fast(), || client.sync_spool(request_bytes.clone())).await {
            Ok(b) => b,
            Err(e) => {
                if let Err(err) = peer_handle.record_failure(peer_address).await {
                    tracing::warn!("failed to record peer failure for {peer_address}: {err}");
                }

                if reached_limit(attempt_count) {
                    return TaskOutcome::Permanent(format!(
                        "sync failed after {} attempts: {e}",
                        attempt_count,
                    ));
                }
                return TaskOutcome::Retryable(format!("sync_spool rpc: {e}"));
            }
        };

        let response: SyncSpoolResponse = match wincode::deserialize(&response_bytes) {
            Ok(r) => r,
            Err(e) => {
                if let Err(err) = peer_handle.record_failure(peer_address).await {
                    tracing::warn!("failed to record peer failure for {peer_address}: {err}");
                }

                if reached_limit(attempt_count) {
                    return TaskOutcome::Permanent(format!(
                        "deserialize response failed after {} attempts: {e}",
                        attempt_count,
                    ));
                }
                return TaskOutcome::Retryable(format!("deserialize response: {e}"));
            }
        };

        for entry in &response.entries {
            let track_address = StorePubkey::new(entry.track_address);
            // Treat every synced slice as untrusted until it passes local
            // metadata and commitment validation.
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
                if let Err(err2) = peer_handle.record_failure(peer_address).await {
                    tracing::warn!("failed to record peer failure for {peer_address}: {err2}");
                }

                if reached_limit(attempt_count) {
                    return TaskOutcome::Permanent(format!(
                        "sync validation failed after {} attempts: {err}",
                        attempt_count,
                    ));
                }
                return TaskOutcome::Retryable(format!("sync validation failed: {err}"));
            }

            // Mark the peer successful only once we know the bytes are safe to
            // persist locally; this keeps peer scoring aligned with durable value.
            if let Err(e) = context
                .store
                .put_slice(spool, track_address, entry.slice_data.clone())
            {
                return TaskOutcome::Retryable(format!("put_slice: {e}"));
            }
        }

        // Update cursor for resume
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

    // Sync complete — only mark Active if still in ActiveSync.
    // An epoch advance may have marked this spool LockedToMove while we were syncing.
    let outcome = promote_active(&context, spool, true);
    if matches!(outcome, TaskOutcome::Success) {
        tracing::info!(spool, "spool sync complete");
    }
    outcome
}

fn promote_active<S: Store, R: Rpc>(
    context: &Arc<NodeContext<S, R>>,
    spool: u16,
    clear_sync_cursor: bool,
) -> TaskOutcome {

    let status = match context.store.get_spool_status(spool) {
        Ok(status) => status,
        Err(e) => return TaskOutcome::Retryable(format!("read spool status: {e}")),
    };

    if !matches!(status, Some(SpoolStatus::ActiveSync)) {
        tracing::info!(
            spool,
            ?status,
            "spool status changed during sync, skipping activation"
        );
        return TaskOutcome::Success;
    }

    if clear_sync_cursor {
        let _ = context.store.remove_spool_sync_cursor(spool);
    }

    if let Err(e) = context.store.set_spool_status(spool, SpoolStatus::Active) {
        return TaskOutcome::Retryable(format!("set spool active: {e}"));
    }

    TaskOutcome::Success
}

fn reached_limit(attempt_count: u32) -> bool {
    attempt_count >= SYNC_FAILURE_THRESHOLD
}
