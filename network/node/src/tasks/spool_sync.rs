//! SpoolSync — sync spool data from a peer that previously owned it.

use std::sync::Arc;

use rpc::Rpc;
use store::Store;
use tape_node_client::{NodeClientBuilder, RetryConfig, with_retry};
use tape_node_api::{SyncSpoolRequest, SyncSpoolResponse};
use tape_core::types::EpochNumber;
use tape_store::ops::{CommitteeOps, MetaOps, SliceOps, SpoolOps};
use tape_store::types::Pubkey as StorePubkey;
use tape_store::types::SpoolStatus;
use tokio_util::sync::CancellationToken;

use crate::core::NodeContext;
use crate::peers::PeerHandle;
use crate::supervisor::TaskOutcome;

const SYNC_BATCH_SIZE: u32 = 100;

pub async fn run<S: Store, R: Rpc>(
    context: Arc<NodeContext<S, R>>,
    peer_handle: PeerHandle,
    spool: u16,
    cancel: CancellationToken,
) -> TaskOutcome {
    // Read current epoch
    let epoch = match context.store.get_chain_epoch() {
        Ok(Some(e)) => e,
        Ok(None) => return TaskOutcome::Retryable("no current epoch".into()),
        Err(e) => return TaskOutcome::Retryable(format!("read epoch: {e}")),
    };

    if epoch.as_u64() == 0 {
        if let Err(e) = context.store.set_spool_status(spool, SpoolStatus::Active) {
            return TaskOutcome::Retryable(format!("set spool active: {e}"));
        }
        return TaskOutcome::Success;
    }

    // Look up previous epoch committee to find the peer that owned this spool
    let prev_epoch = EpochNumber(epoch.as_u64() - 1);
    let prev_committee = match context.store.get_committee(prev_epoch) {
        Ok(Some(c)) => c,
        Ok(None) => {
            return TaskOutcome::Retryable("no committee for previous epoch".into())
        }
        Err(e) => return TaskOutcome::Retryable(format!("read committee: {e}")),
    };

    // Find the node that owned this spool in the previous epoch
    let prev_owner = prev_committee
        .iter()
        .find(|node| node.spools.contains(&spool));

    let prev_owner = match prev_owner {
        Some(n) => n,
        None => {
            tracing::info!(spool, "no previous owner, marking active");
            if let Err(e) = context.store.set_spool_status(spool, SpoolStatus::Active) {
                return TaskOutcome::Retryable(format!("set spool active: {e}"));
            }
            return TaskOutcome::Success;
        }
    };

    // Build client for previous owner
    let addr = match prev_owner.network_address.to_socket_addr() {
        Ok(a) => a,
        Err(e) => return TaskOutcome::Retryable(format!("parse network address: {e}")),
    };

    match peer_handle.is_cooling_down(addr).await {
        Ok(true) => return TaskOutcome::Retryable("peer cooling down".into()),
        Ok(false) => {}
        Err(e) => return TaskOutcome::Retryable(format!("peer tracker unavailable: {e}")),
    }

    let client = match NodeClientBuilder::new().build(&addr.to_string()) {
        Ok(c) => c,
        Err(e) => return TaskOutcome::Retryable(format!("build client: {e}")),
    };

    // Resume from cursor if we have one
    let mut cursor: Option<[u8; 32]> = context
        .store
        .get_spool_sync_cursor(spool)
        .ok()
        .flatten()
        .map(|p| p.0);

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
            Ok(b) => {
                if let Err(e) = peer_handle.record_success(addr).await {
                    tracing::warn!("failed to record peer success for {addr}: {e}");
                }
                b
            }
            Err(e) => {
                if let Err(err) = peer_handle.record_failure(addr).await {
                    tracing::warn!("failed to record peer failure for {addr}: {err}");
                }
                return TaskOutcome::Retryable(format!("sync_spool rpc: {e}"));
            }
        };

        let response: SyncSpoolResponse = match wincode::deserialize(&response_bytes) {
            Ok(r) => r,
            Err(e) => return TaskOutcome::Retryable(format!("deserialize response: {e}")),
        };

        for entry in &response.entries {
            let track_address = StorePubkey::new(entry.track_address);
            if let Err(e) =
                context
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

        match response.next_cursor {
            Some(c) => cursor = Some(c),
            None => break,
        }
    }

    // Sync complete — clean up
    let _ = context.store.remove_spool_sync_cursor(spool);
    if let Err(e) = context.store.set_spool_status(spool, SpoolStatus::Active) {
        return TaskOutcome::Retryable(format!("set spool active: {e}"));
    }

    tracing::info!(spool, "spool sync complete");
    TaskOutcome::Success
}
