//! InvalidateTrack — submit track invalidation on-chain.

use std::sync::Arc;
use std::time::Duration;

use rpc::{Rpc, RpcError};
use rpc_client::parse_tape_error;
use solana_sdk::pubkey::Pubkey;
use store::Store;
use tape_protocol::Api;
use tape_api::errors::TapeError;
use tape_api::program::tapedrive::CommitteeBitmap;
use tape_crypto::Hash;
use tape_store::ops::{MetaOps, TrackOps};
use tokio_util::sync::CancellationToken;

use crate::chain::submit_invalidate_track;
use crate::core::NodeContext;
use crate::TaskOutcome;

const INVALIDATE_TRACK_PENDING_DELAY: Duration = Duration::from_secs(5);

fn classify_invalidate_track_error(err: &RpcError) -> TaskOutcome {
    match parse_tape_error(err) {
        Some(TapeError::AlreadyInvalidated) => TaskOutcome::Success,
        Some(TapeError::BadEpochId) => TaskOutcome::Pending(INVALIDATE_TRACK_PENDING_DELAY),
        Some(TapeError::NoQuorum)
        | Some(TapeError::NoSigners)
        | Some(TapeError::BadMember)
        | Some(TapeError::BadSignature) => {
            TaskOutcome::Permanent(format!("invalidate_track: {err}"))
        }
        _ => TaskOutcome::Retryable(format!("invalidate_track: {err}")),
    }
}

pub async fn run<Db: Store, Cluster: Api, Blockchain: Rpc>(
    context: Arc<NodeContext<Db, Cluster, Blockchain>>,
    track: Pubkey,
    cancel: CancellationToken,
) -> TaskOutcome {
    let store_track: tape_store::types::Pubkey = track.into();

    // Read proof from store
    let proof = match context.store.get_invalidation_proof(store_track) {
        Ok(Some(p)) => p,
        Ok(None) => return TaskOutcome::Permanent("no invalidation proof".into()),
        Err(e) => return TaskOutcome::Retryable(format!("read proof: {e}")),
    };

    if cancel.is_cancelled() {
        return TaskOutcome::Success;
    }

    // Read track info to get tape_address
    let track_info = match context.store.get_track(store_track) {
        Ok(Some(t)) => t,
        Ok(None) => return TaskOutcome::Permanent("track not found in store".into()),
        Err(e) => return TaskOutcome::Retryable(format!("read track: {e}")),
    };

    if cancel.is_cancelled() {
        return TaskOutcome::Success;
    }

    let tape_address: Pubkey = track_info.tape_address.into();

    let epoch = context.peer_manager.state().epoch;

    let bitmap: CommitteeBitmap = bytemuck::cast(proof.bitmap);
    let signature = proof.signature;
    let observed_root = Hash(proof.computed_root);

    let result = tokio::select! {
        r = submit_invalidate_track(
            &context,
            tape_address,
            track,
            epoch,
            bitmap,
            signature,
            observed_root,
        ) => r,
        _ = cancel.cancelled() => return TaskOutcome::Success,
    };
    match result {
        Ok(sig) => {
            tracing::info!(%sig, %track, "invalidate_track submitted");
            let _ = context.store.delete_invalidation_proof(store_track);
            TaskOutcome::Success
        }
        Err(ref e) => {
            let outcome = classify_invalidate_track_error(e);
            if matches!(outcome, TaskOutcome::Success) {
                let _ = context.store.delete_invalidation_proof(store_track);
            }
            outcome
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tx_error(code: u32) -> RpcError {
        RpcError::Transaction(format!("custom program error: 0x{code:x}"))
    }

    #[test]
    fn bad_epoch_id_is_pending() {
        let out = classify_invalidate_track_error(&tx_error(TapeError::BadEpochId as u32));
        assert!(matches!(out, TaskOutcome::Pending(delay) if delay == INVALIDATE_TRACK_PENDING_DELAY));
    }

    #[test]
    fn quorum_signature_errors_are_permanent() {
        let codes = [
            TapeError::NoQuorum as u32,
            TapeError::NoSigners as u32,
            TapeError::BadMember as u32,
            TapeError::BadSignature as u32,
        ];
        for code in codes {
            let out = classify_invalidate_track_error(&tx_error(code));
            assert!(matches!(out, TaskOutcome::Permanent(_)));
        }
    }

    #[test]
    fn already_invalidated_is_success() {
        let out = classify_invalidate_track_error(&tx_error(TapeError::AlreadyInvalidated as u32));
        assert!(matches!(out, TaskOutcome::Success));
    }

    #[test]
    fn already_invalidated_string_is_retryable() {
        let out = classify_invalidate_track_error(&RpcError::Transaction("already invalidated".into()));
        assert!(matches!(out, TaskOutcome::Retryable(_)));
    }

    #[test]
    fn request_error_is_retryable() {
        let out = classify_invalidate_track_error(&RpcError::Request("timeout".into()));
        assert!(matches!(out, TaskOutcome::Retryable(_)));
    }
}
