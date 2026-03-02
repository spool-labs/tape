//! InvalidateTrack — submit track invalidation on-chain.

use std::sync::Arc;
use std::time::Duration;

use rpc::Rpc;
use rpc_client::parse_tape_error;
use solana_sdk::pubkey::Pubkey;
use store::Store;
use tape_api::errors::TapeError;
use tape_api::program::tapedrive::CommitteeBitmap;
use tape_crypto::Hash;
use tape_store::ops::{MetaOps, TrackOps};
use tokio_util::sync::CancellationToken;

use crate::chain::submit_invalidate_track;
use crate::core::NodeContext;
use crate::TaskOutcome;

const INVALIDATE_TRACK_PENDING_DELAY: Duration = Duration::from_secs(30);

pub async fn run<S: Store, R: Rpc>(
    context: Arc<NodeContext<S, R>>,
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

    let epoch = context.chain_state.load().epoch;

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
        Err(ref e) => match parse_tape_error(e) {
            Some(TapeError::BadEpochId) => TaskOutcome::Pending(INVALIDATE_TRACK_PENDING_DELAY),
            Some(TapeError::NoQuorum)
            | Some(TapeError::NoSigners)
            | Some(TapeError::BadMember)
            | Some(TapeError::BadSignature) => {
                TaskOutcome::Permanent(format!("invalidate_track: {e}"))
            }
            _ => TaskOutcome::Retryable(format!("invalidate_track: {e}")),
        },
    }
}
