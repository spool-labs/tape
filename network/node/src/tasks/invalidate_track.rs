//! InvalidateTrack — submit track invalidation on-chain.

use std::sync::Arc;

use rpc::Rpc;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signer::Signer;
use store::Store;
use tape_api::instruction::build_invalidate_track_ix;
use tape_api::program::tapedrive::{epoch_pda, system_pda, CommitteeBitmap};
use tape_core::bls::BlsSignature;
use tape_crypto::bls12254::min_sig::G1CompressedPoint;
use tape_crypto::Hash;
use tape_store::ops::{MetaOps, TrackOps};
use tokio_util::sync::CancellationToken;

use crate::core::NodeContext;
use crate::supervisor::TaskOutcome;

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
    let (system_address, _) = system_pda();
    let (epoch_address, _) = epoch_pda();
    let fee_payer = context.keypair.pubkey();

    let bitmap: CommitteeBitmap = bytemuck::cast(proof.bitmap);
    let signature = BlsSignature(G1CompressedPoint(proof.signature));
    let computed_root = Hash(proof.computed_root);

    let ix = build_invalidate_track_ix(
        fee_payer,
        system_address,
        epoch_address,
        tape_address,
        track,
        bitmap,
        signature,
        computed_root,
    );

    let result = tokio::select! {
        r = context.rpc.send_instructions(&context.keypair, vec![ix]) => r,
        _ = cancel.cancelled() => return TaskOutcome::Success,
    };
    match result {
        Ok(sig) => {
            tracing::info!(%sig, %track, "invalidate_track submitted");
            let _ = context.store.delete_invalidation_proof(store_track);
            TaskOutcome::Success
        }
        Err(e) => {
            let msg = e.to_string();
            if msg.contains("already invalidated") {
                let _ = context.store.delete_invalidation_proof(store_track);
                TaskOutcome::Success
            } else {
                TaskOutcome::Retryable(format!("invalidate_track: {e}"))
            }
        }
    }
}
