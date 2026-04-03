use std::sync::Arc;

use rpc::{Rpc, RpcError};
use solana_sdk::compute_budget::ComputeBudgetInstruction;
use store::Store;
use tape_api::compute::INVALIDATE_TRACK_CU;
use tape_api::instruction::build_invalidate_track_ix;
use tape_api::program::tapedrive::{CommitteeBitmap, epoch_pda, system_pda};
use tape_core::track::types::CompressedTrackProof;
use tape_crypto::Hash;
use tape_crypto::tx::Txid;
use tape_protocol::Api;

use crate::context::NodeContext;

pub async fn submit_invalidate_track<Db: Store, Cluster: Api, Blockchain: Rpc>(
    ctx: &Arc<NodeContext<Db, Cluster, Blockchain>>,
    track: CompressedTrackProof,
    epoch: tape_core::types::EpochNumber,
    bitmap: CommitteeBitmap,
    signature: tape_core::bls::BlsSignature,
    observed_root: Hash,
) -> Result<Txid, RpcError> {
    let fee_payer = ctx.pubkey().into();
    let (system_address, _) = system_pda();
    let (epoch_address, _) = epoch_pda();

    let cu_ix = ComputeBudgetInstruction::set_compute_unit_limit(
        INVALIDATE_TRACK_CU);

    let ix = build_invalidate_track_ix(
        fee_payer,
        system_address,
        epoch_address,
        track,
        epoch,
        bitmap,
        signature,
        observed_root,
    );

    ctx.rpc
        .send_instructions(
            ctx.signer(),
            vec![cu_ix, ix]
    ).await
}
