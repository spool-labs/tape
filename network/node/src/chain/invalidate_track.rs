use std::sync::Arc;

use rpc::{Rpc, RpcError};
use tape_protocol::Api;
use solana_sdk::compute_budget::ComputeBudgetInstruction;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::{Signature, Signer};
use store::Store;
use tape_api::compute::INVALIDATE_TRACK_CU;
use tape_api::instruction::build_invalidate_track_ix;
use tape_api::program::tapedrive::{CommitteeBitmap, epoch_pda, system_pda};
use tape_crypto::Hash;

use crate::core::NodeContext;

pub async fn submit_invalidate_track<Db: Store, Cluster: Api, Blockchain: Rpc>(
    context: &Arc<NodeContext<Db, Cluster, Blockchain>>,
    tape_address: Pubkey,
    track: Pubkey,
    epoch: tape_core::types::EpochNumber,
    bitmap: CommitteeBitmap,
    signature: tape_core::bls::BlsSignature,
    observed_root: Hash,
) -> Result<Signature, RpcError> {
    let fee_payer = context.keypair.pubkey();
    let (system_address, _) = system_pda();
    let (epoch_address, _) = epoch_pda();

    let cu_ix = ComputeBudgetInstruction::set_compute_unit_limit(INVALIDATE_TRACK_CU);
    let ix = build_invalidate_track_ix(
        fee_payer,
        system_address,
        epoch_address,
        tape_address,
        track,
        epoch,
        bitmap,
        signature,
        observed_root,
    );

    context.rpc.send_instructions(&context.keypair, vec![cu_ix, ix]).await
}
