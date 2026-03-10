use std::sync::Arc;

use rpc::{Rpc, RpcError};
use solana_sdk::compute_budget::ComputeBudgetInstruction;
use solana_sdk::signature::Signature;
use store::Store;
use tape_api::compute::SYNC_EPOCH_CU;
use tape_api::instruction::build_epoch_sync_ix;
use tape_core::types::EpochNumber;
use tape_protocol::Api;

use crate::core::NodeContext;

pub async fn submit_sync_epoch<Db: Store, Cluster: Api, Blockchain: Rpc>(
    ctx: &Arc<NodeContext<Db, Cluster, Blockchain>>,
    epoch: EpochNumber,
    owned_spools: &[u16],
) -> Result<Signature, RpcError> {
    let fee_payer = ctx.pubkey();
    let authority = ctx.pubkey();
    let node_address = ctx.node_address();

    let cu_ix = ComputeBudgetInstruction::set_compute_unit_limit(SYNC_EPOCH_CU);

    let ix = build_epoch_sync_ix(
        fee_payer,
        authority,
        node_address,
        epoch,
        owned_spools,
    );

    ctx.rpc
        .send_instructions(
            &ctx.keypair,
            vec![cu_ix, ix],
        )
        .await
}
