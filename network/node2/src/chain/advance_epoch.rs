use std::sync::Arc;

use rpc::{Rpc, RpcError};
use solana_sdk::compute_budget::ComputeBudgetInstruction;
use solana_sdk::signature::Signature;
use store::Store;
use tape_api::compute::ADVANCE_EPOCH_CU;
use tape_api::instruction::build_advance_epoch_ix;
use tape_protocol::Api;

use crate::core::context::NodeContext;

pub async fn submit_advance_epoch<Db: Store, Cluster: Api, Blockchain: Rpc>(
    ctx: &Arc<NodeContext<Db, Cluster, Blockchain>>,
) -> Result<Signature, RpcError> {
    let fee_payer = ctx.pubkey();
    let authority = ctx.pubkey();

    let cu_ix = ComputeBudgetInstruction::set_compute_unit_limit(
        ADVANCE_EPOCH_CU);

    let ix = build_advance_epoch_ix(fee_payer, authority);

    ctx.rpc
        .send_instructions(
            ctx.signer(),
            vec![cu_ix, ix]
    ).await
}
