use std::sync::Arc;

use rpc::{Rpc, RpcError};
use tape_protocol::Api;
use solana_sdk::compute_budget::ComputeBudgetInstruction;
use solana_sdk::signature::{Signature, Signer};
use store::Store;
use tape_api::compute::ADVANCE_POOL_CU;
use tape_api::instruction::build_advance_pool_ix;
use tape_api::program::tapedrive::node_pda;

use crate::core::NodeContext;

pub async fn submit_advance_pool<Db: Store, Cluster: Api, Blockchain: Rpc>(
    ctx: &Arc<NodeContext<Db, Cluster, Blockchain>>,
) -> Result<Signature, RpcError> {

    let fee_payer = ctx.pubkey();
    let authority = ctx.pubkey();

    let (node_address, _) = node_pda(authority);

    let cu_ix = ComputeBudgetInstruction::set_compute_unit_limit(
        ADVANCE_POOL_CU);

    let ix = build_advance_pool_ix(fee_payer, authority, node_address);

    ctx.rpc
        .send_instructions(
            &ctx.keypair,
            vec![cu_ix, ix]
    ).await
}
