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
    context: &Arc<NodeContext<Db, Cluster, Blockchain>>,
) -> Result<Signature, RpcError> {
    let pubkey = context.keypair.pubkey();
    let (node_address, _) = node_pda(pubkey);
    let cu_ix = ComputeBudgetInstruction::set_compute_unit_limit(ADVANCE_POOL_CU);
    let ix = build_advance_pool_ix(pubkey, pubkey, node_address);
    context.rpc.send_instructions(&context.keypair, vec![cu_ix, ix]).await
}
