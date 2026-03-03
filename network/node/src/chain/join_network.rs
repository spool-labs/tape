use std::sync::Arc;

use rpc::Rpc;
use rpc_client::RpcError;
use solana_sdk::compute_budget::ComputeBudgetInstruction;
use solana_sdk::signature::{Signature, Signer};
use store::Store;
use tape_api::compute::JOIN_NETWORK_CU;
use tape_api::instruction::build_join_network_ix;
use tape_api::program::tapedrive::node_pda;

use crate::core::NodeContext;

pub async fn submit_join_network<S: Store, R: Rpc>(
    context: &Arc<NodeContext<S, R>>,
) -> Result<Signature, RpcError> {
    let pubkey = context.keypair.pubkey();
    let (node_address, _) = node_pda(pubkey);
    let cu_ix = ComputeBudgetInstruction::set_compute_unit_limit(JOIN_NETWORK_CU);
    let ix = build_join_network_ix(pubkey, pubkey, node_address);
    context.rpc.send_instructions(&context.keypair, vec![cu_ix, ix]).await
}
