use std::sync::Arc;

use rpc::Rpc;
use rpc_client::RpcError;
use solana_sdk::compute_budget::ComputeBudgetInstruction;
use solana_sdk::signature::{Signature, Signer};
use store::Store;
use tape_api::compute::ADVANCE_EPOCH_CU;
use tape_api::instruction::build_advance_epoch_ix;

use crate::core::NodeContext;

pub async fn submit_advance_epoch<S: Store, R: Rpc>(
    context: &Arc<NodeContext<S, R>>,
) -> Result<Signature, RpcError> {
    let pubkey = context.keypair.pubkey();
    let cu_ix = ComputeBudgetInstruction::set_compute_unit_limit(ADVANCE_EPOCH_CU);
    let ix = build_advance_epoch_ix(pubkey, pubkey);
    context
        .rpc
        .send_instructions(&context.keypair, vec![cu_ix, ix])
        .await
}
