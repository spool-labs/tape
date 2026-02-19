use std::sync::Arc;

use rpc::Rpc;
use rpc_client::RpcError;
use solana_sdk::signature::{Signature, Signer};
use store::Store;
use tape_api::instruction::build_advance_pool_ix;
use tape_api::program::tapedrive::node_pda;

use crate::core::NodeContext;

pub async fn submit_advance_pool<S: Store, R: Rpc>(
    context: &Arc<NodeContext<S, R>>,
) -> Result<Signature, RpcError> {
    let pubkey = context.keypair.pubkey();
    let (node_address, _) = node_pda(pubkey);
    let ix = build_advance_pool_ix(pubkey, pubkey, node_address);
    context.rpc.send_instructions(&context.keypair, vec![ix]).await
}
