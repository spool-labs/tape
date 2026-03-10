use std::sync::Arc;

use rpc::{Rpc, RpcError};
use tape_protocol::Api;
use solana_sdk::compute_budget::ComputeBudgetInstruction;
use solana_sdk::signature::{Signature, Signer};
use store::Store;
use tape_api::compute::SYNC_EPOCH_CU;
use tape_api::instruction::build_epoch_sync_ix;
use tape_api::program::tapedrive::node_pda;
use tape_core::types::EpochNumber;

use crate::core::NodeContext;

pub async fn submit_sync_epoch<Db: Store, Cluster: Api, Blockchain: Rpc>(
    context: &Arc<NodeContext<Db, Cluster, Blockchain>>,
    epoch: EpochNumber,
    owned_spools: &[u16],
) -> Result<Signature, RpcError> {
    let pubkey = context.keypair.pubkey();
    let (node_address, _) = node_pda(pubkey);
    let cu_ix = ComputeBudgetInstruction::set_compute_unit_limit(SYNC_EPOCH_CU);
    let ix = build_epoch_sync_ix(pubkey, pubkey, node_address, epoch, owned_spools);
    context.rpc.send_instructions(&context.keypair, vec![cu_ix, ix]).await
}
