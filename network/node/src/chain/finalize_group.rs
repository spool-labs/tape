use std::sync::Arc;

use tape_rpc::{Rpc, RpcError};
use tape_store_api::Store;
use tape_api::instruction::build_finalize_group_ix;
use tape_core::cert::{ASSIGNMENT_TREE_HEIGHT, AssignmentGroupPayload};
use tape_core::types::EpochNumber;
use tape_crypto::{Hash, tx::Txid};
use tape_protocol::Api;

use crate::context::NodeContext;

pub async fn submit_finalize_group<Db: Store, Cluster: Api, Blockchain: Rpc>(
    ctx: &Arc<NodeContext<Db, Cluster, Blockchain>>,
    epoch: EpochNumber,
    payload: AssignmentGroupPayload,
    proof: [Hash; ASSIGNMENT_TREE_HEIGHT],
) -> Result<Txid, RpcError> {
    let fee_payer = ctx.pubkey().into();
    let ix = build_finalize_group_ix(fee_payer, epoch, payload, proof);

    // Sent under the per-instruction default compute unit limit.
    ctx.rpc.simulate_then_send(ctx.signer(), vec![ix]).await
}
