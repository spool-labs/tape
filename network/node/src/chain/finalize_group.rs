use std::sync::Arc;

use rpc::{Rpc, RpcError};
use store::Store;
use tape_api::compute::FINALIZE_GROUP_CU;
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

    ctx.rpc
        .send_instructions_with_compute_unit_limit(ctx.signer(), FINALIZE_GROUP_CU, vec![ix])
        .await
}
