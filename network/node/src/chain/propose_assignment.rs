use std::sync::Arc;

use rpc::{Rpc, RpcError};
use store::Store;
use tape_api::compute::PROPOSE_ASSIGNMENT_CU;
use tape_api::instruction::build_propose_assignment_ix;
use tape_core::types::EpochNumber;
use tape_crypto::{Hash, tx::Txid};
use tape_protocol::Api;

use crate::context::NodeContext;

pub async fn submit_propose_assignment<Db: Store, Cluster: Api, Blockchain: Rpc>(
    ctx: &Arc<NodeContext<Db, Cluster, Blockchain>>,
    epoch: EpochNumber,
    hash: Hash,
) -> Result<Txid, RpcError> {
    let fee_payer = ctx.pubkey().into();
    let ix = build_propose_assignment_ix(fee_payer, epoch, hash);

    ctx.rpc
        .send_instructions_with_compute_unit_limit(ctx.signer(), PROPOSE_ASSIGNMENT_CU, vec![ix])
        .await
}
