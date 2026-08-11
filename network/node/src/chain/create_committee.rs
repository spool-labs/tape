use std::sync::Arc;

use rpc::{Rpc, RpcError};
use store::Store;
use tape_api::compute::CREATE_COMMITTEE_CU;
use tape_api::instruction::build_create_committee_ix;
use tape_core::types::EpochNumber;
use tape_crypto::tx::Txid;
use tape_protocol::Api;

use crate::context::NodeContext;

pub async fn submit_create_committee<Db: Store, Cluster: Api, Blockchain: Rpc>(
    ctx: &Arc<NodeContext<Db, Cluster, Blockchain>>,
    epoch: EpochNumber,
) -> Result<Txid, RpcError> {
    let fee_payer = ctx.pubkey().into();
    let ix = build_create_committee_ix(fee_payer, epoch);

    ctx.rpc
        .send_instructions_with_compute_unit_limit(ctx.signer(), CREATE_COMMITTEE_CU, vec![ix])
        .await
}
