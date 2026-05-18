use std::sync::Arc;

use rpc::{Rpc, RpcError};
use store::Store;
use tape_api::compute::COMMIT_EPOCH_CU;
use tape_api::instruction::build_commit_epoch_ix;
use tape_crypto::tx::Txid;
use tape_protocol::Api;

use crate::context::NodeContext;

pub async fn submit_commit_epoch<Db: Store, Cluster: Api, Blockchain: Rpc>(
    ctx: &Arc<NodeContext<Db, Cluster, Blockchain>>,
) -> Result<Txid, RpcError> {
    let fee_payer = ctx.pubkey().into();
    let current_epoch = ctx.state().epoch();
    let ix = build_commit_epoch_ix(fee_payer, current_epoch);

    ctx.rpc
        .send_instructions_with_compute_unit_limit(ctx.signer(), COMMIT_EPOCH_CU, vec![ix])
        .await
}
