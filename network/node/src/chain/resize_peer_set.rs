use std::sync::Arc;

use rpc::{Rpc, RpcError};
use store::Store;
use tape_api::compute::RESIZE_PEER_SET_CU;
use tape_api::instruction::build_resize_peer_set_ix;
use tape_crypto::tx::Txid;
use tape_protocol::Api;

use crate::context::NodeContext;

pub async fn submit_resize_peer_set<Db: Store, Cluster: Api, Blockchain: Rpc>(
    ctx: &Arc<NodeContext<Db, Cluster, Blockchain>>,
) -> Result<Txid, RpcError> {
    let fee_payer = ctx.pubkey().into();
    let ix = build_resize_peer_set_ix(fee_payer, ctx.state().epoch());

    ctx.rpc
        .send_instructions_with_compute_unit_limit(ctx.signer(), RESIZE_PEER_SET_CU, vec![ix])
        .await
}
