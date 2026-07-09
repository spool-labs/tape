use std::sync::Arc;

use rpc::{Rpc, RpcError};
use store::Store;
use tape_api::compute::PROPOSE_EVICTION_CU;
use tape_api::instruction::build_propose_eviction_ix;
use tape_core::types::EpochNumber;
use tape_crypto::{Address, tx::Txid};
use tape_protocol::Api;

use crate::context::NodeContext;

pub async fn submit_propose_eviction<Db: Store, Cluster: Api, Blockchain: Rpc>(
    ctx: &Arc<NodeContext<Db, Cluster, Blockchain>>,
    epoch: EpochNumber,
    node: Address,
) -> Result<Txid, RpcError> {
    let fee_payer = ctx.pubkey().into();
    let ix = build_propose_eviction_ix(fee_payer, epoch, node);

    ctx.rpc
        .send_instructions_with_compute_unit_limit(ctx.signer(), PROPOSE_EVICTION_CU, vec![ix])
        .await
}
