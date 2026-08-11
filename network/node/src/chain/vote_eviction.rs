use std::sync::Arc;

use rpc::{Rpc, RpcError};
use store::Store;
use tape_api::compute::VOTE_EVICTION_CU;
use tape_api::instruction::build_vote_eviction_ix;
use tape_core::bls::BlsSignature;
use tape_core::spooler::GroupIndex;
use tape_core::types::{EpochNumber, SpoolBitmap};
use tape_crypto::{Address, tx::Txid};
use tape_protocol::Api;

use crate::context::NodeContext;

pub async fn submit_vote_eviction<Db: Store, Cluster: Api, Blockchain: Rpc>(
    ctx: &Arc<NodeContext<Db, Cluster, Blockchain>>,
    epoch: EpochNumber,
    node: Address,
    group: GroupIndex,
    bitmap: SpoolBitmap,
    signature: BlsSignature,
) -> Result<Txid, RpcError> {
    let fee_payer = ctx.pubkey().into();
    let ix = build_vote_eviction_ix(fee_payer, epoch, node, group, bitmap, signature);

    ctx.rpc
        .send_instructions_with_compute_unit_limit(ctx.signer(), VOTE_EVICTION_CU, vec![ix])
        .await
}
