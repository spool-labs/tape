use std::sync::Arc;

use rpc::{Rpc, RpcError};
use store::Store;
use tape_api::compute::VOTE_SNAPSHOT_CU;
use tape_api::instruction::build_vote_snapshot_ix;
use tape_core::bls::BlsSignature;
use tape_core::spooler::GroupIndex;
use tape_core::types::SpoolBitmap;
use tape_crypto::{Hash, tx::Txid};
use tape_protocol::Api;

use crate::context::NodeContext;

pub async fn submit_vote_snapshot<Db: Store, Cluster: Api, Blockchain: Rpc>(
    ctx: &Arc<NodeContext<Db, Cluster, Blockchain>>,
    hash: Hash,
    group: GroupIndex,
    bitmap: SpoolBitmap,
    signature: BlsSignature,
) -> Result<Txid, RpcError> {
    let fee_payer = ctx.pubkey().into();
    let current_epoch = ctx.state().epoch();
    let ix = build_vote_snapshot_ix(fee_payer, current_epoch, hash, group, bitmap, signature);

    ctx.rpc
        .send_instructions_with_compute_unit_limit(ctx.signer(), VOTE_SNAPSHOT_CU, vec![ix])
        .await
}
