use std::sync::Arc;

use rpc::{Rpc, RpcError};
use store::Store;
use tape_api::compute::VOTE_ASSIGNMENT_CU;
use tape_api::instruction::build_vote_assignment_ix;
use tape_core::bls::BlsSignature;
use tape_core::spooler::GroupIndex;
use tape_core::types::{EpochNumber, SpoolBitmap};
use tape_crypto::{Hash, tx::Txid};
use tape_protocol::Api;

use crate::context::NodeContext;

pub async fn submit_vote_assignment<Db: Store, Cluster: Api, Blockchain: Rpc>(
    ctx: &Arc<NodeContext<Db, Cluster, Blockchain>>,
    epoch: EpochNumber,
    hash: Hash,
    group: GroupIndex,
    bitmap: SpoolBitmap,
    signature: BlsSignature,
) -> Result<Txid, RpcError> {
    let fee_payer = ctx.pubkey().into();
    let ix = build_vote_assignment_ix(fee_payer, epoch, hash, group, bitmap, signature);

    ctx.rpc
        .send_instructions_with_compute_unit_limit(ctx.signer(), VOTE_ASSIGNMENT_CU, vec![ix])
        .await
}
