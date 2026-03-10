use std::sync::Arc;

use rpc::{Rpc, RpcError};
use solana_sdk::compute_budget::ComputeBudgetInstruction;
use solana_sdk::signature::Signature;
use store::Store;
use tape_api::compute::{REGISTER_SNAPSHOT_CU, CERTIFY_SNAPSHOT_CU};
use tape_api::prelude::{build_certify_snapshot_ix, build_register_snapshot_ix};
use tape_api::program::tapedrive::CommitteeBitmap;
use tape_core::encoding::EncodingProfile;
use tape_core::erasure::SPOOL_GROUP_SIZE;
use tape_core::spooler::SpoolGroup;
use tape_core::types::EpochNumber;
use tape_crypto::Hash;
use tape_store::types::{SnapshotCertResult, SnapshotChunkMeta};
use tape_protocol::Api;

use crate::core::NodeContext;

pub async fn submit_register<Db: Store, Cluster: Api, Blockchain: Rpc>(
    ctx: &Arc<NodeContext<Db, Cluster, Blockchain>>,
    local_epoch: EpochNumber,
    group: SpoolGroup,
    commitment: Hash,
    meta: &SnapshotChunkMeta,
) -> Result<Signature, RpcError> {
    let mut leaves = [Hash::default(); SPOOL_GROUP_SIZE];
    for (index, hash) in meta.leaves.iter().enumerate().take(SPOOL_GROUP_SIZE) {
        leaves[index] = *hash;
    }

    let profile = EncodingProfile {
        encoding: meta.encoding_type,
        params: meta.encoding_params,
    };

    let fee_payer = ctx.pubkey();
    let cu_ix = ComputeBudgetInstruction::set_compute_unit_limit(
        REGISTER_SNAPSHOT_CU);

    let ix = build_register_snapshot_ix(
        fee_payer,
        local_epoch,
        group,
        commitment,
        profile,
        meta.stripe_size,
        meta.stripe_count,
        leaves,
    );

    ctx.rpc
        .send_instructions(
            &ctx.keypair,
            vec![cu_ix, ix]
    ).await
}

pub async fn submit_certify<Db: Store, Cluster: Api, Blockchain: Rpc>(
    ctx: &Arc<NodeContext<Db, Cluster, Blockchain>>,
    committee_len: usize,
    local_epoch: EpochNumber,
    signing_epoch: EpochNumber,
    commitment: Hash,
    cert: &SnapshotCertResult,
) -> Result<Signature, RpcError> {
    let member_indices = cert
        .member_indices
        .iter()
        .map(|&index| index as usize)
        .collect::<Vec<_>>();
    let bitmap = CommitteeBitmap::from_indices(&member_indices, committee_len);

    let fee_payer = ctx.pubkey();
    let cu_ix = ComputeBudgetInstruction::set_compute_unit_limit(
        CERTIFY_SNAPSHOT_CU);

    let ix = build_certify_snapshot_ix(
        fee_payer,
        local_epoch,
        signing_epoch,
        commitment,
        bitmap,
        cert.signature,
    );

    ctx.rpc
        .send_instructions(
            &ctx.keypair,
            vec![cu_ix, ix]
    ).await
}
