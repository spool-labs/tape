use std::sync::Arc;

use rpc::Rpc;
use rpc_client::RpcError;
use solana_sdk::compute_budget::ComputeBudgetInstruction;
use solana_sdk::signature::{Signature, Signer};
use store::Store;
use tape_api::prelude::{build_certify_snapshot_ix, build_register_snapshot_ix};
use tape_api::program::tapedrive::CommitteeBitmap;
use tape_core::encoding::EncodingProfile;
use tape_core::erasure::SPOOL_GROUP_SIZE;
use tape_core::spooler::SpoolGroup;
use tape_core::types::EpochNumber;
use tape_crypto::Hash;
use tape_store::types::{SnapshotCertResult, SnapshotChunkMeta};

use crate::core::NodeContext;

const SNAPSHOT_REGISTER_CU: u32 = 700_000;
const SNAPSHOT_CERTIFY_CU: u32 = 1_400_000;

pub async fn submit_register<S: Store, R: Rpc>(
    context: &Arc<NodeContext<S, R>>,
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

    let pubkey = context.keypair.pubkey();
    let cu_ix = ComputeBudgetInstruction::set_compute_unit_limit(SNAPSHOT_REGISTER_CU);
    let ix = build_register_snapshot_ix(
        pubkey,
        local_epoch,
        group,
        commitment,
        profile,
        meta.stripe_size,
        meta.stripe_count,
        leaves,
    );

    context
        .rpc
        .send_instructions(&context.keypair, vec![cu_ix, ix])
        .await
}

pub async fn submit_certify<S: Store, R: Rpc>(
    context: &Arc<NodeContext<S, R>>,
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

    let pubkey = context.keypair.pubkey();
    let cu_ix = ComputeBudgetInstruction::set_compute_unit_limit(SNAPSHOT_CERTIFY_CU);
    let ix = build_certify_snapshot_ix(pubkey, local_epoch, signing_epoch, commitment, bitmap, cert.signature);

    context
        .rpc
        .send_instructions(&context.keypair, vec![cu_ix, ix])
        .await
}
