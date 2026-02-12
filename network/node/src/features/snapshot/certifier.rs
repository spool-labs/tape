//! Snapshot certification coordinator.
//!
//! For each of the 50 snapshot chunks, collects BLS signatures from
//! spool group members and submits RegisterSnapshot + CertifySnapshot on-chain.

use std::collections::HashMap;
use std::sync::Arc;

use solana_sdk::compute_budget::ComputeBudgetInstruction;
use solana_sdk::signer::Signer;
use store::Store;
use tape_api::errors::TapeError;
use tape_api::instruction::{
    build_certify_snapshot_ix, build_register_snapshot_ix,
};
use tape_api::program::tapedrive::CommitteeBitmap;
use tape_core::bft::is_supermajority;
use tape_core::bls::BlsSignature;
use tape_core::encoding::EncodingProfile;
use tape_core::erasure::{
    group_start, spool_for_slice, SPOOL_GROUP_COUNT, SPOOL_GROUP_SIZE,
};
use tape_core::types::{ChunkIndex, EpochNumber};
use tape_crypto::merkle::hash_leaf;
use tape_node_client::{NodeClientBuilder, SignResponse};
use tape_store::ops::CommitteeOps;
use tracing::{debug, info, warn};

use super::builder::{SnapshotBuildResult, SnapshotError};
use crate::core::context::NodeContext;

/// Certify all 50 snapshot tracks for a completed epoch.
///
/// For each chunk (one per spool group), this function:
/// 1. Collects BLS signatures from spool group members
/// 2. Submits RegisterSnapshot on-chain
/// 3. Submits CertifySnapshot on-chain
///
/// Handles "already registered" gracefully (another node won the race).
pub async fn certify_snapshot_tracks<S: Store>(
    ctx: &Arc<NodeContext<S>>,
    epoch: EpochNumber,
    build_result: &SnapshotBuildResult,
) -> Result<(), SnapshotError> {
    info!(
        epoch = epoch.as_u64(),
        chunks = SPOOL_GROUP_COUNT,
        "Starting snapshot certification"
    );

    // Resolve committee members and their network addresses
    let committee = ctx
        .storage
        .store
        .get_committee(epoch)
        .map_err(SnapshotError::Store)?
        .ok_or_else(|| SnapshotError::Encode("no committee found".into()))?;

    // Build spool → member index lookup
    let mut spool_to_member: HashMap<u16, usize> = HashMap::new();
    for (idx, member) in committee.iter().enumerate() {
        for &spool in &member.spools {
            spool_to_member.insert(spool, idx);
        }
    }

    let fee_payer = ctx.keypair.pubkey();
    let insecure = ctx.config.insecure;

    for chunk_index in 0..SPOOL_GROUP_COUNT {
        let commitment = build_result.commitments[chunk_index];

        // Determine which committee members own spools in this group
        let mut group_members: HashMap<usize, Vec<u16>> = HashMap::new();
        for position in 0..SPOOL_GROUP_SIZE {
            let spool = spool_for_slice(chunk_index as u64, position);
            if let Some(&member_idx) = spool_to_member.get(&spool) {
                group_members.entry(member_idx).or_default().push(spool);
            }
        }

        // Collect BLS signatures from group members
        let epoch_val = epoch.as_u64();
        let chunk_idx = ChunkIndex(chunk_index as u64);
        let mut signatures: Vec<(u8, SignResponse)> = Vec::new();
        let mut failures = 0usize;

        for (&member_idx, _spools) in &group_members {
            let member = &committee[member_idx];
            let addr = match member.network_address.to_socket_addr() {
                Ok(a) => a,
                Err(e) => {
                    warn!(
                        member_idx = member_idx,
                        chunk_index = chunk_index,
                        "Failed to resolve network address: {e}"
                    );
                    failures += 1;
                    continue;
                }
            };

            let client = match NodeClientBuilder::new()
                .accept_invalid_certs(insecure)
                .build(&addr.to_string())
            {
                Ok(c) => c,
                Err(e) => {
                    warn!(member_idx = member_idx, "Failed to build client: {e}");
                    failures += 1;
                    continue;
                }
            };

            match client.get_snapshot_signature(epoch_val, chunk_index as u64).await {
                Ok(resp) => {
                    signatures.push((member_idx as u8, resp));
                }
                Err(e) => {
                    debug!(
                        member_idx = member_idx,
                        chunk_index = chunk_index,
                        error = %e,
                        "Failed to get snapshot signature"
                    );
                    failures += 1;
                }
            }

            // Early exit: check if we have spool-weighted supermajority
            let member_indices: Vec<usize> =
                signatures.iter().map(|(idx, _)| *idx as usize).collect();
            let bitmap =
                CommitteeBitmap::from_indices(&member_indices, committee.len());
            let start = group_start(chunk_index as u64);
            let weight = (0..SPOOL_GROUP_SIZE)
                .filter(|&pos| {
                    let spool = start + pos as u16;
                    if let Some(&m) = spool_to_member.get(&spool) {
                        bitmap.is_set(m)
                    } else {
                        false
                    }
                })
                .count() as u64;

            if is_supermajority(weight, SPOOL_GROUP_SIZE as u64) {
                debug!(
                    chunk_index = chunk_index,
                    signatures = signatures.len(),
                    weight = weight,
                    "Early exit: spool group supermajority reached"
                );
                break;
            }
        }

        if signatures.is_empty() {
            warn!(
                chunk_index = chunk_index,
                failures = failures,
                "No signatures collected for chunk, skipping"
            );
            continue;
        }

        // Final supermajority check
        let member_indices: Vec<usize> =
            signatures.iter().map(|(idx, _)| *idx as usize).collect();
        let bitmap = CommitteeBitmap::from_indices(&member_indices, committee.len());
        let start = group_start(chunk_index as u64);
        let final_weight = (0..SPOOL_GROUP_SIZE)
            .filter(|&pos| {
                let spool = start + pos as u16;
                if let Some(&m) = spool_to_member.get(&spool) {
                    bitmap.is_set(m)
                } else {
                    false
                }
            })
            .count() as u64;

        if !is_supermajority(final_weight, SPOOL_GROUP_SIZE as u64) {
            warn!(
                chunk_index = chunk_index,
                weight = final_weight,
                signatures = signatures.len(),
                "Insufficient spool weight for supermajority, skipping"
            );
            continue;
        }

        // Aggregate BLS signatures
        let bls_sigs: Vec<BlsSignature> = signatures
            .iter()
            .map(|(_, resp)| {
                BlsSignature(tape_crypto::bls12254::min_sig::G1CompressedPoint(
                    resp.signature,
                ))
            })
            .collect();

        let aggregated = match BlsSignature::aggregate(&bls_sigs) {
            Ok(sig) => sig,
            Err(e) => {
                warn!(chunk_index = chunk_index, error = ?e, "BLS aggregation failed");
                continue;
            }
        };

        // Get leaf hashes for RegisterSnapshot
        let leaves: [tape_crypto::Hash; SPOOL_GROUP_SIZE] =
            match &build_result.group_slices[chunk_index] {
                Some(slices) => {
                    let mut arr = [tape_crypto::Hash::default(); SPOOL_GROUP_SIZE];
                    for (i, slice) in slices.iter().enumerate() {
                        arr[i] = hash_leaf(slice);
                    }
                    arr
                }
                None => {
                    // We don't own this group — another node will register it
                    debug!(
                        chunk_index = chunk_index,
                        "We don't own slices in this group, skipping registration"
                    );
                    continue;
                }
            };

        let profile = EncodingProfile::clay_default();

        // Submit RegisterSnapshot on-chain
        let chunk_size = build_result.chunk_sizes[chunk_index] as u64;
        let register_ix = build_register_snapshot_ix(
            fee_payer,
            epoch,
            chunk_idx,
            commitment,
            profile,
            chunk_size,
            1, // stripe_count
            leaves,
        );

        match ctx.rpc.send_instructions(&ctx.keypair, vec![register_ix]).await {
            Ok(sig) => {
                debug!(
                    chunk_index = chunk_index,
                    signature = %sig,
                    "RegisterSnapshot submitted"
                );
            }
            Err(e) => {
                let err_str = e.to_string();
                if err_str.contains("already in use")
                    || err_str.contains("already initialized")
                    || err_str.contains("uninitialized account")
                {
                    debug!(chunk_index = chunk_index, "Snapshot track already registered");
                } else {
                    warn!(chunk_index = chunk_index, error = %e, "RegisterSnapshot failed");
                    continue;
                }
            }
        }

        // Submit CertifySnapshot on-chain (BLS verification needs extra compute)
        let certify_ix = build_certify_snapshot_ix(
            fee_payer,
            epoch,
            chunk_idx,
            bitmap,
            aggregated,
        );

        let compute_ix = ComputeBudgetInstruction::set_compute_unit_limit(400_000);
        match ctx.rpc.send_instructions(&ctx.keypair, vec![compute_ix, certify_ix]).await {
            Ok(sig) => {
                debug!(
                    chunk_index = chunk_index,
                    signature = %sig,
                    "CertifySnapshot submitted"
                );
            }
            Err(e) => {
                let err_str = e.to_string();
                if let Some(tape_err) = TapeError::from_error_string(&err_str) {
                    if tape_err.is_already_done() {
                        debug!(chunk_index = chunk_index, "Snapshot track already certified");
                    } else {
                        warn!(chunk_index = chunk_index, error = %e, "CertifySnapshot failed");
                    }
                } else {
                    warn!(chunk_index = chunk_index, error = %e, "CertifySnapshot failed");
                }
            }
        }
    }

    info!(
        epoch = epoch.as_u64(),
        "Snapshot certification complete"
    );

    Ok(())
}
