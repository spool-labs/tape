//! Snapshot certification coordinator.
//!
//! For each of the 50 snapshot chunks, collects BLS signatures from
//! spool group members and submits RegisterSnapshot + CertifySnapshot on-chain.
//!
//! Chunks can be registered in any order (PDA is keyed by commitment hash).
//! The certifier retries failed chunks with backoff when peers haven't
//! finished building their snapshots yet.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use solana_sdk::compute_budget::ComputeBudgetInstruction;
use solana_sdk::signer::Signer;
use store::Store;
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
use tape_core::types::EpochNumber;
use tape_crypto::merkle::hash_leaf;
use tape_node_client::{NodeClientBuilder, SignResponse};
use tape_store::ops::CommitteeOps;
use tape_store::types::NodeInfo;
use tracing::{debug, info, warn};

use super::builder::{SnapshotBuildResult, SnapshotError};
use crate::core::context::NodeContext;

/// Maximum retry rounds for chunks that fail due to insufficient weight.
const MAX_RETRY_ROUNDS: u32 = 8;

/// Initial delay between retry rounds (doubles each round, capped at MAX_RETRY_DELAY).
const INITIAL_RETRY_DELAY: Duration = Duration::from_secs(2);

/// Maximum delay between retry rounds.
const MAX_RETRY_DELAY: Duration = Duration::from_secs(30);

/// Certify all 50 snapshot tracks for a completed epoch.
///
/// For each chunk (one per spool group), this function:
/// 1. Collects BLS signatures from spool group members
/// 2. Submits RegisterSnapshot on-chain
/// 3. Submits CertifySnapshot on-chain
///
/// Chunks are retried with exponential backoff when peers haven't finished building yet.
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

    // Track which chunks still need certification.
    let mut pending: Vec<usize> = (0..SPOOL_GROUP_COUNT).collect();
    let mut round = 0u32;
    let mut delay = INITIAL_RETRY_DELAY;

    while !pending.is_empty() && round <= MAX_RETRY_ROUNDS {
        if round > 0 {
            info!(
                epoch = epoch.as_u64(),
                round = round,
                remaining = pending.len(),
                delay_secs = delay.as_secs(),
                "Retrying uncertified chunks"
            );
            tokio::time::sleep(delay).await;
            delay = (delay * 2).min(MAX_RETRY_DELAY);
        }

        let mut still_pending = Vec::new();

        for &chunk_index in &pending {
            let result = try_certify_chunk(
                ctx,
                epoch,
                build_result,
                &committee,
                &spool_to_member,
                fee_payer,
                insecure,
                chunk_index,
            )
            .await;

            match result {
                ChunkResult::Certified | ChunkResult::AlreadyDone => {}
                ChunkResult::InsufficientWeight | ChunkResult::Failed => {
                    still_pending.push(chunk_index);
                }
            }
        }

        // If no progress was made this round, increment retry counter
        if still_pending.len() == pending.len() {
            round += 1;
        } else {
            // Progress — reset retry counter but keep the delay
            round = 0;
        }

        pending = still_pending;
    }

    if !pending.is_empty() {
        warn!(
            epoch = epoch.as_u64(),
            uncertified = pending.len(),
            chunks = ?pending,
            "Snapshot certification incomplete after retries"
        );
    }

    info!(
        epoch = epoch.as_u64(),
        certified = SPOOL_GROUP_COUNT - pending.len(),
        "Snapshot certification complete"
    );

    Ok(())
}

/// Result of attempting to certify a single chunk.
enum ChunkResult {
    /// Successfully registered and certified on-chain.
    Certified,
    /// Another node already handled this chunk.
    AlreadyDone,
    /// Not enough spool weight (peers likely still building).
    InsufficientWeight,
    /// Registration or certification failed (retriable).
    Failed,
}

/// Attempt to certify a single snapshot chunk.
async fn try_certify_chunk<S: Store>(
    ctx: &Arc<NodeContext<S>>,
    epoch: EpochNumber,
    build_result: &SnapshotBuildResult,
    committee: &[NodeInfo],
    spool_to_member: &HashMap<u16, usize>,
    fee_payer: solana_sdk::pubkey::Pubkey,
    insecure: bool,
    chunk_index: usize,
) -> ChunkResult {
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
    let mut signatures: Vec<(u8, SignResponse)> = Vec::new();
    let mut failures = 0usize;

    for (&member_idx, _spools) in &group_members {
        let member = &committee[member_idx];
        let addr: std::net::SocketAddr = match member.network_address.to_socket_addr() {
            Ok(a) => a,
            Err(e) => {
                debug!(
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
                debug!(member_idx = member_idx, "Failed to build client: {e}");
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
        let weight = compute_spool_weight(&signatures, spool_to_member, committee.len(), chunk_index);
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
        debug!(
            chunk_index = chunk_index,
            failures = failures,
            "No signatures collected for chunk"
        );
        return ChunkResult::InsufficientWeight;
    }

    // Final supermajority check
    let final_weight = compute_spool_weight(&signatures, spool_to_member, committee.len(), chunk_index);

    if !is_supermajority(final_weight, SPOOL_GROUP_SIZE as u64) {
        debug!(
            chunk_index = chunk_index,
            weight = final_weight,
            signatures = signatures.len(),
            "Insufficient spool weight for supermajority"
        );
        return ChunkResult::InsufficientWeight;
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
            return ChunkResult::Failed;
        }
    };

    let member_indices: Vec<usize> =
        signatures.iter().map(|(idx, _)| *idx as usize).collect();
    let bitmap = CommitteeBitmap::from_indices(&member_indices, committee.len());

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
                // We don't own this group — another node will register it.
                // Treat as "already done" so we don't block the retry loop.
                debug!(
                    chunk_index = chunk_index,
                    "We don't own slices in this group, skipping registration"
                );
                return ChunkResult::AlreadyDone;
            }
        };

    let profile = EncodingProfile::clay_default();

    // Submit RegisterSnapshot on-chain
    let chunk_size = build_result.chunk_sizes[chunk_index] as u64;
    let register_ix = build_register_snapshot_ix(
        fee_payer,
        epoch,
        chunk_index as u64,
        commitment,
        profile,
        chunk_size,
        1, // stripe_count
        leaves,
    );

    debug!(
        chunk_index = chunk_index,
        epoch = epoch.as_u64(),
        commitment = %hex::encode(commitment.0),
        "Submitting RegisterSnapshot"
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
                return ChunkResult::Failed;
            }
        }
    }

    // Submit CertifySnapshot on-chain (BLS verification needs extra compute)
    let certify_ix = build_certify_snapshot_ix(
        fee_payer,
        epoch,
        commitment,
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
            if let Some(tape_err) = tape_api::errors::TapeError::from_error_string(&err_str) {
                if tape_err.is_already_done() {
                    debug!(chunk_index = chunk_index, "Snapshot track already certified");
                    return ChunkResult::AlreadyDone;
                }
            }
            warn!(chunk_index = chunk_index, error = %e, "CertifySnapshot failed");
            return ChunkResult::Failed;
        }
    }

    ChunkResult::Certified
}

/// Compute spool weight for collected signatures in a given chunk's spool group.
fn compute_spool_weight(
    signatures: &[(u8, SignResponse)],
    spool_to_member: &HashMap<u16, usize>,
    committee_len: usize,
    chunk_index: usize,
) -> u64 {
    let member_indices: Vec<usize> =
        signatures.iter().map(|(idx, _)| *idx as usize).collect();
    let bitmap = CommitteeBitmap::from_indices(&member_indices, committee_len);
    let start = group_start(chunk_index as u64);
    (0..SPOOL_GROUP_SIZE)
        .filter(|&pos| {
            let spool = start + pos as u16;
            if let Some(&m) = spool_to_member.get(&spool) {
                bitmap.is_set(m)
            } else {
                false
            }
        })
        .count() as u64
}
