//! Inconsistency detection and proof submission.
//!
//! When full recovery detects that re-encoded slices don't match the on-chain
//! commitment, the node fans out to spool group peers for independent
//! verification, collects BLS attestations, aggregates, and submits an
//! InvalidateTrack instruction on-chain.

use std::sync::Arc;

use futures::stream::{self, StreamExt};
use solana_sdk::signer::Signer;
use store::Store;
use tape_api::instruction::build_invalidate_track_ix;
use tape_api::program::tapedrive::{epoch_pda, system_pda, CommitteeBitmap};
use tape_core::bft::is_supermajority;
use tape_core::bls::BlsSignature;
use tape_crypto::bls12254::min_sig::G1CompressedPoint;
use tape_core::cert::invalidate::InvalidateMessage;
use tape_core::erasure::SPOOL_GROUP_SIZE;
use tape_crypto::Hash;
use tape_node_api::InconsistencyRequest;
use tape_slicer::merkle_helpers::blob_merkle_root;
use tape_store::types::{Pubkey, TrackInfo};
use tracing::{debug, info, warn};

use crate::core::context::NodeContext;

use crate::features::recovery::RecoveryError;
use crate::features::recovery::helpers::resolve_group_helpers;

/// Maximum concurrent inconsistency attestation requests.
const ATTESTATION_CONCURRENCY: usize = 8;

/// Result of an inconsistency check.
#[derive(Debug)]
pub enum InconsistencyResult {
    /// Slices are consistent with on-chain commitment.
    Consistent,
    /// Inconsistency detected but proof generation not yet implemented.
    DetectedButUnproven {
        track: Pubkey,
        expected_root: Hash,
        computed_root: Hash,
    },
}

/// Check slice consistency against an on-chain commitment.
///
/// Computes the merkle root of the re-encoded slices and compares it
/// against the on-chain commitment hash. Returns `DetectedButUnproven`
/// if they differ (BLS attestation not yet implemented).
pub fn check_consistency(
    track: Pubkey,
    commitment: &Hash,
    reencoded_slices: &[Vec<u8>],
) -> InconsistencyResult {
    let computed_root = blob_merkle_root(reencoded_slices);
    if computed_root != *commitment {
        InconsistencyResult::DetectedButUnproven {
            track,
            expected_root: *commitment,
            computed_root,
        }
    } else {
        InconsistencyResult::Consistent
    }
}

/// Fan out to spool group peers, collect BLS attestations, aggregate,
/// and submit an InvalidateTrack instruction on-chain.
pub async fn handle_inconsistency<S: Store>(
    ctx: Arc<NodeContext<S>>,
    track_address: Pubkey,
    computed_root: Hash,
    track_info: &TrackInfo,
) -> Result<(), RecoveryError> {
    let spool_group = track_info.spool_group;
    let start = tape_core::erasure::group_start(spool_group);
    let insecure = ctx.config.insecure;

    // Resolve spool group helpers
    let helpers = resolve_group_helpers(&ctx, start, insecure)?;

    if helpers.is_empty() {
        return Err(RecoveryError::NotEnoughHelpers {
            needed: 1,
            available: 0,
        });
    }

    let track_id = track_address.to_string();
    let request = InconsistencyRequest { computed_root };

    // Fan out attestation requests to all group helpers
    let results: Vec<_> = stream::iter(helpers.into_iter())
        .map(|helper| {
            let tid = track_id.clone();
            let req = request.clone();
            let position = helper.position;
            async move {
                let result = helper.client.post_inconsistency(&tid, &req).await;
                (position, result)
            }
        })
        .buffer_unordered(ATTESTATION_CONCURRENCY)
        .collect()
        .await;

    // Collect successful attestations
    let mut signatures = Vec::new();
    let mut member_indices = Vec::new();

    // Include our own signature
    let epoch = ctx.control_plane.current_epoch();
    let invalidate_message = InvalidateMessage::new(
        epoch,
        track_address.to_bytes(),
        computed_root.0,
    );
    let message = invalidate_message.to_bytes();

    let our_sig = ctx
        .bls_keypair
        .sign(&message)
        .map_err(|e| RecoveryError::RepairFailed(format!("BLS signing failed: {:?}", e)))?;

    let system = ctx.control_plane.get_system();
    let node_id = ctx.control_plane.our_node_id();
    if let Some(our_index) = system.committee.index_of(&node_id) {
        signatures.push(our_sig);
        member_indices.push(our_index);
    }

    // Collect peer attestations
    for (position, result) in results {
        match result {
            Ok(resp) => {
                signatures.push(BlsSignature(G1CompressedPoint(resp.signature)));
                member_indices.push(resp.member_index as usize);
                debug!(position, node_id = resp.node_id, "collected inconsistency attestation");
            }
            Err(e) => {
                warn!(position, error = %e, "inconsistency attestation request failed");
            }
        }
    }

    // Check spool-group-weighted supermajority
    let committee_size = system.committee.size();
    let bitmap = CommitteeBitmap::from_indices(&member_indices, committee_size);
    let weight = system.spools.group_weight(spool_group, &bitmap);

    if !is_supermajority(weight, SPOOL_GROUP_SIZE as u64) {
        return Err(RecoveryError::RepairFailed(format!(
            "insufficient attestation weight: {weight}/{SPOOL_GROUP_SIZE}"
        )));
    }

    // Aggregate BLS signatures
    let agg_sig = BlsSignature::aggregate(&signatures)
        .map_err(|e| RecoveryError::RepairFailed(format!("BLS aggregation failed: {:?}", e)))?;

    // Build and submit InvalidateTrack instruction
    let (system_address, _) = system_pda();
    let (epoch_address, _) = epoch_pda();

    let ix = build_invalidate_track_ix(
        ctx.keypair.pubkey().into(),
        system_address.into(),
        epoch_address.into(),
        track_info.tape_address.into(),
        track_address.into(),
        bitmap,
        agg_sig,
        computed_root,
    );

    ctx.rpc
        .send_instructions(&ctx.keypair, vec![ix])
        .await
        .map_err(|e| RecoveryError::RepairFailed(format!("InvalidateTrack submission failed: {}", e)))?;

    info!(
        track = %track_address,
        signers = signatures.len(),
        weight,
        "InvalidateTrack submitted successfully"
    );

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tape_slicer::merkle_helpers::blob_merkle_root;

    #[test]
    fn matching_slices_are_consistent() {
        let slices: Vec<Vec<u8>> = (0..20).map(|i| vec![i; 100]).collect();
        let root = blob_merkle_root(&slices);

        let result = check_consistency(Pubkey([0u8; 32]), &root, &slices);
        assert!(matches!(result, InconsistencyResult::Consistent));
    }

    #[test]
    fn mismatched_slices_detected() {
        let slices: Vec<Vec<u8>> = (0..20).map(|i| vec![i; 100]).collect();
        let wrong_root = Hash::default();

        let result = check_consistency(Pubkey([1u8; 32]), &wrong_root, &slices);
        match result {
            InconsistencyResult::DetectedButUnproven {
                expected_root,
                computed_root,
                ..
            } => {
                assert_eq!(expected_root, wrong_root);
                assert_eq!(computed_root, blob_merkle_root(&slices));
            }
            InconsistencyResult::Consistent => panic!("expected inconsistency"),
        }
    }
}
