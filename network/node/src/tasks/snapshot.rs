//! Snapshot tasks — build, certify, register, certify on-chain.

use std::collections::HashSet;
use std::sync::Arc;

use rpc::Rpc;
use store::Store;
use tape_core::encoding::ClayParams;
use tape_core::erasure::{group_for_spool, spool_for_slice, SPOOL_GROUP_COUNT, SPOOL_GROUP_SIZE};
use tape_core::snapshot::SnapshotLog;
use tape_core::types::{ChunkIndex, EpochNumber};
use tape_crypto::merkle::hash_leaf;
use tape_slicer::{blob_merkle_root, ClayCoder, ErasureCoder, OuterCoder, Slicer, DEFAULT_K_OUTER};
use tape_store::ops::{CommitteeOps, EventLogOps, MetaOps, SliceOps, SpoolOps};
use tape_node_client::{RetryConfig, with_retry};
use tape_store::types::{NodeInfo, Pubkey, SnapshotChunkMeta};
use tokio_util::sync::CancellationToken;

use crate::core::NodeContext;
use crate::supervisor::TaskOutcome;

/// Bootstrap from a snapshot: download slices from peers, decode, replay.
pub async fn run_bootstrap<S: Store, R: Rpc>(
    context: Arc<NodeContext<S, R>>,
    cancel: CancellationToken,
) -> TaskOutcome {
    let current = match context.store.get_current_epoch() {
        Ok(Some(e)) => e,
        Ok(None) => return TaskOutcome::Retryable("no current epoch".into()),
        Err(e) => return TaskOutcome::Retryable(format!("read epoch: {e}")),
    };

    if current.0 < 2 {
        return TaskOutcome::Success;
    }

    let target = EpochNumber(current.0 - 1);

    // Idempotent: skip if we already have synced past this snapshot
    if let Ok(Some(cursor)) = context.store.get_sync_cursor() {
        if cursor.0 > 0 {
            return TaskOutcome::Success;
        }
    }

    // Need committee to find peers
    let committee: Vec<NodeInfo> = match context.store.get_committee(current) {
        Ok(Some(c)) => c,
        Ok(None) => return TaskOutcome::Retryable("no committee".into()),
        Err(e) => return TaskOutcome::Retryable(format!("read committee: {e}")),
    };

    if committee.is_empty() {
        return TaskOutcome::Retryable("empty committee".into());
    }

    // Pick a peer and fetch commitments
    let commitments = {
        let mut fetched = None;
        for member in &committee {
            let addr = match member.network_address.to_socket_addr() {
                Ok(a) => a,
                Err(_) => continue,
            };
            let client = match tape_node_client::NodeClientBuilder::new()
                .build(&addr.to_string())
            {
                Ok(c) => c,
                Err(_) => continue,
            };
            match with_retry(&RetryConfig::fast(), || client.get_snapshot_commitments(target.0))
                .await
            {
                Ok(c) if c.len() == SPOOL_GROUP_COUNT => {
                    fetched = Some(c);
                    break;
                }
                _ => continue,
            }
        }
        match fetched {
            Some(c) => c,
            None => return TaskOutcome::Retryable("could not fetch commitments".into()),
        }
    };

    let clay_k = ClayParams::default().k() as usize;

    // Download and decode each spool group (inner Clay decode)
    let mut decoded_chunks: Vec<Option<(usize, Vec<u8>)>> = vec![None; SPOOL_GROUP_COUNT];
    let mut successful_chunks = 0usize;

    for group in 0..SPOOL_GROUP_COUNT {
        if cancel.is_cancelled() {
            return TaskOutcome::Success;
        }

        let commitment = commitments[group];
        let (track_pda, _) = tape_api::program::tapedrive::snapshot_pda(target, commitment);
        let track_addr = Pubkey::new(track_pda.to_bytes());

        // Collect slices from committee peers that own spools in this group
        let mut slices: Vec<(usize, Vec<u8>)> = Vec::new();

        for member in &committee {
            if slices.len() >= clay_k {
                break;
            }
            let member_spools_in_group: Vec<u16> = member
                .spools
                .iter()
                .copied()
                .filter(|&s| group_for_spool(s) == group as u64)
                .collect();

            if member_spools_in_group.is_empty() {
                continue;
            }

            let addr = match member.network_address.to_socket_addr() {
                Ok(a) => a,
                Err(_) => continue,
            };
            let client = match tape_node_client::NodeClientBuilder::new()
                .build(&addr.to_string())
            {
                Ok(c) => c,
                Err(_) => continue,
            };

            for spool in member_spools_in_group {
                if slices.len() >= clay_k {
                    break;
                }
                let slice_in_group = (spool as usize) % SPOOL_GROUP_SIZE;
                // Skip if we already have this slice index
                if slices.iter().any(|(idx, _)| *idx == slice_in_group) {
                    continue;
                }

                match with_retry(&RetryConfig::fast(), || {
                    client.get_slice(track_addr, spool)
                })
                .await
                {
                    Ok(data) if !data.is_empty() => {
                        slices.push((slice_in_group, data));
                    }
                    _ => continue,
                }
            }
        }

        if slices.len() < clay_k {
            tracing::debug!(group, got = slices.len(), need = clay_k, "not enough slices");
            continue;
        }

        // Inner Clay decode
        let mut slicer = Slicer::new(ClayCoder::from_params(ClayParams::default()));
        slicer.set_chunk_index(group as u64);

        let slice_refs: Vec<(usize, &[u8])> =
            slices.iter().map(|(i, d)| (*i, d.as_slice())).collect();
        match slicer.decode(&slice_refs) {
            Ok(chunk_data) => {
                decoded_chunks[group] = Some((group, chunk_data));
                successful_chunks += 1;
            }
            Err(e) => {
                tracing::debug!(group, "inner decode failed: {e}");
            }
        }
    }

    if successful_chunks < DEFAULT_K_OUTER {
        return TaskOutcome::Retryable(format!(
            "only decoded {successful_chunks}/{DEFAULT_K_OUTER} chunks"
        ));
    }

    if cancel.is_cancelled() {
        return TaskOutcome::Success;
    }

    // Outer RS decode
    let outer_input: Vec<(usize, Vec<u8>)> = decoded_chunks
        .into_iter()
        .flatten()
        .collect();
    let outer_refs: Vec<(usize, &[u8])> = outer_input
        .iter()
        .map(|(i, d)| (*i, d.as_slice()))
        .collect();

    let mut outer = OuterCoder::new(DEFAULT_K_OUTER);
    let decoded = match outer.decode(&outer_refs) {
        Ok(d) => d,
        Err(e) => return TaskOutcome::Retryable(format!("outer decode: {e}")),
    };

    // Deserialize snapshot log
    let log: SnapshotLog = match wincode::deserialize(&decoded) {
        Ok(l) => l,
        Err(e) => return TaskOutcome::Retryable(format!("deserialize log: {e}")),
    };

    // Replay into local state
    let fsm = crate::fsm::Fsm::new(context.clone());
    if let Err(e) = fsm.replay_snapshot(&log) {
        return TaskOutcome::Retryable(format!("replay: {e}"));
    }

    tracing::info!(
        epoch = target.0,
        end_slot = log.end_slot.0,
        entries = log.entries.len(),
        "snapshot bootstrap complete"
    );
    TaskOutcome::Success
}

/// Build snapshot: serialize event log, outer RS encode into 50 chunks,
/// inner Clay encode each chunk into 20 slices, store commitments + slices.
pub async fn run_build<S: Store, R: Rpc>(
    context: Arc<NodeContext<S, R>>,
    cancel: CancellationToken,
) -> TaskOutcome {
    let current = match context.store.get_current_epoch() {
        Ok(Some(e)) => e,
        Ok(None) => return TaskOutcome::Retryable("no current epoch".into()),
        Err(e) => return TaskOutcome::Retryable(format!("read epoch: {e}")),
    };

    if current.0 < 2 {
        return TaskOutcome::Success;
    }

    let target = EpochNumber(current.0 - 1);

    // Idempotent: skip if already built
    match context.store.get_snapshot_commitment(target, ChunkIndex(0)) {
        Ok(Some(_)) => return TaskOutcome::Success,
        Ok(None) => {}
        Err(e) => return TaskOutcome::Retryable(format!("check commitment: {e}")),
    }

    // Read event log
    let entries = match context.store.get_epoch_events(target) {
        Ok(e) => e,
        Err(e) => return TaskOutcome::Retryable(format!("read events: {e}")),
    };

    if entries.is_empty() {
        return TaskOutcome::Success;
    }

    // Build snapshot log
    let start_slot = entries.first().unwrap().slot;
    let end_slot = entries.last().unwrap().slot;
    let log = SnapshotLog {
        version: 1,
        epoch: target,
        start_slot,
        end_slot,
        entries,
    };

    let serialized = match wincode::serialize(&log) {
        Ok(b) => b,
        Err(e) => return TaskOutcome::Retryable(format!("serialize log: {e}")),
    };

    // Outer RS encode into 50 chunks
    let mut outer = OuterCoder::new(DEFAULT_K_OUTER);
    let chunks = match outer.encode(&serialized) {
        Ok(c) => c,
        Err(e) => return TaskOutcome::Retryable(format!("outer encode: {e}")),
    };

    // Collect owned spools for slice storage
    let owned_spools: HashSet<u16> = match context.store.iter_all_spools() {
        Ok(spools) => spools.into_iter().map(|(id, _)| id).collect(),
        Err(e) => return TaskOutcome::Retryable(format!("read spools: {e}")),
    };

    // Process each chunk (one per spool group)
    for group in 0..SPOOL_GROUP_COUNT {
        if cancel.is_cancelled() {
            return TaskOutcome::Success;
        }

        let chunk_data = &chunks[group];
        let chunk_index = ChunkIndex(group as u64);

        // Inner Clay encode
        let mut slicer = Slicer::new(ClayCoder::from_params(ClayParams::default()));
        slicer.set_chunk_index(group as u64);

        let slices = match slicer.encode(chunk_data) {
            Ok(s) => s,
            Err(e) => return TaskOutcome::Retryable(format!("inner encode group {group}: {e}")),
        };

        // Compute commitment (merkle root) and per-slice leaf hashes
        let commitment = blob_merkle_root(&slices);
        let leaves: Vec<tape_crypto::Hash> = slices.iter().map(|s| hash_leaf(s)).collect();

        // Store commitment
        if let Err(e) = context
            .store
            .set_snapshot_commitment(target, chunk_index, commitment)
        {
            return TaskOutcome::Retryable(format!("store commitment: {e}"));
        }

        // Store metadata for RegisterSnapshot
        let stripe_count = if chunk_data.is_empty() {
            1
        } else {
            ((chunk_data.len() + slicer.stripe_size() - 1) / slicer.stripe_size()) as u64
        };
        let profile = slicer.profile();
        let meta = SnapshotChunkMeta {
            leaves: leaves.clone(),
            stripe_size: slicer.stripe_size() as u64,
            stripe_count,
            encoding_type: profile.encoding,
            encoding_params: profile.params,
        };
        if let Err(e) = context
            .store
            .set_snapshot_metadata(target, chunk_index, meta)
        {
            return TaskOutcome::Retryable(format!("store metadata: {e}"));
        }

        // Store slices for spools we own in this group
        let track_addr = {
            let (pda, _) = tape_api::program::tapedrive::snapshot_pda(target, commitment);
            Pubkey::new(pda.to_bytes())
        };

        for slice_idx in 0..SPOOL_GROUP_SIZE {
            let spool = spool_for_slice(group as u64, slice_idx);
            if owned_spools.contains(&spool) {
                if let Err(e) =
                    context
                        .store
                        .put_slice(spool, track_addr, slices[slice_idx].clone())
                {
                    return TaskOutcome::Retryable(format!("put slice: {e}"));
                }
            }
        }
    }

    // GC event log
    if let Err(e) = context.store.delete_epoch_events(target) {
        tracing::warn!(epoch = target.0, "failed to GC event log: {e}");
    }

    tracing::info!(epoch = target.0, chunks = SPOOL_GROUP_COUNT, "snapshot build complete");
    TaskOutcome::Success
}

/// Collect BLS signatures from committee peers for snapshot chunks we own.
pub async fn run_certify<S: Store, R: Rpc>(
    context: Arc<NodeContext<S, R>>,
    cancel: CancellationToken,
) -> TaskOutcome {
    let current = match context.store.get_current_epoch() {
        Ok(Some(e)) => e,
        Ok(None) => return TaskOutcome::Retryable("no current epoch".into()),
        Err(e) => return TaskOutcome::Retryable(format!("read epoch: {e}")),
    };

    if current.0 < 2 {
        return TaskOutcome::Success;
    }

    let target = EpochNumber(current.0 - 1);

    // Guard: commitments must exist (build completed)
    match context.store.get_snapshot_commitment(target, ChunkIndex(0)) {
        Ok(Some(_)) => {}
        Ok(None) => return TaskOutcome::Retryable("build not yet completed".into()),
        Err(e) => return TaskOutcome::Retryable(format!("check commitment: {e}")),
    }

    // Load committee for signature collection
    let committee: Vec<NodeInfo> = match context.store.get_committee(current) {
        Ok(Some(c)) => c,
        Ok(None) => return TaskOutcome::Retryable("no committee".into()),
        Err(e) => return TaskOutcome::Retryable(format!("read committee: {e}")),
    };

    // Determine which spool groups we're responsible for certifying
    let owned_spools: HashSet<u16> = match context.store.iter_all_spools() {
        Ok(spools) => spools.into_iter().map(|(id, _)| id).collect(),
        Err(e) => return TaskOutcome::Retryable(format!("read spools: {e}")),
    };

    let mut our_groups: HashSet<u64> = HashSet::new();
    for &spool in &owned_spools {
        our_groups.insert(group_for_spool(spool));
    }

    for group in our_groups {
        if cancel.is_cancelled() {
            return TaskOutcome::Success;
        }

        let chunk_index = ChunkIndex(group);

        // Skip if already certified
        match context
            .store
            .get_snapshot_certification(target, chunk_index)
        {
            Ok(Some(_)) => continue,
            Ok(None) => {}
            Err(e) => return TaskOutcome::Retryable(format!("check cert: {e}")),
        }

        let _commitment = match context
            .store
            .get_snapshot_commitment(target, chunk_index)
        {
            Ok(Some(c)) => c,
            Ok(None) => continue,
            Err(e) => return TaskOutcome::Retryable(format!("read commitment: {e}")),
        };

        // Collect signatures from committee members that own spools in this group
        let mut signatures = Vec::new();
        let mut member_indices = Vec::new();
        let mut weight: u64 = 0;

        for (idx, member) in committee.iter().enumerate() {
            if cancel.is_cancelled() {
                return TaskOutcome::Success;
            }

            // Weight = number of spools this member owns in our group
            let member_weight: u64 = member
                .spools
                .iter()
                .filter(|&&s| group_for_spool(s) == group)
                .count() as u64;

            if member_weight == 0 {
                continue;
            }

            let addr: std::net::SocketAddr = match member.network_address.to_socket_addr() {
                Ok(a) => a,
                Err(_) => continue,
            };

            let client = match tape_node_client::NodeClientBuilder::new()
                .build(&addr.to_string())
            {
                Ok(c) => c,
                Err(_) => continue,
            };

            let resp = match with_retry(&RetryConfig::fast(), || client.get_snapshot_signature(target.0, group as u64)).await {
                Ok(r) => r,
                Err(e) => {
                    tracing::debug!(member = idx, "snapshot sign failed: {e}");
                    continue;
                }
            };

            if resp.epoch != target.0 {
                tracing::warn!(member = idx, "epoch mismatch in sign response");
                continue;
            }

            signatures.push(tape_core::bls::BlsSignature(
                tape_crypto::bls12254::min_sig::G1CompressedPoint(resp.signature),
            ));
            member_indices.push(resp.member_index);
            weight += member_weight;

            if tape_core::bft::is_supermajority(weight, SPOOL_GROUP_SIZE as u64) {
                break;
            }
        }

        if !tape_core::bft::is_supermajority(weight, SPOOL_GROUP_SIZE as u64) {
            return TaskOutcome::Retryable(format!(
                "insufficient signatures for group {group}: {weight}/{}",
                SPOOL_GROUP_SIZE
            ));
        }

        // Aggregate signatures
        let aggregated = match tape_core::bls::BlsSignature::aggregate(&signatures) {
            Ok(s) => s,
            Err(e) => return TaskOutcome::Retryable(format!("aggregate sigs: {e:?}")),
        };

        // Store result
        let cert = tape_store::types::SnapshotCertResult {
            member_indices: member_indices.to_vec(),
            signature: (aggregated.0).0,
            epoch: target.0,
        };

        if let Err(e) = context
            .store
            .set_snapshot_certification(target, chunk_index, cert)
        {
            return TaskOutcome::Retryable(format!("store cert: {e}"));
        }
    }

    tracing::info!(epoch = target.0, "snapshot certification collected");
    TaskOutcome::Success
}

/// Register snapshot commitments on-chain.
pub async fn run_register<S: Store, R: Rpc>(
    context: Arc<NodeContext<S, R>>,
    cancel: CancellationToken,
) -> TaskOutcome {
    use solana_sdk::signer::Signer;

    let current = match context.store.get_current_epoch() {
        Ok(Some(e)) => e,
        Ok(None) => return TaskOutcome::Retryable("no current epoch".into()),
        Err(e) => return TaskOutcome::Retryable(format!("read epoch: {e}")),
    };

    if current.0 < 2 {
        return TaskOutcome::Success;
    }

    let target = EpochNumber(current.0 - 1);

    // Guard: build must have completed
    match context.store.get_snapshot_commitment(target, ChunkIndex(0)) {
        Ok(Some(_)) => {}
        Ok(None) => return TaskOutcome::Retryable("build not yet completed".into()),
        Err(e) => return TaskOutcome::Retryable(format!("check commitment: {e}")),
    }

    let pubkey = context.keypair.pubkey();

    for group in 0..SPOOL_GROUP_COUNT {
        if cancel.is_cancelled() {
            return TaskOutcome::Success;
        }

        let chunk_index = ChunkIndex(group as u64);

        let commitment = match context
            .store
            .get_snapshot_commitment(target, chunk_index)
        {
            Ok(Some(c)) => c,
            Ok(None) => continue,
            Err(e) => return TaskOutcome::Retryable(format!("read commitment: {e}")),
        };

        let meta = match context
            .store
            .get_snapshot_metadata(target, chunk_index)
        {
            Ok(Some(m)) => m,
            Ok(None) => continue,
            Err(e) => return TaskOutcome::Retryable(format!("read metadata: {e}")),
        };

        // Convert leaves Vec to fixed-size array
        let mut leaves = [tape_crypto::Hash::default(); SPOOL_GROUP_SIZE];
        for (i, h) in meta.leaves.iter().enumerate().take(SPOOL_GROUP_SIZE) {
            leaves[i] = *h;
        }

        let profile = tape_core::encoding::EncodingProfile {
            encoding: meta.encoding_type,
            params: meta.encoding_params,
        };

        let ix = tape_api::prelude::build_register_snapshot_ix(
            pubkey,
            target,
            group as u64,
            commitment,
            profile,
            meta.stripe_size,
            meta.stripe_count,
            leaves,
        );

        match context.rpc.send_instructions(&context.keypair, vec![ix]).await {
            Ok(sig) => {
                tracing::info!(%sig, group, epoch = target.0, "register_snapshot submitted");
            }
            Err(e) => {
                let err_str = e.to_string();
                if err_str.contains("already") {
                    tracing::debug!(group, "snapshot chunk already registered");
                } else {
                    return TaskOutcome::Retryable(format!(
                        "register_snapshot group {group}: {e}"
                    ));
                }
            }
        }
    }

    tracing::info!(epoch = target.0, "all snapshot chunks registered");
    TaskOutcome::Success
}

/// Submit snapshot certifications on-chain with BLS aggregate signatures.
pub async fn run_certify_onchain<S: Store, R: Rpc>(
    context: Arc<NodeContext<S, R>>,
    cancel: CancellationToken,
) -> TaskOutcome {
    use solana_sdk::signer::Signer;

    let current = match context.store.get_current_epoch() {
        Ok(Some(e)) => e,
        Ok(None) => return TaskOutcome::Retryable("no current epoch".into()),
        Err(e) => return TaskOutcome::Retryable(format!("read epoch: {e}")),
    };

    if current.0 < 2 {
        return TaskOutcome::Success;
    }

    let target = EpochNumber(current.0 - 1);
    let pubkey = context.keypair.pubkey();

    // Get committee for bitmap reconstruction
    let committee: Vec<NodeInfo> = match context.store.get_committee(current) {
        Ok(Some(c)) => c,
        Ok(None) => return TaskOutcome::Retryable("no committee".into()),
        Err(e) => return TaskOutcome::Retryable(format!("read committee: {e}")),
    };

    // Determine our spool groups
    let owned_spools: HashSet<u16> = match context.store.iter_all_spools() {
        Ok(spools) => spools.into_iter().map(|(id, _)| id).collect(),
        Err(e) => return TaskOutcome::Retryable(format!("read spools: {e}")),
    };

    let mut our_groups: HashSet<u64> = HashSet::new();
    for &spool in &owned_spools {
        our_groups.insert(group_for_spool(spool));
    }

    for group in our_groups {
        if cancel.is_cancelled() {
            return TaskOutcome::Success;
        }

        let chunk_index = ChunkIndex(group);

        let cert = match context
            .store
            .get_snapshot_certification(target, chunk_index)
        {
            Ok(Some(c)) => c,
            Ok(None) => continue,
            Err(e) => return TaskOutcome::Retryable(format!("read cert: {e}")),
        };

        let commitment = match context
            .store
            .get_snapshot_commitment(target, chunk_index)
        {
            Ok(Some(c)) => c,
            Ok(None) => continue,
            Err(e) => return TaskOutcome::Retryable(format!("read commitment: {e}")),
        };

        // Reconstruct bitmap and signature
        let bitmap = tape_api::program::tapedrive::CommitteeBitmap::from_indices(
            &cert
                .member_indices
                .iter()
                .map(|&i| i as usize)
                .collect::<Vec<_>>(),
            committee.len(),
        );
        let sig = tape_core::bls::BlsSignature(
            tape_crypto::bls12254::min_sig::G1CompressedPoint(cert.signature),
        );

        let ix = tape_api::prelude::build_certify_snapshot_ix(
            pubkey,
            target,
            commitment,
            bitmap,
            sig,
        );

        match context.rpc.send_instructions(&context.keypair, vec![ix]).await {
            Ok(tx_sig) => {
                tracing::info!(%tx_sig, group, epoch = target.0, "certify_snapshot submitted");
            }
            Err(e) => {
                let err_str = e.to_string();
                if err_str.contains("already") {
                    tracing::debug!(group, "snapshot chunk already certified");
                } else {
                    return TaskOutcome::Retryable(format!(
                        "certify_snapshot group {group}: {e}"
                    ));
                }
            }
        }
    }

    // GC: delete stored snapshot data (keep commitments — needed by bootstrap peers)
    let _ = context.store.delete_snapshot_metadata(target);
    let _ = context.store.delete_snapshot_certifications(target);

    tracing::info!(epoch = target.0, "snapshot certification submitted");
    TaskOutcome::Success
}

#[cfg(test)]
mod tests {
    use super::*;

    use tape_core::erasure::SPOOL_GROUP_COUNT;
    use tape_core::snapshot::ReplayableEvent;
    use tape_core::types::SlotNumber;
    use tape_crypto::Hash;

    use crate::test_util::test_context;

    #[tokio::test]
    async fn build_skips_early_epochs() {
        let ctx = test_context();
        ctx.store.set_current_epoch(EpochNumber(1)).unwrap();

        let cancel = CancellationToken::new();
        let result = run_build(ctx, cancel).await;
        assert!(matches!(result, TaskOutcome::Success));
    }

    #[tokio::test]
    async fn build_empty_epoch() {
        let ctx = test_context();
        ctx.store.set_current_epoch(EpochNumber(3)).unwrap();

        let cancel = CancellationToken::new();
        let result = run_build(ctx, cancel).await;
        assert!(matches!(result, TaskOutcome::Success));
    }

    #[tokio::test]
    async fn build_stores_commitments() {
        let ctx = test_context();
        let target = EpochNumber(2);
        ctx.store.set_current_epoch(EpochNumber(3)).unwrap();

        // Populate event log
        ctx.store
            .append_event(
                target,
                SlotNumber(100),
                &ReplayableEvent::AdvanceEpoch {
                    old_epoch: EpochNumber(1),
                    new_epoch: EpochNumber(2),
                },
            )
            .unwrap();

        let cancel = CancellationToken::new();
        let result = run_build(ctx.clone(), cancel).await;
        assert!(matches!(result, TaskOutcome::Success));

        // All 50 commitments stored
        for i in 0..SPOOL_GROUP_COUNT {
            assert!(
                ctx.store
                    .get_snapshot_commitment(target, ChunkIndex(i as u64))
                    .unwrap()
                    .is_some(),
                "commitment missing for chunk {i}"
            );
        }

        // All 50 metadata entries stored
        for i in 0..SPOOL_GROUP_COUNT {
            let meta = ctx
                .store
                .get_snapshot_metadata(target, ChunkIndex(i as u64))
                .unwrap();
            assert!(meta.is_some(), "metadata missing for chunk {i}");
            let meta = meta.unwrap();
            assert_eq!(meta.leaves.len(), SPOOL_GROUP_SIZE);
        }

        // Event log cleaned up
        assert!(!ctx.store.has_epoch_events(target).unwrap());
    }

    #[tokio::test]
    async fn bootstrap_early_epoch() {
        let ctx = test_context();
        ctx.store.set_current_epoch(EpochNumber(1)).unwrap();

        let cancel = CancellationToken::new();
        let result = run_bootstrap(ctx, cancel).await;
        assert!(matches!(result, TaskOutcome::Success));
    }

    #[tokio::test]
    async fn bootstrap_no_committee() {
        let ctx = test_context();
        ctx.store.set_current_epoch(EpochNumber(3)).unwrap();

        let cancel = CancellationToken::new();
        let result = run_bootstrap(ctx, cancel).await;
        assert!(matches!(result, TaskOutcome::Retryable(_)));
    }

    #[tokio::test]
    async fn bootstrap_idempotent() {
        let ctx = test_context();
        ctx.store.set_current_epoch(EpochNumber(3)).unwrap();
        // Simulate an already-synced node
        ctx.store.set_sync_cursor(SlotNumber(500)).unwrap();

        let cancel = CancellationToken::new();
        let result = run_bootstrap(ctx, cancel).await;
        assert!(matches!(result, TaskOutcome::Success));
    }

    #[tokio::test]
    async fn build_idempotent() {
        let ctx = test_context();
        let target = EpochNumber(2);
        ctx.store.set_current_epoch(EpochNumber(3)).unwrap();

        // Pre-set commitment for chunk 0
        ctx.store
            .set_snapshot_commitment(target, ChunkIndex(0), Hash::new_unique())
            .unwrap();

        // Add events (shouldn't be processed)
        ctx.store
            .append_event(
                target,
                SlotNumber(100),
                &ReplayableEvent::AdvanceEpoch {
                    old_epoch: EpochNumber(1),
                    new_epoch: EpochNumber(2),
                },
            )
            .unwrap();

        let cancel = CancellationToken::new();
        let result = run_build(ctx.clone(), cancel).await;
        assert!(matches!(result, TaskOutcome::Success));

        // Chunk 1 should NOT have a commitment (build was skipped)
        assert!(ctx
            .store
            .get_snapshot_commitment(target, ChunkIndex(1))
            .unwrap()
            .is_none());

        // Event log should NOT have been deleted (build was skipped)
        assert!(ctx.store.has_epoch_events(target).unwrap());
    }
}
