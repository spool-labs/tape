//! Snapshot tasks — build, certify, register, certify on-chain.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::{Duration, Instant};

use rpc::Rpc;
use tape_api::program::tapedrive::snapshot_pda;
use store::Store;
use tape_core::bls::BlsSignature;
use tape_core::bft::{is_supermajority, min_correct};
use tape_core::encoding::ClayParams;
use tape_core::erasure::{group_for_spool, spool_for_slice, SPOOL_GROUP_COUNT, SPOOL_GROUP_SIZE};
use tape_core::spooler::SpoolGroup;
use tape_core::snapshot::SnapshotLog;
use tape_core::types::{ChunkIndex, EpochNumber, SlotNumber};
use tape_crypto::hash::hashv;
use tape_crypto::merkle::hash_leaf;
use tape_slicer::{
    blob_merkle_root, ClayCoder, ErasureCoder, OuterCoder, Slicer, DEFAULT_K_OUTER,
};
use tape_store::ops::{CommitteeOps, EventLogOps, MetaOps, SliceOps, SpoolOps};
use tape_store::types::{NodeInfo, Pubkey, SnapshotCertResult, SnapshotChunkMeta};
use tokio_util::sync::CancellationToken;

use crate::chain::{submit_certify, submit_register};
use crate::runtime::NodeContext;
use crate::fsm::Fsm;
use crate::runtime::PeerHandle;
use crate::snapshot::{
    GroupPartials, SnapshotContext, SnapshotNeed, SubmitClass, classify_submit_error,
    collect_group_partials, collect_group_slices, fetch_commitments, load_snapshot_context,
    snapshot_epochs,
};
use crate::supervisor::TaskOutcome;

const SNAPSHOT_SIGN_TIMEOUT: Duration = Duration::from_secs(6);
const SNAPSHOT_PENDING_DELAY: Duration = Duration::from_secs(2);

/// Bootstrap from a snapshot: download slices from peers, decode, replay.
pub async fn run_bootstrap<S: Store, R: Rpc>(
    context: Arc<NodeContext<S, R>>,
    peer_handle: PeerHandle,
    cancel: CancellationToken,
) -> TaskOutcome {
    let (current, target) = match snapshot_epochs(&context, SnapshotNeed::AllowMissing) {
        Ok(v) => v,
        Err(outcome) => return outcome,
    };

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

    let commitments = match fetch_commitments(&peer_handle, &committee, target).await {
        Ok(c) => c,
        Err(outcome) => return outcome,
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
        let (track_pda, _) = snapshot_pda(target, commitment);
        let track_addr = Pubkey::new(track_pda.to_bytes());

        let slices = match collect_group_slices(
            &peer_handle,
            &committee,
            group as SpoolGroup,
            track_addr,
            clay_k,
        )
        .await
        {
            Ok(slices) => slices,
            Err(outcome) => return outcome,
        };

        if slices.len() < clay_k {
            tracing::debug!(group, got = slices.len(), need = clay_k, "not enough slices");
            continue;
        }

        match decode_group(group, &slices) {
            Ok(chunk_data) => {
                decoded_chunks[group] = Some((group, chunk_data));
                successful_chunks += 1;
            }
            Err(e) => tracing::debug!(group, "inner decode failed: {e}"),
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

    let decoded = match decode_outer(decoded_chunks) {
        Ok(d) => d,
        Err(e) => return TaskOutcome::Retryable(format!("outer decode: {e}")),
    };

    // Deserialize snapshot log
    let log: SnapshotLog = match wincode::deserialize(&decoded) {
        Ok(l) => l,
        Err(e) => return TaskOutcome::Retryable(format!("deserialize log: {e}")),
    };

    // Replay into local state
    let fsm = Fsm::new(context.clone());
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
    let (_, target) = match snapshot_epochs(&context, SnapshotNeed::RequireBuild) {
        Ok(v) => v,
        Err(outcome) => return outcome,
    };

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
    let event_count: usize = entries.iter().map(|entry| entry.events.len()).sum();

    // Build snapshot log
    let (start_slot, end_slot) = match (entries.first(), entries.last()) {
        (Some(first), Some(last)) => (first.slot, last.slot),
        _ => (SlotNumber(0), SlotNumber(0)),
    };
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
    let pre_erasure_hash = hashv(&[serialized.as_slice()]);
    tracing::warn!(
        epoch = target.0,
        ?pre_erasure_hash,
        event_count,
        entry_count = log.entries.len(),
        snapshot_bytes = serialized.len(),
        "snapshot pre-erasure payload"
    );

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
            let (pda, _) = snapshot_pda(target, commitment);
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
    peer_handle: PeerHandle,
    cancel: CancellationToken,
) -> TaskOutcome {
    let snapshot = match load_snapshot_context(&context, SnapshotNeed::RequireCertify, true) {
        Ok(snapshot) => snapshot,
        Err(outcome) => return outcome,
    };
    let current = snapshot.current;
    let target = snapshot.target;
    tracing::debug!(current_epoch = current.0, target_epoch = target.0, "run_certify start");

    // Guard: commitments must exist (build completed)
    match context.store.get_snapshot_commitment(target, ChunkIndex(0)) {
        Ok(Some(_)) => {}
        Ok(None) => {
            tracing::debug!(target_epoch = target.0, "run_certify waiting for build");
            return TaskOutcome::Retryable("build not yet completed".into());
        }
        Err(e) => return TaskOutcome::Retryable(format!("check commitment: {e}")),
    }

    let SnapshotContext {
        committee,
        groups: our_groups,
        member_index,
        owned_spools,
        ..
    } = snapshot;
    let our_member_index = member_index.unwrap_or(0);
    let owned_spools = owned_spools.unwrap_or(0);
    tracing::debug!(
        target_epoch = target.0,
        committee_size = committee.len(),
        owned_spools,
        groups = our_groups.len(),
        "run_certify inputs"
    );

    let mut groups: Vec<SpoolGroup> = our_groups.into_iter().collect();
    groups.sort_unstable();
    if !groups.is_empty() {
        let offset = our_member_index % groups.len();
        groups.rotate_left(offset);
    }

    let mut certified_groups = 0usize;
    let mut pending_quorum = 0usize;
    let mut failed: Vec<String> = Vec::new();
    let mut groups_attempted = 0usize;

    for group in groups {
        if cancel.is_cancelled() {
            return TaskOutcome::Success;
        }

        match context
            .store
            .get_snapshot_certification(target, ChunkIndex(group))
        {
            Ok(Some(_)) => continue,
            Ok(None) => {}
            Err(e) => return TaskOutcome::Retryable(format!("check cert: {e}")),
        }

        groups_attempted += 1;

        let result = match certify_group(
            &context,
            &peer_handle,
            &committee,
            target,
            group,
            &cancel,
        )
        .await
        {
            Ok(result) => result,
            Err(outcome) => return outcome,
        };
        match result {
            GroupResult::Skip => {}
            GroupResult::Pending => pending_quorum += 1,
            GroupResult::Cert => certified_groups += 1,
            GroupResult::Fail(err) => failed.push(format!("group {group}: {err}")),
        }
    }

    if !failed.is_empty() {
        return TaskOutcome::Retryable(format!(
            "snapshot certify progress epoch={} certified={} pending_quorum={} attempted={} failed={} {}",
            target.0,
            certified_groups,
            pending_quorum,
            groups_attempted,
            failed.len(),
            failed.first().cloned().unwrap_or_default()
        ));
    }

    if pending_quorum > 0 {
        tracing::debug!(
            epoch = target.0,
            certified_groups,
            pending_quorum,
            groups_attempted,
            "snapshot certify waiting for more partial signatures"
        );
        return TaskOutcome::Pending(SNAPSHOT_PENDING_DELAY);
    }

    tracing::info!(
        epoch = target.0,
        certified_groups,
        groups_attempted,
        "snapshot certification collected"
    );
    TaskOutcome::Success
}

/// Register snapshot commitments on-chain.
pub async fn run_register<S: Store, R: Rpc>(
    context: Arc<NodeContext<S, R>>,
    _peer_handle: PeerHandle,
    cancel: CancellationToken,
) -> TaskOutcome {
    let (_, target) = match snapshot_epochs(&context, SnapshotNeed::RequireRegister) {
        Ok(v) => v,
        Err(outcome) => return outcome,
    };
    // Guard: build must have completed
    match context.store.get_snapshot_commitment(target, ChunkIndex(0)) {
        Ok(Some(_)) => {}
        Ok(None) => return TaskOutcome::Retryable("build not yet completed".into()),
        Err(e) => return TaskOutcome::Retryable(format!("check commitment: {e}")),
    }

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

        match submit_register(&context, target, group as SpoolGroup, commitment, &meta).await {
            Ok(sig) => {
                tracing::info!(%sig, group, epoch = target.0, "register_snapshot submitted");
            }
            Err(ref e) => match classify_submit_error(e) {
                SubmitClass::Done | SubmitClass::Pending => {
                    tracing::debug!(group, "snapshot chunk already registered");
                }
                SubmitClass::Retryable => {
                    return TaskOutcome::Retryable(format!("register_snapshot group {group}: {e}"));
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
    _peer_handle: PeerHandle,
    cancel: CancellationToken,
) -> TaskOutcome {
    let snapshot = match load_snapshot_context(&context, SnapshotNeed::AllowMissing, false) {
        Ok(snapshot) => snapshot,
        Err(outcome) => return outcome,
    };
    let current = snapshot.current;
    let target = snapshot.target;
    tracing::debug!(
        current_epoch = current.0,
        target_epoch = target.0,
        "run_certify_onchain start"
    );
    let committee = snapshot.committee;
    let our_groups = snapshot.groups;
    tracing::debug!(
        target_epoch = target.0,
        committee_size = committee.len(),
        groups = our_groups.len(),
        "run_certify_onchain inputs"
    );
    let mut submitted = 0usize;
    let mut missing_local = 0usize;
    let mut pending_register = 0usize;
    let mut failed: Vec<String> = Vec::new();

    for &group in &our_groups {
        if cancel.is_cancelled() {
            return TaskOutcome::Success;
        }

        let chunk_index = ChunkIndex(group);

        let cert = match context
            .store
            .get_snapshot_certification(target, chunk_index)
        {
            Ok(Some(c)) => {
                tracing::debug!(
                    epoch = target.0,
                    group,
                    members = c.member_indices.len(),
                    "snapshot certify_onchain found local cert"
                );
                c
            }
            Ok(None) => {
                tracing::trace!(
                    epoch = target.0,
                    group,
                    "snapshot certify_onchain missing local cert"
                );
                missing_local += 1;
                continue;
            }
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

        match submit_certify(&context, committee.len(), target, commitment, &cert).await {
            Ok(tx_sig) => {
                tracing::info!(%tx_sig, group, epoch = target.0, "certify_snapshot submitted");
                submitted += 1;
            }
            Err(ref e) => match classify_submit_error(e) {
                SubmitClass::Done => {
                    tracing::debug!(group, "snapshot chunk already certified");
                    submitted += 1;
                }
                SubmitClass::Pending => pending_register += 1,
                SubmitClass::Retryable => {
                    failed.push(format!("group {group}: {e}"));
                }
            }
        }
    }

    if !failed.is_empty() {
        return TaskOutcome::Retryable(format!(
            "certify_snapshot progress epoch={} submitted={} missing_local={} pending_register={} failed={} {}",
            target.0,
            submitted,
            missing_local,
            pending_register,
            failed.len(),
            failed.first().cloned().unwrap_or_default()
        ));
    }

    if missing_local > 0 || pending_register > 0 {
        tracing::debug!(
            epoch = target.0,
            submitted,
            missing_local,
            pending_register,
            "certify_snapshot waiting for local certs and/or register completion"
        );
        return TaskOutcome::Pending(SNAPSHOT_PENDING_DELAY);
    }

    // GC: delete stored snapshot data (keep commitments — needed by bootstrap peers)
    let _ = context.store.delete_snapshot_metadata(target);
    let _ = context.store.delete_snapshot_certifications(target);

    tracing::info!(epoch = target.0, "snapshot certification submitted");
    TaskOutcome::Success
}

enum GroupResult {
    Skip,
    Pending,
    Cert,
    Fail(String),
}

fn decode_group(group: usize, slices: &[(usize, Vec<u8>)]) -> Result<Vec<u8>, String> {
    let refs: Vec<(usize, &[u8])> = slices.iter().map(|(i, data)| (*i, data.as_slice())).collect();
    let mut slicer = Slicer::new(ClayCoder::from_params(ClayParams::default()));
    slicer.set_chunk_index(group as u64);
    slicer
        .decode(&refs)
        .map_err(|e| format!("inner decode group {group}: {e}"))
}

fn decode_outer(decoded_chunks: Vec<Option<(usize, Vec<u8>)>>) -> Result<Vec<u8>, String> {
    let refs: Vec<(usize, &[u8])> = decoded_chunks
        .iter()
        .filter_map(|chunk| chunk.as_ref().map(|(index, data)| (*index, data.as_slice())))
        .collect();
    if refs.len() < DEFAULT_K_OUTER {
        return Err(format!(
            "not enough decoded chunks: {}/{}",
            refs.len(),
            DEFAULT_K_OUTER
        ));
    }

    let mut outer = OuterCoder::new(DEFAULT_K_OUTER);
    outer.decode(&refs).map_err(|e| format!("{e}"))
}


async fn certify_group<S: Store, R: Rpc>(
    context: &Arc<NodeContext<S, R>>,
    peer_handle: &PeerHandle,
    committee: &[NodeInfo],
    target: EpochNumber,
    group: SpoolGroup,
    cancel: &CancellationToken,
) -> Result<GroupResult, TaskOutcome> {
    let group_start = Instant::now();
    let chunk_index = ChunkIndex(group);

    let commitment = match context.store.get_snapshot_commitment(target, chunk_index) {
        Ok(Some(commitment)) => commitment,
        Ok(None) => return Ok(GroupResult::Skip),
        Err(e) => return Err(TaskOutcome::Retryable(format!("read commitment: {e}"))),
    };

    let mut partials = collect_group_partials(
        peer_handle,
        committee,
        target,
        group,
        commitment,
        SNAPSHOT_SIGN_TIMEOUT,
        cancel,
    )
    .await?;

    let group_total_weight: u64 = committee
        .iter()
        .map(|member| {
            member
                .spools
                .iter()
                .filter(|&&spool| group_for_spool(spool) == group)
                .count() as u64
        })
        .sum();
    let quorum_needed = min_correct(SPOOL_GROUP_SIZE as u64);
    let quorum = is_supermajority(partials.weight, SPOOL_GROUP_SIZE as u64);
    tracing::info!(
        epoch = target.0,
        group,
        quorum,
        gathered_weight = partials.weight,
        needed_weight = quorum_needed,
        group_total_weight,
        group_total_capacity = SPOOL_GROUP_SIZE,
        signatures = partials.signatures.len(),
        members_considered = partials.metrics.members_considered,
        members_no_weight = partials.metrics.members_no_weight,
        peer_addr_invalid = partials.metrics.peer_addr_invalid,
        peer_client_build_fail = partials.metrics.peer_client_build_fail,
        peer_rpc_success = partials.metrics.peer_rpc_success,
        peer_rpc_fail = partials.metrics.peer_rpc_fail,
        epoch_mismatch = partials.metrics.epoch_mismatch,
        sig_invalid = partials.metrics.sig_invalid,
        member_index_overflow = partials.metrics.member_index_overflow,
        group_elapsed_ms = group_start.elapsed().as_millis() as u64,
        "snapshot certify group summary"
    );

    if !quorum {
        return Ok(GroupResult::Pending);
    }

    if let Err(e) = store_group_cert(context, target, chunk_index, &mut partials) {
        return Ok(GroupResult::Fail(format!("store cert: {e}")));
    }

    Ok(GroupResult::Cert)
}

fn store_group_cert<S: Store, R: Rpc>(
    context: &Arc<NodeContext<S, R>>,
    target: EpochNumber,
    chunk_index: ChunkIndex,
    partials: &mut GroupPartials,
) -> Result<(), String> {
    let signature =
        BlsSignature::aggregate(&partials.signatures).map_err(|e| format!("aggregate sigs: {e:?}"))?;

    let cert = SnapshotCertResult {
        member_indices: partials.member_indices.to_vec(),
        signature,
        epoch: target.0,
    };

    context
        .store
        .set_snapshot_certification(target, chunk_index, cert)
        .map_err(|e| e.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    use bytemuck::Zeroable;
    use solana_sdk::signature::Signer;
    use tape_api::program::tapedrive::node_pda;
    use tape_core::bls::{BlsPubkey, BlsSignature};
    use tape_core::erasure::SPOOL_GROUP_COUNT;
    use tape_core::snapshot::ReplayableEvent;
    use tape_core::types::network::NetworkAddress;
    use tape_core::types::{EpochNumber, SlotNumber};
    use tape_crypto::Hash;
    use tape_crypto::bls12254::min_sig::G1CompressedPoint;
    use tape_store::types::{NodeInfo, Pubkey as StorePubkey, SnapshotCertResult};

    use crate::runtime::PeerService;
    use crate::runtime::test_utils::test_context;

    #[tokio::test]
    async fn build_waits_epoch2() {
        let ctx = test_context();
        ctx.store.set_chain_epoch(EpochNumber(1)).unwrap();

        let cancel = CancellationToken::new();
        let result = run_build(ctx, cancel).await;
        assert!(matches!(result, TaskOutcome::Retryable(_)));
    }

    #[tokio::test]
    async fn build_empty_epoch() {
        let ctx = test_context();
        let target = EpochNumber(2);
        ctx.store.set_chain_epoch(EpochNumber(3)).unwrap();

        let cancel = CancellationToken::new();
        let result = run_build(ctx.clone(), cancel).await;
        assert!(matches!(result, TaskOutcome::Success));
        assert!(
            ctx.store
                .get_snapshot_commitment(target, ChunkIndex(0))
                .unwrap()
                .is_some()
        );
    }

    #[tokio::test]
    async fn build_stores_commitments() {
        let ctx = test_context();
        let target = EpochNumber(2);
        ctx.store.set_chain_epoch(EpochNumber(3)).unwrap();

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
        ctx.store.set_chain_epoch(EpochNumber(1)).unwrap();

        let cancel = CancellationToken::new();
        let (_peer_service, peer_handle) = PeerService::new();
        let result = run_bootstrap(ctx, peer_handle, cancel).await;
        assert!(matches!(result, TaskOutcome::Success));
    }

    #[tokio::test]
    async fn bootstrap_no_committee() {
        let ctx = test_context();
        ctx.store.set_chain_epoch(EpochNumber(3)).unwrap();

        let cancel = CancellationToken::new();
        let (_peer_service, peer_handle) = PeerService::new();
        let result = run_bootstrap(ctx, peer_handle, cancel).await;
        assert!(matches!(result, TaskOutcome::Retryable(_)));
    }

    #[tokio::test]
    async fn bootstrap_idempotent() {
        let ctx = test_context();
        ctx.store.set_chain_epoch(EpochNumber(3)).unwrap();
        // Simulate an already-synced node
        ctx.store.set_sync_cursor(SlotNumber(500)).unwrap();

        let cancel = CancellationToken::new();
        let (_peer_service, peer_handle) = PeerService::new();
        let result = run_bootstrap(ctx, peer_handle, cancel).await;
        assert!(matches!(result, TaskOutcome::Success));
    }

    #[tokio::test]
    async fn build_idempotent() {
        let ctx = test_context();
        let target = EpochNumber(2);
        ctx.store.set_chain_epoch(EpochNumber(3)).unwrap();

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

    #[tokio::test]
    async fn certify_resume() {
        let ctx = test_context();
        let current = EpochNumber(3);
        let target = EpochNumber(2);
        ctx.store.set_chain_epoch(current).unwrap();

        let (node_address, _) = node_pda(ctx.keypair.pubkey());
        ctx.store
            .put_committee(
                current,
                vec![NodeInfo {
                    node_address: StorePubkey::new(node_address.to_bytes()),
                    bls_pubkey: BlsPubkey::zeroed(),
                    tls_pubkey: StorePubkey::new([0u8; 32]),
                    network_address: NetworkAddress::new_ipv4([127, 0, 0, 1], 8000),
                    spools: vec![5],
                }],
            )
            .unwrap();

        let group = group_for_spool(5);
        let chunk = ChunkIndex(group);
        ctx.store
            .set_snapshot_commitment(target, chunk, Hash::new_unique())
            .unwrap();
        ctx.store
            .set_snapshot_certification(
                target,
                chunk,
                SnapshotCertResult {
                    member_indices: vec![0],
                    signature: BlsSignature(G1CompressedPoint([7u8; 32])),
                    epoch: target.0,
                },
            )
            .unwrap();

        let cancel = CancellationToken::new();
        let (_peer_service, peer_handle) = PeerService::new();
        let result = run_certify(ctx.clone(), peer_handle, cancel).await;
        assert!(matches!(result, TaskOutcome::Success));
    }
}
