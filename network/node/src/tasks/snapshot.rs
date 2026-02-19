//! Snapshot tasks — build, certify, register, certify on-chain.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::{Duration, Instant};

use solana_sdk::signature::Signer;
use rpc::Rpc;
use tape_api::program::tapedrive::snapshot_pda;
use store::Store;
use tape_node_api::SnapshotSignatureSubmission;
use tape_core::bls::BlsSignature;
use tape_core::bft::{is_supermajority, min_correct};
use tape_core::encoding::ClayParams;
use tape_core::cert::snapshot::SnapshotMessage;
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
use tape_store::types::{NodeInfo, Pubkey, SnapshotCertResult, SnapshotChunkMeta, SnapshotPartialSignature};
use tokio_util::sync::CancellationToken;

use crate::chain::{submit_certify, submit_register};
use crate::runtime::NodeContext;
use crate::fsm::Fsm;
use crate::runtime::PeerHandle;
use crate::runtime::committee::{our_member_index, our_snapshot_groups};
use crate::snapshot::{
    SnapshotContext, SnapshotNeed, SubmitClass, classify_submit_error, collect_group_slices,
    fetch_commitments, is_snapshot_build_complete, is_snapshot_chunk_ready, load_snapshot_context,
    peer_client, snapshot_epochs,
};
use crate::supervisor::TaskOutcome;

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
    peer_handle: PeerHandle,
    cancel: CancellationToken,
) -> TaskOutcome {
    let (current, target) = match snapshot_epochs(&context, SnapshotNeed::RequireBuild) {
        Ok(v) => v,
        Err(outcome) => return outcome,
    };

    // Idempotent: skip if already built
    match is_snapshot_build_complete(&context, target) {
        Ok(true) => return TaskOutcome::Success,
        Ok(false) => {}
        Err(e) => return TaskOutcome::Retryable(format!("check build state: {e}")),
    }

    // Read event log
    let entries = match context.store.get_epoch_events(target) {
        Ok(e) => e,
        Err(e) => return TaskOutcome::Retryable(format!("read events: {e}")),
    };
    let event_count: usize = entries.iter().map(|entry| entry.events.len()).sum();

    let committee = context.store.get_committee(current).ok().flatten();
    let our_member_index = match &committee {
        Some(committee) => our_member_index(committee, context.keypair.pubkey()).ok(),
        None => None,
    };
    let maybe_our_groups = match (&committee, our_member_index) {
        (Some(committee), Some(_)) => our_snapshot_groups(committee, context.keypair.pubkey()).ok(),
        _ => None,
    };

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

        let local_signature = if let
            (Some(committee), Some(our_member_index), Some(our_groups)) =
            (&committee, our_member_index, &maybe_our_groups)
        {
            if our_groups.contains(&(group as u64)) {
                let message = SnapshotMessage::new(target, commitment.0).to_bytes();
                let signature = match context.bls_keypair.sign(&message) {
                    Ok(signature) => signature,
                    Err(e) => {
                        return TaskOutcome::Retryable(format!("bls sign snapshot: {e:?}"));
                    }
                };
                if let Err(e) = context.store.set_snapshot_partial_signature(
                    target,
                    group as u64,
                    SnapshotPartialSignature {
                        member_index: our_member_index as u8,
                        signature,
                        epoch: target.0,
                    },
                ) {
                    return TaskOutcome::Retryable(format!("store snapshot partial: {e}"));
                }
                Some((committee, our_member_index, signature))
            } else {
                None
            }
        } else {
            None
        };

        // Push our signature to peers that own this spool group.
        if let Some((committee, our_member_index, signature)) = local_signature {
            let request = SnapshotSignatureSubmission {
                signature,
                member_index: our_member_index as u8,
                epoch: target,
            };

            if let Err(e) = broadcast_snapshot_signature(
                &context,
                &peer_handle,
                committee,
                our_member_index,
                target,
                group as u64,
                &request,
            )
            .await
            {
                tracing::warn!(
                    epoch = target.0,
                    group,
                    "failed to broadcast snapshot signature: {e:?}"
                );
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

async fn broadcast_snapshot_signature<S: Store, R: Rpc>(
    _context: &Arc<NodeContext<S, R>>,
    peer_handle: &PeerHandle,
    committee: &[NodeInfo],
    our_member_index: usize,
    target: EpochNumber,
    group: u64,
    request: &SnapshotSignatureSubmission,
) -> Result<(), TaskOutcome> {
    for (member_index, member) in committee.iter().enumerate() {
        if member_index == our_member_index {
            continue;
        }

        let member_weight = member
            .spools
            .iter()
            .filter(|&&spool| group_for_spool(spool) == group)
            .count();
        if member_weight == 0 {
            continue;
        }

        let Some((addr, client)) = peer_client(peer_handle, member).await? else {
            continue;
        };

        if let Err(err) = client
            .post_snapshot_signature(target.0, group, request)
            .await
        {
            tracing::debug!(
                epoch = target.0,
                group,
                member = member_index,
                "snapshot signature post failed: {err}"
            );
            if let Err(e) = peer_handle.record_failure(addr).await {
                tracing::warn!("peer failure record failed for {addr}: {e}");
            }
            continue;
        }

        if let Err(e) = peer_handle.record_success(addr).await {
            tracing::warn!("failed to record peer success for {addr}: {e}");
        }
    }

    Ok(())
}

/// Collect BLS signatures from committee peers for snapshot chunks we own.
pub async fn run_certify<S: Store, R: Rpc>(
    context: Arc<NodeContext<S, R>>,
    _peer_handle: PeerHandle,
    cancel: CancellationToken,
) -> TaskOutcome {
    let snapshot = match load_snapshot_context(&context, SnapshotNeed::RequireCertify, true) {
        Ok(snapshot) => snapshot,
        Err(outcome) => return outcome,
    };
    let current = snapshot.current;
    let target = snapshot.target;
    tracing::debug!(current_epoch = current.0, target_epoch = target.0, "run_certify start");

    tracing::trace!(current_epoch = current.0, target_epoch = target.0, "run_certify ready");

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
            GroupResult::Skip => pending_quorum += 1,
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
    let snapshot = match load_snapshot_context(&context, SnapshotNeed::RequireRegister, false) {
        Ok(snapshot) => snapshot,
        Err(outcome) => return outcome,
    };
    let current = snapshot.current;
    let target = snapshot.target;
    let mut groups: Vec<_> = snapshot.groups.into_iter().collect();
    groups.sort_unstable();

    if groups.is_empty() {
        return TaskOutcome::Success;
    }

    for group in groups {
        if cancel.is_cancelled() {
            return TaskOutcome::Success;
        }

        match is_snapshot_chunk_ready(&context, target, group) {
            Ok(false) => continue,
            Ok(true) => {}
            Err(e) => {
                return TaskOutcome::Retryable(format!("check chunk build state: {e}"));
            }
        }

        let chunk_index = ChunkIndex(group);
        let commitment = match context
            .store
            .get_snapshot_commitment(target, chunk_index)
        {
            Ok(Some(commitment)) => commitment,
            Ok(None) => {
                return TaskOutcome::Retryable("snapshot chunk commitment missing".to_string());
            }
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

    tracing::info!(epoch = target.0, current = current.0, "all owned snapshot chunks registered");
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
    let _ = context.store.delete_snapshot_partial_signatures_for_epoch(target);

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

    let message = SnapshotMessage::new(target, commitment.0).to_bytes();
    let partials = match context
        .store
        .get_snapshot_partial_signatures(target, group as u64)
    {
        Ok(partials) => partials,
        Err(e) => return Err(TaskOutcome::Retryable(format!("read partial sigs: {e}"))),
    };

    let mut signatures = Vec::new();
    let mut member_indices = Vec::new();
    let mut gathered_weight = 0u64;

    for partial in partials {
        if cancel.is_cancelled() {
            return Err(TaskOutcome::Success);
        }

        if partial.epoch != target.0 {
            continue;
        }

        let member_index = partial.member_index as usize;
        let member = match committee.get(member_index) {
            Some(member) => member,
            None => {
                tracing::debug!(group, member_index, "partial signature for unknown member");
                continue;
            }
        };

        let member_weight = member
            .spools
            .iter()
            .filter(|&&spool| group_for_spool(spool) == group)
            .count() as u64;
        if member_weight == 0 {
            continue;
        }

        if partial
            .signature
            .verify_aggregate(message, &[member.bls_pubkey])
            .is_err()
        {
            tracing::debug!(group, member_index, "invalid snapshot partial signature");
            continue;
        }

        signatures.push(partial.signature);
        member_indices.push(partial.member_index);
        gathered_weight += member_weight;
    }

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

    if group_total_weight == 0 {
        return Ok(GroupResult::Skip);
    }

    if is_supermajority(gathered_weight, group_total_weight) {
        tracing::trace!(group, gathered_weight, group_total_weight, "snapshot certify group reached quorum");
    }

    let quorum_needed = min_correct(group_total_weight);
    let quorum = is_supermajority(gathered_weight, group_total_weight);
    tracing::info!(
        epoch = target.0,
        group,
        quorum,
        gathered_weight,
        needed_weight = quorum_needed,
        group_total_weight,
        group_total_capacity = group_total_weight,
        signatures = signatures.len(),
        group_elapsed_ms = group_start.elapsed().as_millis() as u64,
        "snapshot certify group summary"
    );

    if !quorum {
        return Ok(GroupResult::Pending);
    }

    if signatures.is_empty() || member_indices.is_empty() {
        return Ok(GroupResult::Pending);
    }

    if let Err(e) = store_group_cert(
        context,
        target,
        chunk_index,
        &signatures,
        &member_indices,
    ) {
        return Ok(GroupResult::Fail(format!("store cert: {e}")));
    }

    Ok(GroupResult::Cert)
}

fn store_group_cert<S: Store, R: Rpc>(
    context: &Arc<NodeContext<S, R>>,
    target: EpochNumber,
    chunk_index: ChunkIndex,
    signatures: &[BlsSignature],
    member_indices: &[u8],
) -> Result<(), String> {
    let signature =
        BlsSignature::aggregate(signatures).map_err(|e| format!("aggregate sigs: {e:?}"))?;

    let cert = SnapshotCertResult {
        member_indices: member_indices.to_vec(),
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
use tape_core::bls::{BlsPrivateKey, BlsPubkey, BlsSignature};
use tape_core::cert::snapshot::SnapshotMessage;
use tape_core::erasure::{SPOOL_GROUP_COUNT, group_for_spool};
    use tape_core::snapshot::ReplayableEvent;
    use tape_core::types::network::NetworkAddress;
    use tape_core::types::{EpochNumber, SlotNumber};
    use tape_crypto::Hash;
    use tape_crypto::bls12254::min_sig::G1CompressedPoint;
    use tape_store::types::{NodeInfo, Pubkey as StorePubkey, SnapshotCertResult};

    use crate::runtime::PeerService;
    use crate::runtime::test_utils::test_context;

    fn mark_snapshot_build_complete<S: Store, R: Rpc>(
        ctx: &Arc<NodeContext<S, R>>,
        target: EpochNumber,
    ) {
        for group in 0..SPOOL_GROUP_COUNT {
            let chunk_index = ChunkIndex(group as u64);
            ctx.store
                .set_snapshot_commitment(target, chunk_index, Hash::new_unique())
                .unwrap();
            ctx.store
                .set_snapshot_metadata(
                    target,
                    chunk_index,
                    SnapshotChunkMeta {
                        leaves: Vec::new(),
                        stripe_size: 0,
                        stripe_count: 0,
                        encoding_type: 0,
                        encoding_params: 0,
                    },
                )
                .unwrap();
        }
    }

    fn set_group_ready<S: Store, R: Rpc>(ctx: &Arc<NodeContext<S, R>>, target: EpochNumber, group: u64) {
        let chunk_index = ChunkIndex(group);
        ctx.store
            .set_snapshot_commitment(target, chunk_index, Hash::new_unique())
            .unwrap();
        ctx.store
            .set_snapshot_metadata(
                target,
                chunk_index,
                SnapshotChunkMeta {
                    leaves: Vec::new(),
                    stripe_size: 0,
                    stripe_count: 0,
                    encoding_type: 0,
                    encoding_params: 0,
                },
            )
            .unwrap();
    }

    #[tokio::test]
    async fn build_waits_epoch2() {
        let ctx = test_context();
        ctx.store.set_chain_epoch(EpochNumber(1)).unwrap();

        let cancel = CancellationToken::new();
        let (_peer_service, peer_handle) = PeerService::new();
        let result = run_build(ctx, peer_handle, cancel).await;
        assert!(matches!(result, TaskOutcome::Retryable(_)));
    }

    #[tokio::test]
    async fn build_empty_epoch() {
        let ctx = test_context();
        let target = EpochNumber(2);
        ctx.store.set_chain_epoch(EpochNumber(3)).unwrap();

        let cancel = CancellationToken::new();
        let (_peer_service, peer_handle) = PeerService::new();
        let result = run_build(ctx.clone(), peer_handle, cancel).await;
        assert!(matches!(result, TaskOutcome::Success));
        assert!(is_snapshot_build_complete(&ctx, target).unwrap());
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
        let (_peer_service, peer_handle) = PeerService::new();
        let result = run_build(ctx.clone(), peer_handle, cancel).await;
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

        mark_snapshot_build_complete(&ctx, target);

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
        let (_peer_service, peer_handle) = PeerService::new();
        let result = run_build(ctx.clone(), peer_handle, cancel).await;
        assert!(matches!(result, TaskOutcome::Success));

        // Build was skipped entirely
        for i in 0..SPOOL_GROUP_COUNT {
            assert!(
                ctx.store
                    .get_snapshot_commitment(target, ChunkIndex(i as u64))
                    .unwrap()
                    .is_some(),
                "commitment missing for chunk {i}"
            );
        }

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
        mark_snapshot_build_complete(&ctx, target);

        let group = group_for_spool(5);
        let chunk = ChunkIndex(group);
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

    #[tokio::test]
    async fn single_owner_cert() {
        let ctx = test_context();
        let current = EpochNumber(3);
        let target = EpochNumber(2);
        ctx.store.set_chain_epoch(current).unwrap();

        let group = group_for_spool(5);
        set_group_ready(&ctx, target, group);

        let (node_address, _) = node_pda(ctx.keypair.pubkey());
        let bls_pubkey = ctx.bls_keypair.public_key().unwrap();
        ctx.store
            .put_committee(
                current,
                vec![NodeInfo {
                    node_address: StorePubkey::new(node_address.to_bytes()),
                    bls_pubkey,
                    tls_pubkey: StorePubkey::new([0u8; 32]),
                    network_address: NetworkAddress::new_ipv4([127, 0, 0, 1], 8000),
                    spools: vec![5],
                }],
            )
            .unwrap();

        let commitment = ctx
            .store
            .get_snapshot_commitment(target, ChunkIndex(group))
            .unwrap()
            .unwrap();
        let message = SnapshotMessage::new(target, commitment.into()).to_bytes();
        let signature = ctx.bls_keypair.sign(&message).unwrap();
        ctx.store
            .set_snapshot_partial_signature(
                target,
                group,
                SnapshotPartialSignature {
                    member_index: 0,
                    signature,
                    epoch: target.0,
                },
            )
            .unwrap();

        let cancel = CancellationToken::new();
        let (_peer_service, peer_handle) = PeerService::new();
        let result = run_certify(ctx.clone(), peer_handle, cancel).await;
        assert!(matches!(result, TaskOutcome::Success));

        let cert = ctx
            .store
            .get_snapshot_certification(target, ChunkIndex(group))
            .unwrap();
        assert!(cert.is_some());
        let cert = cert.unwrap();
        assert_eq!(cert.member_indices, vec![0]);
    }

    #[tokio::test]
    async fn build_unreachable_peer_fallback() {
        let ctx = test_context();
        let current = EpochNumber(3);
        let target = EpochNumber(2);
        ctx.store.set_chain_epoch(current).unwrap();

        let group = group_for_spool(5);
        let mut dead_addr = NetworkAddress::default();
        dead_addr.set_flags(2);
        let own_addr = NetworkAddress::new_ipv4([127, 0, 0, 1], 8000);

        let (node_address, _) = node_pda(ctx.keypair.pubkey());
        ctx.store
            .put_committee(
                current,
                vec![
                    NodeInfo {
                        node_address: StorePubkey::new(node_address.to_bytes()),
                        bls_pubkey: ctx.bls_keypair.public_key().unwrap(),
                        tls_pubkey: StorePubkey::new([0u8; 32]),
                        network_address: own_addr,
                        spools: vec![5],
                    },
                    NodeInfo {
                        node_address: StorePubkey::new_unique(),
                        bls_pubkey: BlsPrivateKey::from_random().public_key().unwrap(),
                        tls_pubkey: StorePubkey::new([1u8; 32]),
                        network_address: dead_addr,
                        spools: vec![6],
                    },
                ],
            )
            .unwrap();

        let cancel = CancellationToken::new();
        let (_peer_service, peer_handle) = PeerService::new();
        let result = run_build(ctx.clone(), peer_handle, cancel).await;
        assert!(matches!(result, TaskOutcome::Success));

        let signature = ctx
            .store
            .get_snapshot_partial_signature(target, group, 0)
            .unwrap();
        assert!(signature.is_some());
        assert_eq!(signature.unwrap().member_index, 0);
    }
}
