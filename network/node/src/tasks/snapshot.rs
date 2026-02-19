//! Snapshot tasks — build, certify, register, certify on-chain.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::{Duration, Instant};

use rpc::Rpc;
use solana_sdk::signature::Signer;
use store::Store;
use tape_api::errors::is_account_state_pending_error;
use tape_core::cert::snapshot::SnapshotMessage;
use tape_core::encoding::ClayParams;
use tape_core::erasure::{group_for_spool, spool_for_slice, SPOOL_GROUP_COUNT, SPOOL_GROUP_SIZE};
use tape_core::snapshot::SnapshotLog;
use tape_core::types::{ChunkIndex, EpochNumber, SlotNumber};
use tape_crypto::hash::hashv;
use tape_crypto::merkle::hash_leaf;
use tape_slicer::{blob_merkle_root, ClayCoder, ErasureCoder, OuterCoder, Slicer, DEFAULT_K_OUTER};
use tape_store::ops::{CommitteeOps, EventLogOps, MetaOps, SliceOps, SpoolOps};
use tape_node_client::{RetryConfig, with_retry};
use tape_store::types::{NodeInfo, Pubkey, SnapshotChunkMeta};
use tokio_util::sync::CancellationToken;

use crate::core::NodeContext;
use crate::core::committee::{our_member, our_member_index, our_snapshot_groups};
use crate::leader::LeaderSchedule;
use crate::state::GroupState;
use crate::supervisor::TaskOutcome;
use crate::tasks::parse_tape_error;

const SNAPSHOT_REGISTER_CU: u32 = 700_000;
const SNAPSHOT_CERTIFY_CU: u32 = 1_400_000;
const SNAPSHOT_SIGN_TIMEOUT: Duration = Duration::from_secs(6);
const SNAPSHOT_TAKEOVER_WINDOW_SECS: u64 = 15;
const SNAPSHOT_PENDING_DELAY: Duration = Duration::from_secs(2);

fn takeover_windows_elapsed<S: Store, R: Rpc>(
    context: &NodeContext<S, R>,
    epoch: EpochNumber,
) -> usize {
    let elapsed = context
        .epoch_clock
        .lock()
        .unwrap()
        .elapsed_or_reset(epoch)
        .as_secs();
    let window = Duration::from_secs(SNAPSHOT_TAKEOVER_WINDOW_SECS.max(1));
    (elapsed / window.as_secs()) as usize
}

fn snapshot_progress_reset_if_needed<S: Store, R: Rpc>(
    context: &NodeContext<S, R>,
    epoch: EpochNumber,
) {
    let mut progress = context.snapshot_progress.lock().unwrap();
    if progress.epoch() != epoch {
        progress.reset(epoch);
    }
}

fn snapshot_progress_has_local_cert<S: Store, R: Rpc>(
    context: &NodeContext<S, R>,
    group: usize,
) -> bool {
    context.snapshot_progress.lock().unwrap().has_local_cert(group)
}

fn snapshot_progress_is_registered<S: Store, R: Rpc>(
    context: &NodeContext<S, R>,
    group: usize,
) -> bool {
    context.snapshot_progress.lock().unwrap().is_registered(group)
}

fn snapshot_progress_is_done_onchain<S: Store, R: Rpc>(
    context: &NodeContext<S, R>,
    group: usize,
) -> bool {
    context.snapshot_progress.lock().unwrap().is_done_onchain(group)
}

fn snapshot_progress_advance<S: Store, R: Rpc>(
    context: &NodeContext<S, R>,
    epoch: EpochNumber,
    group: usize,
    state: GroupState,
) {
    let mut progress = context.snapshot_progress.lock().unwrap();
    if progress.epoch() == epoch {
        progress.advance(group, state);
    }
}

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
            if context.peer_health.lock().unwrap().is_cooling_down(&addr) {
                continue;
            }
            let client = match tape_node_client::NodeClientBuilder::new().build(&addr.to_string()) {
                Ok(c) => c,
                Err(_) => continue,
            };
            match with_retry(&RetryConfig::fast(), || client.get_snapshot_commitments(target.0))
                .await
            {
                Ok(c) if c.len() == SPOOL_GROUP_COUNT => {
                    context.peer_health.lock().unwrap().record_success(&addr);
                    fetched = Some(c);
                    break;
                }
                Ok(_) => continue,
                Err(_) => {
                    context.peer_health.lock().unwrap().record_failure(&addr);
                    continue;
                }
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
            if context.peer_health.lock().unwrap().is_cooling_down(&addr) {
                continue;
            }
            let client = match tape_node_client::NodeClientBuilder::new().build(&addr.to_string()) {
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
                        context.peer_health.lock().unwrap().record_success(&addr);
                        slices.push((slice_in_group, data));
                    }
                    Ok(_) => continue,
                    Err(_) => {
                        context.peer_health.lock().unwrap().record_failure(&addr);
                        continue;
                    }
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
        return TaskOutcome::Retryable("snapshot certify requires epoch >= 2".into());
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
        return TaskOutcome::Retryable("snapshot register requires epoch >= 2".into());
    }

    let target = EpochNumber(current.0 - 1);
    snapshot_progress_reset_if_needed(&context, current);
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

    // Load committee for signature collection
    let committee: Vec<NodeInfo> = match context.store.get_committee(current) {
        Ok(Some(c)) => c,
        Ok(None) => {
            tracing::debug!(current_epoch = current.0, "run_certify no committee in local store");
            return TaskOutcome::Retryable("no committee".into());
        }
        Err(e) => return TaskOutcome::Retryable(format!("read committee: {e}")),
    };

    // Derive owned groups from committee-assigned (global) spool IDs.
    let our_groups: HashSet<u64> = match our_snapshot_groups(&committee, context.keypair.pubkey()) {
        Ok(groups) => groups,
        Err(e) => return TaskOutcome::Retryable(e.into()),
    };
    let our_member_index = match our_member_index(&committee, context.keypair.pubkey()) {
        Ok(index) => index,
        Err(e) => return TaskOutcome::Retryable(e.into()),
    };
    let owned_spools = match our_member(&committee, context.keypair.pubkey()) {
        Ok(member) => member.spools.len(),
        Err(e) => return TaskOutcome::Retryable(e.into()),
    };
    tracing::debug!(
        target_epoch = target.0,
        committee_size = committee.len(),
        owned_spools,
        groups = our_groups.len(),
        "run_certify inputs"
    );

    // Build leader schedule from epoch nonce for deterministic ordering.
    let schedule = match context.store.get_epoch_nonce(current) {
        Ok(Some(n)) => Some(LeaderSchedule::new(committee.len(), n)),
        Ok(None) => None, // fallback: allow all nodes
        Err(e) => return TaskOutcome::Retryable(format!("get_epoch_nonce: {e}")),
    };

    let mut groups: Vec<u64> = our_groups.into_iter().collect();
    groups.sort_unstable();
    if !groups.is_empty() {
        let offset = our_member_index % groups.len();
        groups.rotate_left(offset);
    }

    // Rebuild progress from durable cert state once per run so subsequent
    // retries can skip already-completed groups via the in-memory cache.
    for &group in &groups {
        if snapshot_progress_has_local_cert(&context, group as usize) {
            continue;
        }
        match context
            .store
            .get_snapshot_certification(target, ChunkIndex(group))
        {
            Ok(Some(_)) => {
                snapshot_progress_advance(&context, current, group as usize, GroupState::Certified);
            }
            Ok(None) => {}
            Err(e) => return TaskOutcome::Retryable(format!("check cert: {e}")),
        }
    }

    let windows_elapsed = takeover_windows_elapsed(&context, current);
    let mut certified_groups = 0usize;
    let mut pending_quorum = 0usize;
    let mut failed: Vec<String> = Vec::new();
    let mut groups_attempted = 0usize;
    let mut groups_skipped_not_owner = 0usize;
    let mut groups_taken_over = 0usize;

    for group in groups {
        if cancel.is_cancelled() {
            return TaskOutcome::Success;
        }

        if snapshot_progress_has_local_cert(&context, group as usize) {
            continue;
        }

        let position = schedule
            .as_ref()
            .map(|s| s.position_for(our_member_index, group as usize))
            .unwrap_or(0);
        if position > windows_elapsed {
            groups_skipped_not_owner += 1;
            continue;
        }
        if position > 0 {
            groups_taken_over += 1;
        }
        groups_attempted += 1;

        let group_start = Instant::now();

        let chunk_index = ChunkIndex(group);

        let commitment = match context
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
        let mut members_considered = 0usize;
        let mut members_no_weight = 0usize;
        let mut peer_addr_invalid = 0usize;
        let mut peer_client_build_fail = 0usize;
        let mut peer_rpc_success = 0usize;
        let mut peer_rpc_fail = 0usize;
        let mut epoch_mismatch = 0usize;
        let mut sig_invalid = 0usize;
        let mut member_index_overflow = 0usize;
        let group_total_weight: u64 = committee
            .iter()
            .map(|member| {
                member
                    .spools
                    .iter()
                    .filter(|&&s| group_for_spool(s) == group)
                    .count() as u64
            })
            .sum();
        let quorum_needed = tape_core::bft::min_correct(SPOOL_GROUP_SIZE as u64);

        for (idx, member) in committee.iter().enumerate() {
            if cancel.is_cancelled() {
                return TaskOutcome::Success;
            }
            members_considered += 1;

            let member_weight: u64 = member
                .spools
                .iter()
                .filter(|&&s| group_for_spool(s) == group)
                .count() as u64;
            if member_weight == 0 {
                members_no_weight += 1;
                continue;
            }

            let addr: std::net::SocketAddr = match member.network_address.to_socket_addr() {
                Ok(a) => a,
                Err(_) => {
                    peer_addr_invalid += 1;
                    continue;
                }
            };

            // Skip peers in cooldown
            if context.peer_health.lock().unwrap().is_cooling_down(&addr) {
                peer_rpc_fail += 1;
                continue;
            }

            let client = match tape_node_client::NodeClientBuilder::new().build(&addr.to_string()) {
                Ok(c) => c,
                Err(e) => {
                    peer_client_build_fail += 1;
                    tracing::debug!(epoch = target.0, group, member = idx, "snapshot peer client build failed: {e}");
                    continue;
                }
            };

            tracing::debug!(
                epoch = target.0,
                group,
                member = idx,
                timeout_secs = SNAPSHOT_SIGN_TIMEOUT.as_secs(),
                "snapshot sign request start"
            );
            let call_start = Instant::now();
            let resp = match tokio::time::timeout(
                SNAPSHOT_SIGN_TIMEOUT,
                client.get_snapshot_signature(target.0, group as u64),
            )
            .await
            {
                Ok(Ok(r)) => {
                    peer_rpc_success += 1;
                    context.peer_health.lock().unwrap().record_success(&addr);
                    tracing::debug!(
                        epoch = target.0,
                        group,
                        member = idx,
                        elapsed_ms = call_start.elapsed().as_millis() as u64,
                        "snapshot sign request success"
                    );
                    r
                }
                Ok(Err(e)) => {
                    peer_rpc_fail += 1;
                    context.peer_health.lock().unwrap().record_failure(&addr);
                    tracing::debug!(
                        epoch = target.0,
                        group,
                        member = idx,
                        elapsed_ms = call_start.elapsed().as_millis() as u64,
                        "snapshot sign request failed: {e}"
                    );
                    continue;
                }
                Err(_) => {
                    peer_rpc_fail += 1;
                    context.peer_health.lock().unwrap().record_failure(&addr);
                    tracing::warn!(
                        epoch = target.0,
                        group,
                        member = idx,
                        elapsed_ms = call_start.elapsed().as_millis() as u64,
                        timeout_secs = SNAPSHOT_SIGN_TIMEOUT.as_secs(),
                        "snapshot sign request timed out"
                    );
                    continue;
                }
            };

            if resp.epoch != target {
                epoch_mismatch += 1;
                tracing::warn!(member = idx, "epoch mismatch in sign response");
                continue;
            }

            let sig = resp.signature;
            let msg = SnapshotMessage::new(target, commitment.0).to_bytes();
            if sig.verify_aggregate(msg, &[member.bls_pubkey]).is_err() {
                sig_invalid += 1;
                tracing::warn!(member = idx, "invalid snapshot partial signature");
                continue;
            }

            let member_index = match u8::try_from(idx) {
                Ok(i) => i,
                Err(_) => {
                    member_index_overflow += 1;
                    tracing::warn!(member = idx, "committee index overflow");
                    continue;
                }
            };

            signatures.push(sig);
            member_indices.push(member_index);
            weight += member_weight;
            if tape_core::bft::is_supermajority(weight, SPOOL_GROUP_SIZE as u64) {
                break;
            }
        }

        let quorum = tape_core::bft::is_supermajority(weight, SPOOL_GROUP_SIZE as u64);
        tracing::info!(
            epoch = target.0,
            group,
            quorum,
            gathered_weight = weight,
            needed_weight = quorum_needed,
            group_total_weight,
            group_total_capacity = SPOOL_GROUP_SIZE,
            signatures = signatures.len(),
            members_considered,
            members_no_weight,
            peer_addr_invalid,
            peer_client_build_fail,
            peer_rpc_success,
            peer_rpc_fail,
            epoch_mismatch,
            sig_invalid,
            member_index_overflow,
            group_elapsed_ms = group_start.elapsed().as_millis() as u64,
            "snapshot certify group summary"
        );

        if !quorum {
            pending_quorum += 1;
            continue;
        }

        // Aggregate signatures
        let aggregated = match tape_core::bls::BlsSignature::aggregate(&signatures) {
            Ok(s) => s,
            Err(e) => {
                failed.push(format!("group {group}: aggregate sigs: {e:?}"));
                continue;
            }
        };

        // Store result
        let cert = tape_store::types::SnapshotCertResult {
            member_indices: member_indices.to_vec(),
            signature: aggregated,
            epoch: target.0,
        };

        if let Err(e) = context
            .store
            .set_snapshot_certification(target, chunk_index, cert)
        {
            failed.push(format!("group {group}: store cert: {e}"));
            continue;
        }

        certified_groups += 1;
        snapshot_progress_advance(&context, current, group as usize, GroupState::Certified);
    }

    if !failed.is_empty() {
        return TaskOutcome::Retryable(format!(
            "snapshot certify progress epoch={} certified={} pending_quorum={} attempted={} skipped_not_owner={} taken_over={} failed={} {}",
            target.0,
            certified_groups,
            pending_quorum,
            groups_attempted,
            groups_skipped_not_owner,
            groups_taken_over,
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
            groups_skipped_not_owner,
            groups_taken_over,
            "snapshot certify waiting for more partial signatures"
        );
        return TaskOutcome::Success;
    }

    tracing::info!(
        epoch = target.0,
        certified_groups,
        groups_attempted,
        groups_skipped_not_owner,
        groups_taken_over,
        "snapshot certification collected"
    );
    TaskOutcome::Success
}

/// Register snapshot commitments on-chain.
pub async fn run_register<S: Store, R: Rpc>(
    context: Arc<NodeContext<S, R>>,
    cancel: CancellationToken,
) -> TaskOutcome {
    use solana_sdk::compute_budget::ComputeBudgetInstruction;
    use solana_sdk::signer::Signer;

    let current = match context.store.get_current_epoch() {
        Ok(Some(e)) => e,
        Ok(None) => return TaskOutcome::Retryable("no current epoch".into()),
        Err(e) => return TaskOutcome::Retryable(format!("read epoch: {e}")),
    };

    if current.0 < 2 {
        return TaskOutcome::Retryable("snapshot onchain certify requires epoch >= 2".into());
    }

    let target = EpochNumber(current.0 - 1);
    snapshot_progress_reset_if_needed(&context, current);

    // Guard: build must have completed
    match context.store.get_snapshot_commitment(target, ChunkIndex(0)) {
        Ok(Some(_)) => {}
        Ok(None) => return TaskOutcome::Retryable("build not yet completed".into()),
        Err(e) => return TaskOutcome::Retryable(format!("check commitment: {e}")),
    }

    let pubkey = context.keypair.pubkey();

    // Leader schedule gating: stagger RegisterSnapshot across committee members.
    let committee: Vec<NodeInfo> = match context.store.get_committee(current) {
        Ok(Some(c)) => c,
        Ok(None) => return TaskOutcome::Retryable("no committee".into()),
        Err(e) => return TaskOutcome::Retryable(format!("get committee: {e}")),
    };
    let our_idx = match our_member_index(&committee, pubkey) {
        Ok(i) => i,
        Err(e) => return TaskOutcome::Retryable(e.into()),
    };
    let schedule = match context.store.get_epoch_nonce(current) {
        Ok(Some(n)) => Some(LeaderSchedule::new(committee.len(), n)),
        Ok(None) => None, // fallback: allow all nodes
        Err(e) => return TaskOutcome::Retryable(format!("get_epoch_nonce: {e}")),
    };
    let windows_elapsed = takeover_windows_elapsed(&context, current);

    for group in 0..SPOOL_GROUP_COUNT {
        if cancel.is_cancelled() {
            return TaskOutcome::Success;
        }

        if snapshot_progress_is_registered(&context, group) {
            continue;
        }

        let position = schedule
            .as_ref()
            .map(|s| s.position_for(our_idx, group))
            .unwrap_or(0);
        if position > windows_elapsed {
            continue;
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

        let cu_ix = ComputeBudgetInstruction::set_compute_unit_limit(SNAPSHOT_REGISTER_CU);
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

        match context
            .rpc
            .send_instructions(&context.keypair, vec![cu_ix, ix])
            .await
        {
            Ok(sig) => {
                tracing::info!(%sig, group, epoch = target.0, "register_snapshot submitted");
                snapshot_progress_advance(&context, current, group, GroupState::Registered);
            }
            Err(ref e) => {
                if parse_tape_error(e).map(|err| err.is_already_done()).unwrap_or(false)
                    || is_account_state_pending_error(&e.to_string())
                {
                    tracing::debug!(group, "snapshot chunk already registered");
                    snapshot_progress_advance(&context, current, group, GroupState::Registered);
                } else {
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
    cancel: CancellationToken,
) -> TaskOutcome {
    use solana_sdk::compute_budget::ComputeBudgetInstruction;
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
    snapshot_progress_reset_if_needed(&context, current);
    tracing::debug!(
        current_epoch = current.0,
        target_epoch = target.0,
        "run_certify_onchain start"
    );
    let pubkey = context.keypair.pubkey();

    // Need committee for bitmap reconstruction.
    let committee: Vec<NodeInfo> = match context.store.get_committee(current) {
        Ok(Some(c)) => c,
        Ok(None) => {
            tracing::debug!(
                current_epoch = current.0,
                "run_certify_onchain no committee in local store"
            );
            return TaskOutcome::Retryable("no committee".into());
        }
        Err(e) => return TaskOutcome::Retryable(format!("read committee: {e}")),
    };
    // Only submit certifications for groups we own.
    let our_groups: HashSet<u64> = match our_snapshot_groups(&committee, pubkey) {
        Ok(groups) => groups,
        Err(e) => return TaskOutcome::Retryable(e.into()),
    };
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

        let group_usize = group as usize;
        if !snapshot_progress_has_local_cert(&context, group_usize) {
            continue;
        }
        if snapshot_progress_is_done_onchain(&context, group_usize) {
            continue;
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

        // Reconstruct bitmap and signature
        let bitmap = tape_api::program::tapedrive::CommitteeBitmap::from_indices(
            &cert
                .member_indices
                .iter()
                .map(|&i| i as usize)
                .collect::<Vec<_>>(),
            committee.len(),
        );
        let sig = cert.signature;

        let cu_ix = ComputeBudgetInstruction::set_compute_unit_limit(SNAPSHOT_CERTIFY_CU);
        let ix = tape_api::prelude::build_certify_snapshot_ix(
            pubkey,
            target,
            commitment,
            bitmap,
            sig,
        );

        match context
            .rpc
            .send_instructions(&context.keypair, vec![cu_ix, ix])
            .await
        {
            Ok(tx_sig) => {
                tracing::info!(%tx_sig, group, epoch = target.0, "certify_snapshot submitted");
                submitted += 1;
                snapshot_progress_advance(
                    &context,
                    current,
                    group_usize,
                    GroupState::CertifiedOnchain,
                );
            }
            Err(ref e) => {
                let err_text = e.to_string();
                if parse_tape_error(e).map(|err| err.is_already_done()).unwrap_or(false) {
                    tracing::debug!(group, "snapshot chunk already certified");
                    submitted += 1;
                    snapshot_progress_advance(
                        &context,
                        current,
                        group_usize,
                        GroupState::CertifiedOnchain,
                    );
                } else if is_account_state_pending_error(&err_text) {
                    // RegisterSnapshot for this chunk has not landed yet.
                    pending_register += 1;
                } else {
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

#[cfg(test)]
mod tests {
    use super::*;

    use bytemuck::Zeroable;
    use tape_api::program::tapedrive::node_pda;
    use tape_core::bls::{BlsPubkey, BlsSignature};
    use tape_core::erasure::SPOOL_GROUP_COUNT;
    use tape_core::snapshot::ReplayableEvent;
    use tape_core::types::SlotNumber;
    use tape_crypto::Hash;
    use tape_crypto::bls12254::min_sig::G1CompressedPoint;
    use tape_store::types::{NodeInfo, SnapshotCertResult};

    use crate::test_util::test_context;

    #[tokio::test]
    async fn build_waits_epoch2() {
        let ctx = test_context();
        ctx.store.set_current_epoch(EpochNumber(1)).unwrap();

        let cancel = CancellationToken::new();
        let result = run_build(ctx, cancel).await;
        assert!(matches!(result, TaskOutcome::Retryable(_)));
    }

    #[tokio::test]
    async fn build_empty_epoch() {
        let ctx = test_context();
        let target = EpochNumber(2);
        ctx.store.set_current_epoch(EpochNumber(3)).unwrap();

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

    #[test]
    fn progress_advance_ignores_stale_epoch() {
        let ctx = test_context();
        ctx.snapshot_progress
            .lock()
            .unwrap()
            .reset(EpochNumber(4));

        snapshot_progress_advance(&ctx, EpochNumber(3), 0, GroupState::CertifiedOnchain);
        assert_eq!(
            ctx.snapshot_progress.lock().unwrap().get(0),
            GroupState::Pending
        );
    }

    #[tokio::test]
    async fn certify_rebuilds_progress_and_skips_already_certified_groups() {
        let ctx = test_context();
        let current = EpochNumber(3);
        let target = EpochNumber(2);
        ctx.store.set_current_epoch(current).unwrap();

        let (node_address, _) = node_pda(ctx.keypair.pubkey());
        ctx.store
            .put_committee(
                current,
                vec![NodeInfo {
                    node_address: tape_store::types::Pubkey::new(node_address.to_bytes()),
                    bls_pubkey: BlsPubkey::zeroed(),
                    tls_pubkey: tape_store::types::Pubkey::new([0u8; 32]),
                    network_address: tape_core::types::network::NetworkAddress::new_ipv4(
                        [127, 0, 0, 1],
                        8000,
                    ),
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
        let result = run_certify(ctx.clone(), cancel).await;
        assert!(matches!(result, TaskOutcome::Success));
        assert_eq!(
            ctx.snapshot_progress.lock().unwrap().get(group as usize),
            GroupState::Certified
        );
    }
}
