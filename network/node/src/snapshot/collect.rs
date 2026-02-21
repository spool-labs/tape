use std::sync::Arc;
use std::time::Instant;

use rpc::Rpc;
use store::Store;
use tape_core::bft::{is_supermajority, min_correct};
use tape_core::cert::snapshot::SnapshotMessage;
use tape_core::erasure::group_for_spool;
use tape_core::spooler::SpoolGroup;
use tape_core::types::{ChunkIndex, EpochNumber};
use tape_core::bls::BlsSignature;
use tape_store::types::{NodeInfo, SnapshotCertResult};
use tape_store::ops::MetaOps;
use tokio_util::sync::CancellationToken;

use crate::runtime::{NodeContext, PeerHandle};
use crate::snapshot::{
    load_snapshot_task_context, missing_state, skip_if_cancelled, SnapshotNeed, SNAPSHOT_PENDING_DELAY,
};
use crate::runtime::TaskOutcome;

/// Collect snapshot certifications from peer-submitted partial signatures.
pub async fn run_collect<S: Store, R: Rpc>(
    context: Arc<NodeContext<S, R>>,
    _peer_handle: PeerHandle,
    cancel: CancellationToken,
) -> TaskOutcome {
    if let Some(outcome) = skip_if_cancelled(&cancel) {
        return outcome;
    }

    let snapshot = match load_snapshot_task_context(&context, SnapshotNeed::RequireCertify, true) {
        Ok(snapshot) => snapshot,
        Err(outcome) => return outcome,
    };

    let local_epoch = snapshot.local_epoch;
    let our_member_index = snapshot.member_index.unwrap_or(0);

    tracing::trace!(
        current_chain_epoch = snapshot.current_chain_epoch.0,
        local_epoch = local_epoch.0,
        "snapshot collect started",
    );

    let mut groups: Vec<SpoolGroup> = snapshot.owned_groups.into_iter().collect();
    groups.sort_unstable();
    if !groups.is_empty() {
        let offset = our_member_index % groups.len();
        groups.rotate_left(offset);
    }

    let mut certified_groups = 0usize;
    let mut pending_quorum = 0usize;
    let mut groups_attempted = 0usize;
    let mut failed: Vec<String> = Vec::new();

    for group in groups {
        if let Some(outcome) = skip_if_cancelled(&cancel) {
            return outcome;
        }

        match context.store.get_snapshot_cert(local_epoch, ChunkIndex(group)) {
            Ok(Some(_)) => continue,
            Ok(None) => {}
            Err(e) => {
                return missing_state(format!(
                    "read cert for epoch={} group={group}: {e}",
                    local_epoch.0
                ));
            }
        }

        groups_attempted += 1;
        match certify_group(&context, &snapshot.committee, local_epoch, group, &cancel).await {
            Ok(GroupResult::Skip) => pending_quorum += 1,
            Ok(GroupResult::Pending) => pending_quorum += 1,
            Ok(GroupResult::Cert) => certified_groups += 1,
            Ok(GroupResult::Fail(err)) => failed.push(format!("group {group}: {err}")),
            Err(outcome) => return outcome,
        }
    }

    if !failed.is_empty() {
        return TaskOutcome::Retryable(format!(
            "snapshot collect progress epoch={} certified={} pending_quorum={} attempted={} failed={} {}",
            local_epoch.0,
            certified_groups,
            pending_quorum,
            groups_attempted,
            failed.len(),
            failed.first().cloned().unwrap_or_default()
        ));
    }

    if pending_quorum > 0 {
        tracing::debug!(
            epoch = local_epoch.0,
            certified_groups,
            pending_quorum,
            groups_attempted,
            "snapshot collect waiting for more partial signatures",
        );
        return TaskOutcome::Pending(SNAPSHOT_PENDING_DELAY);
    }

    tracing::info!(
        epoch = local_epoch.0,
        certified_groups,
        groups_attempted,
        "snapshot collect complete"
    );
    TaskOutcome::Success
}

#[allow(clippy::enum_variant_names)]
enum GroupResult {
    Skip,
    Pending,
    Cert,
    Fail(String),
}

async fn certify_group<S: Store, R: Rpc>(
    context: &Arc<NodeContext<S, R>>,
    committee: &[NodeInfo],
    local_epoch: EpochNumber,
    group: SpoolGroup,
    cancel: &CancellationToken,
) -> Result<GroupResult, TaskOutcome> {
    let group_start = Instant::now();
    let chunk_index = ChunkIndex(group);

    let commitment = match context.store.get_snapshot_commitment(local_epoch, chunk_index) {
        Ok(Some(commitment)) => commitment,
        Ok(None) => return Ok(GroupResult::Skip),
        Err(e) => return Err(TaskOutcome::Retryable(format!("read commitment: {e}"))),
    };

    let message = SnapshotMessage::new(local_epoch, commitment.0).to_bytes();
    let partials = match context.store.get_snapshot_partial_signatures(local_epoch, group as u64) {
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

        if partial.epoch != local_epoch.0 {
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

    let quorum = is_supermajority(gathered_weight, group_total_weight);
    if quorum {
        tracing::trace!(group, gathered_weight, group_total_weight, "snapshot collect reached quorum");
    }

    let quorum_needed = min_correct(group_total_weight);
    tracing::info!(
        epoch = local_epoch.0,
        group,
        quorum,
        gathered_weight,
        needed_weight = quorum_needed,
        group_total_weight,
        signatures = signatures.len(),
        group_elapsed_ms = group_start.elapsed().as_millis() as u64,
        "snapshot collect group summary"
    );

    if !quorum {
        return Ok(GroupResult::Pending);
    }
    if signatures.is_empty() || member_indices.is_empty() {
        return Ok(GroupResult::Pending);
    }

    if let Err(e) = store_group_cert(context, local_epoch, chunk_index, &signatures, &member_indices) {
        return Ok(GroupResult::Fail(format!("store cert: {e}")));
    }

    Ok(GroupResult::Cert)
}

fn store_group_cert<S: Store, R: Rpc>(
    context: &Arc<NodeContext<S, R>>,
    local_epoch: EpochNumber,
    chunk_index: ChunkIndex,
    signatures: &[BlsSignature],
    member_indices: &[u8],
) -> Result<(), String> {
    let signature =
        BlsSignature::aggregate(signatures).map_err(|e| format!("aggregate sigs: {e:?}"))?;

    let cert = SnapshotCertResult {
        member_indices: member_indices.to_vec(),
        signature,
        epoch: local_epoch.0,
    };

    context
        .store
        .set_snapshot_cert(local_epoch, chunk_index, cert)
        .map_err(|e| e.to_string())
}
