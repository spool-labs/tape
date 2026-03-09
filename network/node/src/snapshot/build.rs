//! Snapshot build task.

use std::collections::HashSet;
use std::sync::Arc;

use rpc::Rpc;
use tape_protocol::Api;
use store::Store;
use tape_api::program::tapedrive::snapshot_pda;
use tape_store::ops::{EventLogOps, MetaOps, SliceOps, SpoolOps};
use tape_crypto::hash::hashv;
use tape_crypto::merkle::hash_leaf;
use tape_core::cert::snapshot::SnapshotMessage;
use tape_core::encoding::ClayParams;
use tape_core::erasure::{group_for_spool, spool_for_slice, SPOOL_GROUP_COUNT, SPOOL_GROUP_SIZE};
use tape_core::spooler::SpoolGroup;
use tape_core::snapshot::SnapshotLog;
use tape_core::types::{ChunkIndex, EpochNumber, SlotNumber};
use tape_protocol::api::SnapshotSignatureSubmission;
use tape_slicer::{blob_merkle_root, ClayCoder, DEFAULT_K_OUTER, ErasureCoder, OuterCoder, Slicer};
use tape_protocol::state::ProtocolState;
use tape_store::types::{Pubkey, SnapshotChunkMeta, SnapshotPartialSignature};
use tokio_util::sync::CancellationToken;
use wincode;

use crate::core::NodeContext;
use crate::snapshot::{
    is_snapshot_build_complete, load_snapshot_task_context, skip_if_cancelled,
    SnapshotNeed,
};
use crate::TaskOutcome;

/// Build snapshot: serialize event log, outer RS encode into 50 chunks,
/// inner Clay encode each chunk into 20 slices, store commitments + slices.
pub async fn run_build<Db: Store, Cluster: Api, Blockchain: Rpc>(
    context: Arc<NodeContext<Db, Cluster, Blockchain>>,
    cancel: CancellationToken,
) -> TaskOutcome {
    if let Some(outcome) = skip_if_cancelled(&cancel) {
        return outcome;
    }

    let snapshot = match load_snapshot_task_context(&context, SnapshotNeed::RequireBuild, true) {
        Ok(snapshot) => snapshot,
        Err(outcome) => return outcome,
    };

    let current_chain_epoch = snapshot.current_chain_epoch;
    let local_epoch = snapshot.local_epoch;
    let our_member_index = snapshot.member_index.expect("member_index required for build");

    tracing::debug!(
        current_chain_epoch = current_chain_epoch.0,
        local_epoch = local_epoch.0,
        "snapshot build task started"
    );

    match is_snapshot_build_complete(&context, local_epoch) {
        Ok(true) => return TaskOutcome::Success,
        Ok(false) => {}
        Err(e) => return TaskOutcome::Retryable(format!("check build state: {e}")),
    }

    let entries = match context.store.get_epoch_events(local_epoch) {
        Ok(e) => e,
        Err(e) => return TaskOutcome::Retryable(format!("read events: {e}")),
    };
    let event_count: usize = entries.iter().map(|entry| entry.events.len()).sum();

    let (start_slot, end_slot) = match (entries.first(), entries.last()) {
        (Some(first), Some(last)) => (first.slot, last.slot),
        _ => (SlotNumber(0), SlotNumber(0)),
    };
    let log = SnapshotLog {
        version: 1,
        epoch: local_epoch,
        start_slot,
        end_slot,
        entries,
    };

    let serialized = match wincode::serialize(&log) {
        Ok(bytes) => bytes,
        Err(e) => return TaskOutcome::Retryable(format!("serialize log: {e}")),
    };

    let pre_erasure_hash = hashv(&[serialized.as_slice()]);
    tracing::info!(
        epoch = local_epoch.0,
        member_index = our_member_index,
        ?pre_erasure_hash,
        event_count,
        entry_count = log.entries.len(),
        snapshot_bytes = serialized.len(),
        "snapshot build pre-encoded payload hash"
    );

    let mut outer = OuterCoder::new(DEFAULT_K_OUTER);
    let chunks = match outer.encode(&serialized) {
        Ok(chunks) => chunks,
        Err(e) => return TaskOutcome::Retryable(format!("outer encode: {e}")),
    };

    let owned_spools: HashSet<u16> = match context.store.iter_all_spools() {
        Ok(spools) => spools.into_iter().map(|(id, _)| id).collect(),
        Err(e) => return TaskOutcome::Retryable(format!("read spools: {e}")),
    };

    let our_groups: HashSet<SpoolGroup> = snapshot.owned_groups;

    for group in 0..SPOOL_GROUP_COUNT {
        if let Some(outcome) = skip_if_cancelled(&cancel) {
            return outcome;
        }

        let chunk_data = &chunks[group];
        let chunk_index = ChunkIndex(group as u64);

        let mut slicer = Slicer::new(ClayCoder::from_params(ClayParams::default()));
        slicer.set_chunk_index(group as u64);

        let slices: Vec<Vec<u8>> = match slicer.encode(chunk_data) {
            Ok(slices) => slices,
            Err(e) => return TaskOutcome::Retryable(format!("inner encode group {group}: {e}")),
        };

        let commitment = blob_merkle_root(&slices);
        let leaves: Vec<tape_crypto::Hash> = slices.iter().map(|s| hash_leaf(s)).collect();

        if let Err(e) = context
            .store
            .set_snapshot_commitment(local_epoch, chunk_index, commitment)
        {
            return TaskOutcome::Retryable(format!("store commitment: {e}"));
        }

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
            .set_snapshot_metadata(local_epoch, chunk_index, meta)
        {
            return TaskOutcome::Retryable(format!("store metadata: {e}"));
        }

        let track_addr = {
            let (pda, _) = snapshot_pda(local_epoch, commitment);
            Pubkey::new(pda.to_bytes())
        };

        for slice_idx in 0..SPOOL_GROUP_SIZE {
            let spool = spool_for_slice(SpoolGroup(group as u64), slice_idx);
            if owned_spools.contains(&spool) {
                if let Err(e) = context
                    .store
                    .put_slice(spool, track_addr, slices[slice_idx].clone())
                {
                    return TaskOutcome::Retryable(format!("put slice: {e}"));
                }
            }
        }

        let maybe_local_signature = if our_groups.contains(&SpoolGroup(group as u64)) {
            let message = SnapshotMessage::new(local_epoch, commitment.0).to_bytes();
            let signature = match context.bls_keypair.sign(&message) {
                Ok(signature) => signature,
                Err(e) => return TaskOutcome::Retryable(format!("bls sign snapshot: {e:?}")),
            };

            if let Err(e) = context.store.set_snapshot_partial_signature(
                local_epoch,
                group as u64,  // store API takes u64
                SnapshotPartialSignature {
                    member_index: our_member_index as u8,
                    signature,
                    epoch: local_epoch.0,
                },
            ) {
                return TaskOutcome::Retryable(format!("store snapshot partial: {e}"));
            }

            Some((group, our_member_index as u16, signature))
        } else {
            None
        };

        if let Some((group, member_index, signature)) = maybe_local_signature {
            let request = SnapshotSignatureSubmission {
                signature,
                member_index: member_index as u8,
                epoch: local_epoch,
            };

            let protocol_state = context.peer_manager.state();
            if let Err(err) = broadcast_snapshot_signature(
                &context,
                &protocol_state,
                member_index as usize,
                local_epoch,
                SpoolGroup(group as u64),
                &request,
            )
            .await
            {
                tracing::warn!(
                    epoch = local_epoch.0,
                    group,
                    "failed to broadcast snapshot signature: {err:?}"
                );
            }
        }
    }

    if let Err(e) = context.store.delete_epoch_events(local_epoch) {
        tracing::warn!(epoch = local_epoch.0, "failed to GC event log: {e}");
    }

    tracing::info!(
        current_chain_epoch = current_chain_epoch.0,
        local_epoch = local_epoch.0,
        chunks = SPOOL_GROUP_COUNT,
        "snapshot build complete"
    );
    TaskOutcome::Success
}

async fn broadcast_snapshot_signature<Db: Store, Cluster: Api, Blockchain: Rpc>(
    context: &Arc<NodeContext<Db, Cluster, Blockchain>>,
    state: &ProtocolState,
    our_member_index: usize,
    local_epoch: EpochNumber,
    group: SpoolGroup,
    request: &SnapshotSignatureSubmission,
) -> Result<(), TaskOutcome> {
    let api = &context.api;

    for (member_index, member) in state.committee.iter().enumerate() {
        if member_index == our_member_index {
            continue;
        }

        let member_weight = state
            .member_spools(member_index)
            .iter()
            .filter(|&&spool| group_for_spool(spool) == group)
            .count();
        if member_weight == 0 {
            continue;
        }

        if !context.peer_manager.is_healthy(member.id) {
            continue;
        }

        let req = tape_protocol::api::PutSnapshotReq {
            epoch: local_epoch,
            chunk_index: group.0,
            submission: request.clone(),
        };
        match api.put_snapshot(member.id, &req).await {
            Ok(_) => {
                context.peer_manager.report_success(member.id);
            }
            Err(err) => {
                tracing::debug!(
                    epoch = local_epoch.0,
                    %group,
                    member = member_index,
                    "snapshot signature post failed: {err}"
                );
                context.peer_manager.report_failure(member.id);
            }
        }
    }

    Ok(())
}
