use std::sync::Arc;

use rpc::Rpc;
use tape_protocol::Api;
use store::Store;
use tape_api::program::tapedrive::snapshot_pda;
use tape_core::encoding::ClayParams;
use tape_core::erasure::SPOOL_GROUP_COUNT;
use tape_core::spooler::SpoolGroup;
use tape_core::snapshot::SnapshotLog;
use tape_crypto::hash::hashv;
use tape_slicer::DEFAULT_K_OUTER;
use tape_store::ops::MetaOps;
use tape_store::types::Pubkey;
use tokio_util::sync::CancellationToken;

use crate::fsm::Fsm;
use crate::core::NodeContext;
use crate::snapshot::{
    collect_group_slices, decode_group, decode_outer, fetch_commitments, load_snapshot_task_context,
    missing_state, skip_if_cancelled, SnapshotNeed,
};
use crate::TaskOutcome;

pub async fn run_bootstrap<Db: Store, Cluster: Api, Blockchain: Rpc>(
    context: Arc<NodeContext<Db, Cluster, Blockchain>>,
    cancel: CancellationToken,
) -> TaskOutcome {
    if let Some(outcome) = skip_if_cancelled(&cancel) {
        return outcome;
    }

    let snapshot = match load_snapshot_task_context(&context, SnapshotNeed::AllowMissing, false) {
        Ok(snapshot) => snapshot,
        Err(outcome) => return outcome,
    };

    let current_chain_epoch = snapshot.current_chain_epoch;
    let local_epoch = snapshot.local_epoch;

    if let Ok(Some(cursor)) = context.store.get_sync_cursor() {
        if cursor.0 > 0 {
            let should_skip = match context.store.get_bootstrap_target_epoch() {
                Ok(Some(marked)) if marked == local_epoch => true,
                Ok(Some(marked)) => {
                    tracing::info!(
                        current_epoch = local_epoch.0,
                        bootstrapped_epoch = marked.0,
                        "sync cursor exists but marker is for a different snapshot epoch; retrying bootstrap",
                    );
                    false
                }
                Ok(None) => {
                    tracing::warn!(
                        current_epoch = local_epoch.0,
                        "sync cursor is set but bootstrap snapshot marker is missing; re-running bootstrap",
                    );
                    false
                }
                Err(err) => {
                    tracing::warn!(
                        current_epoch = local_epoch.0,
                        error = %err,
                        "failed to read bootstrap snapshot marker; re-running bootstrap",
                    );
                    false
                }
            };

            if should_skip {
                return TaskOutcome::Success;
            }
        }
    }

    if snapshot.committee_len == 0 {
        return missing_state("snapshot bootstrap committee not available");
    }

    let protocol_state = context.state();

    let commitments = match fetch_commitments(
        &context,
        &protocol_state,
        local_epoch,
    )
    .await
    {
        Ok(commitments) => commitments,
        Err(outcome) => return outcome,
    };

    let clay_k = ClayParams::default().k() as usize;
    let mut decoded_chunks: Vec<Option<(usize, Vec<u8>)>> = vec![None; SPOOL_GROUP_COUNT];
    let mut successful_chunks = 0usize;

    for group in 0..SPOOL_GROUP_COUNT {
        if let Some(outcome) = skip_if_cancelled(&cancel) {
            return outcome;
        }

        let commitment = commitments[group];
        let (track_pda, _) = snapshot_pda(local_epoch, commitment);
        let track_addr = Pubkey::new(track_pda.to_bytes());

        let slices = match collect_group_slices(
            &context,
            &protocol_state,
            SpoolGroup(group as u64),
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
            Err(error) => tracing::debug!(group, "inner decode failed: {error}"),
        }
    }

    if successful_chunks < DEFAULT_K_OUTER {
        return TaskOutcome::Retryable(format!(
            "only decoded {successful_chunks}/{DEFAULT_K_OUTER} chunks"
        ));
    }

    if let Some(outcome) = skip_if_cancelled(&cancel) {
        return outcome;
    }

    let decoded = match decode_outer(decoded_chunks) {
        Ok(d) => d,
        Err(err) => return TaskOutcome::Retryable(format!("outer decode: {err}")),
    };

    let log: SnapshotLog = match wincode::deserialize(&decoded) {
        Ok(log) => log,
        Err(err) => return TaskOutcome::Retryable(format!("deserialize log: {err}")),
    };

    let fsm = Fsm::new(context.clone());
    if let Err(err) = fsm.replay_snapshot(&log) {
        return TaskOutcome::Retryable(format!("replay: {err}"));
    }

    tracing::info!(
        current_chain_epoch = current_chain_epoch.0,
        local_epoch = local_epoch.0,
        end_slot = log.end_slot.0,
        entries = log.entries.len(),
        "snapshot bootstrap complete"
    );

    let pre_erasure_hash = hashv(&[decoded.as_slice()]);
    tracing::debug!(epoch = local_epoch.0, ?pre_erasure_hash, "snapshot bootstrap hash");

    if let Err(err) = context.store.set_bootstrap_target_epoch(local_epoch) {
        tracing::warn!(
            epoch = local_epoch.0,
            error = %err,
            "failed to persist bootstrap snapshot marker"
        );
    }

    TaskOutcome::Success
}
