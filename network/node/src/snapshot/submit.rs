use std::sync::Arc;

use rpc::Rpc;
use store::Store;
use tape_core::types::ChunkIndex;
use tape_store::ops::MetaOps;
use tokio_util::sync::CancellationToken;

use crate::chain::submit_certify;
use crate::runtime::NodeContext;
use crate::runtime::PeerHandle;
use crate::snapshot::{
    classify_submit_error, load_group_artifacts, load_snapshot_task_context, missing_state,
    skip_if_cancelled, SNAPSHOT_PENDING_DELAY, SnapshotNeed, SubmitClass,
};
use crate::supervisor::TaskOutcome;

/// Submit completed snapshot certifications on-chain.
pub async fn run_submit<S: Store, R: Rpc>(
    context: Arc<NodeContext<S, R>>,
    _peer_handle: PeerHandle,
    cancel: CancellationToken,
) -> TaskOutcome {
    if let Some(outcome) = skip_if_cancelled(&cancel) {
        return outcome;
    }

    let snapshot = match load_snapshot_task_context(&context, SnapshotNeed::AllowMissing, false) {
        Ok(snapshot) => snapshot,
        Err(outcome) => return outcome,
    };

    let local_epoch = snapshot.local_epoch;
    let current = snapshot.current_chain_epoch;
    let committee_len = snapshot.committee.len();
    let mut groups: Vec<_> = snapshot.owned_groups.into_iter().collect();
    groups.sort_unstable();

    tracing::debug!(
        current_epoch = current.0,
        local_epoch = local_epoch.0,
        group_count = groups.len(),
        "snapshot submit started"
    );

    let mut submitted = 0usize;
    let mut missing_local = 0usize;
    let mut pending_register = 0usize;
    let mut failed: Vec<String> = Vec::new();

    for &group in &groups {
        if let Some(outcome) = skip_if_cancelled(&cancel) {
            return outcome;
        }

        let chunk_index = ChunkIndex(group);
        let artifacts = match load_group_artifacts::<S, R>(&context, local_epoch, group) {
            Ok(artifacts) => artifacts,
            Err(e) => return missing_state(format!("snapshot submit read artifacts for group {group}: {e}")),
        };

        let cert = match artifacts.cert {
            Some(cert) => cert,
            None => {
                missing_local += 1;
                continue;
            }
        };

        let commitment = match artifacts.commitment {
            Some(commitment) => commitment,
            None => {
                return TaskOutcome::Permanent(format!(
                    "snapshot submit missing commitment for epoch {} group {}",
                    local_epoch.0, group
                ));
            }
        };

        match submit_certify(&context, committee_len, local_epoch, commitment, &cert).await {
            Ok(_tx_sig) => {
                tracing::info!(
                    group,
                    local_epoch = local_epoch.0,
                    "snapshot submit completed"
                );
                submitted += 1;
            }
            Err(ref e) => match classify_submit_error(e) {
                SubmitClass::Done => {
                    tracing::debug!(group, "snapshot group already submitted");
                    submitted += 1;
                }
                SubmitClass::Pending => {
                    pending_register += 1;
                }
                SubmitClass::Retryable => {
                    failed.push(format!("group {group}: {e}"));
                }
            },
        }
    }

    if !failed.is_empty() {
        return TaskOutcome::Retryable(format!(
            "snapshot submit progress epoch={} submitted={} missing_local={} pending_register={} failed={} {}",
            local_epoch.0,
            submitted,
            missing_local,
            pending_register,
            failed.len(),
            failed.first().cloned().unwrap_or_default()
        ));
    }

    if missing_local > 0 || pending_register > 0 {
        tracing::debug!(
            epoch = local_epoch.0,
            submitted,
            missing_local,
            pending_register,
            "snapshot submit waiting for local certs or register confirmations"
        );
        return TaskOutcome::Pending(SNAPSHOT_PENDING_DELAY);
    }

    let _ = context.store.delete_snapshot_metadata(local_epoch);
    let _ = context.store.delete_snapshot_cert(local_epoch);
    let _ = context.store.delete_snapshot_partial_signatures_for_epoch(local_epoch);

    tracing::info!(epoch = local_epoch.0, "snapshot submit complete");
    TaskOutcome::Success
}
