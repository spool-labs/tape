use std::sync::Arc;

use rpc::Rpc;
use store::Store;
use tape_core::spooler::SpoolGroup;
use tokio_util::sync::CancellationToken;

use crate::chain::submit_register;
use crate::runtime::NodeContext;
use crate::runtime::PeerHandle;
use crate::snapshot::{
    classify_submit_error, load_group_artifacts, load_snapshot_task_context, skip_if_cancelled,
    SnapshotNeed, SubmitClass,
};
use crate::runtime::TaskOutcome;

/// Register built snapshot chunks on-chain once local artifacts are ready.
pub async fn run_register<S: Store, R: Rpc>(
    context: Arc<NodeContext<S, R>>,
    _peer_handle: PeerHandle,
    cancel: CancellationToken,
) -> TaskOutcome {
    if let Some(outcome) = skip_if_cancelled(&cancel) {
        return outcome;
    }

    let snapshot = match load_snapshot_task_context(&context, SnapshotNeed::RequireRegister, false) {
        Ok(snapshot) => snapshot,
        Err(outcome) => return outcome,
    };

    let local_epoch = snapshot.local_epoch;
    let current = snapshot.current_chain_epoch;

    let mut groups: Vec<_> = snapshot.owned_groups.into_iter().collect();
    groups.sort_unstable();

    if groups.is_empty() {
        return TaskOutcome::Success;
    }

    for group in groups {
        if let Some(outcome) = skip_if_cancelled(&cancel) {
            return outcome;
        }

        let artifacts = match load_group_artifacts::<S, R>(&context, local_epoch, group) {
            Ok(artifacts) => artifacts,
            Err(e) => return TaskOutcome::Retryable(e),
        };

        let commitment = match artifacts.commitment {
            Some(commitment) => commitment,
            None => {
                continue;
            }
        };
        let metadata = match artifacts.metadata {
            Some(metadata) => metadata,
            None => {
                return TaskOutcome::Permanent(format!(
                    "snapshot register group {group} is missing metadata (local epoch {})",
                    local_epoch.0
                ));
            }
        };
        let group = group as SpoolGroup;
        let request = match submit_register(&context, local_epoch, group, commitment, &metadata).await {
            Ok(sig) => {
                tracing::info!(
                    %sig,
                    local_epoch = local_epoch.0,
                    group,
                    "snapshot register submitted"
                );
                continue;
            }
            Err(err) => classify_submit_error(&err),
        };

        match request {
            SubmitClass::Done => {
                tracing::debug!(group, "snapshot chunk already registered");
            }
            SubmitClass::Pending => {
                tracing::debug!(group, "snapshot register pending");
            }
            SubmitClass::Retryable => {
                return TaskOutcome::Retryable(format!(
                    "snapshot register group {group}: submit failed with retryable error"
                ))
            }
        }
    }

    tracing::info!(
        epoch = local_epoch.0,
        current = current.0,
        "snapshot register complete"
    );
    TaskOutcome::Success
}
