use std::sync::Arc;

use rpc::Rpc;
use store::Store;
use tape_core::types::EpochNumber;
use tape_protocol::Api;
use tape_store::ops::EventLogOps;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info};

use crate::core::config::SnapshotConfig;
use crate::core::context::NodeContext;
use crate::core::error::NodeError;

pub struct SnapshotManager<Db: Store, Cluster: Api, Blockchain: Rpc> {
    context: Arc<NodeContext<Db, Cluster, Blockchain>>,
    config: SnapshotConfig,
    cancel: CancellationToken,
}

impl<Db: Store, Cluster: Api, Blockchain: Rpc> SnapshotManager<Db, Cluster, Blockchain> {
    pub fn new(
        context: Arc<NodeContext<Db, Cluster, Blockchain>>,
        config: SnapshotConfig,
        cancel: CancellationToken,
    ) -> Self {
        Self {
            context,
            config,
            cancel,
        }
    }

    pub async fn run(self) -> Result<(), NodeError> {
        debug!(
            node_id = self.context.node_id().0,
            config = ?self.config,
            "snapshot manager started"
        );

        let mut observed_epoch = self.context.state().epoch;
        let mut next_snapshot_epoch = EpochNumber(1);

        self.schedule_closed_epochs(&mut next_snapshot_epoch, observed_epoch)?;

        loop {
            let target_epoch = observed_epoch.saturating_add(EpochNumber(1));
            let state = match self
                .context
                .state
                .wait_for_epoch(target_epoch, &self.cancel)
                .await
            {
                Ok(state) => state,
                Err(NodeError::StateUnavailable { expected_epoch })
                    if expected_epoch == target_epoch && self.cancel.is_cancelled() =>
                {
                    return Ok(());
                }
                Err(error) => return Err(error),
            };

            observed_epoch = state.epoch;
            self.schedule_closed_epochs(&mut next_snapshot_epoch, observed_epoch)?;
        }
    }

    fn schedule_closed_epochs(
        &self,
        next_snapshot_epoch: &mut EpochNumber,
        current_epoch: EpochNumber,
    ) -> Result<(), NodeError> {

        for epoch in pending_snapshot_epochs(*next_snapshot_epoch, current_epoch) {
            self.log_snapshot_epoch(epoch)?;

            let mut next = epoch;
            next.increment();
            *next_snapshot_epoch = next;
        }

        Ok(())
    }

    fn log_snapshot_epoch(&self, epoch: EpochNumber) -> Result<(), NodeError> {

        let has_events = self
            .context
            .store
            .has_epoch_events(epoch)
            .map_err(store_error)?;

        info!(
            node_id = self.context.node_id().0,
            snapshot_epoch = epoch.0,
            has_events,
            "snapshot build pending"
        );

        Ok(())
    }
}

fn pending_snapshot_epochs(
    next_snapshot_epoch: EpochNumber,
    current_epoch: EpochNumber,
) -> Vec<EpochNumber> {
    let Some(latest_closed_epoch) = latest_closed_snapshot_epoch(current_epoch) else {
        return Vec::new();
    };

    let mut epochs = Vec::new();
    let mut epoch = next_snapshot_epoch;
    while epoch <= latest_closed_epoch {
        epochs.push(epoch);
        epoch.increment();
    }

    epochs
}

fn latest_closed_snapshot_epoch(current_epoch: EpochNumber) -> Option<EpochNumber> {
    if current_epoch >= EpochNumber(2) {
        Some(current_epoch - EpochNumber(1))
    } else {
        None
    }
}

fn store_error(error: impl std::fmt::Display) -> NodeError {
    NodeError::Store(error.to_string())
}

#[cfg(test)]
mod tests {
    use tape_core::types::EpochNumber;

    use super::pending_snapshot_epochs;

    #[test]
    fn pending_snapshot_epochs_waits_for_epoch_two() {
        assert!(pending_snapshot_epochs(EpochNumber(1), EpochNumber(0)).is_empty());
        assert!(pending_snapshot_epochs(EpochNumber(1), EpochNumber(1)).is_empty());
    }

    #[test]
    fn pending_snapshot_epochs_catches_up_closed_epochs() {
        assert_eq!(
            pending_snapshot_epochs(EpochNumber(1), EpochNumber(2)),
            vec![EpochNumber(1)]
        );
        assert_eq!(
            pending_snapshot_epochs(EpochNumber(3), EpochNumber(5)),
            vec![EpochNumber(3), EpochNumber(4)]
        );
    }
}
