use std::sync::Arc;

use rpc::Rpc;
use store::Store;
use tape_core::types::EpochNumber;
use tape_protocol::{Api, ProtocolState};
use tape_store::ops::EventLogOps;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info};

use crate::context::NodeContext;
use crate::core::error::NodeError;

pub struct SnapshotManager<Db: Store, Cluster: Api, Blockchain: Rpc> {
    context: Arc<NodeContext<Db, Cluster, Blockchain>>,
    cancel: CancellationToken,
}

impl<Db: Store, Cluster: Api, Blockchain: Rpc> SnapshotManager<Db, Cluster, Blockchain> {
    pub fn new(
        context: Arc<NodeContext<Db, Cluster, Blockchain>>,
        cancel: CancellationToken,
    ) -> Self {
        Self {
            context,
            cancel,
        }
    }

    pub async fn run(self) -> Result<(), NodeError> {
        debug!(
            node_id = self.context.node_id().0,
            "snapshot manager started"
        );

        let mut observed_epoch = self.context.state().epoch;
        let mut last_logged_epoch = None;

        self.schedule_snapshot_if_ready(self.context.state().as_ref(), &mut last_logged_epoch)?;

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
            self.schedule_snapshot_if_ready(state.as_ref(), &mut last_logged_epoch)?;
        }
    }

    fn schedule_snapshot_if_ready(
        &self,
        state: &ProtocolState,
        last_logged_epoch: &mut Option<EpochNumber>,
    ) -> Result<(), NodeError> {
        let Some(snapshot_epoch) = snapshot_epoch_to_build(state, self.context.node_id()) else {
            return Ok(());
        };

        if last_logged_epoch == &Some(snapshot_epoch) {
            return Ok(());
        }

        self.log_snapshot_epoch(snapshot_epoch)?;
        *last_logged_epoch = Some(snapshot_epoch);

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

fn snapshot_epoch_to_build(
    state: &ProtocolState,
    node_id: tape_core::types::NodeId,
) -> Option<EpochNumber> {
    if state.epoch < EpochNumber(2) {
        return None;
    }

    if state.find_member(node_id).is_none() {
        return None;
    }

    Some(state.epoch - EpochNumber(1))
}

fn store_error(error: impl std::fmt::Display) -> NodeError {
    NodeError::Store(error.to_string())
}

#[cfg(test)]
mod tests {
    use bytemuck::Zeroable;
    use tape_core::system::CommitteeMember;
    use tape_core::types::{EpochNumber, NodeId};
    use tape_protocol::ProtocolState;

    use super::snapshot_epoch_to_build;

    #[test]
    fn waits_epoch_two() {
        let mut state = ProtocolState::default();
        state.epoch = EpochNumber(0);

        assert_eq!(snapshot_epoch_to_build(&state, NodeId(7)), None);

        state.epoch = EpochNumber(1);
        assert_eq!(snapshot_epoch_to_build(&state, NodeId(7)), None);
    }

    #[test]
    fn needs_committee() {
        let mut state = ProtocolState::default();
        state.epoch = EpochNumber(4);

        assert_eq!(snapshot_epoch_to_build(&state, NodeId(7)), None);

        let mut member = CommitteeMember::zeroed();
        member.id = NodeId(7);
        state.committee.push(member);

        assert_eq!(
            snapshot_epoch_to_build(&state, NodeId(7)),
            Some(EpochNumber(3))
        );
    }
}
