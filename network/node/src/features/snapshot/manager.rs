use std::sync::Arc;

use rpc::Rpc;
use store::Store;
use tape_protocol::{Api, ProtocolState};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

use tape_core::snapshot::info::SnapshotStatus;
use tape_core::types::EpochNumber;
use tape_store::ops::SnapshotOps;

use crate::chain::submit_init_snapshot_epoch;
use crate::context::NodeContext;
use crate::core::chain_tx::{TxOutcome, classify_tx};
use crate::core::error::NodeError;
use crate::core::types::ChannelName;
use crate::features::block::ingestor::ParsedBlock;
use crate::features::snapshot::build::build_snapshot_epoch;
use crate::features::snapshot::certify::certify_snapshot_groups;
use crate::features::snapshot::finalize::try_finalize_snapshot;
use crate::features::snapshot::observe::{observe_block, observe_state};

pub struct SnapshotManager<Db: Store, Cluster: Api, Blockchain: Rpc> {
    context: Arc<NodeContext<Db, Cluster, Blockchain>>,
    snapshot_rx: mpsc::Receiver<Arc<ParsedBlock>>,
    cancel: CancellationToken,
}

impl<Db: Store, Cluster: Api, Blockchain: Rpc> SnapshotManager<Db, Cluster, Blockchain> {
    pub fn new(
        context: Arc<NodeContext<Db, Cluster, Blockchain>>,
        snapshot_rx: mpsc::Receiver<Arc<ParsedBlock>>,
        cancel: CancellationToken,
    ) -> Self {
        Self {
            context,
            snapshot_rx,
            cancel,
        }
    }

    pub async fn run(mut self) -> Result<(), NodeError> {
        debug!(
            node_id = self.context.node_id().0,
            "snapshot manager started"
        );

        let mut state_rx = self.context.subscribe_state();

        loop {
            tokio::select! {
                _ = self.cancel.cancelled() => return Ok(()),
                changed = state_rx.changed() => {
                    if changed.is_err() {
                        return self.channel_closed_or_cancelled(ChannelName::SnapshotManager);
                    }

                    let state = state_rx.borrow().clone();
                    self.handle_state(state).await?;
                }
                received = self.snapshot_rx.recv() => {
                    let Some(block) = received else {
                        return self.channel_closed_or_cancelled(ChannelName::SnapshotManager);
                    };

                    self.handle_block(block).await?;
                }
            }
        }
    }

    async fn handle_state(&self, state: Arc<ProtocolState>) -> Result<(), NodeError> {
        observe_state(&self.context, state.clone()).await?;

        if state.epoch >= EpochNumber(2) {
            let snapshot_epoch = state.epoch.saturating_sub(EpochNumber(1));
            self.maybe_build(snapshot_epoch).await?;
            self.maybe_advance(snapshot_epoch).await?;
        }

        Ok(())
    }

    async fn maybe_build(&self, snapshot_epoch: EpochNumber) -> Result<(), NodeError> {
        let epoch_info = self
            .context
            .store
            .get_snapshot_info(snapshot_epoch)
            .map_err(|e| NodeError::Store(format!("get_snapshot_info({snapshot_epoch}): {e}")))?;

        let Some(info) = epoch_info else {
            return Ok(());
        };

        if info.status != SnapshotStatus::Pending {
            return Ok(());
        }

        build_snapshot_epoch(&self.context, snapshot_epoch).await
    }

    async fn handle_block(&self, block: Arc<ParsedBlock>) -> Result<(), NodeError> {
        observe_block(&self.context, block).await?;

        let state = self.context.state();
        if state.epoch >= EpochNumber(2) {
            let snapshot_epoch = state.epoch.saturating_sub(EpochNumber(1));
            self.maybe_advance(snapshot_epoch).await?;
        }

        Ok(())
    }

    /// Drives the snapshot state machine forward based on current epoch status.
    ///
    /// Called after both state changes and block observations. Submission
    /// errors are logged but do not propagate -- the next event re-triggers
    /// this method for automatic retry.
    async fn maybe_advance(&self, snapshot_epoch: EpochNumber) -> Result<(), NodeError> {
        let epoch_info = self
            .context
            .store
            .get_snapshot_info(snapshot_epoch)
            .map_err(|e| NodeError::Store(format!("get_snapshot_info({snapshot_epoch}): {e}")))?;

        let Some(info) = epoch_info else {
            return Ok(());
        };

        match info.status {
            SnapshotStatus::Pending | SnapshotStatus::Finalized => return Ok(()),
            SnapshotStatus::Built => {
                self.try_init(snapshot_epoch).await;
            }
            SnapshotStatus::Initialized | SnapshotStatus::PartiallyCertified => {
                certify_snapshot_groups(&self.context, snapshot_epoch).await?;
                try_finalize_snapshot(&self.context, snapshot_epoch).await?;
            }
        }

        Ok(())
    }

    /// Submits `InitSnapshotEpoch` for a built epoch.
    ///
    /// If another node already initialized the epoch, the resulting
    /// program error is treated as success.
    async fn try_init(&self, epoch: EpochNumber) {
        let result = submit_init_snapshot_epoch(&self.context, epoch).await;
        match classify_tx(result) {
            TxOutcome::Confirmed(txid) => {
                debug!(epoch = epoch.0, ?txid, "snapshot epoch init submitted");
            }
            TxOutcome::Program(error) if error.is_already_done() => {
                debug!(epoch = epoch.0, "snapshot epoch already initialized");
            }
            TxOutcome::Program(error) => {
                warn!(epoch = epoch.0, ?error, "init snapshot program error");
            }
            TxOutcome::Transport(error) => {
                warn!(epoch = epoch.0, ?error, "init snapshot transport error");
            }
        }
    }

    fn channel_closed_or_cancelled(&self, channel: ChannelName) -> Result<(), NodeError> {
        if self.cancel.is_cancelled() {
            Ok(())
        } else {
            Err(NodeError::ChannelClosed { channel })
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;
    use std::sync::Arc;

    use tokio::sync::mpsc;
    use tokio::time::timeout;
    use tokio_util::sync::CancellationToken;

    use super::SnapshotManager;
    use crate::context::test_utils::test_context;
    use crate::features::block::ingestor::ParsedBlock;

    #[tokio::test]
    async fn returns_when_cancelled() {
        let context = test_context();
        let (_tx, rx) = mpsc::channel::<Arc<ParsedBlock>>(1);

        let cancel = CancellationToken::new();
        let manager = SnapshotManager::new(context, rx, cancel.clone());
        cancel.cancel();

        timeout(Duration::from_secs(1), manager.run())
            .await
            .expect("manager completed in time")
            .expect("manager returned ok");
    }
}
