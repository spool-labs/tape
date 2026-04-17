use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use rpc::Rpc;
use store::Store;
use tape_blocks::ParsedInstruction;
use tape_core::snapshot::types::SnapshotState;
use tape_core::spooler::SpoolGroup;
use tape_core::types::{ChunkNumber, EpochNumber};
use tape_crypto::address::Address;
use tape_protocol::Api;
use tape_store::ops::SnapshotOps;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::chain::reserve_snapshot::submit_reserve_snapshot;
use crate::chain::sign_snapshot::submit_sign_snapshot;
use crate::chain::write_snapshot::submit_write_snapshot;
use crate::context::NodeContext;
use crate::core::error::NodeError;
use crate::core::types::ChannelName;
use crate::features::block::ingestor::ParsedBlock;
use crate::features::snapshot::build::build_local_snapshot_epoch;
use crate::features::snapshot::fanout;
use crate::features::snapshot::gc;
use crate::features::snapshot::quorum::{aggregate_finalize_quorum, aggregate_write_quorum};

const SNAPSHOT_HEARTBEAT: Duration = Duration::from_secs(1);

pub struct SnapshotManager<Db: Store, Cluster: Api, Blockchain: Rpc> {
    context: Arc<NodeContext<Db, Cluster, Blockchain>>,
    snapshot_rx: mpsc::Receiver<Arc<ParsedBlock>>,
    cancel: CancellationToken,
    cycle_cancel: CancellationToken,
    fanout_task: Option<JoinHandle<()>>,
    active_epoch: Option<EpochNumber>,
    signed_groups: HashSet<SpoolGroup>,
}

impl<Db: Store + 'static, Cluster: Api + 'static, Blockchain: Rpc + 'static>
    SnapshotManager<Db, Cluster, Blockchain>
{
    pub fn new(
        context: Arc<NodeContext<Db, Cluster, Blockchain>>,
        snapshot_rx: mpsc::Receiver<Arc<ParsedBlock>>,
        cancel: CancellationToken,
    ) -> Self {
        let cycle_cancel = cancel.child_token();
        Self {
            context,
            snapshot_rx,
            cancel,
            cycle_cancel,
            fanout_task: None,
            active_epoch: None,
            signed_groups: HashSet::new(),
        }
    }

    pub async fn run(mut self) -> Result<(), NodeError> {
        let mut heartbeat = tokio::time::interval(SNAPSHOT_HEARTBEAT);

        loop {
            tokio::select! {
                _ = self.cancel.cancelled() => {
                    self.stop_cycle().await;
                    return Ok(());
                }
                received = self.snapshot_rx.recv() => {
                    let Some(block) = received else {
                        return if self.cancel.is_cancelled() {
                            Ok(())
                        } else {
                            Err(NodeError::ChannelClosed { channel: ChannelName::SnapshotManager })
                        };
                    };
                    self.handle_block(block).await?;
                }
                _ = heartbeat.tick() => {
                    if let Some(epoch) = self.active_epoch {
                        self.submit_ready(epoch).await?;
                    }
                }
            }
        }
    }

    async fn handle_block(&mut self, block: Arc<ParsedBlock>) -> Result<(), NodeError> {
        for ix in &block.instructions {
            match ix {
                ParsedInstruction::AdvanceEpoch { event } => {
                    self.on_advance_epoch(event.old_epoch).await?;
                }
                ParsedInstruction::ReserveSnapshot { event } => {
                    self.on_snapshot_reserved(event.epoch).await?;
                }
                ParsedInstruction::WriteSnapshot {
                    group,
                    chunk,
                    event,
                    ..
                } => {
                    self.on_snapshot_written(event.epoch, *group, *chunk, event.track)
                        .await?;
                }
                ParsedInstruction::SignSnapshot { event } => {
                    self.on_snapshot_signed(event.epoch, event.group, event.state).await?;
                }
                _ => {}
            }
        }
        Ok(())
    }

    async fn on_advance_epoch(&mut self, snapshot_epoch: EpochNumber) -> Result<(), NodeError> {
        self.stop_cycle().await;
        self.signed_groups.clear();
        gc::clear_stale_snapshot_epochs(&self.context, snapshot_epoch)?;

        match submit_reserve_snapshot(&self.context, snapshot_epoch).await {
            Ok(txid) => {
                info!(epoch = snapshot_epoch.0, ?txid, "snapshot: reserve submitted");
            }
            Err(error) => {
                debug!(?error, epoch = snapshot_epoch.0, "snapshot: reserve raced / already exists");
            }
        }

        self.cycle_cancel = self.cancel.child_token();
        Ok(())
    }

    async fn on_snapshot_reserved(&mut self, epoch: EpochNumber) -> Result<(), NodeError> {
        let summary = build_local_snapshot_epoch(&self.context, epoch)?;
        if summary.groups == 0 {
            debug!(epoch = epoch.0, "snapshot: no local groups to build");
            return Ok(());
        }

        self.active_epoch = Some(epoch);
        self.start_fanout(epoch);
        self.submit_ready(epoch).await?;

        info!(
            epoch = epoch.0,
            groups = summary.groups,
            chunks = summary.chunks,
            "snapshot: local build complete"
        );
        Ok(())
    }

    async fn on_snapshot_written(
        &mut self,
        epoch: EpochNumber,
        group: SpoolGroup,
        chunk: ChunkNumber,
        track: Address,
    ) -> Result<(), NodeError> {
        gc::flush_snapshot_write(&self.context, epoch, group, chunk, track)?;
        if self.active_epoch == Some(epoch) {
            self.submit_ready(epoch).await?;
        }
        Ok(())
    }

    async fn on_snapshot_signed(
        &mut self,
        epoch: EpochNumber,
        group: SpoolGroup,
        state: u64,
    ) -> Result<(), NodeError> {
        if self.active_epoch == Some(epoch) {
            self.signed_groups.insert(group);
        }

        if SnapshotState::try_from(state) == Ok(SnapshotState::Finalized) {
            gc::finalize_snapshot_epoch(&self.context, epoch)?;
            self.stop_cycle().await;
            info!(epoch = epoch.0, "snapshot: epoch finalized");
        }

        Ok(())
    }

    async fn submit_ready(&self, epoch: EpochNumber) -> Result<(), NodeError> {
        for group in local_groups(&self.context) {
            let artifacts = self
                .context
                .store
                .iter_snapshot_artifacts(epoch, group)
                .map_err(|e| NodeError::Store(format!("iter_snapshot_artifacts({epoch},{group}): {e}")))?;

            for (chunk, artifact) in &artifacts {
                if artifact.is_written() {
                    continue;
                }

                let Some(quorum) = aggregate_write_quorum(&self.context, epoch, group, *chunk)? else {
                    continue;
                };
                match submit_write_snapshot(
                    &self.context,
                    epoch,
                    group,
                    *chunk,
                    quorum.bitmap,
                    quorum.signature,
                    &artifact.blob,
                )
                .await
                {
                    Ok(txid) => {
                        info!(epoch = epoch.0, group = group.0, chunk = chunk.0, ?txid, "snapshot write: submitted");
                    }
                    Err(error) => {
                        debug!(error = %error, epoch = epoch.0, group = group.0, chunk = chunk.0, "snapshot write: submit failed");
                    }
                }
            }

            if self.signed_groups.contains(&group) {
                continue;
            }

            let progress = self
                .context
                .store
                .snapshot_group_progress(epoch, group)
                .map_err(|e| NodeError::Store(format!("snapshot_group_progress({epoch},{group}): {e}")))?;
            if !progress.is_complete() {
                continue;
            }

            let Some(quorum) = aggregate_finalize_quorum(&self.context, epoch, group)? else {
                continue;
            };
            match submit_sign_snapshot(&self.context, epoch, group, quorum.bitmap, quorum.signature)
                .await
            {
                Ok(txid) => {
                    info!(epoch = epoch.0, group = group.0, ?txid, "snapshot finalize: submitted");
                }
                Err(error) => {
                    debug!(error = %error, epoch = epoch.0, group = group.0, "snapshot finalize: submit failed");
                }
            }
        }

        Ok(())
    }

    fn start_fanout(&mut self, epoch: EpochNumber) {
        self.cycle_cancel.cancel();
        if let Some(task) = self.fanout_task.take() {
            task.abort();
        }

        self.cycle_cancel = self.cancel.child_token();
        let cancel = self.cycle_cancel.clone();
        let ctx = self.context.clone();
        self.fanout_task = Some(tokio::spawn(async move {
            fanout::run(ctx, epoch, cancel).await;
        }));
    }

    async fn stop_cycle(&mut self) {
        self.cycle_cancel.cancel();
        if let Some(task) = self.fanout_task.take() {
            match task.await {
                Ok(()) => {}
                Err(error) if error.is_cancelled() => {}
                Err(error) => warn!(?error, "snapshot fanout task failed"),
            }
        }
        self.active_epoch = None;
    }
}

fn local_groups<Db: Store, Cluster: Api, Blockchain: Rpc>(
    context: &NodeContext<Db, Cluster, Blockchain>,
) -> Vec<SpoolGroup> {
    let mut groups: Vec<_> = context.my_spools().into_iter().map(SpoolGroup::of).collect();
    groups.sort_unstable_by_key(|group| group.0);
    groups.dedup_by_key(|group| group.0);
    groups
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

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
