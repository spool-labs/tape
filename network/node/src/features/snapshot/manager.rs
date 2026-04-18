use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use rpc::Rpc;
use store::Store;
use tape_blocks::ParsedInstruction;
use tape_core::snapshot::types::SnapshotState;
use tape_core::spooler::SpoolGroup;
use tape_core::types::{ChunkNumber, EpochNumber};
use tape_crypto::Hash;
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

const SNAPSHOT_HEARTBEAT: Duration = Duration::from_secs(30);

type SnapshotWriteKey = (EpochNumber, SpoolGroup, ChunkNumber, Hash);
type SnapshotFinalizeKey = (EpochNumber, SpoolGroup);

pub struct SnapshotManager<Db: Store, Cluster: Api, Blockchain: Rpc> {
    context: Arc<NodeContext<Db, Cluster, Blockchain>>,
    snapshot_rx: mpsc::Receiver<Arc<ParsedBlock>>,
    active_epoch: Option<EpochNumber>,
    cancel: CancellationToken,
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
            active_epoch: None,
        }
    }

    pub async fn run(mut self) -> Result<(), NodeError> {
        let mut heartbeat = tokio::time::interval(SNAPSHOT_HEARTBEAT);

        loop {
            tokio::select! {
                _ = self.cancel.cancelled() => {
                    todo!();

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
                        todo!();
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
                ParsedInstruction::WriteSnapshot { group, chunk, event, .. } => {
                    self.on_snapshot_written(event.epoch, *group, *chunk, event.track).await?;
                }
                ParsedInstruction::SignSnapshot { event } => {
                    let state = SnapshotState::from(event.state);
                    self.on_snapshot_signed(event.epoch, event.group, state).await?;
                }
                _ => {}
            }
        }
        Ok(())
    }

    async fn on_advance_epoch(&mut self, snapshot_epoch: EpochNumber) -> Result<(), NodeError> {
        self.clear().await?;

        todo!();

        match submit_reserve_snapshot(&self.context, snapshot_epoch).await {
            Ok(txid) => {
                info!(epoch = snapshot_epoch.0, ?txid, "snapshot: reserve submitted");
            }
            Err(error) => {
                debug!(?error, epoch = snapshot_epoch.0, "snapshot: reserve raced / already exists");
            }
        }

        Ok(())
    }

    async fn on_snapshot_reserved(&mut self, epoch: EpochNumber) -> Result<(), NodeError> {
        self.build().await?;
        self.fanout().await?;

        todo!();

        Ok(())
    }

    async fn on_snapshot_written(
        &mut self,
        epoch: EpochNumber,
        group: SpoolGroup,
        chunk: ChunkNumber,
        track: Address,
    ) -> Result<(), NodeError> {

        todo!();

        Ok(())
    }

    async fn on_snapshot_signed(
        &mut self,
        epoch: EpochNumber,
        group: SpoolGroup,
        state: SnapshotState,
    ) -> Result<(), NodeError> {

        todo!();

        Ok(())
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
}
