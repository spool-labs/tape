use std::sync::Arc;
use std::time::Duration;

use rpc::Rpc;
use store::Store;
use tape_api::event::{SnapshotSigned, SnapshotWritten};
use tape_core::snapshot::types::SnapshotState;
use tape_api::program::tapedrive::snapshot_tape_pda;
use tape_blocks::ParsedInstruction;
use tape_core::snapshot::chunk::snapshot_chunk_key;
use tape_core::spooler::SpoolGroup;
use tape_core::track::blob::BlobInfo;
use tape_core::track::types::{CompressedTrack, TrackKind, TrackState};
use tape_core::types::{ChunkNumber, EpochNumber};
use tape_protocol::Api;
use tape_store::ops::{EventLogOps, SliceOps, SnapshotOps, TrackOps};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info};

use crate::chain::submit_reserve_snapshot;
use crate::context::NodeContext;
use crate::core::error::NodeError;
use crate::core::types::ChannelName;
use crate::features::block::ingestor::ParsedBlock;
use crate::features::snapshot::build::build_snapshot;
use crate::features::snapshot::fanout::{fanout_finalize_sigs, fanout_write_sigs};
use crate::features::snapshot::submit::{submit_ready_finalizes, submit_ready_writes};
use crate::features::snapshot::utils::bitmap_index_in_group;

const SNAPSHOT_HEARTBEAT: Duration = Duration::from_secs(30);

pub struct SnapshotManager<Db: Store, Cluster: Api, Blockchain: Rpc> {
    context: Arc<NodeContext<Db, Cluster, Blockchain>>,
    block_rx: mpsc::Receiver<Arc<ParsedBlock>>,
    cancel: CancellationToken,
}

impl<Db, Cluster, Blockchain> SnapshotManager<Db, Cluster, Blockchain>
where
    Db: Store + 'static,
    Cluster: Api + 'static,
    Blockchain: Rpc + 'static,
{
    pub fn new(
        context: Arc<NodeContext<Db, Cluster, Blockchain>>,
        block_rx: mpsc::Receiver<Arc<ParsedBlock>>,
        cancel: CancellationToken,
    ) -> Self {
        Self {
            context,
            block_rx,
            cancel,
        }
    }

    pub async fn run(mut self) -> Result<(), NodeError> {
        let mut heartbeat = tokio::time::interval(SNAPSHOT_HEARTBEAT);

        loop {
            tokio::select! {
                _ = self.cancel.cancelled() => return Ok(()),
                received = self.block_rx.recv() => {
                    let Some(block) = received else {
                        return if self.cancel.is_cancelled() {
                            Ok(())
                        } else {
                            Err(NodeError::ChannelClosed { channel: ChannelName::SnapshotManager })
                        };
                    };
                    self.on_block(block).await?;
                }
                _ = heartbeat.tick() => {
                    self.on_heartbeat().await?;
                }
            }
        }
    }

    async fn on_block(&self, block: Arc<ParsedBlock>) -> Result<(), NodeError> {
        for ix in &block.instructions {
            match ix {
                ParsedInstruction::AdvanceEpoch { event } => {
                    self.on_advance_epoch(event.old_epoch, event.new_epoch).await?;
                }
                ParsedInstruction::ReserveSnapshot { event } => {
                    self.on_snapshot_reserved(event.epoch).await?;
                }
                ParsedInstruction::WriteSnapshot { group, chunk, blob, event, } => {
                    self.on_snapshot_written(*event, *group, *chunk, *blob).await?;
                }
                ParsedInstruction::SignSnapshot { event } => {
                    self.on_snapshot_signed(*event).await?;
                }
                _ => {}
            }
        }
        Ok(())
    }

    /// On `AdvanceEpoch`: we need to issue a `ReserveSnapshot` for the epoch that just closed
    async fn on_advance_epoch(
        &self,
        _old: EpochNumber,
        new: EpochNumber,
    ) -> Result<(), NodeError> {
        let snapshot_epoch = EpochNumber(new.0.saturating_sub(1));

        self.context
            .store
            .delete_snapshot_epochs_except(snapshot_epoch)
            .map_err(|e| NodeError::Store(format!("delete_snapshot_epochs_except: {e}")))?;

        match submit_reserve_snapshot(&self.context, snapshot_epoch).await {
            Ok(txid) => info!(epoch = snapshot_epoch.0, ?txid, "snapshot: reserve submitted"),
            Err(error) => {
                debug!(?error, epoch = snapshot_epoch.0, "snapshot: reserve raced / already exists")
            }
        }

        Ok(())
    }

    /// On `SignSnapshot` for a finalized epoch: the snapshot is now immutable and
    /// we can drop all artifacts, tracks, slices, and events for that epoch.
    async fn on_snapshot_signed(&self, event: SnapshotSigned) -> Result<(), NodeError> {
        if event.state != SnapshotState::Finalized as u64 {
            return Ok(());
        }

        self.context
            .store
            .delete_epoch_events(event.epoch)
            .map_err(|e| NodeError::Store(format!("delete_epoch_events: {e}")))?;

        debug!(epoch = event.epoch.0, "snapshot: epoch event log dropped");

        Ok(())
    }

    /// On `ReserveSnapshot`: build our local chunks, persist our partials,
    /// and push them to group peers.
    async fn on_snapshot_reserved(&self, epoch: EpochNumber) -> Result<(), NodeError> {
        build_snapshot(&self.context, epoch, &self.cancel).await?;
        fanout_write_sigs(&self.context, epoch, &self.cancel).await?;
        fanout_finalize_sigs(&self.context, epoch, &self.cancel).await?;
        Ok(())
    }

    /// On `WriteSnapshot`: if the chunk is one we staged and the submitted
    /// blob matches ours, persist the slice + catalog row. Otherwise drop the
    /// stale artifact.
    async fn on_snapshot_written(
        &self,
        event: SnapshotWritten,
        group: SpoolGroup,
        chunk: ChunkNumber,
        blob: BlobInfo,
    ) -> Result<(), NodeError> {
        let store = self.context.store.as_ref();

        let Some(artifact) = store
            .get_snapshot_artifact(event.epoch, group, chunk)
            .map_err(|e| NodeError::Store(format!("get_snapshot_artifact: {e}")))?
        else {
            return Ok(());
        };

        // Divergence: another submitter put a different blob on-chain. Drop
        // our artifact and walk away; the chain is the source of truth.
        if artifact.blob != blob {
            store
                .delete_snapshot_artifact(event.epoch, group, chunk)
                .map_err(|e| NodeError::Store(format!("delete_snapshot_artifact: {e}")))?;
            return Ok(());
        }

        let state = self.context.state();
        let our_node_id = self.context.node_id();

        let Some(my_index) = bitmap_index_in_group(&state, group, our_node_id) else {
            store
                .delete_snapshot_artifact(event.epoch, group, chunk)
                .map_err(|e| NodeError::Store(format!("delete_snapshot_artifact: {e}")))?;
            return Ok(());
        };

        let spool_index = group.base() + my_index;

        let track = CompressedTrack {
            tape: snapshot_tape_pda(event.epoch).0,
            key: snapshot_chunk_key(event.epoch, group, chunk),
            track_number: event.track_number,
            kind: TrackKind::Blob as u64,
            state: TrackState::Certified as u64,
            size: blob.size,
            spool_group: group,
            value_hash: blob.get_hash(),
        };

        store
            .put_track(event.track, track)
            .map_err(|e| NodeError::Store(format!("put_track: {e}")))?;
        store
            .put_slice(spool_index, event.track, artifact.local_slice)
            .map_err(|e| NodeError::Store(format!("put_slice: {e}")))?;
        store
            .delete_snapshot_artifact(event.epoch, group, chunk)
            .map_err(|e| NodeError::Store(format!("delete_snapshot_artifact: {e}")))?;

        Ok(())
    }

    /// Heartbeat tick: check our store for chunks/groups that have crossed
    /// the supermajority threshold and submit them, then re-push our own
    /// partials in case peers are still behind.
    async fn on_heartbeat(&self) -> Result<(), NodeError> {
        let state = self.context.state();
        if state.epoch.0 == 0 {
            return Ok(());
        }

        let epoch = EpochNumber(state.epoch.0 - 1);

        submit_ready_writes(&self.context, epoch, &self.cancel).await?;
        submit_ready_finalizes(&self.context, epoch, &self.cancel).await?;

        fanout_write_sigs(&self.context, epoch, &self.cancel).await?;
        fanout_finalize_sigs(&self.context, epoch, &self.cancel).await?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {}
