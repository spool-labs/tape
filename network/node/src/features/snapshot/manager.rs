//! Drives the snapshot chunk build / post / finalize pipeline for each epoch.
//!
//! Consumes the parsed-block stream and reacts to the three on-chain
//! instructions that shape the snapshot lifecycle:
//!
//! - `AdvanceEpoch` — the previous epoch has advanced. Kick off local chunk
//!   build for every group this node has a spool in, and start collecting
//!   write signatures for those chunks.
//! - `WriteSnapshot` — a chunk has landed on-chain. If we have a matching
//!   cache entry, persist the one slice we computed for it into the regular
//!   `SliceCol` under the now-known track address. When every chunk in one
//!   of our groups has landed, start collecting finalize signatures for that
//!   group.
//! - `SignSnapshot` — a group's finalize signature posted. Once the on-chain
//!   snapshot is `Finalized` (all groups signed) we can drop any cached
//!   state for the epoch and gc its event log.
//!
//! **This is a scaffold.** The select loop and instruction dispatch are in
//! place; every reaction is currently a `trace!` + `TODO` so the manager
//! drains the snapshot channel without performing work. Filling these in is
//! the remaining task for the snapshot refactor.

use std::sync::Arc;

use rpc::Rpc;
use store::Store;
use tape_blocks::ParsedInstruction;
use tape_core::spooler::SpoolGroup;
use tape_core::types::{ChunkNumber, EpochNumber};
use tape_crypto::address::Address;
use tape_protocol::Api;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{debug, trace};

use crate::context::NodeContext;
use crate::core::error::NodeError;
use crate::core::types::ChannelName;
use crate::features::block::ingestor::ParsedBlock;

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

        loop {
            tokio::select! {
                _ = self.cancel.cancelled() => return Ok(()),
                received = self.snapshot_rx.recv() => {
                    let Some(block) = received else {
                        return if self.cancel.is_cancelled() {
                            Ok(())
                        } else {
                            Err(NodeError::ChannelClosed {
                                channel: ChannelName::SnapshotManager,
                            })
                        };
                    };
                    self.handle_block(block).await?;
                }
            }
        }
    }

    async fn handle_block(&self, block: Arc<ParsedBlock>) -> Result<(), NodeError> {
        for ix in &block.instructions {
            match ix {
                ParsedInstruction::AdvanceEpoch { event } => {
                    self.on_advance_epoch(event.old_epoch).await?;
                }
                ParsedInstruction::WriteSnapshot {
                    group,
                    chunk_index,
                    event,
                    ..
                } => {
                    self.on_snapshot_written(event.epoch, *group, *chunk_index, event.track)
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

    /// Reacts to the previous epoch advance
    ///
    /// For each group this node has a spool in, build the chunks deterministically
    /// from the local event log, stash them in `snapshot_cache`, and start
    /// per-chunk write-sig collection.
    async fn on_advance_epoch(&self, epoch: EpochNumber) -> Result<(), NodeError> {
        trace!(epoch = epoch.0, "snapshot: advance_epoch observed (stub)");
        // TODO:
        //   1. let my_groups = local_groups(&self.context) (dedup spools → SpoolGroups)
        //   2. if my_groups.is_empty() { return Ok(()) }
        //   3. let chunks = build_snapshot_epoch(&self.context.store, epoch)?;
        //   4. for each chunk where my_groups.contains(chunk.group):
        //        self.context.snapshot_cache.insert(key, blob, slices)
        //   5. spawn per-chunk write driver: collect 14/20 sigs, submit_write_snapshot.
        Ok(())
    }

    /// Reacts to a chunk landing on-chain.
    ///
    /// If this node built the chunk, persist the one slice it computed for
    /// its spool in the group into `SliceCol` under the on-chain track address,
    /// then drop the cache entry. If this write completes a local group, kick
    /// off finalize-sig collection for that group.
    async fn on_snapshot_written(
        &self,
        epoch: EpochNumber,
        group: SpoolGroup,
        chunk_index: ChunkNumber,
        track: Address,
    ) -> Result<(), NodeError> {
        trace!(
            epoch = epoch.0,
            group = group.0,
            chunk = chunk_index.0,
            ?track,
            "snapshot: write observed (stub)"
        );
        // TODO:
        //   1. let key = ChunkKey::new(epoch, group, chunk_index);
        //   2. let Some(slices) = self.context.snapshot_cache.mark_posted(&key, track) else { return Ok(()) };
        //   3. find this node's single spool in `group` (state.group_peers filter)
        //      let my_slice_idx = group.slice_of(my_spool)? ;
        //      self.context.store.put_slice(my_spool, track, slices[my_slice_idx].take())?;
        //   4. self.context.snapshot_cache.drop_chunk(&key);
        //   5. if self.context.snapshot_cache.group_progress(epoch, group).is_complete():
        //        spawn finalize driver: collect 14/20 sigs on SnapshotSignMessage,
        //        submit_sign_snapshot(ctx, epoch, group, bitmap, signature).
        Ok(())
    }

    /// Reacts to a group finalize signature landing on-chain.
    ///
    /// When the on-chain state transitions to `Finalized` (all 50 groups
    /// have signed), the epoch's event log and any lingering cache state
    /// can be dropped.
    async fn on_snapshot_signed(
        &self,
        epoch: EpochNumber,
        group: SpoolGroup,
        state: u64,
    ) -> Result<(), NodeError> {
        trace!(
            epoch = epoch.0,
            group = group.0,
            state,
            "snapshot: sign observed (stub)"
        );
        // TODO:
        //   - if SnapshotState::try_from(state) == Ok(SnapshotState::Finalized):
        //       self.context.snapshot_cache.drop_epoch(epoch);
        //       self.context.store.delete_epoch_events(epoch)?;
        Ok(())
    }
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

    #[tokio::test]
    async fn returns_when_channel_closed_after_cancel() {
        let context = test_context();
        let (tx, rx) = mpsc::channel::<Arc<ParsedBlock>>(1);

        let cancel = CancellationToken::new();
        let manager = SnapshotManager::new(context, rx, cancel.clone());
        drop(tx);
        cancel.cancel();

        timeout(Duration::from_secs(1), manager.run())
            .await
            .expect("manager completed in time")
            .expect("manager returned ok");
    }
}
