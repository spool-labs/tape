//! Drives the snapshot chunk build / post / finalize pipeline for each epoch.
//!
//! Consumes the parsed-block stream and reacts to the four on-chain
//! instructions that shape the snapshot lifecycle:
//!
//! - `AdvanceEpoch` — the previous epoch has advanced. Cancel any stale
//!   in-flight snapshot work and submit the permissionless `ReserveSnapshot`
//!   transaction for the just-closed epoch.
//! - `ReserveSnapshot` — the manifest + tape for one closed epoch now exist
//!   on-chain. Build local chunks for every group this node owns and start
//!   collecting write signatures for those chunks.
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

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use rpc::Rpc;
use store::Store;
use tape_blocks::ParsedInstruction;
use tape_core::snapshot::types::SnapshotState;
use tape_core::spooler::{SpoolGroup, SpoolIndex};
use tape_core::types::{ChunkNumber, EpochNumber};
use tape_crypto::address::Address;
use tape_protocol::Api;
use tape_store::ops::{EventLogOps, SliceOps};
use tokio::sync::mpsc;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, trace, warn};

use crate::chain::reserve_snapshot::submit_reserve_snapshot;
use crate::context::NodeContext;
use crate::core::error::NodeError;
use crate::core::types::ChannelName;
use crate::features::block::ingestor::ParsedBlock;
use crate::features::snapshot::build::build_snapshot_epoch;
use crate::features::snapshot::cache::ChunkKey;
use crate::features::snapshot::{finalize, write};

pub struct SnapshotManager<Db: Store, Cluster: Api, Blockchain: Rpc> {
    context: Arc<NodeContext<Db, Cluster, Blockchain>>,
    snapshot_rx: mpsc::Receiver<Arc<ParsedBlock>>,
    cancel: CancellationToken,
    /// Parent token for the current snapshot cycle. Cancelled on each new
    /// `AdvanceEpoch` so every in-flight task tied to the old epoch exits.
    cycle_cancel: CancellationToken,
    /// Per-chunk tokens under `cycle_cancel`. Cancelled on `WriteSnapshot`
    /// so a still-collecting write task exits once its chunk has landed.
    write_cancels: HashMap<ChunkKey, CancellationToken>,
    write_tasks: JoinSet<()>,
    /// Per-group tokens under `cycle_cancel`. Cancelled on `SignSnapshot`
    /// so a still-collecting finalize task exits once its group has signed.
    finalize_cancels: HashMap<SpoolGroup, CancellationToken>,
    finalize_tasks: JoinSet<()>,
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
            write_cancels: HashMap::new(),
            write_tasks: JoinSet::new(),
            finalize_cancels: HashMap::new(),
            finalize_tasks: JoinSet::new(),
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

    /// Reacts to the previous epoch advance.
    ///
    /// Tasks from any prior snapshot cycle are cancelled via `cycle_cancel`
    /// before the new cycle begins. Once the epoch is closed, any node may
    /// reserve the manifest/tape accounts for it. Local chunk build waits for
    /// the actual `ReserveSnapshot` instruction to appear on-chain so every
    /// peer signs against a reserved snapshot epoch, not merely against an
    /// observed epoch transition.
    async fn on_advance_epoch(&mut self, epoch: EpochNumber) -> Result<(), NodeError> {
        self.cycle_cancel.cancel();
        drain_tasks(&mut self.write_tasks, "snapshot write").await;
        drain_tasks(&mut self.finalize_tasks, "snapshot finalize").await;
        self.write_cancels.clear();
        self.finalize_cancels.clear();
        self.cycle_cancel = self.cancel.child_token();

        match submit_reserve_snapshot(&self.context, epoch).await {
            Ok(txid) => {
                info!(
                    epoch = epoch.0,
                    ?txid,
                    "snapshot: reserve submitted"
                );
            }
            Err(error) => {
                debug!(
                    ?error,
                    epoch = epoch.0,
                    "snapshot: reserve raced / already exists"
                );
            }
        }

        Ok(())
    }

    /// Reacts to the on-chain reservation of one closed epoch's snapshot.
    ///
    /// For each group this node has a spool in, build the chunks
    /// deterministically from the local event log, stash them in
    /// `snapshot_cache`, and spawn a per-chunk write task that collects a
    /// 14-of-20 BLS quorum and submits the on-chain `WriteSnapshot`.
    async fn on_snapshot_reserved(&mut self, epoch: EpochNumber) -> Result<(), NodeError> {
        let my_groups = local_groups(&self.context);
        if my_groups.is_empty() {
            trace!(
                epoch = epoch.0,
                "snapshot: no local groups — nothing to build"
            );
            return Ok(());
        }

        let chunks = build_snapshot_epoch(self.context.store.as_ref(), epoch)?;
        let mut spawned = 0usize;
        for chunk in chunks {
            if !my_groups.contains(&chunk.group) {
                continue;
            }

            let key = ChunkKey::new(epoch, chunk.group, chunk.chunk);
            let blob = chunk.blob;
            self.context
                .snapshot_cache
                .insert(key, blob, chunk.slices);

            let chunk_cancel = self.cycle_cancel.child_token();
            self.write_cancels.insert(key, chunk_cancel.clone());

            let ctx = self.context.clone();
            self.write_tasks.spawn(async move {
                write::run(ctx, epoch, chunk.group, chunk.chunk, blob, chunk_cancel).await;
            });
            spawned += 1;
        }

        if spawned == 0 {
            debug!(
                epoch = epoch.0,
                groups = my_groups.len(),
                "snapshot: build produced no chunks for local groups"
            );
        } else {
            info!(
                epoch = epoch.0,
                chunks = spawned,
                groups = my_groups.len(),
                "snapshot: cycle started"
            );
        }

        Ok(())
    }

    /// Reacts to a chunk landing on-chain.
    ///
    /// If this node built the chunk, persist the one slice it computed for
    /// its spool in the group into `SliceCol` under the on-chain track address,
    /// then drop the cache entry. If this write completes a local group, kick
    /// off finalize-sig collection for that group.
    async fn on_snapshot_written(
        &mut self,
        epoch: EpochNumber,
        group: SpoolGroup,
        chunk: ChunkNumber,
        track: Address,
    ) -> Result<(), NodeError> {
        let key = ChunkKey::new(epoch, group, chunk);

        // Cancel the write task for this chunk — its submission attempt is
        // moot now that the on-chain write has landed (whether by us or a
        // peer).
        if let Some(token) = self.write_cancels.remove(&key) {
            token.cancel();
        }

        let Some(mut slices) = self.context.snapshot_cache.mark_posted(&key, track) else {
            // We didn't build this chunk — nothing to flush locally.
            return Ok(());
        };

        // A node owns at most one spool per group; find its local position.
        if let Some(my_spool) = my_spool_in_group(&self.context, group) {
            if let Some(local_idx) = group.slice_of(my_spool) {
                let data = std::mem::take(&mut slices[local_idx as usize]);
                if let Err(error) = self
                    .context
                    .store
                    .put_slice(my_spool, track, data)
                {
                    warn!(
                        ?error,
                        epoch = epoch.0,
                        group = group.0,
                        chunk = chunk.0,
                        ?track,
                        "snapshot: put_slice failed"
                    );
                }
            }
        }

        // If every local chunk for this group is posted, kick off finalize.
        let progress = self.context.snapshot_cache.group_progress(epoch, group);
        if progress.is_complete() && !self.finalize_cancels.contains_key(&group) {
            let group_cancel = self.cycle_cancel.child_token();
            self.finalize_cancels.insert(group, group_cancel.clone());
            let ctx = self.context.clone();
            self.finalize_tasks.spawn(async move {
                finalize::run(ctx, epoch, group, group_cancel).await;
            });
            debug!(
                epoch = epoch.0,
                group = group.0,
                chunks = progress.built,
                "snapshot: finalize cycle started for group"
            );
        }

        Ok(())
    }

    /// Reacts to a group finalize signature landing on-chain.
    ///
    /// When the on-chain state transitions to `Finalized` (all 50 groups
    /// have signed), the epoch's event log and any lingering cache state
    /// can be dropped.
    async fn on_snapshot_signed(
        &mut self,
        epoch: EpochNumber,
        group: SpoolGroup,
        state: u64,
    ) -> Result<(), NodeError> {
        // Cancel our own finalize task for this group — another member has
        // already landed the group's sign instruction.
        if let Some(token) = self.finalize_cancels.remove(&group) {
            token.cancel();
        }

        if SnapshotState::try_from(state) != Ok(SnapshotState::Finalized) {
            return Ok(());
        }

        self.context.snapshot_cache.drop_epoch(epoch);
        if let Err(error) = self.context.store.delete_epoch_events(epoch) {
            warn!(
                ?error,
                epoch = epoch.0,
                "snapshot: delete_epoch_events failed"
            );
        }

        info!(
            epoch = epoch.0,
            last_group = group.0,
            "snapshot: epoch finalized"
        );
        Ok(())
    }
}

fn local_groups<Db: Store, Cluster: Api, Blockchain: Rpc>(
    context: &NodeContext<Db, Cluster, Blockchain>,
) -> HashSet<SpoolGroup> {
    context
        .my_spools()
        .into_iter()
        .map(SpoolGroup::of)
        .collect()
}

/// Which spool this node owns inside a given group. At most one — the
/// spooler enforces single-slot-per-group membership.
fn my_spool_in_group<Db: Store, Cluster: Api, Blockchain: Rpc>(
    context: &NodeContext<Db, Cluster, Blockchain>,
    group: SpoolGroup,
) -> Option<SpoolIndex> {
    context
        .my_spools()
        .into_iter()
        .find(|spool| group.contains(*spool))
}

/// Cancel + drain any outstanding tasks in the set. Errors are logged but
/// don't propagate — we're about to start a fresh cycle regardless.
async fn drain_tasks(tasks: &mut JoinSet<()>, label: &'static str) {
    tasks.abort_all();
    while let Some(result) = tasks.join_next().await {
        if let Err(error) = result {
            if !error.is_cancelled() {
                warn!(?error, label, "snapshot: task drain failed");
            }
        }
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
