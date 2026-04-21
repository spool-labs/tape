use std::sync::Arc;
use std::time::Duration;

use rpc::Rpc;
use store::Store;
use tape_api::event::{SnapshotSigned, SnapshotWritten};
use tape_blocks::ParsedInstruction;
use tape_core::snapshot::types::SnapshotState;
use tape_core::spooler::SpoolGroup;
use tape_core::track::blob::BlobInfo;
use tape_core::types::{ChunkNumber, EpochNumber};
use tape_protocol::Api;
use tape_store::ops::{EventLogOps, SliceOps, SnapshotOps};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info};

use crate::chain::submit_reserve_snapshot;
use crate::context::NodeContext;
use crate::core::error::NodeError;
use crate::core::types::ChannelName;
use crate::features::block::ingestor::ParsedBlock;
use crate::features::snapshot::build::build_snapshot;
use crate::features::snapshot::fanout::{fanout_finalize_votes, fanout_write_votes};
use crate::features::snapshot::submit::{submit_ready_finalizes, submit_ready_writes};
use crate::features::snapshot::vote::{
    create_snapshot_finalize_votes, create_snapshot_write_votes,
};

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

    async fn on_advance_epoch(
        &self,
        old: EpochNumber,
        _new: EpochNumber,
    ) -> Result<(), NodeError> {
        let snapshot_epoch = old;

        self.context
            .store
            .delete_snapshot_epochs_except(snapshot_epoch)
            .map_err(|e| NodeError::Store(format!("delete_snapshot_epochs_except: {e}")))?;

        match submit_reserve_snapshot(&self.context, snapshot_epoch).await {
            Ok(txid) => {
                info!(epoch = snapshot_epoch.0, ?txid, "snapshot: reserve submitted")
            },
            Err(error) => {
                debug!(?error, epoch = snapshot_epoch.0, "snapshot: reserve raced / already exists")
            }
        }

        Ok(())
    }

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

    async fn on_snapshot_reserved(&self, epoch: EpochNumber) -> Result<(), NodeError> {
        build_snapshot(&self.context, epoch, &self.cancel).await?;
        create_snapshot_write_votes(&self.context, epoch, &self.cancel).await?;
        create_snapshot_finalize_votes(&self.context, epoch, &self.cancel).await?;
        fanout_write_votes(&self.context, epoch, &self.cancel).await?;
        fanout_finalize_votes(&self.context, epoch, &self.cancel).await?;

        Ok(())
    }

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

        if artifact.blob == blob {
            store
                .put_slice(artifact.spool_index, event.track, artifact.slice)
                .map_err(|e| NodeError::Store(format!("put_slice: {e}")))?;
        }

        store
            .delete_snapshot_artifact(event.epoch, group, chunk)
            .map_err(|e| NodeError::Store(format!("delete_snapshot_artifact: {e}")))?;

        Ok(())
    }

    async fn on_heartbeat(&self) -> Result<(), NodeError> {
        let state = self.context.state();
        if state.epoch == EpochNumber(0) {
            return Ok(());
        }

        let snapshot_epoch = state.epoch
            .saturating_sub(EpochNumber(1));

        submit_ready_writes(&self.context, snapshot_epoch, &self.cancel).await?;
        submit_ready_finalizes(&self.context, snapshot_epoch, &self.cancel).await?;

        fanout_write_votes(&self.context, snapshot_epoch, &self.cancel).await?;
        fanout_finalize_votes(&self.context, snapshot_epoch, &self.cancel).await?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use tape_api::event::SnapshotWritten;
    use tape_core::encoding::EncodingProfile;
    use tape_core::erasure::SPOOL_GROUP_SIZE;
    use tape_core::spooler::SpoolGroup;
    use tape_core::track::blob::BlobInfo;
    use tape_core::types::{ChunkNumber, EpochNumber, StorageUnits, StripeCount, TrackNumber};
    use tape_crypto::address::Address;
    use tape_crypto::Hash;
    use tape_store::ops::{ObjectInfoOps, SliceOps, SnapshotOps, TrackDataOps, TrackOps};
    use tape_store::types::SnapshotArtifact;
    use tokio::sync::mpsc;
    use tokio_util::sync::CancellationToken;

    use super::SnapshotManager;
    use crate::context::test_utils::test_context;

    fn blob(tag: u8) -> BlobInfo {
        BlobInfo {
            size: StorageUnits::from_bytes(64),
            commitment: Hash::from([tag; 32]),
            profile: EncodingProfile::clay_default(),
            stripe_size: StorageUnits::from_bytes(64),
            stripe_count: StripeCount(1),
            leaves: [Hash::from([tag; 32]); SPOOL_GROUP_SIZE],
        }
    }

    #[tokio::test]
    async fn snapshot_written_promotes_matching_artifact_slice_only() {
        let ctx = test_context();
        let (_tx, rx) = mpsc::channel(1);
        let manager = SnapshotManager::new(ctx.clone(), rx, CancellationToken::new());
        let epoch = EpochNumber(9);
        let group = SpoolGroup(0);
        let chunk = ChunkNumber(0);
        let track = Address::new_unique();
        let artifact = SnapshotArtifact {
            blob: blob(0x11),
            spool_index: group.spool_at(3),
            slice: vec![1, 2, 3],
        };

        ctx.store
            .put_snapshot_artifact(epoch, group, chunk, &artifact)
            .unwrap();

        manager
            .on_snapshot_written(
                SnapshotWritten {
                    epoch,
                    group,
                    track,
                    track_number: TrackNumber(7),
                    track_hash: Hash::new_unique(),
                },
                group,
                chunk,
                artifact.blob,
            )
            .await
            .unwrap();

        assert_eq!(
            ctx.store.get_slice(artifact.spool_index, track).unwrap(),
            Some(artifact.slice),
        );
        assert!(ctx
            .store
            .get_snapshot_artifact(epoch, group, chunk)
            .unwrap()
            .is_none());
        assert!(ctx.store.get_track(track).unwrap().is_none());
        assert!(ctx.store.get_track_data(track).unwrap().is_none());
        assert!(ctx.store.get_object_info(track).unwrap().is_none());
    }

    #[tokio::test]
    async fn snapshot_written_drops_divergent_artifact() {
        let ctx = test_context();
        let (_tx, rx) = mpsc::channel(1);
        let manager = SnapshotManager::new(ctx.clone(), rx, CancellationToken::new());
        let epoch = EpochNumber(9);
        let group = SpoolGroup(0);
        let chunk = ChunkNumber(0);
        let track = Address::new_unique();
        let artifact = SnapshotArtifact {
            blob: blob(0x11),
            spool_index: group.spool_at(3),
            slice: vec![1, 2, 3],
        };

        ctx.store
            .put_snapshot_artifact(epoch, group, chunk, &artifact)
            .unwrap();

        manager
            .on_snapshot_written(
                SnapshotWritten {
                    epoch,
                    group,
                    track,
                    track_number: TrackNumber(7),
                    track_hash: Hash::new_unique(),
                },
                group,
                chunk,
                blob(0x22),
            )
            .await
            .unwrap();

        assert!(ctx
            .store
            .get_snapshot_artifact(epoch, group, chunk)
            .unwrap()
            .is_none());
        assert!(ctx
            .store
            .get_slice(artifact.spool_index, track)
            .unwrap()
            .is_none());
    }
}
