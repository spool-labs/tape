use std::sync::Arc;
use std::time::Duration;

use rpc::Rpc;
use store::Store;
use tape_blocks::ParsedInstruction;
use tape_core::system::{EpochPhase, VoteKind};
use tape_core::types::EpochNumber;
use tape_crypto::Hash;
use tape_protocol::Api;
use tape_store::ops::{EventLogOps, SnapshotOps, VoteOps};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

use crate::context::NodeContext;
use crate::core::error::NodeError;
use crate::core::types::ChannelName;
use crate::features::block::ingestor::ParsedBlock;
use crate::features::snapshot::build::{
    build_snapshot, persist_snapshot_candidate, SnapshotCandidate,
};
use crate::features::snapshot::fanout::fanout_snapshot_votes;
use crate::features::snapshot::submit::{
    submit_ready_snapshot_votes, submit_snapshot_finalization, submit_snapshot_proposal,
};
use crate::features::snapshot::vote::create_snapshot_votes;

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

    async fn on_block(&mut self, block: Arc<ParsedBlock>) -> Result<(), NodeError> {
        for ix in &block.instructions {
            match ix {
                ParsedInstruction::AdvanceEpoch { event } => {
                    self.on_advance_epoch(event.old_epoch, event.new_epoch).await?;
                }
                ParsedInstruction::VoteSnapshot { event, .. } => {
                    if is_landed_snapshot_vote(event) {
                        self.on_snapshot_canonical(event.target_epoch, event.hash).await?;
                    }
                }
                ParsedInstruction::FinalizeSnapshot { event, .. } => {
                    self.on_snapshot_finalized(event.epoch, event.hash).await?;
                }
                _ => {}
            }
        }
        Ok(())
    }

    async fn on_advance_epoch(
        &self,
        old: EpochNumber,
        new: EpochNumber,
    ) -> Result<(), NodeError> {
        self.context
            .store
            .delete_snapshot_epochs_except(old)
            .map_err(|e| NodeError::Store(format!("delete_snapshot_epochs_except: {e}")))?;
        self.context
            .store
            .delete_vote_epochs_except(new)
            .map_err(|e| NodeError::Store(format!("delete_vote_epochs_except: {e}")))?;
        Ok(())
    }

    async fn on_snapshot_canonical(
        &self,
        epoch: EpochNumber,
        hash: Hash,
    ) -> Result<(), NodeError> {
        let Some(candidate) = self.build_candidate(epoch).await? else {
            return Ok(());
        };

        if candidate.hash != hash {
            warn!(
                epoch = epoch.0,
                local_hash = ?candidate.hash,
                canonical_hash = ?hash,
                "snapshot: local candidate does not match canonical hash"
            );
            return Ok(());
        }

        submit_snapshot_finalization(&self.context, &candidate, &self.cancel).await
    }

    async fn on_snapshot_finalized(&self, epoch: EpochNumber, hash: Hash) -> Result<(), NodeError> {
        if let Some(candidate) = self.build_candidate(epoch).await? {
            if candidate.hash == hash {
                persist_snapshot_candidate(self.context.as_ref(), &candidate)?;
            } else {
                warn!(
                    epoch = epoch.0,
                    local_hash = ?candidate.hash,
                    finalized_hash = ?hash,
                    "snapshot: finalized hash does not match local candidate"
                );
            }
        }

        self.context
            .store
            .delete_epoch_events(epoch)
            .map_err(|e| NodeError::Store(format!("delete_epoch_events: {e}")))?;
        self.context
            .store
            .delete_snapshot_epoch(epoch)
            .map_err(|e| NodeError::Store(format!("delete_snapshot_epoch: {e}")))?;
        self.context
            .store
            .delete_vote_epoch(epoch + EpochNumber(1))
            .map_err(|e| NodeError::Store(format!("delete_vote_epoch: {e}")))?;

        debug!(epoch = epoch.0, "snapshot: finalized local cleanup complete");
        Ok(())
    }

    async fn on_heartbeat(&self) -> Result<(), NodeError> {
        let state = self.context.state();
        if state.epoch().is_zero() {
            return Ok(());
        }

        let snapshot_epoch = state.epoch().saturating_sub(EpochNumber(1));

        if state.phase() != EpochPhase::Snapshot {
            return Ok(());
        }

        if let Some(hash) = canonical_snapshot_hash(&state, snapshot_epoch) {
            drop(state);
            self.on_snapshot_canonical(snapshot_epoch, hash).await?;
            return Ok(());
        }

        drop(state);
        let Some(candidate) = self.build_candidate(snapshot_epoch).await? else {
            return Ok(());
        };

        self.run_vote_round(&candidate).await
    }

    async fn build_candidate(
        &self,
        epoch: EpochNumber,
    ) -> Result<Option<SnapshotCandidate>, NodeError> {
        build_snapshot(&self.context, epoch, &self.cancel).await
    }

    async fn run_vote_round(&self, candidate: &SnapshotCandidate) -> Result<(), NodeError> {
        submit_snapshot_proposal(&self.context, candidate, &self.cancel).await?;
        create_snapshot_votes(&self.context, candidate, &self.cancel).await?;
        fanout_snapshot_votes(&self.context, candidate, &self.cancel).await?;
        submit_ready_snapshot_votes(&self.context, candidate, &self.cancel).await?;
        Ok(())
    }
}

fn canonical_snapshot_hash(
    state: &tape_protocol::ProtocolState,
    snapshot_epoch: EpochNumber,
) -> Option<Hash> {
    let previous = state.previous.as_ref()?;
    if previous.epoch.id != snapshot_epoch || !previous.epoch.has_snapshot_hash() {
        return None;
    }
    Some(previous.epoch.snapshot_hash)
}

fn is_landed_snapshot_vote(event: &tape_api::event::VoteRecorded) -> bool {
    event.kind == VoteKind::Snapshot as u64
        && u64::from_le_bytes(event.signed_groups) == u64::from_le_bytes(event.total_groups)
}
