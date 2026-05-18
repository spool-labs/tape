use std::sync::Arc;
use std::time::Duration;

use rpc::Rpc;
use store::Store;
use tape_blocks::ParsedInstruction;
use tape_core::system::{EpochPhase, VoteKind};
use tape_core::types::EpochNumber;
use tape_crypto::Hash;
use tape_protocol::Api;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

use crate::context::NodeContext;
use crate::core::error::NodeError;
use crate::core::types::ChannelName;
use crate::features::assignment::build::{AssignmentCandidate, build_assignment};
use crate::features::assignment::fanout::fanout_assignment_votes;
use crate::features::assignment::submit::{
    submit_assignment_finalization, submit_assignment_proposal, submit_ready_assignment_votes,
};
use crate::features::assignment::vote::create_assignment_votes;
use crate::features::block::ingestor::ParsedBlock;

const ASSIGNMENT_HEARTBEAT: Duration = Duration::from_secs(30);

pub struct AssignmentManager<Db: Store, Cluster: Api, Blockchain: Rpc> {
    context: Arc<NodeContext<Db, Cluster, Blockchain>>,
    block_rx: mpsc::Receiver<Arc<ParsedBlock>>,
    cancel: CancellationToken,
}

impl<Db, Cluster, Blockchain> AssignmentManager<Db, Cluster, Blockchain>
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
        let mut heartbeat = tokio::time::interval(ASSIGNMENT_HEARTBEAT);

        loop {
            tokio::select! {
                _ = self.cancel.cancelled() => return Ok(()),
                received = self.block_rx.recv() => {
                    let Some(block) = received else {
                        return if self.cancel.is_cancelled() {
                            Ok(())
                        } else {
                            Err(NodeError::ChannelClosed { channel: ChannelName::AssignmentManager })
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
                ParsedInstruction::VoteAssignment { event, .. } => {
                    if is_landed_assignment_vote(event) {
                        self.on_assignment_canonical(event.target_epoch, event.hash).await?;
                    }
                }
                ParsedInstruction::FinalizeGroup { event, .. } => {
                    debug!(
                        epoch = event.epoch.0,
                        group = event.group.0,
                        total_groups = u64::from_le_bytes(event.total_groups),
                        "assignment: observed finalized group"
                    );
                }
                _ => {}
            }
        }
        Ok(())
    }

    async fn on_heartbeat(&self) -> Result<(), NodeError> {
        let state = self.context.state();
        if state.phase() != EpochPhase::Closing {
            return Ok(());
        }

        let Some(next_epoch) = state.next_epoch.as_ref() else {
            return Ok(());
        };
        let target_epoch = next_epoch.id;

        if let Some(hash) = canonical_assignment_hash(&state, target_epoch) {
            drop(state);
            self.on_assignment_canonical(target_epoch, hash).await?;
            return Ok(());
        }

        drop(state);
        let Some(candidate) = self.build_candidate().await? else {
            return Ok(());
        };

        self.run_vote_round(&candidate).await
    }

    async fn on_assignment_canonical(
        &self,
        epoch: EpochNumber,
        hash: Hash,
    ) -> Result<(), NodeError> {
        let Some(candidate) = self.build_candidate().await? else {
            return Ok(());
        };

        if candidate.target_epoch != epoch || candidate.hash != hash {
            warn!(
                epoch = epoch.0,
                local_epoch = candidate.target_epoch.0,
                local_hash = ?candidate.hash,
                canonical_hash = ?hash,
                "assignment: local candidate does not match canonical assignment"
            );
            return Ok(());
        }

        submit_assignment_finalization(&self.context, &candidate, &self.cancel).await
    }

    async fn build_candidate(&self) -> Result<Option<AssignmentCandidate>, NodeError> {
        build_assignment(&self.context, &self.cancel).await
    }

    async fn run_vote_round(&self, candidate: &AssignmentCandidate) -> Result<(), NodeError> {
        submit_assignment_proposal(&self.context, candidate, &self.cancel).await?;
        create_assignment_votes(&self.context, candidate, &self.cancel).await?;
        fanout_assignment_votes(&self.context, candidate, &self.cancel).await?;
        submit_ready_assignment_votes(&self.context, candidate, &self.cancel).await?;
        Ok(())
    }
}

fn canonical_assignment_hash(
    state: &tape_protocol::ProtocolState,
    target_epoch: EpochNumber,
) -> Option<Hash> {
    let next_epoch = state.next_epoch.as_ref()?;
    if next_epoch.id != target_epoch || !next_epoch.has_assignment_hash() {
        return None;
    }
    Some(next_epoch.assignment_hash)
}

fn is_landed_assignment_vote(event: &tape_api::event::VoteRecorded) -> bool {
    event.kind == VoteKind::Assignment as u64
        && u64::from_le_bytes(event.signed_groups) == u64::from_le_bytes(event.total_groups)
}
