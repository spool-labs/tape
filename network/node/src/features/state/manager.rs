use std::sync::Arc;

use rpc::Rpc;
use store::Store;
use tape_blocks::ParsedInstruction;
use tape_protocol::Api;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::debug;

use crate::context::NodeContext;
use crate::core::error::NodeError;
use crate::core::types::ChannelName;
use crate::features::block::ingestor::ParsedBlock;
use crate::features::state::handlers::ProtocolStateHandlers;

pub struct StateManager<Db: Store, Cluster: Api, Blockchain: Rpc> {
    context: Arc<NodeContext<Db, Cluster, Blockchain>>,
    rx: mpsc::Receiver<Arc<ParsedBlock>>,
    cancel: CancellationToken,
}

impl<Db: Store, Cluster: Api, Blockchain: Rpc> StateManager<Db, Cluster, Blockchain> {
    pub fn new(
        context: Arc<NodeContext<Db, Cluster, Blockchain>>,
        rx: mpsc::Receiver<Arc<ParsedBlock>>,
        cancel: CancellationToken,
    ) -> Self {
        Self {
            context,
            rx,
            cancel,
        }
    }

    pub async fn run(mut self) -> Result<(), NodeError> {
        let handlers = ProtocolStateHandlers::new(self.context.clone(), self.cancel.clone());

        debug!(
            node_id = self.context.node_id().0,
            "state manager started"
        );

        loop {
            tokio::select! {
                _ = self.cancel.cancelled() => return Ok(()),

                received = self.rx.recv() => {
                    let Some(block) = received else {
                        return if self.cancel.is_cancelled() {
                            Ok(())
                        } else {
                            Err(NodeError::ChannelClosed { channel: ChannelName::StateManager })
                        };
                    };

                    self.handle_block(&handlers, block).await?;
                }
            }
        }
    }

    async fn handle_block(
        &self,
        handlers: &ProtocolStateHandlers<Db, Cluster, Blockchain>,
        block: Arc<ParsedBlock>,
    ) -> Result<(), NodeError> {
        debug!(slot = block.slot.0, "state manager received block");

        for instruction in &block.instructions {
            match instruction {
                ParsedInstruction::CreateEpoch { event, .. } => {
                    handlers.handle_create_epoch(*event).await?;
                }
                ParsedInstruction::CreateCommittee { event, .. } => {
                    handlers.handle_create_committee(*event).await?;
                }
                ParsedInstruction::ResizeCommittee { event, .. } => {
                    handlers.handle_resize_committee(*event).await?;
                }
                ParsedInstruction::ResizePeerSet { event } => {
                    handlers.handle_resize_peer_set(*event).await?;
                }
                ParsedInstruction::CommitEpoch { event } => {
                    handlers
                        .handle_commit_epoch(event.epoch, event.next_nonce, event.preferences)
                        .await?;
                }
                ParsedInstruction::AdvanceEpoch { event } => {
                    handlers.handle_advance_epoch(event.new_epoch).await?;
                }
                ParsedInstruction::StartNetwork => {
                    handlers.handle_start_network().await?;
                }
                ParsedInstruction::SyncSpool { event, .. } => {
                    handlers.handle_sync_spool(*event).await?;
                }
                ParsedInstruction::AdvancePool { node, event, .. } => {
                    handlers.handle_advance_pool(*node, event.epoch).await?;
                }
                ParsedInstruction::JoinCommittee { event, .. } => {
                    handlers.handle_join_committee(*event).await?;
                }
                ParsedInstruction::VoteSnapshot { event, .. } => {
                    handlers.handle_snapshot_vote(*event).await?;
                }
                ParsedInstruction::FinalizeSnapshot { event, .. } => {
                    handlers.handle_snapshot_finalized(*event).await?;
                }
                ParsedInstruction::VoteAssignment { event, .. } => {
                    handlers.handle_assignment_vote(*event).await?;
                }
                ParsedInstruction::FinalizeGroup { event, .. } => {
                    handlers.handle_finalize_group(*event).await?;
                }
                _ => {}
            }
        }

        Ok(())
    }
}
