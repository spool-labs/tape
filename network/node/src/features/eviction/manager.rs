use std::sync::Arc;
use std::time::Duration;

use rpc::Rpc;
use store::Store;
use tape_blocks::ParsedInstruction;
use tape_protocol::{Api, ProtocolState};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::info;

use crate::context::NodeContext;
use crate::core::error::NodeError;
use crate::core::types::ChannelName;
use crate::features::block::ingestor::ParsedBlock;
use crate::features::eviction::build::{EvictionCandidate, build_eviction};
use crate::features::eviction::fanout::fanout_eviction_votes;
use crate::features::eviction::submit::{submit_eviction_proposal, submit_ready_eviction_votes};
use crate::features::eviction::vote::create_eviction_votes;

const EVICTION_HEARTBEAT: Duration = Duration::from_secs(30);

pub struct EvictionManager<Db: Store, Cluster: Api, Blockchain: Rpc> {
    context: Arc<NodeContext<Db, Cluster, Blockchain>>,
    block_rx: mpsc::Receiver<Arc<ParsedBlock>>,
    cancel: CancellationToken,
}

impl<Db, Cluster, Blockchain> EvictionManager<Db, Cluster, Blockchain>
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
        let mut heartbeat = tokio::time::interval(EVICTION_HEARTBEAT);

        loop {
            tokio::select! {
                _ = self.cancel.cancelled() => return Ok(()),
                received = self.block_rx.recv() => {
                    let Some(block) = received else {
                        return if self.cancel.is_cancelled() {
                            Ok(())
                        } else {
                            Err(NodeError::ChannelClosed { channel: ChannelName::EvictionManager })
                        };
                    };
                    self.on_block(block).await?;
                }
                _ = heartbeat.tick() => {
                    self.try_progress().await?;
                }
            }
        }
    }

    /// Clear landed evictions, then drive a voting round off every block.
    ///
    /// The voting window runs from when the next epoch is set up until the
    /// epoch enters its closing phase. The heartbeat alone fires per node on
    /// its own clock, so voters seldom align on the same voting epoch and the
    /// per-group supermajority never assembles. Reacting to each ingested block
    /// keeps the committee voting in step across the window, which is what lets
    /// the partial signatures accumulate into a landed eviction.
    async fn on_block(&self, block: Arc<ParsedBlock>) -> Result<(), NodeError> {
        for ix in &block.instructions {
            if let ParsedInstruction::NodeEvicted { event } = ix {
                self.context.eviction_queue.remove(&event.node);
                info!(node = %event.node, "eviction: landed, cleared target");
            }
        }

        self.try_progress().await
    }

    async fn try_progress(&self) -> Result<(), NodeError> {
        if self.context.eviction_queue.is_empty() {
            return Ok(());
        }

        // Readiness gate: never sign or submit off stale local state.
        if !self.context.is_at_tip() {
            return Ok(());
        }

        let state = self.context.state();
        if state.find_member(self.context.node_address()).is_none() {
            return Ok(());
        }

        for node in self.context.eviction_queue.snapshot() {
            let Some(candidate) = build_eviction(&state, node) else {
                // The voting window is closed for now (the epoch has entered its
                // closing phase or the next epoch is not set up yet). Keep the
                // target queued and retry when the window reopens; a landed
                // eviction is drained from the queue on its NodeEvicted event.
                continue;
            };

            self.run_round(&state, &candidate).await?;
        }

        Ok(())
    }

    async fn run_round(
        &self,
        state: &ProtocolState,
        candidate: &EvictionCandidate,
    ) -> Result<(), NodeError> {
        submit_eviction_proposal(&self.context, candidate, &self.cancel).await?;
        create_eviction_votes(&self.context, state, candidate, &self.cancel).await?;
        fanout_eviction_votes(&self.context, state, candidate, &self.cancel).await?;
        submit_ready_eviction_votes(&self.context, state, candidate, &self.cancel).await?;
        Ok(())
    }
}
