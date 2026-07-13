use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use rpc::Rpc;
use store::Store;
use tape_blocks::ParsedInstruction;
use tape_core::types::EpochNumber;
use tape_crypto::Address;
use tape_protocol::api::{GetHealthReq, GetHealthRes};
use tape_protocol::{Api, ProtocolState};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info};

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
    // Voting epoch of the last failed probe per target. A target is re-probed
    // once per epoch so a recovered node stops collecting votes.
    probe_failed: HashMap<Address, EpochNumber>,
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
            probe_failed: HashMap::new(),
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

    /// Track open eviction votes, then drive a voting round off every block.
    ///
    /// An observed proposal only marks the target as having an open vote; the
    /// decision to sign is made per epoch by this node's own probe. The voting
    /// window runs from when the next epoch is set up until the epoch enters
    /// its closing phase. The heartbeat alone fires per node on its own clock,
    /// so voters seldom align on the same voting epoch and the per-group
    /// supermajority never assembles. Reacting to each ingested block keeps
    /// the committee voting in step across the window, which is what lets the
    /// partial signatures accumulate into a landed eviction.
    async fn on_block(&mut self, block: Arc<ParsedBlock>) -> Result<(), NodeError> {
        for ix in &block.instructions {
            match ix {
                ParsedInstruction::ProposeEviction { node, .. } => {
                    if *node != self.context.node_address() {
                        info!(node = %node, "eviction: vote opened, judging target");
                        self.context.eviction_queue.insert(*node);
                    }
                }
                ParsedInstruction::NodeEvicted { event } => {
                    self.context.eviction_queue.remove(&event.node);
                    self.probe_failed.remove(&event.node);
                    info!(node = %event.node, "eviction: landed, cleared target");
                }
                _ => {}
            }
        }

        self.try_progress().await
    }

    async fn try_progress(&mut self) -> Result<(), NodeError> {
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
            if !self.judge_target(node, state.epoch()).await {
                continue;
            }

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

    /// Judge the target with this node's own probe, at most once per voting
    /// epoch. A proposal alone never recruits a signature: only a target this
    /// node observes failing stays queued, and a recovered target is dropped.
    async fn judge_target(&mut self, node: Address, epoch: EpochNumber) -> bool {
        if self.probe_failed.get(&node) == Some(&epoch) {
            return true;
        }

        let healthy = matches!(
            self.context.api.get_health(node, &GetHealthReq).await,
            Ok(GetHealthRes { ok: true })
        );
        if healthy {
            debug!(node = %node, "eviction: target probed healthy, dropping");
            self.context.eviction_queue.remove(&node);
            self.probe_failed.remove(&node);
            return false;
        }

        info!(node = %node, epoch = epoch.0, "eviction: target probe failed, voting to evict");
        self.probe_failed.insert(node, epoch);
        true
    }

    async fn run_round(
        &self,
        state: &ProtocolState,
        candidate: &EvictionCandidate,
    ) -> Result<(), NodeError> {
        let round = async {
            submit_eviction_proposal(&self.context, candidate).await?;
            create_eviction_votes(&self.context, state, candidate).await?;
            fanout_eviction_votes(&self.context, state, candidate).await?;
            submit_ready_eviction_votes(&self.context, state, candidate).await
        };

        self.cancel.run_until_cancelled(round).await.unwrap_or(Ok(()))
    }
}
