use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use rpc::Rpc;
use store::Store;
use tape_core::challenge::record::{NodeVerdict, PeerRecord, node_verdict};
use tape_core::types::EpochNumber;
use tape_crypto::Address;
use tape_protocol::api::{GetHealthReq, GetHealthRes};
use tape_store::ops::ChallengeOps;
use tape_blocks::ParsedInstruction;
use tape_protocol::{Api, ProtocolState};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info};

use crate::context::NodeContext;
use crate::core::error::NodeError;
use crate::core::types::ChannelName;
use crate::features::block::ingestor::ParsedBlock;
use crate::features::challenge::fold::holds_spool;
use crate::features::eviction::build::{EvictionCandidate, build_eviction};
use crate::features::eviction::queue::Opened;
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
                        info!(node = %node, "eviction: vote opened, signing");
                        self.context
                            .eviction_queue
                            .insert(*node, self.context.state().epoch(), Opened::Proposal);
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

        // Nor off a view the node has stopped trusting. Every record behind the
        // queue was folded from that view, so proposing on it would evict a
        // group for this node's own drift.
        if self.context.challenge_tripwire.is_realigning() {
            return Ok(());
        }

        let state = self.context.state();
        if state.find_member(self.context.node_address()).is_none() {
            return Ok(());
        }

        // A proposal dies with its voting epoch, so a target that never gathered
        // its supermajority goes with it rather than being re-proposed forever.
        self.context.eviction_queue.retain_epoch(state.epoch());

        for (node, opened) in self.context.eviction_queue.snapshot() {
            // A proposal is a decision already taken. A record is this node's
            // own, and the run arm it fires on is the one a live probe clears.
            if opened == Opened::Record && !self.judge_target(&state, node).await {
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
    /// epoch, so a recovered target is dropped rather than voted out.
    ///
    /// Judged on the challenge record once it holds enough rounds, on a health
    /// ping below that. The arms differ: a run claims the peer stopped
    /// answering, so answering now clears it. The rate arm claims nothing about
    /// reachability, so a probe cannot clear it.
    async fn judge_target(&mut self, state: &ProtocolState, node: Address) -> bool {
        let epoch = state.epoch();
        if self.probe_failed.get(&node) == Some(&epoch) {
            return true;
        }

        // Only the spools it still answers for, or a handoff would hand its
        // successor the old owner's run.
        let records: Vec<PeerRecord> = self
            .context
            .store
            .records_for_peer(node)
            .unwrap_or_default()
            .into_iter()
            .filter(|(spool, _)| holds_spool(state, node, *spool))
            .map(|(_, record)| record)
            .collect();

        let healthy = match node_verdict(&records) {
            NodeVerdict::RateFailed => false,
            NodeVerdict::Unproven | NodeVerdict::RunFailed => self.answers(node).await,
            NodeVerdict::Healthy => true,
        };
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

    /// Whether the target answers a health request right now.
    async fn answers(&self, node: Address) -> bool {
        matches!(
            self.context.api.get_health(node, &GetHealthReq).await,
            Ok(GetHealthRes { ok: true })
        )
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
