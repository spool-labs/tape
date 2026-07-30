//! Runs this node's challenge rounds and keeps its record of every group-mate.
//!
//! Rounds sit on a slot grid derived from finalized epoch state, so the whole
//! group reaches the same schedule without coordinating. A round opens a short
//! window; the first finalized block to land inside it is the round's entropy
//! block, and a window that finalizes no block is a void round that counts
//! against nobody.
//!
//! What comes out is a local record per peer, not a verdict. A missing answer is
//! this node's own observation, so only a sustained pattern proposes anything, and
//! the proposal still needs the group and then the network to agree.

use std::sync::Arc;

use rpc::Rpc;
use store::Store;
use tape_core::challenge::schedule::{SLOT_MS, Schedule};
use tape_core::challenge::PeerRecord;
use tape_core::erasure::group_for_spool;
use tape_core::system::EpochPhase;
use tape_core::types::{EpochNumber, RoundNumber};
use tape_crypto::Address;
use tape_protocol::{Api, ProtocolState};
use tape_store::ops::ChallengeOps;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, trace};

use crate::context::NodeContext;
use crate::core::error::NodeError;
use crate::core::types::ChannelName;
use crate::features::block::ingestor::ParsedBlock;
use crate::features::eviction::challenge::{Entropy, challenge_target};

pub struct ChallengeManager<Db: Store, Cluster: Api, Blockchain: Rpc> {
    context: Arc<NodeContext<Db, Cluster, Blockchain>>,
    block_rx: mpsc::Receiver<Arc<ParsedBlock>>,
    cancel: CancellationToken,
    // The last round run. A round's window spans several slots and only its first
    // finalized block seeds it, so the rest of the window is ignored.
    last_run: Option<(EpochNumber, RoundNumber)>,
}

impl<Db, Cluster, Blockchain> ChallengeManager<Db, Cluster, Blockchain>
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
            last_run: None,
        }
    }

    pub async fn run(mut self) -> Result<(), NodeError> {
        loop {
            tokio::select! {
                _ = self.cancel.cancelled() => return Ok(()),
                received = self.block_rx.recv() => {
                    let Some(block) = received else {
                        return if self.cancel.is_cancelled() {
                            Ok(())
                        } else {
                            Err(NodeError::ChannelClosed { channel: ChannelName::ChallengeManager })
                        };
                    };
                    self.on_block(block).await?;
                }
            }
        }
    }

    /// Fire a round when a finalized block lands in one's entropy window.
    ///
    /// There is no timer here on purpose. The schedule is expressed in slots and
    /// the entropy has to come from a block that finalized, so a block arriving is
    /// the only thing that can open a round.
    async fn on_block(&mut self, block: Arc<ParsedBlock>) -> Result<(), NodeError> {
        // Never judge a peer off stale state: a node still catching up would read
        // an old committee and challenge for spools nobody owns any more.
        if !self.context.is_at_tip() {
            return Ok(());
        }

        let state = self.context.state();
        if state.phase() != EpochPhase::Active {
            return Ok(());
        }

        let Some(schedule) = challenge_schedule(&state) else {
            return Ok(());
        };
        let Some(round) = schedule.round_at(block.slot) else {
            return Ok(());
        };

        let epoch = state.epoch();
        if self.last_run == Some((epoch, round)) {
            return Ok(());
        }
        self.last_run = Some((epoch, round));

        self.run_round(&state, epoch, round, &block).await
    }

    /// Challenge every group-mate once, and fold each answer into its record.
    async fn run_round(
        &self,
        state: &ProtocolState,
        epoch: EpochNumber,
        round: RoundNumber,
        block: &ParsedBlock,
    ) -> Result<(), NodeError> {
        let entropy = Entropy {
            slot: block.slot,
            hash: block.blockhash,
        };
        let mates = group_mates(state, self.context.node_address());
        trace!(epoch = epoch.0, round = round.0, peers = mates.len(), "challenge: round opens");

        for peer in mates {
            let round_task = challenge_target(&self.context, state, peer, entropy);
            let Some(proved) = self.cancel.run_until_cancelled(round_task).await.flatten() else {
                // No judgement reached, which is not a miss. Recording it as one
                // would punish a peer for our own missing data.
                continue;
            };

            self.record(peer, epoch, round, proved)?;
        }

        Ok(())
    }

    /// Fold one outcome into a peer's record, and queue it if the rule fires.
    fn record(
        &self,
        peer: Address,
        epoch: EpochNumber,
        round: RoundNumber,
        proved: bool,
    ) -> Result<(), NodeError> {
        let mut record = self
            .context
            .store
            .peer_record(peer)
            .unwrap_or_else(|_| PeerRecord::default());

        if !record.record(epoch, round, proved) {
            return Ok(());
        }

        if let Err(error) = self.context.store.put_peer_record(peer, record) {
            debug!(%error, node = %peer, "challenge: record not persisted");
        }

        if record.eviction_fires() {
            info!(
                node = %peer,
                misses = record.consecutive_misses,
                rate = record.success_rate().0,
                "challenge: peer failed the local rule, queuing for eviction"
            );
            self.context.eviction_queue.insert(peer);
        }

        Ok(())
    }
}

/// The current epoch's round grid, if it holds any round at all.
pub fn challenge_schedule(state: &ProtocolState) -> Option<Schedule> {
    let epoch = &state.current.epoch;
    let seconds = epoch.preferences.epoch_duration.0;
    let schedule = Schedule::for_epoch(epoch.start_slot, seconds * 1_000 / SLOT_MS);

    schedule.validate().ok().map(|()| schedule)
}

/// Every other node holding a spool in a group this node also holds one in.
///
/// The group is the unit that challenges itself: only a group-mate holds slices
/// of the same tracks, so only a group-mate can pose the question.
pub fn group_mates(state: &ProtocolState, me: Address) -> Vec<Address> {
    let mut mates = Vec::new();

    for mine in state.member_spools(me) {
        let Some(spools) = state.spools_in_group(group_for_spool(mine)) else {
            continue;
        };
        for (spool, _) in spools {
            let Some(owner) = state.spool_owner(spool) else {
                continue;
            };
            if owner != me && !mates.contains(&owner) {
                mates.push(owner);
            }
        }
    }

    mates
}

#[cfg(test)]
mod tests {
    use tape_core::challenge::schedule::round_width_slots;
    use tape_core::erasure::GROUP_SIZE;
    use tape_core::types::{EpochDuration, SlotNumber};

    use super::*;
    use crate::harness::{NodeHarness, TestContext};

    async fn harness() -> NodeHarness {
        NodeHarness::builder()
            .nodes(25)
            .no_prev_snapshot_tape()
            .build()
            .await
            .expect("build harness")
    }

    fn address_of(harness: &NodeHarness, index: usize) -> Address {
        Address::from(harness.node(index).node_address.to_bytes())
    }

    #[tokio::test]
    async fn a_node_challenges_its_group_and_not_itself() {
        let harness = harness().await;
        let ctx: TestContext = harness.ctx_for(0);
        let me = ctx.node_address();
        let mates = group_mates(&ctx.state(), me);

        // Everyone else holding a position in the group, counted once each.
        assert_eq!(mates.len(), GROUP_SIZE - 1);
        assert!(!mates.contains(&me), "a node cannot challenge itself");

        let mut sorted = mates.clone();
        sorted.sort_unstable();
        sorted.dedup();
        assert_eq!(sorted.len(), mates.len(), "a peer appears twice");
    }

    #[tokio::test]
    async fn a_node_holding_no_spool_is_not_challenged() {
        // The harness seats 25 nodes into a 20-wide group, so some own nothing
        // and there is no question to ask them.
        let harness = harness().await;
        let ctx: TestContext = harness.ctx_for(0);
        let state = ctx.state();
        let mates = group_mates(&state, ctx.node_address());

        for index in 0..25 {
            let peer = address_of(&harness, index);
            if state.member_spools(peer).is_empty() {
                assert!(!mates.contains(&peer), "challenged a node holding no spool");
            }
        }
        assert!(mates.iter().all(|peer| !state.member_spools(*peer).is_empty()));
    }

    #[tokio::test]
    async fn the_schedule_comes_from_the_epoch_on_chain() {
        let harness = harness().await;
        let ctx: TestContext = harness.ctx_for(0);
        let mut state = (*ctx.state()).clone();
        state.current.epoch.start_slot = SlotNumber(4_000);
        state.current.epoch.preferences.epoch_duration = EpochDuration(3_600);

        let schedule = challenge_schedule(&state).expect("a usable grid");
        assert_eq!(schedule.epoch_start_slot, SlotNumber(4_000));
        assert_eq!(schedule.rounds(), 60);
        assert!(schedule.interval_slots >= round_width_slots());
        assert!(schedule.validate().is_ok());
    }

    #[tokio::test]
    async fn an_epoch_with_no_duration_schedules_nothing() {
        // What the node sees before an epoch is set up. Firing rounds off a grid
        // derived from a zero-length epoch would challenge on every block.
        let harness = harness().await;
        let ctx: TestContext = harness.ctx_for(0);
        let mut state = (*ctx.state()).clone();
        state.current.epoch.preferences.epoch_duration = EpochDuration(0);

        assert!(challenge_schedule(&state).is_none());
    }
}
