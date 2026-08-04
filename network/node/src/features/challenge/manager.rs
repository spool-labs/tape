//! Runs this node's challenge rounds and keeps its record of every group-mate.
//!
//! Rounds sit on a slot grid derived from finalized epoch state, so the whole
//! group reaches the same schedule without coordinating. A round opens on the
//! first produced block to land in its window, as the paper has it, so the
//! request is unpredictable and the answer is due while the branch is live.
//! A candidate that loses voids its round, and a window that finalizes no block
//! is a void round too. Neither counts against anybody.
//!
//! When a round opens this node answers its own challenge and broadcasts, and
//! settles the previous round: any spool whose answer did not certify by then is
//! a local miss. What comes out is a record per peer, not a verdict, so only a
//! sustained pattern proposes anything and the proposal still needs the group and
//! then the network to agree.

use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use rpc::Rpc;
use store::Store;
use tape_core::challenge::schedule::{SLOT_MS, Schedule};
use tape_core::erasure::group_for_spool;
use tape_core::system::EpochPhase;
use tape_core::types::{EpochNumber, GroupIndex, RoundNumber, SpoolIndex};
use tape_crypto::Address;
use tape_protocol::api::ProofOfAccessReq;
use tape_protocol::{Api, ProtocolState};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, trace};

use crate::context::NodeContext;
use crate::core::error::NodeError;
use crate::core::channels::ChainEvent;
use crate::core::types::ChannelName;
use crate::features::block::ingestor::ParsedBlock;
use crate::features::challenge::audition::{Round, build_answer, group_members, spawn_attest};
use crate::features::challenge::fold::fold_outcome;

// What settling needs from a round, captured when the round opened. Settling
// lands on epoch boundaries where live state is mid-roll, so it never asks
// live state about the round it is judging.
struct OpenRound {
    round: Round,
    spools: Vec<SpoolIndex>,
    /// Whether the entropy block survived to finality.
    ///
    /// A round is only evidence once its block is part of history. Settling one
    /// that never finalized would charge every spool in the group a miss for a
    /// branch that lost, and three of those queue the whole group for eviction.
    finalized: bool,
}

pub struct ChallengeManager<Db: Store, Cluster: Api, Blockchain: Rpc> {
    context: Arc<NodeContext<Db, Cluster, Blockchain>>,
    chain_rx: mpsc::Receiver<ChainEvent>,
    cancel: CancellationToken,
    // The round this node last opened in each group it holds a spool in. A node
    // may own one spool in each of several groups and owes an answer in every
    // one of them, and a group is the only unit that can check its own answers,
    // so the rounds are tracked apart. A round's window spans several slots and
    // only its first finalized block seeds it, so the rest of the window is
    // ignored, and a group's previous round settles when its next one opens.
    open_rounds: HashMap<GroupIndex, OpenRound>,
}

impl<Db, Cluster, Blockchain> ChallengeManager<Db, Cluster, Blockchain>
where
    Db: Store + 'static,
    Cluster: Api + 'static,
    Blockchain: Rpc + 'static,
{
    pub fn new(
        context: Arc<NodeContext<Db, Cluster, Blockchain>>,
        chain_rx: mpsc::Receiver<ChainEvent>,
        cancel: CancellationToken,
    ) -> Self {
        Self {
            context,
            chain_rx,
            cancel,
            open_rounds: HashMap::new(),
        }
    }

    pub async fn run(mut self) -> Result<(), NodeError> {
        loop {
            tokio::select! {
                _ = self.cancel.cancelled() => return Ok(()),
                received = self.chain_rx.recv() => {
                    let Some(event) = received else {
                        return if self.cancel.is_cancelled() {
                            Ok(())
                        } else {
                            Err(NodeError::ChannelClosed { channel: ChannelName::ChallengeManager })
                        };
                    };
                    match event {
                        ChainEvent::Produced(block) => self.on_produced(block).await?,
                        ChainEvent::Rolled(hashes) => self.on_rolled(&hashes),
                        ChainEvent::Finalized(hash) => self.on_finalized(hash),
                    }
                }
            }
        }
    }

    /// Open a round when a produced block lands in one's entropy window.
    ///
    /// There is no timer here on purpose. The schedule is expressed in slots, so
    /// a block arriving is the only thing that can open a round.
    async fn on_produced(&mut self, block: Arc<ParsedBlock>) -> Result<(), NodeError> {
        // Never take part off stale state: a node still catching up would read an
        // old committee and answer for spools nobody owns any more.
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
        let Some(number) = schedule.round_at(block.slot) else {
            return Ok(());
        };

        let epoch = state.epoch();
        let mine = state.member_spools(self.context.node_address());
        if mine.is_empty() {
            return Ok(());
        }

        // A spool this node no longer holds is one it can no longer judge, so
        // its group's pending round is dropped rather than settled.
        let held: Vec<GroupIndex> = mine.iter().map(|spool| group_for_spool(*spool)).collect();
        self.open_rounds.retain(|group, _| held.contains(group));

        for spool in mine {
            let group = group_for_spool(spool);
            let round = Round {
                epoch,
                group,
                round: number,
                block: block.blockhash,
            };

            // Match on the round, not on the block. A window spans several slots
            // and each carries a different hash, so comparing whole rounds would
            // treat every slot in the window as a new round: it would re-open the
            // round three more times and settle the previous one a slot after it
            // opened, before any attestation could have arrived. The first block
            // in the window seeds the round and the rest is already answered.
            let reopening = self.open_rounds.get(&group).is_some_and(|open| {
                (open.round.epoch, open.round.round) == (round.epoch, round.round)
            });
            if reopening {
                continue;
            }

            self.settle_previous(&state, group);
            self.open_rounds.insert(
                group,
                OpenRound {
                    round,
                    spools: group_spools(&state, group),
                    finalized: false,
                },
            );
            self.context
                .challenge_counters
                .opened
                .fetch_add(1, Ordering::Relaxed);

            self.answer_and_broadcast(&state, &round, spool).await;
        }

        // Retire behind the oldest round any group still has open, not behind
        // this one. Groups settle independently, so a cutoff taken from the
        // group that just opened would drop another group's evidence before it
        // settles, and that round would read as a miss its owner never earned.
        // It shows at epoch boundaries, where round numbers restart and one
        // group's first round of the new epoch outranks every pending round of
        // the old one.
        if let Some((epoch, round)) = self
            .open_rounds
            .values()
            .map(|open| (open.round.epoch, open.round.round))
            .min()
        {
            self.context.round_buffer.retire_before(epoch, round);
        }

        Ok(())
    }

    /// Record how one group's previous round went for every spool in it.
    ///
    /// A spool whose answer certified is a success; one that did not is a local
    /// miss. Settling on the next round's opening is what gives a late certificate
    /// the whole interval to arrive and replace a miss before anyone acts on it.
    fn settle_previous(&self, state: &ProtocolState, group: GroupIndex) {
        let Some(open) = self.open_rounds.get(&group) else {
            return;
        };

        // A round whose entropy block never finalized is void. Nobody owed an
        // answer on a branch that lost, so nobody is charged for one.
        if !open.finalized {
            self.context
                .challenge_counters
                .voided
                .fetch_add(1, Ordering::Relaxed);
            debug!(group = group.0, "challenge: round voided, entropy block never finalized");
            return;
        }

        let round = &open.round;

        for spool in &open.spools {
            let Some(owner) = state.spool_owner(*spool) else {
                continue;
            };

            let certified = self.context.round_buffer.is_certified(round.key(*spool));
            let counters = &self.context.challenge_counters;

            // Our own spool keeps no record here, but the quorum our answer
            // gathered is the only local read of what the group made of it, and
            // an operator has nothing else to watch its own standing by.
            if owner == self.context.node_address() {
                if certified {
                    counters.own_certified.fetch_add(1, Ordering::Relaxed);
                } else {
                    counters.own_missed.fetch_add(1, Ordering::Relaxed);
                }
                continue;
            }

            if certified {
                counters.settled_certified.fetch_add(1, Ordering::Relaxed);
            } else {
                counters.settled_missed.fetch_add(1, Ordering::Relaxed);
            }
            debug!(
                spool = %spool,
                round = round.round.0,
                certified,
                "challenge: settling"
            );
            self.record(owner, round.epoch, round.round, certified);
        }
    }

    /// Drop any round a losing candidate seeded, without settling it.
    ///
    /// The evidence goes too: signatures made against a block that lost cannot
    /// aggregate with the replacement's, so keeping them only risks a stale
    /// lookup answering for the new round.
    fn on_rolled(&mut self, hashes: &[tape_crypto::hash::Hash]) {
        for hash in hashes {
            self.context.round_buffer.discard_block(*hash);
            self.open_rounds.retain(|_, open| open.round.block != *hash);
            self.context
                .challenge_counters
                .discarded
                .fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Mark the round a block seeded as standing evidence.
    fn on_finalized(&mut self, hash: tape_crypto::hash::Hash) {
        for open in self.open_rounds.values_mut() {
            if open.round.block == hash {
                open.finalized = true;
            }
        }
    }

    /// Answer this node's own challenge and push it to the group.
    async fn answer_and_broadcast(
        &self,
        state: &ProtocolState,
        round: &Round,
        mine: SpoolIndex,
    ) {
        let Some(answer) = build_answer(&self.context, state, round, mine) else {
            debug!(spool = %mine, round = round.round.0, "challenge: no answer to give");
            return;
        };

        // Hold our own answer, so a peer relaying it back is a duplicate rather
        // than something to verify again.
        self.context.round_buffer.accept_answer(round.key(mine), answer.clone());

        // Attest to it as well. The threshold counts this node among the
        // group's members, so leaving its own signature out costs a position
        // the quorum cannot spare. No relaying: the broadcast below reaches
        // everyone already.
        spawn_attest(&self.context, state, &answer, false);

        let members = group_members(state, round.group);
        trace!(round = round.round.0, peers = members.len(), "challenge: broadcasting");

        for peer in members {
            if peer == self.context.node_address() {
                continue;
            }
            let sent = self
                .context
                .api
                .proof_of_access(peer, &ProofOfAccessReq { answer: answer.clone() })
                .await;
            if let Err(error) = sent {
                trace!(node = %peer, %error, "challenge: broadcast failed");
            }
        }
    }

    /// Fold one outcome into a peer's record, and queue it if the rule fires.
    fn record(&self, peer: Address, epoch: EpochNumber, round: RoundNumber, certified: bool) {
        let Some(record) = fold_outcome(&self.context.store, peer, epoch, round, certified)
        else {
            return;
        };

        if record.eviction_fires() {
            info!(
                node = %peer,
                misses = record.consecutive_misses,
                rate = record.success_rate().0,
                "challenge: peer failed the local rule, queuing for eviction"
            );
            self.context.eviction_queue.insert(peer);
        }
    }
}

/// The current epoch's round grid, if it holds any round at all.
pub fn challenge_schedule(state: &ProtocolState) -> Option<Schedule> {
    schedule_for(state, state.epoch())
}

/// The round grid for an epoch this node still holds state for.
///
/// Settling reaches one epoch back: a round that settles at the boundary
/// belongs to the epoch that just closed, whose bundle the state still keeps.
pub fn schedule_for(state: &ProtocolState, epoch: EpochNumber) -> Option<Schedule> {
    let bundle = if state.current.epoch.id == epoch {
        &state.current.epoch
    } else {
        let previous = state.previous.as_ref()?;
        if previous.epoch.id != epoch {
            return None;
        }
        &previous.epoch
    };

    let seconds = bundle.preferences.epoch_duration.0;
    let schedule = Schedule::for_epoch(bundle.start_slot, seconds * 1_000 / SLOT_MS, &bundle.nonce);
    schedule.validate().ok().map(|()| schedule)
}

/// Every spool position in a group.
pub fn group_spools(state: &ProtocolState, group: GroupIndex) -> Vec<SpoolIndex> {
    state
        .spools_in_group(group)
        .map(|spools| spools.map(|(spool, _)| spool).collect())
        .unwrap_or_default()
}

/// Every other node holding a spool in a group this node also holds one in.
///
/// The group is the unit that challenges itself: only a group-mate holds slices
/// of the same tracks, so only a group-mate can check the answer.
pub fn group_mates(state: &ProtocolState, me: Address) -> Vec<Address> {
    let mut mates = Vec::new();

    for mine in state.member_spools(me) {
        for owner in group_members(state, group_for_spool(mine)) {
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

    // a node challenges everyone else holding a position in its group, once
    // each, and never itself
    #[tokio::test]
    async fn group_not_self() {
        let harness = harness().await;
        let ctx: TestContext = harness.ctx_for(0);
        let me = ctx.node_address();
        let mates = group_mates(&ctx.state(), me);

        assert_eq!(mates.len(), GROUP_SIZE - 1);
        assert!(!mates.contains(&me), "a node cannot challenge itself");

        let mut sorted = mates.clone();
        sorted.sort_unstable();
        sorted.dedup();
        assert_eq!(sorted.len(), mates.len(), "a peer appears twice");
    }

    // a node may hold one spool in each of several groups and owes an answer in
    // every one of them, so a round has to open per group rather than once
    #[tokio::test]
    async fn spools_across_groups() {
        let harness = NodeHarness::builder()
            .nodes(25)
            .current_group_count(2)
            .no_prev_snapshot_tape()
            .build()
            .await
            .expect("build harness");
        let ctx: TestContext = harness.ctx_for(0);
        let state = ctx.state();

        let mut groups: Vec<GroupIndex> = state
            .member_spools(ctx.node_address())
            .into_iter()
            .map(group_for_spool)
            .collect();
        let spread = groups.len();
        groups.sort_unstable();
        groups.dedup();

        assert_eq!(groups.len(), spread, "two spools in one group");
        assert!(spread > 1, "the harness seated this node in one group only");
        for group in groups {
            assert_eq!(group_spools(&state, group).len(), GROUP_SIZE);
        }
    }

    // the harness seats 25 nodes into a 20-wide group, so some hold nothing and
    // there is no question to ask them
    #[tokio::test]
    async fn no_spool() {
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

    // the grid is derived from the epoch's own start slot and duration
    #[tokio::test]
    async fn schedule_from_chain() {
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

    // a zero-length epoch is what a node sees before one is set up, and a grid
    // derived from it would challenge on every block
    #[tokio::test]
    async fn zero_duration() {
        let harness = harness().await;
        let ctx: TestContext = harness.ctx_for(0);
        let mut state = (*ctx.state()).clone();
        state.current.epoch.preferences.epoch_duration = EpochDuration(0);

        assert!(challenge_schedule(&state).is_none());
    }
}
