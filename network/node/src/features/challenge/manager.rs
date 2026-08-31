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
use tracing::{debug, info, trace, warn};

use crate::context::NodeContext;
use crate::core::error::NodeError;
use crate::core::channels::ChainEvent;
use crate::core::types::ChannelName;
use crate::features::block::ingestor::ParsedBlock;
use crate::features::challenge::audit::{
    Round, build_answer, group_members, has_sample_set, spawn_attest,
};
use crate::features::challenge::fold::fold_outcome;
use crate::features::challenge::tripwire::Judgement;
use crate::features::eviction::queue::Opened;
use crate::features::state::realign::{RealignCause, spawn_realign};

// Capture settlement inputs when the round opens because settlement may cross
// an epoch boundary. Unfinalized or unaskable rounds are void rather than
// charging every spool in the group a miss.
struct OpenRound {
    round: Round,
    spools: Vec<SpoolIndex>,
    finalized: bool,
    askable: bool,
}

pub struct ChallengeManager<Db: Store, Cluster: Api, Blockchain: Rpc> {
    context: Arc<NodeContext<Db, Cluster, Blockchain>>,
    chain_rx: mpsc::Receiver<ChainEvent>,
    cancel: CancellationToken,
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

        // A node re-reading its view judges nobody: every verdict it would reach
        // comes out of the view under suspicion. It still answers for its own
        // spools, because a spool that goes quiet earns the misses that evict
        // it, and that is the harm the suspension exists to avoid.
        let is_suspended = self.context.challenge_tripwire.is_realigning();

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

            if !is_suspended {
                let judgement = self.settle_previous(&state, group);
                self.check_tripwire(group, judgement);
            }

            self.open_rounds.insert(
                group,
                OpenRound {
                    round,
                    spools: group_spools(&state, group),
                    finalized: false,
                    askable: has_sample_set(&self.context, &state, &round),
                },
            );
            self.context
                .challenge_counters
                .opened
                .fetch_add(1, Ordering::Relaxed);

            self.answer_and_broadcast(&state, &round, spool, is_suspended).await;
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

    /// Settles the previous round when the next opens, leaving the full interval
    /// available for a late certificate to replace a miss.
    fn settle_previous(&self, state: &ProtocolState, group: GroupIndex) -> Judgement {
        let mut judgement = Judgement::default();

        let Some(open) = self.open_rounds.get(&group) else {
            return judgement;
        };

        // A round whose entropy block never finalized is void. Nobody owed an
        // answer on a branch that lost, so nobody is charged for one.
        if !open.finalized {
            self.context
                .challenge_counters
                .voided
                .fetch_add(1, Ordering::Relaxed);
            debug!(group = group.0, "challenge: round voided, entropy block never finalized");
            return judgement;
        }

        // Nor did anyone owe an answer to a question the group had no data to
        // ask. Every owner declines the draw and every observer would otherwise
        // charge every one of them a miss, which is the whole group at once.
        if !open.askable {
            self.context
                .challenge_counters
                .voided
                .fetch_add(1, Ordering::Relaxed);
            debug!(group = group.0, "challenge: round voided, group had an empty sample set");
            return judgement;
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

            // Count what the record ends up holding, not what the buffer still
            // has. A certificate folded when it formed is gone from the buffer
            // by the time its round settles, and counting the buffer's silence
            // reports a miss against a peer this node already accepted.
            let stands = self.record(owner, *spool, round.epoch, round.round, certified);
            judgement.peers += 1;
            if stands {
                counters.settled_certified.fetch_add(1, Ordering::Relaxed);
                judgement.certified += 1;
            } else {
                counters.settled_missed.fetch_add(1, Ordering::Relaxed);
            }
            debug!(
                spool = %spool,
                round = round.round.0,
                certified = stands,
                "challenge: settling"
            );
        }

        judgement
    }

    /// Feeds one group's settled round to the tripwire.
    ///
    /// A void round and a group whose whole set of positions is ours judge
    /// nobody, and a round that judged nobody is no evidence either way.
    fn check_tripwire(&mut self, group: GroupIndex, judgement: Judgement) {
        if judgement.peers == 0 {
            return;
        }

        let Some(delay) = self.context.challenge_tripwire.record_round(group, judgement) else {
            return;
        };

        warn!(
            group = group.0,
            peers = judgement.peers,
            "challenge: nothing stood for a run of rounds, realigning"
        );

        // Every target queued so far was judged against the view now under
        // suspicion, and the run arm fires at three misses while this fires
        // later. Drop them rather than propose an eviction off a view the node
        // has already stopped trusting.
        self.context.eviction_queue.clear();
        spawn_realign(&self.context, delay, RealignCause::Tripwire);
    }

    fn on_rolled(&mut self, hashes: &[tape_crypto::hash::Hash]) {
        for hash in hashes {
            self.context.round_buffer.discard_block(*hash);
        }

        // Count the rounds dropped, not the blocks rolled. A window is four
        // slots of an interval a hundred times longer, so almost no rolled
        // block seeded one.
        let before = self.open_rounds.len();
        self.open_rounds.retain(|_, open| !hashes.contains(&open.round.block));
        let dropped = before - self.open_rounds.len();
        if dropped > 0 {
            self.context
                .challenge_counters
                .discarded
                .fetch_add(dropped as u64, Ordering::Relaxed);
        }
    }

    fn on_finalized(&mut self, hash: tape_crypto::hash::Hash) {
        for open in self.open_rounds.values_mut() {
            if open.round.block == hash {
                open.finalized = true;
            }
        }
    }

    async fn answer_and_broadcast(
        &self,
        state: &ProtocolState,
        round: &Round,
        mine: SpoolIndex,
        is_suspended: bool,
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
        // everyone already. A suspended node signs nothing: an attestation is a
        // verdict, and its verdicts are what it has stopped trusting.
        if !is_suspended {
            spawn_attest(&self.context, state, &answer, false);
        }

        let members = group_members(state, round.group);
        trace!(round = round.round.0, peers = members.len(), "challenge: broadcasting");

        // Off the loop so a quiet group cannot stall ingest, one at a time so
        // the burst does not trip the peers' rate limits.
        let context = self.context.clone();
        let me = self.context.node_address();
        let answer = answer.clone();
        tokio::spawn(async move {
            for peer in members {
                if peer == me {
                    continue;
                }
                let req = ProofOfAccessReq { answer: answer.clone() };
                if let Err(error) = context.api.proof_of_access(peer, &req).await {
                    debug!(node = %peer, %error, "challenge: broadcast failed");
                }
            }
        });
    }

    /// Applies one spool outcome, then evaluates eviction across the peer's spools.
    fn record(
        &self,
        peer: Address,
        spool: SpoolIndex,
        epoch: EpochNumber,
        round: RoundNumber,
        certified: bool,
    ) -> bool {
        let folded = fold_outcome(&self.context.store, peer, spool, epoch, round, certified);
        let Some(record) = folded.record else {
            return folded.certified;
        };

        if record.eviction_fires() {
            info!(
                node = %peer,
                spool = spool.0,
                misses = record.consecutive_misses,
                rate = record.success_rate().0,
                "challenge: peer failed the local rule"
            );
            // With eviction off there is no manager draining the queue, so
            // leave it empty. The record is kept either way.
            if self.context.config.eviction_enabled() {
                self.context
                    .eviction_queue
                    .insert(peer, epoch, Opened::Record);
            }
        }

        folded.certified
    }
}

pub fn challenge_schedule(state: &ProtocolState) -> Option<Schedule> {
    schedule_for(state, state.epoch())
}

/// Returns the schedule for the current or immediately preceding epoch.
///
/// The previous schedule remains available for rounds that settle across an
/// epoch boundary.
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

pub fn group_spools(state: &ProtocolState, group: GroupIndex) -> Vec<SpoolIndex> {
    state
        .spools_in_group(group)
        .map(|spools| spools.map(|(spool, _)| spool).collect())
        .unwrap_or_default()
}

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
    use crate::core::ingest::IngestState;
    use crate::features::block::ingestor::ParsedBlock;
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

    /// A context at the tip, in an active epoch, on a grid that holds rounds.
    async fn ready_context() -> TestContext {
        let harness = harness().await;
        let ctx: TestContext = harness.ctx_for(0);

        let mut state = (*ctx.state()).clone();
        state.current.epoch.preferences.epoch_duration = EpochDuration(3_600);
        state.current.epoch.state.phase = EpochPhase::Active as u64;
        ctx.set_state(state).expect("publish");
        ctx.ingest.publish(IngestState::AtTip);

        ctx
    }

    fn manager_for(ctx: &TestContext) -> ChallengeManager<
        store_memory::MemoryStore,
        peer_memory::MemoryApi,
        rpc_litesvm::LiteSvmRpc,
    > {
        let (_tx, rx) = mpsc::channel(4);
        ChallengeManager::new(ctx.clone(), rx, CancellationToken::new())
    }

    fn first_round_block(ctx: &TestContext) -> Arc<ParsedBlock> {
        let schedule = challenge_schedule(&ctx.state()).expect("a usable grid");
        Arc::new(ParsedBlock {
            slot: schedule.first_slot(),
            blockhash: tape_crypto::hash::Hash([0x11; 32]),
            ..ParsedBlock::default()
        })
    }

    // a suspended node stops judging its peers, and goes on answering for its
    // own spools. Going quiet would earn it the consecutive misses that evict
    // it, which is the harm the suspension exists to prevent.
    #[tokio::test]
    async fn suspended_node_still_opens_its_own_rounds() {
        let ctx = ready_context().await;
        let mut manager = manager_for(&ctx);

        assert!(ctx.challenge_tripwire.trip().is_some());
        assert!(ctx.challenge_tripwire.is_realigning());

        manager
            .on_produced(first_round_block(&ctx))
            .await
            .expect("produced");

        assert!(!manager.open_rounds.is_empty(), "a suspended node stopped answering");
        assert!(ctx.challenge_counters.opened.load(Ordering::Relaxed) > 0);
        assert_eq!(ctx.challenge_counters.settled_missed.load(Ordering::Relaxed), 0);
        assert_eq!(ctx.challenge_counters.settled_certified.load(Ordering::Relaxed), 0);
    }

    // and an unsuspended one opens them the same way, so the assertion above is
    // about the suspension rather than about the grid
    #[tokio::test]
    async fn healthy_node_opens_its_own_rounds() {
        let ctx = ready_context().await;
        let mut manager = manager_for(&ctx);

        manager
            .on_produced(first_round_block(&ctx))
            .await
            .expect("produced");

        assert!(!manager.open_rounds.is_empty());
        assert!(ctx.challenge_counters.opened.load(Ordering::Relaxed) > 0);
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
