use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::Ordering;
use std::sync::Arc;

use futures::StreamExt;
use rpc::Rpc;
use store::Store;
use tape_core::challenge::schedule::{
    HANDOVER_GRACE_ROUNDS, MAX_PENDING_ROUNDS, SETTLE_DEADLINE_SLOTS, SLOT_MS, Schedule,
    round_width_slots,
};
use tape_core::erasure::group_for_spool;
use tape_core::system::EpochPhase;
use tape_core::types::{EpochNumber, GroupIndex, RoundNumber, SlotNumber, SpoolIndex};
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
use crate::features::challenge::audit::{
    Round, build_answer, group_members, has_sample_set, spawn_attest,
};
use crate::features::challenge::fold::fold_outcome;
use crate::features::challenge::trace::{MarkKind, TraceClose};
use crate::features::eviction::queue::Opened;

/// Peers an answer goes to at once, bounded to stay under their rate limits.
const BROADCAST_CONCURRENCY: usize = 8;

// Capture settlement inputs when the round opens because settlement may cross
// an epoch boundary. Unfinalized or unaskable rounds are void rather than
// charging every spool in the group a miss.
struct OpenRound {
    round: Round,
    /// The spools asked, with who owed the answer when they were asked.
    ///
    /// Captured here rather than read at settlement: a round settles once its
    /// block roots, which is well after it opened and can be the far side of an
    /// epoch, and the spool may have changed hands by then. Judging against the
    /// owner of the moment charges a miss to a node that was never asked.
    spools: Vec<Asked>,
    opened_slot: SlotNumber,
    finalized: bool,
    askable: bool,
}

/// One spool a round put a question to.
struct Asked {
    spool: SpoolIndex,
    /// Who owed the answer when the round opened.
    owner: Address,
    /// Whether this spool changed hands at the epoch boundary, which both sides
    /// read off the assignments already in state rather than being told.
    handed: bool,
}

impl OpenRound {
    /// Its block rooted and the round has had its full width to answer, or it
    /// waited past the point where the block could still root.
    ///
    /// Rooting alone is not enough: a chain that roots inside a round would
    /// settle it before its proofs are due and charge honest owners a miss for
    /// certificates still forming.
    fn settles_at(&self, now: SlotNumber) -> bool {
        let elapsed = now.as_u64().saturating_sub(self.opened_slot.as_u64());
        (self.finalized && elapsed >= round_width_slots()) || elapsed >= SETTLE_DEADLINE_SLOTS
    }
}

pub struct ChallengeManager<Db: Store, Cluster: Api, Blockchain: Rpc> {
    context: Arc<NodeContext<Db, Cluster, Blockchain>>,
    chain_rx: mpsc::Receiver<ChainEvent>,
    cancel: CancellationToken,
    // Every round a group is waiting on, oldest first: judged on its block
    // rooting, not on the next round opening.
    open_rounds: HashMap<GroupIndex, VecDeque<OpenRound>>,
    /// The first round this node opened in an epoch, which the handover grace
    /// counts from: the grid's early positions fall in phases that do not
    /// challenge, so an epoch's first round is rarely round zero.
    first_round: HashMap<EpochNumber, RoundNumber>,
    /// Spools that have certified at least once this epoch, so a handover is
    /// forgiven only until the new owner shows it holds the data.
    certified_in: HashSet<(EpochNumber, SpoolIndex)>,
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
            first_round: HashMap::new(),
            certified_in: HashSet::new(),
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

        // Before the phase gate: an epoch that left Active still owes a
        // verdict on every round it opened.
        self.settle_finalized(&state, block.slot);

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
            let pending = self.open_rounds.entry(group).or_default();
            let reopening = pending.back().is_some_and(|open| {
                (open.round.epoch, open.round.round) == (round.epoch, round.round)
            });
            if reopening {
                continue;
            }

            // Past its deadline already, so it settles rather than vanishing.
            let stale = (pending.len() >= MAX_PENDING_ROUNDS)
                .then(|| pending.pop_front())
                .flatten();
            if let Some(stale) = stale {
                self.settle_round(&state, &stale);
            }

            self.open_rounds.entry(group).or_default().push_back(OpenRound {
                round,
                spools: group_spools(&state, group)
                    .into_iter()
                    .filter_map(|spool| {
                        let owner = state.spool_owner(spool)?;
                        let handed = state.spool_owner_prev(spool) != Some(owner);
                        Some(Asked { spool, owner, handed })
                    })
                    .collect(),
                opened_slot: block.slot,
                finalized: false,
                askable: has_sample_set(&self.context, &state, &round),
            });
            self.first_round.entry(epoch).or_insert(number);
            // Two epochs is everything a settling round can reach back to.
            let keep = epoch.as_u64().saturating_sub(1);
            self.first_round.retain(|held, _| held.as_u64() >= keep);
            self.certified_in.retain(|(held, _)| held.as_u64() >= keep);
            self.context
                .challenge_counters
                .opened
                .fetch_add(1, Ordering::Relaxed);
            self.context.round_traces.open(
                epoch,
                number,
                group,
                block.slot,
                block.blockhash,
            );

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
            .flatten()
            .map(|open| (open.round.epoch, open.round.round))
            .min()
        {
            self.context.round_buffer.retire_before(epoch, round);
            // Ends the senders gathering for those rounds too: their signatures
            // no longer have evidence to join.
            self.context.attest_queue.retire_before(epoch, round);
        }

        Ok(())
    }

    /// Judges rounds whose block rooted, voids those that waited too long.
    /// Oldest first, so it stops at the first round still waiting.
    fn settle_finalized(&mut self, state: &ProtocolState, now: SlotNumber) {
        let mut ready = Vec::new();
        for pending in self.open_rounds.values_mut() {
            while pending.front().is_some_and(|open| open.settles_at(now)) {
                if let Some(open) = pending.pop_front() {
                    ready.push(open);
                }
            }
        }
        self.open_rounds.retain(|_, pending| !pending.is_empty());
        for open in &ready {
            self.settle_round(state, open);
        }
    }

    /// Judges one round: what the group made of every spool it was asked about.
    fn settle_round(&mut self, state: &ProtocolState, open: &OpenRound) {

        // A round whose entropy block never finalized is void. Nobody owed an
        // answer on a branch that lost, so nobody is charged for one.
        if !open.finalized {
            self.context
                .challenge_counters
                .voided
                .fetch_add(1, Ordering::Relaxed);
            self.close_trace(&open.round, TraceClose::Unfinalized);
            debug!(group = open.round.group.0, "challenge: round voided, entropy block never finalized");
            return;
        }

        // Nor did anyone owe an answer to a question the group had no data to
        // ask. Every owner declines the draw and every observer would otherwise
        // charge every one of them a miss, which is the whole group at once.
        if !open.askable {
            self.context
                .challenge_counters
                .voided
                .fetch_add(1, Ordering::Relaxed);
            self.close_trace(&open.round, TraceClose::Nothing);
            debug!(group = open.round.group.0, "challenge: round voided, group had an empty sample set");
            return;
        }

        let round = &open.round;

        let first = self.first_round.get(&round.epoch).copied().unwrap_or(round.round);
        let grace = first.as_u64() + HANDOVER_GRACE_ROUNDS;

        for asked in &open.spools {
            let (spool, owner) = (asked.spool, asked.owner);
            let certified = self.context.round_buffer.is_certified(round.key(spool));

            // A spool that changed hands is not charged while its new owner
            // fetches what it was handed: it holds none of it yet, and a miss
            // here is for data it never had the chance to keep. Ends at its
            // first certificate, so an owner that proves it holds the spool is
            // judged from then on, and at the grace bound, so one that never
            // fetches is still caught.
            if certified {
                self.certified_in.insert((round.epoch, spool));
            } else if asked.handed
                && round.round.as_u64() < grace
                && !self.certified_in.contains(&(round.epoch, spool))
            {
                debug!(
                    spool = %spool,
                    node = %owner,
                    round = round.round.0,
                    "challenge: spool changed hands, not charged while it fetches"
                );
                continue;
            }

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
                self.settle_trace(round, spool, owner, certified);
                continue;
            }

            // Count what the record ends up holding, not what the buffer still
            // has. A certificate folded when it formed is gone from the buffer
            // by the time its round settles, and counting the buffer's silence
            // reports a miss against a peer this node already accepted.
            let stands = self.record(owner, spool, round.epoch, round.round, certified);
            if stands {
                counters.settled_certified.fetch_add(1, Ordering::Relaxed);
            } else {
                counters.settled_missed.fetch_add(1, Ordering::Relaxed);
            }
            self.settle_trace(round, spool, owner, stands);
            debug!(
                spool = %spool,
                round = round.round.0,
                certified = stands,
                "challenge: settling"
            );
        }

        self.close_trace(round, TraceClose::Settled);
    }

    /// Records one mark against the round's trace.
    fn mark_trace(&self, round: &Round, spool: SpoolIndex, kind: MarkKind, peer: Option<Address>) {
        self.context.round_traces.mark(
            round.epoch,
            round.round,
            round.group,
            spool,
            kind,
            peer,
        );
    }

    /// Records how one spool's round settled.
    fn settle_trace(&self, round: &Round, spool: SpoolIndex, owner: Address, certified: bool) {
        self.context.round_traces.settle(
            round.epoch,
            round.round,
            round.group,
            spool,
            owner,
            certified,
        );
    }

    fn close_trace(&self, round: &Round, close: TraceClose) {
        self.context.round_traces.close(round.epoch, round.round, round.group, close);
    }

    fn on_rolled(&mut self, hashes: &[tape_crypto::hash::Hash]) {
        for hash in hashes {
            self.context.round_buffer.discard_block(*hash);
            self.context.attest_queue.discard_block(*hash);
        }

        // Count the rounds dropped, not the blocks rolled. A window is four
        // slots of an interval a hundred times longer, so almost no rolled
        // block seeded one.
        let before: usize = self.open_rounds.values().map(VecDeque::len).sum();
        for pending in self.open_rounds.values_mut() {
            pending.retain(|open| !hashes.contains(&open.round.block));
        }
        let after: usize = self.open_rounds.values().map(VecDeque::len).sum();
        let dropped = before - after;
        if dropped > 0 {
            self.context
                .challenge_counters
                .discarded
                .fetch_add(dropped as u64, Ordering::Relaxed);
        }
    }

    fn on_finalized(&mut self, hash: tape_crypto::hash::Hash) {
        for open in self.open_rounds.values_mut().flatten() {
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
    ) {
        let Some(answer) = build_answer(&self.context, state, round, mine) else {
            debug!(spool = %mine, round = round.round.0, "challenge: no answer to give");
            return;
        };

        // Hold our own answer, so a peer relaying it back is a duplicate rather
        // than something to verify again.
        self.context.round_buffer.accept_answer(round.key(mine), answer.clone());
        self.mark_trace(round, mine, MarkKind::AnswerOut, Some(self.context.node_address()));

        // Attest to it as well. The threshold counts this node among the
        // group's members, so leaving its own signature out costs a position
        // the quorum cannot spare. No relaying: the broadcast below reaches
        // everyone already.
        spawn_attest(&self.context, state, &answer, false);

        let members = group_members(state, round.group);
        trace!(round = round.round.0, peers = members.len(), "challenge: broadcasting");

        // Off the loop so a quiet group cannot stall ingest. Sending one at a
        // time put the last peer nineteen round-trips behind the first, which
        // was most of the time a quorum took.
        let context = self.context.clone();
        let me = self.context.node_address();
        let answer = answer.clone();
        tokio::spawn(async move {
            let sends = members.into_iter().filter(|peer| *peer != me).map(|peer| {
                let context = context.clone();
                let answer = answer.clone();
                async move {
                    let req = ProofOfAccessReq { answer };
                    if let Err(error) = context.api.proof_of_access(peer, &req).await {
                        debug!(node = %peer, %error, "challenge: broadcast failed");
                    }
                }
            });
            futures::stream::iter(sends)
                .for_each_concurrent(BROADCAST_CONCURRENCY, |send| send)
                .await;
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

    let voted_seconds = bundle.preferences.epoch_duration.0;
    let span = epoch_span_slots(state, voted_seconds);
    let schedule = Schedule::for_epoch(bundle.start_slot, span, &bundle.nonce);
    schedule.validate().ok().map(|()| schedule)
}

/// Slots to lay the grid over: the last epoch's realized span.
///
/// Both epochs on hand are sized by it. The previous one's span is exactly what
/// elapsed between the two starts, and the current one has not ended, so what
/// the cluster just did is the best guess at what it will do.
///
/// Read off two on-chain start slots, so every node computes the same number and
/// the grid stays something they agree on without measuring anything locally. A
/// cluster running at a different slot time than `SLOT_MS` self-corrects within
/// an epoch, where converting the voted seconds stays wrong by that ratio for
/// good. Falls back to the conversion at genesis, when there is no previous
/// epoch to have realized anything.
fn epoch_span_slots(state: &ProtocolState, voted_seconds: u64) -> u64 {
    state
        .previous
        .as_ref()
        .and_then(|previous| {
            state
                .current
                .epoch
                .start_slot
                .as_u64()
                .checked_sub(previous.epoch.start_slot.as_u64())
        })
        .filter(|span| *span > 0)
        .unwrap_or(voted_seconds * 1_000 / SLOT_MS)
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

    // the grid starts at the epoch's own start slot and covers the span the
    // epoch before it realized
    #[tokio::test]
    async fn schedule_from_chain() {
        let harness = harness().await;
        let ctx: TestContext = harness.ctx_for(0);
        let mut state = (*ctx.state()).clone();
        state.current.epoch.start_slot = SlotNumber(13_000);
        state.current.epoch.preferences.epoch_duration = EpochDuration(3_600);
        state.previous = None;

        let schedule = challenge_schedule(&state).expect("a usable grid");
        assert_eq!(schedule.epoch_start_slot, SlotNumber(13_000));
        assert_eq!(schedule.rounds(), 1_286);
        assert!(schedule.interval_slots >= round_width_slots());
        assert!(schedule.validate().is_ok());
    }

    // a cluster whose slots are faster than SLOT_MS realizes more slots per
    // epoch, and the grid has to cover them rather than the nominal count
    #[tokio::test]
    async fn grid_follows_the_realized_span() {
        let harness = harness().await;
        let ctx: TestContext = harness.ctx_for(0);
        let mut state = (*ctx.state()).clone();
        state.current.epoch.start_slot = SlotNumber(30_000);
        state.current.epoch.preferences.epoch_duration = EpochDuration(3_600);

        // No previous epoch: nothing has been realized, so the voted seconds
        // are all there is to go on.
        state.previous = None;
        let nominal = challenge_schedule(&state).expect("a usable grid").rounds();

        // The last epoch ran 21,800 slots for those same 3,600 seconds, which is
        // the ~165ms slot devnet actually produces rather than the nominal 400.
        let mut previous = state.current.clone();
        previous.epoch.start_slot = SlotNumber(30_000 - 21_800);
        previous.epoch.id = EpochNumber(state.current.epoch.id.as_u64().saturating_sub(1));
        state.previous = Some(previous);

        let realized = challenge_schedule(&state).expect("a usable grid");
        assert_eq!(realized.epoch_slots, 21_800, "the span is what elapsed");
        assert!(
            realized.rounds() > nominal * 2,
            "a grid sized off {SLOT_MS}ms slots covers under half a 165ms epoch: \
             {} rounds against {nominal}",
            realized.rounds(),
        );
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
