use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use tape_rpc::Rpc;
use tape_store_api::Store;
use tape_core::challenge::schedule::{
    MAX_PENDING_ROUNDS, SETTLE_DEADLINE_SLOTS, SLOT_MS, Schedule, round_width_slots,
};
use tape_core::erasure::group_for_spool;
use tape_core::system::EpochPhase;
use tape_core::types::{EpochNumber, GroupIndex, RoundNumber, SlotNumber, SpoolIndex};
use tape_crypto::Address;
use tape_crypto::hash::Hash;
use tape_protocol::api::ProofOfAccessReq;
use tape_protocol::{Api, ProtocolState};
use tokio::sync::mpsc;
use tokio::time::Instant;
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
use crate::features::challenge::chain_phase::{ChainPhase, PhaseLedger};
use crate::features::challenge::fold::fold_outcome;
use crate::features::challenge::trace::{MarkKind, TraceClose};
use crate::features::eviction::queue::Opened;
use crate::features::state::realign::{RealignCause, spawn_realign};

/// Peers an answer goes to at once, bounded to stay under their rate limits.
const BROADCAST_CONCURRENCY: usize = 8;

// Capture settlement inputs when the round opens because settlement may cross
// an epoch boundary. A round nobody could answer is void, which charges nobody,
// where a verdict would charge every spool in the group a miss.
struct OpenRound {
    round: Round,
    spools: Vec<Asked>,
    opened_slot: SlotNumber,
    opened_at: Instant,
    confirmed: bool,
    askable: bool,
    is_suspect: bool,
}

/// One spool a round put a question to.
struct Asked {
    spool: SpoolIndex,
    owner: Address,
    handed: bool,
}

impl OpenRound {
    /// Its block confirmed and the round has had its full width to answer, or it
    /// waited past the point where the block could still confirm.
    ///
    /// The width runs on this node's clock from the opening. A node behind the
    /// tip ingests the blocks after the opening in a burst, and counting them
    /// settled its rounds before a certificate could form.
    fn settles_at(&self, now: SlotNumber, width: Duration) -> bool {
        let elapsed = now.as_u64().saturating_sub(self.opened_slot.as_u64());
        (self.confirmed && self.opened_at.elapsed() >= width) || elapsed >= SETTLE_DEADLINE_SLOTS
    }
}

pub struct ChallengeManager<Db: Store, Cluster: Api, Blockchain: Rpc> {
    context: Arc<NodeContext<Db, Cluster, Blockchain>>,
    chain_rx: mpsc::Receiver<ChainEvent>,
    cancel: CancellationToken,
    phase: PhaseLedger,
    open_rounds: HashMap<GroupIndex, VecDeque<OpenRound>>,
    first_round: HashMap<EpochNumber, RoundNumber>,
    certified_in: HashSet<(EpochNumber, SpoolIndex)>,
    rolled_rounds: HashSet<(EpochNumber, RoundNumber)>,
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
        // Built before ingest has promoted a block, so the state still holds the
        // bootstrap checkpoint the lane's first block follows.
        let state = context.state();
        let phase = PhaseLedger::new(ChainPhase {
            epoch: state.epoch(),
            phase: state.phase(),
        });
        Self {
            context,
            chain_rx,
            cancel,
            phase,
            open_rounds: HashMap::new(),
            first_round: HashMap::new(),
            certified_in: HashSet::new(),
            rolled_rounds: HashSet::new(),
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
                        ChainEvent::Confirmed(hash) => self.on_confirmed(hash),
                    }
                }
            }
        }
    }

    async fn on_produced(&mut self, block: Arc<ParsedBlock>) -> Result<(), NodeError> {
        // The phase the blocks before this one left. Protocol state takes a
        // block later and on another task, so gating on it let two nodes on the
        // same chain differ about a round that opened beside a phase change.
        let seen = self.phase.advance(&block);
        let chain = seen.produced_in;
        let state = self.context.state();

        // Retain the epoch's grid while its rounds can still settle, including
        // after the phase or membership changes.
        self.context.schedules.observe(&state, block.slot);

        if let Some(epoch) = seen.turned_active {
            self.anchor_grace(epoch, block.slot);
        }

        // Before the phase gate: an epoch that left Active still owes a
        // verdict on every round it opened.
        self.settle_confirmed(block.slot);

        if chain.phase != EpochPhase::Active {
            return Ok(());
        }

        // Membership comes from the state, so it has to be on the round's
        // epoch. A state still behind it or already past it takes no part.
        if state.epoch() != chain.epoch {
            return Ok(());
        }

        let Some(schedule) = self.context.schedules.get(chain.epoch) else {
            return Ok(());
        };

        let epoch = chain.epoch;
        let mine = state.member_spools(self.context.node_address());
        if mine.is_empty() {
            return Ok(());
        }

        // A spool this node no longer holds is one it can no longer settle, so
        // its group's pending round is dropped without a verdict.
        let held: Vec<GroupIndex> = mine.iter().map(|spool| group_for_spool(*spool)).collect();
        self.open_rounds.retain(|group, _| held.contains(group));

        self.void_skipped(&schedule, epoch, &block, held.len() as u64);

        let Some(number) = schedule.round_at(block.slot) else {
            return Ok(());
        };

        // A node that started after the chain turned Active never saw the turn,
        // so its grace counts from the first opening it saw, which is later
        // than its peers' anchor and only lengthens the grace.
        self.first_round.entry(epoch).or_insert(number);
        // The chain put a block back at a lost opening, which opens afresh.
        self.rolled_rounds.remove(&(epoch, number));
        // Two epochs is everything a settling round can reach back to.
        let keep = epoch.as_u64().saturating_sub(1);
        self.first_round.retain(|held, _| held.as_u64() >= keep);
        self.certified_in.retain(|(held, _)| held.as_u64() >= keep);
        self.rolled_rounds.retain(|(held, _)| held.as_u64() >= keep);

        // Readiness bounds taking part and nothing else: the round is the
        // chain's either way. A node that reached the opening too late can
        // neither answer nor judge it, so it counts the round void and builds
        // nothing for it. The opening block is past rooting by then, so no
        // rollback can take the count back.
        if !self.is_ready_at(block.slot) {
            self.context
                .challenge_counters
                .voided
                .fetch_add(held.len() as u64, Ordering::Relaxed);
            debug!(round = number.0, "challenge: round void, its opening reached behind the tip");
            return Ok(());
        }

        for spool in mine {
            let group = group_for_spool(spool);
            let round = Round {
                epoch,
                group,
                round: number,
                block: block.blockhash,
            };

            // Past its deadline already, so settle it on the way out.
            let pending = self.open_rounds.entry(group).or_default();
            let stale = (pending.len() >= MAX_PENDING_ROUNDS)
                .then(|| pending.pop_front())
                .flatten();
            if let Some(stale) = stale {
                let outcome = self.settle_round(&stale);
                self.observe_outcome(group, outcome);
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
                opened_at: Instant::now(),
                confirmed: false,
                askable: has_sample_set(&self.context, &round),
                is_suspect: self.context.challenge_tripwire.is_realigning(),
            });
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

    /// Anchors an epoch's handover grace on its first round opening after the
    /// block that turned it Active, the same round on every node that ingested
    /// that block whatever it held or how far behind the tip it ran.
    fn anchor_grace(&mut self, epoch: EpochNumber, turned_at: SlotNumber) {
        let Some(schedule) = self.context.schedules.get(epoch) else {
            return;
        };
        let first = schedule.round_from(SlotNumber(turned_at.as_u64() + 1));
        self.first_round.insert(epoch, first);
    }

    /// Whether this node reached a round's opening in time to take part in it.
    ///
    /// Read inline from the chain tip as the block arrives, in slots, so the
    /// round's own block is never in question. A node that reaches the opening
    /// `SETTLE_DEADLINE_SLOTS` or more behind the tip finds the round's
    /// settlement deadline already come, and can neither certify a peer nor be
    /// certified. The ingest monitor's 500 ms tick is too coarse for a round
    /// five slots wide.
    fn is_ready_at(&self, slot: SlotNumber) -> bool {
        let progress = self.context.ingest.progress();
        let tip = progress.known_tip().unwrap_or(slot.as_u64());
        tip.saturating_sub(slot.as_u64()) < SETTLE_DEADLINE_SLOTS
    }

    /// Counts the round openings the chain skipped, which charge nobody anywhere,
    /// once for each group this node holds a spool in, as an opened round is.
    ///
    /// A block and its parent are consecutive in the chain's own history, so
    /// every node that ingests the block reads the same openings as skipped and
    /// opens none of them. Only openings inside the settlement horizon are
    /// counted: an older one was void before this node could have reached it.
    /// An opening this node saw on a block that lost already counted as
    /// discarded, so it counts nothing here.
    fn void_skipped(
        &mut self,
        schedule: &Schedule,
        epoch: EpochNumber,
        block: &ParsedBlock,
        groups: u64,
    ) {
        let horizon = block.slot.as_u64().saturating_sub(SETTLE_DEADLINE_SLOTS);
        let from = SlotNumber(block.parent_slot.as_u64().max(horizon));
        for number in schedule.skipped_rounds(from, block.slot) {
            if self.rolled_rounds.remove(&(epoch, number)) {
                continue;
            }
            self.context
                .challenge_counters
                .skipped
                .fetch_add(groups, Ordering::Relaxed);
            debug!(round = number.0, "challenge: round void, its slot produced no block");
        }
    }

    /// Verified rounds whose block confirmed, voids those that waited too long.
    /// Oldest first, so it stops at the first round still waiting.
    fn settle_confirmed(&mut self, now: SlotNumber) {
        let slot_ms = self.context.ingest.progress().slot_ms().unwrap_or(SLOT_MS);
        let width = Duration::from_millis(round_width_slots() * slot_ms);
        let mut ready = Vec::new();
        for pending in self.open_rounds.values_mut() {
            while pending.front().is_some_and(|open| open.settles_at(now, width)) {
                if let Some(open) = pending.pop_front() {
                    ready.push(open);
                }
            }
        }
        self.open_rounds.retain(|_, pending| !pending.is_empty());
        for open in &ready {
            let group = open.round.group;
            let outcome = self.settle_round(open);
            self.observe_outcome(group, outcome);
        }
    }

    /// Why this round charges nobody, or nothing when the group can be judged.
    fn void_cause(&self, open: &OpenRound) -> Option<(TraceClose, &'static str)> {
        // Nobody owed an answer on a branch that lost.
        if !open.confirmed {
            return Some((TraceClose::Unconfirmed, "entropy block never confirmed"));
        }

        // Evidence opened under a suspect view, or still pending when another
        // group trips the view, cannot safely verify an owner.
        if open.is_suspect || self.context.challenge_tripwire.is_realigning() {
            return Some((TraceClose::Nothing, "realigning"));
        }

        // Nor did anyone owe an answer to a question the group had no data to
        // ask. Every owner declines the draw and every observer would otherwise
        // charge every one of them a miss, which is the whole group at once. A
        // store short of the round's cut holds no set either, and judging off
        // it would charge the group for a question this node never held.
        if !open.askable {
            return Some((TraceClose::Nothing, "no sample set to draw from"));
        }

        None
    }

    /// Verify one round: what the group made of every spool it was asked about.
    fn settle_round(&mut self, open: &OpenRound) -> Option<bool> {
        if let Some((close, why)) = self.void_cause(open) {
            self.context
                .challenge_counters
                .voided
                .fetch_add(1, Ordering::Relaxed);
            self.close_trace(&open.round, close);
            debug!(group = open.round.group.0, why, "challenge: round voided");
            return None;
        }

        let round = &open.round;
        let mut weighed = 0u64;
        let mut stood = 0u64;

        let first = self.first_round.get(&round.epoch).copied().unwrap_or(round.round);
        let grace = first.as_u64() + self.context.config.challenge_handover_grace_rounds();

        for asked in &open.spools {
            let (spool, owner) = (asked.spool, asked.owner);
            let certified = self.context.round_buffer.is_certified(round.key(spool));

            // A spool that changed hands is not charged while its new owner
            // fetches what it was handed: it holds none of it yet, and a miss
            // here is for data it never had the chance to keep. Ends at its
            // first certificate, so an owner that proves it holds the spool is
            // verified from then on, and at the grace bound, so one that never
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
            weighed += 1;
            if stands {
                counters.settled_certified.fetch_add(1, Ordering::Relaxed);
                stood += 1;
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
        (weighed > 0).then_some(stood > 0)
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

    /// Feeds one settled group round into the view-drift tripwire.
    fn observe_outcome(&mut self, group: GroupIndex, outcome: Option<bool>) {
        match outcome {
            None => {}
            Some(true) => self.context.challenge_tripwire.record_clean_round(group),
            Some(false) => self.on_blank_round(group),
        }
    }

    /// Suspends and re-reads state once a group's run of blank rounds is long
    /// enough to say the view is wrong rather than the group.
    fn on_blank_round(&mut self, group: GroupIndex) {
        let Some(delay) = self.context.challenge_tripwire.record_blank_round(group) else {
            return;
        };

        warn!(group = group.0, "challenge: nothing stood for a run of rounds, realigning");

        // Every target queued so far was weighed against the view now under
        // suspicion. Drop them rather than propose an eviction off a view the node
        // has already stopped trusting.
        self.context.eviction_queue.clear_records();
        // Rounds already open were derived from the same suspect view. Mark
        // them before the asynchronous refresh can complete so none are verified
        // after the suspension lifts.
        for pending in self.open_rounds.values_mut() {
            for open in pending {
                open.is_suspect = true;
            }
        }
        spawn_realign(&self.context, delay, RealignCause::Tripwire);
    }

    fn on_rolled(&mut self, hashes: &[Hash]) {
        self.phase.rolled(hashes);
        for hash in hashes {
            self.context.round_buffer.discard_block(*hash);
            self.context.attest_queue.discard_block(*hash);
        }

        // The count is rounds dropped. One slot of an interval a hundred times
        // longer opens a round, so almost no rolled block seeded one.
        let before: usize = self.open_rounds.values().map(VecDeque::len).sum();
        let mut lost: Vec<(EpochNumber, RoundNumber)> = Vec::new();
        for pending in self.open_rounds.values_mut() {
            pending.retain(|open| {
                if !hashes.contains(&open.round.block) {
                    return true;
                }
                lost.push((open.round.epoch, open.round.round));
                // The trace closes with the block it opened on and re-opens on
                // whatever the chain puts in that block's place.
                self.context.round_traces.close(
                    open.round.epoch,
                    open.round.round,
                    open.round.group,
                    TraceClose::Unconfirmed,
                );
                false
            });
        }
        let after: usize = self.open_rounds.values().map(VecDeque::len).sum();
        let dropped = before - after;
        if dropped > 0 {
            self.context
                .challenge_counters
                .discarded
                .fetch_add(dropped as u64, Ordering::Relaxed);
        }
        self.rolled_rounds.extend(lost);
    }

    fn on_confirmed(&mut self, hash: Hash) {
        self.phase.confirmed(hash);
        for open in self.open_rounds.values_mut().flatten() {
            if open.round.block == hash {
                open.confirmed = true;
            }
        }
    }

    async fn answer_and_broadcast(
        &self,
        state: &ProtocolState,
        round: &Round,
        mine: SpoolIndex,
    ) {
        let Some(answer) = build_answer(&self.context, round, mine) else {
            debug!(spool = %mine, round = round.round.0, "challenge: no answer to give");
            return;
        };

        // Hold our own answer, so a peer relaying it back is a duplicate rather
        // than something to verify again.
        self.context.round_buffer.accept_answer(round.key(mine), answer.clone());
        self.mark_trace(round, mine, MarkKind::AnswerOut, Some(self.context.node_address()));

        // Attest to it as well, suspended or not. The threshold counts this node
        // among the group's members, so leaving its own signature out costs a
        // position the quorum cannot spare, and the position it costs is in its
        // own round. The message names the round and nothing about the
        // committee, so it stands whatever the view turns out to be. No
        // relaying: the broadcast below reaches everyone already.
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

        if record.eviction_fires(&self.context.config.eviction.policy) {
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
    use bytemuck::Zeroable;
    use tape_api::event::{EpochCommitted, SnapshotFinalized};
    use tape_blocks::ParsedInstruction;

    use super::*;
    use crate::core::ingest::IngestState;
    use crate::features::block::ingestor::ParsedBlock;
    use crate::harness::{NodeHarness, TestContext};

    /// Nodes the harness seats: five more than one group holds.
    const NODES: usize = 25;

    /// Far enough behind the tip that a round can no longer settle here.
    const FAR_BEHIND_SLOTS: u64 = SETTLE_DEADLINE_SLOTS * 4;

    /// Cadences a fleet may run, in slots: tighter than the settlement deadline,
    /// the deployed devnet grid, and the widest preset.
    const INTERVALS: [u64; 3] = [7, 180, 300];

    type TestManager =
        ChallengeManager<tape_store_memory::MemoryStore, peer_memory::MemoryApi, rpc_litesvm::LiteSvmRpc>;

    async fn harness() -> NodeHarness {
        harness_at(None).await
    }

    /// A harness whose nodes share one challenge grid, or the preset's own.
    async fn harness_at(interval_slots: Option<u64>) -> NodeHarness {
        let mut builder = NodeHarness::builder().nodes(NODES).no_prev_snapshot_tape();
        if let Some(slots) = interval_slots {
            builder = builder.challenge_interval(slots);
        }
        builder.build().await.expect("build harness")
    }

    fn address_of(harness: &NodeHarness, index: usize) -> Address {
        Address::from(harness.node(index).node_address.to_bytes())
    }

    /// Put a context in an active epoch on a grid that holds rounds.
    ///
    /// The ingest monitor is left saying the opposite of what the round path
    /// reads, so nothing below can pass through the watch channel.
    fn seat(ctx: &TestContext) {
        let mut state = (*ctx.state()).clone();
        state.current.epoch.start_slot = SlotNumber(13_000);
        let mut previous = state.current.clone();
        previous.epoch.id = EpochNumber(state.epoch().as_u64().saturating_sub(1));
        previous.epoch.start_slot = SlotNumber(4_000);
        state.previous = Some(previous);
        state.current.epoch.state.phase = EpochPhase::Active as u64;
        ctx.set_state(state).expect("publish");

        let first = schedule(ctx).first_slot();
        ctx.ingest.progress().record_tip(first.as_u64());
        ctx.ingest.publish(IngestState::Catching { lag_slots: u64::MAX });
    }

    /// A context at the tip, in an active epoch, on a grid that holds rounds.
    async fn ready_context() -> TestContext {
        ready_context_at(None).await
    }

    /// The same, on a stated interval, so a test can sweep the cadence.
    async fn ready_context_at(interval_slots: Option<u64>) -> TestContext {
        let harness = harness_at(interval_slots).await;
        let ctx: TestContext = harness.ctx_for(0);
        seat(&ctx);

        ctx
    }

    /// Two seated contexts from one harness, each holding a spool.
    async fn seated_pair(interval_slots: Option<u64>) -> (TestContext, TestContext) {
        let harness = harness_at(interval_slots).await;
        let mut seated: Vec<TestContext> = Vec::new();
        for index in 0..NODES {
            let ctx: TestContext = harness.ctx_for(index);
            if ctx.state().member_spools(ctx.node_address()).is_empty() {
                continue;
            }
            seat(&ctx);
            seated.push(ctx);
            if seated.len() == 2 {
                break;
            }
        }

        let second = seated.pop().expect("a second seated node");
        let first = seated.pop().expect("a first seated node");
        (first, second)
    }

    fn schedule(ctx: &TestContext) -> Schedule {
        let state = ctx.state();
        ctx.schedules.observe(&state, state.current.epoch.start_slot);
        ctx.schedules.get(state.epoch()).expect("a usable grid")
    }

    fn manager_for(ctx: &TestContext) -> TestManager {
        let (_tx, rx) = mpsc::channel(4);
        ChallengeManager::new(ctx.clone(), rx, CancellationToken::new())
    }

    /// The only round one manager holds open.
    fn only_round(manager: &TestManager) -> &OpenRound {
        let pending = manager.open_rounds.values().next().expect("one group");
        pending.front().expect("a round opened")
    }

    fn block_at(slot: SlotNumber, blockhash: Hash) -> Arc<ParsedBlock> {
        Arc::new(ParsedBlock {
            slot,
            parent_slot: SlotNumber(slot.as_u64() - 1),
            blockhash,
            ..ParsedBlock::default()
        })
    }

    /// A block at a slot holding the given instructions.
    fn block_with(
        slot: SlotNumber,
        blockhash: Hash,
        instructions: Vec<ParsedInstruction>,
    ) -> Arc<ParsedBlock> {
        Arc::new(ParsedBlock {
            slot,
            parent_slot: SlotNumber(slot.as_u64() - 1),
            blockhash,
            instructions,
            ..ParsedBlock::default()
        })
    }

    /// The epoch moving to Closing, as the chain records it.
    fn commit_epoch(epoch: EpochNumber) -> ParsedInstruction {
        ParsedInstruction::CommitEpoch {
            event: EpochCommitted {
                epoch,
                ..EpochCommitted::zeroed()
            },
        }
    }

    /// The previous epoch's snapshot finalizing, which makes this epoch Active.
    fn finalize_snapshot(epoch: EpochNumber) -> ParsedInstruction {
        let previous = EpochNumber(epoch.as_u64() - 1);
        ParsedInstruction::FinalizeSnapshot {
            epoch: previous,
            event: SnapshotFinalized {
                epoch: previous,
                ..SnapshotFinalized::zeroed()
            },
        }
    }

    /// Move one node's own protocol state, as its state manager would.
    fn set_phase(ctx: &TestContext, phase: EpochPhase) {
        let mut state = (*ctx.state()).clone();
        state.current.epoch.state.phase = phase as u64;
        ctx.set_state(state).expect("publish");
    }

    fn first_round_block(ctx: &TestContext) -> Arc<ParsedBlock> {
        block_at(schedule(ctx).first_slot(), Hash([0x11; 32]))
    }

    fn second_round_block(ctx: &TestContext) -> Arc<ParsedBlock> {
        let schedule = schedule(ctx);
        block_at(
            SlotNumber(schedule.first_slot().0 + schedule.interval_slots),
            Hash([0x22; 32]),
        )
    }

    // a suspended node stops weighing its peers, and goes on answering for its
    // own spools. Going quiet would earn it the consecutive misses that evict
    // it, which is the harm the suspension exists to prevent.
    #[tokio::test]
    async fn suspended_node_still_answers() {
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

    // a round opened while suspended was weighed against the view under
    // suspicion, so the first settle after the suspension lifts charges nobody
    #[tokio::test]
    async fn suspect_rounds_settle_void() {
        let ctx = ready_context().await;
        let mut manager = manager_for(&ctx);

        assert!(ctx.challenge_tripwire.trip().is_some());
        manager
            .on_produced(first_round_block(&ctx))
            .await
            .expect("produced");
        for open in manager.open_rounds.values_mut().flatten() {
            open.confirmed = true;
            open.askable = true;
            open.opened_at = width_ago();
        }

        // Suspension lifts, and the round opened under it settles.
        ctx.challenge_tripwire.settled();
        let now = manager
            .open_rounds
            .values()
            .flatten()
            .map(|open| open.opened_slot.as_u64())
            .max()
            .expect("an open round")
            + round_width_slots();
        manager.settle_confirmed(SlotNumber(now));

        assert_eq!(ctx.challenge_counters.settled_missed.load(Ordering::Relaxed), 0);
        assert!(ctx.challenge_counters.voided.load(Ordering::Relaxed) > 0);
    }

    // suspension is re-read per group: a trip in one group has already emptied
    // the eviction queue, and a later group settling on would refill it
    #[tokio::test]
    async fn suspension_is_read_per_group() {
        let ctx = ready_context().await;
        let mut manager = manager_for(&ctx);

        manager
            .on_produced(first_round_block(&ctx))
            .await
            .expect("produced");
        let opened = ctx.challenge_counters.opened.load(Ordering::Relaxed);

        // Tripped between blocks, as a trip in an earlier group would be.
        assert!(ctx.challenge_tripwire.trip().is_some());
        for open in manager.open_rounds.values_mut().flatten() {
            open.confirmed = true;
            open.askable = true;
        }
        manager
            .on_produced(second_round_block(&ctx))
            .await
            .expect("produced");

        assert!(ctx.challenge_counters.opened.load(Ordering::Relaxed) > opened);
        assert_eq!(ctx.challenge_counters.settled_missed.load(Ordering::Relaxed), 0);
        assert_eq!(ctx.challenge_counters.settled_certified.load(Ordering::Relaxed), 0);
    }

    // and an unsuspended one opens them the same way, so the assertion above is
    // about the suspension rather than about the grid
    #[tokio::test]
    async fn healthy_node_opens_rounds() {
        let ctx = ready_context().await;
        let mut manager = manager_for(&ctx);

        manager
            .on_produced(first_round_block(&ctx))
            .await
            .expect("produced");

        assert!(!manager.open_rounds.is_empty());
        assert!(ctx.challenge_counters.opened.load(Ordering::Relaxed) > 0);
    }

    // one slot seeds a round at every cadence a fleet may run: the slot after an
    // opening belongs to no round, and a late block leaves the next opening alone
    #[tokio::test]
    async fn opens_on_its_slot() {
        for interval in INTERVALS {
            let ctx = ready_context_at(Some(interval)).await;
            let mut manager = manager_for(&ctx);
            assert_eq!(schedule(&ctx).interval_slots, interval);

            let first = first_round_block(&ctx);
            manager
                .on_produced(first.clone())
                .await
                .expect("produce the opening");

            assert_eq!(manager.open_rounds.len(), 1);
            assert_eq!(only_round(&manager).round.round, RoundNumber(0));
            assert_eq!(only_round(&manager).round.block, first.blockhash);
            assert_eq!(only_round(&manager).opened_slot, first.slot);

            let after = block_at(SlotNumber(first.slot.as_u64() + 1), Hash([0x33; 32]));
            manager
                .on_produced(after)
                .await
                .expect("produce the slot after");

            assert_eq!(
                ctx.challenge_counters.opened.load(Ordering::Relaxed),
                1,
                "a slot between openings opened a round at interval {interval}"
            );

            let next = second_round_block(&ctx);
            manager
                .on_produced(next.clone())
                .await
                .expect("produce the next opening");

            let pending = manager.open_rounds.values().next().expect("one group");
            let open = pending.back().expect("next round opened");
            assert_eq!(open.round.round, RoundNumber(1));
            assert_eq!(open.round.block, next.blockhash);
            assert_eq!(open.opened_slot, next.slot);
            assert_eq!(ctx.challenge_counters.opened.load(Ordering::Relaxed), 2);
            assert_eq!(ctx.challenge_counters.settled_missed.load(Ordering::Relaxed), 0);

            // A cadence tighter than the settlement deadline holds both rounds;
            // a wider one has settled the first by now.
            let held = if interval < SETTLE_DEADLINE_SLOTS { 2 } else { 1 };
            assert_eq!(pending.len(), held, "at interval {interval}");
        }
    }

    // a node that reaches an opening too late to take part counts the round
    // void at once and builds nothing for it, while a node at the tip opens it
    // on the block, and both anchor the epoch's grace on the same round
    #[tokio::test]
    async fn lagging_voids() {
        for interval in INTERVALS {
            lagging_at(interval).await;
        }
    }

    async fn lagging_at(interval: u64) {
        let (ahead, behind) = seated_pair(Some(interval)).await;
        let epoch = ahead.state().epoch();
        let block = first_round_block(&ahead);
        behind
            .ingest
            .progress()
            .record_tip(block.slot.as_u64() + FAR_BEHIND_SLOTS);

        let mut at_tip = manager_for(&ahead);
        let mut lagging = manager_for(&behind);
        at_tip.on_produced(block.clone()).await.expect("produced");
        lagging.on_produced(block.clone()).await.expect("produced");

        assert_eq!(only_round(&at_tip).round.block, block.blockhash);
        assert_eq!(ahead.challenge_counters.opened.load(Ordering::Relaxed), 1);

        assert!(lagging.open_rounds.is_empty(), "a node far behind built a round");
        assert!(behind.round_traces.snapshot().is_empty(), "a node far behind traced a round");
        assert_eq!(behind.challenge_counters.opened.load(Ordering::Relaxed), 0);
        assert_eq!(behind.challenge_counters.voided.load(Ordering::Relaxed), 1);
        assert_eq!(behind.challenge_counters.settled_missed.load(Ordering::Relaxed), 0);
        assert_eq!(lagging.first_round.get(&epoch), at_tip.first_round.get(&epoch));
    }

    // readiness is read once, when the opening arrives, so a round already open
    // finishes even if the node falls behind while it runs
    #[tokio::test]
    async fn outlives_a_catch_up() {
        let ctx = ready_context().await;
        let mut manager = manager_for(&ctx);
        let block = first_round_block(&ctx);

        manager.on_produced(block.clone()).await.expect("produced");
        ctx.ingest
            .progress()
            .record_tip(block.slot.as_u64() + FAR_BEHIND_SLOTS);
        for open in manager.open_rounds.values_mut().flatten() {
            open.confirmed = true;
            open.askable = true;
            open.opened_at = width_ago();
        }
        manager.settle_confirmed(SlotNumber(block.slot.as_u64() + round_width_slots()));

        assert_eq!(
            ctx.challenge_counters.voided.load(Ordering::Relaxed),
            0,
            "a round open before the lag was thrown away"
        );
        assert!(ctx.challenge_counters.settled_missed.load(Ordering::Relaxed) > 0);
    }

    // a node behind the tip ingests the blocks after an opening in a burst, and
    // the round still waits out its width on the clock before it settles
    #[tokio::test]
    async fn burst_waits_the_width() {
        let ctx = ready_context().await;
        let mut manager = manager_for(&ctx);
        let block = first_round_block(&ctx);

        manager.on_produced(block.clone()).await.expect("produced");
        for open in manager.open_rounds.values_mut().flatten() {
            open.confirmed = true;
            open.askable = true;
        }
        let burst = SlotNumber(block.slot.as_u64() + round_width_slots());

        manager.settle_confirmed(burst);
        assert_eq!(ctx.challenge_counters.settled_missed.load(Ordering::Relaxed), 0);
        assert!(!manager.open_rounds.is_empty());

        for open in manager.open_rounds.values_mut().flatten() {
            open.opened_at = width_ago();
        }
        manager.settle_confirmed(burst);
        assert!(ctx.challenge_counters.settled_missed.load(Ordering::Relaxed) > 0);
    }

    /// An opening a full round width back on the clock, at the slowest slot time.
    fn width_ago() -> Instant {
        let width = Duration::from_millis(round_width_slots() * SLOT_MS);
        Instant::now().checked_sub(width).expect("a clock past the width")
    }

    // the chain skipped the round's opening slot, so the round is void on the
    // node at the tip and on the one behind it alike: nobody opens it, nobody
    // answers it, and nobody is charged for it
    #[tokio::test]
    async fn skipped_opening_voids() {
        for interval in INTERVALS {
            skipped_at(interval).await;
        }
    }

    async fn skipped_at(interval: u64) {
        let (ahead, behind) = seated_pair(Some(interval)).await;
        let first = schedule(&ahead).first_slot().as_u64();
        behind
            .ingest
            .progress()
            .record_tip(first + FAR_BEHIND_SLOTS);

        // The opening produced nothing, which the next block's parent says.
        let block = Arc::new(ParsedBlock {
            slot: SlotNumber(first + 1),
            parent_slot: SlotNumber(first - 1),
            blockhash: Hash([0x33; 32]),
            ..ParsedBlock::default()
        });

        for ctx in [&ahead, &behind] {
            let mut manager = manager_for(ctx);
            manager.on_produced(block.clone()).await.expect("produced");

            assert!(manager.open_rounds.is_empty());
            assert_eq!(ctx.challenge_counters.opened.load(Ordering::Relaxed), 0);
            assert_eq!(ctx.challenge_counters.skipped.load(Ordering::Relaxed), 1);
            assert_eq!(ctx.challenge_counters.settled_missed.load(Ordering::Relaxed), 0);
        }
    }

    // the chain committed the epoch in the block before an opening, so a node
    // whose state has taken that block and one whose state has not both pass the
    // round by
    #[tokio::test]
    async fn closing_flip_agrees() {
        let (ahead, behind) = seated_pair(None).await;
        let opening = first_round_block(&ahead);
        let commit = block_with(
            SlotNumber(opening.slot.as_u64() - 1),
            Hash([0x44; 32]),
            vec![commit_epoch(ahead.state().epoch())],
        );

        for (ctx, is_applied) in [(&ahead, true), (&behind, false)] {
            let mut manager = manager_for(ctx);
            manager.on_produced(commit.clone()).await.expect("produced");
            if is_applied {
                set_phase(ctx, EpochPhase::Closing);
            }
            manager.on_produced(opening.clone()).await.expect("produced");

            assert!(manager.open_rounds.is_empty(), "applied: {is_applied}");
            assert_eq!(ctx.challenge_counters.opened.load(Ordering::Relaxed), 0);
        }
    }

    // the chain finalized the snapshot in the block before an opening, so the
    // round opens on a node whose state has taken that block and on one whose
    // state has not
    #[tokio::test]
    async fn active_flip_agrees() {
        let (ahead, behind) = seated_pair(None).await;
        for ctx in [&ahead, &behind] {
            set_phase(ctx, EpochPhase::Snapshot);
        }
        let opening = first_round_block(&ahead);
        let finalize = block_with(
            SlotNumber(opening.slot.as_u64() - 1),
            Hash([0x55; 32]),
            vec![finalize_snapshot(ahead.state().epoch())],
        );

        for (ctx, is_applied) in [(&ahead, true), (&behind, false)] {
            let mut manager = manager_for(ctx);
            manager.on_produced(finalize.clone()).await.expect("produced");
            if is_applied {
                set_phase(ctx, EpochPhase::Active);
            }
            manager.on_produced(opening.clone()).await.expect("produced");

            assert_eq!(only_round(&manager).round.round, RoundNumber(0), "applied: {is_applied}");
            assert_eq!(ctx.challenge_counters.opened.load(Ordering::Relaxed), 1);
        }
    }

    // a commit inside the opening block lands after the block was produced, so
    // the round it seeds still opens and the next opening falls in Closing
    #[tokio::test]
    async fn commit_in_opening() {
        let ctx = ready_context().await;
        let mut manager = manager_for(&ctx);
        let opening = first_round_block(&ctx);
        let committing = block_with(
            opening.slot,
            opening.blockhash,
            vec![commit_epoch(ctx.state().epoch())],
        );

        manager.on_produced(committing).await.expect("produced");

        assert_eq!(only_round(&manager).round.round, RoundNumber(0));
        assert_eq!(ctx.challenge_counters.opened.load(Ordering::Relaxed), 1);

        manager.on_produced(second_round_block(&ctx)).await.expect("produced");

        assert_eq!(ctx.challenge_counters.opened.load(Ordering::Relaxed), 1);
    }

    // a state already on the next epoch holds another epoch's membership, so the
    // node takes no part in a round the chain still opens in this one
    #[tokio::test]
    async fn state_ahead() {
        let ctx = ready_context().await;
        let mut manager = manager_for(&ctx);
        let mut state = (*ctx.state()).clone();
        state.current.epoch.id = state.epoch().next();
        ctx.set_state(state).expect("publish");

        manager.on_produced(first_round_block(&ctx)).await.expect("produced");

        assert!(manager.open_rounds.is_empty());
        assert_eq!(ctx.challenge_counters.opened.load(Ordering::Relaxed), 0);
    }

    // a node that started from a state already Active ahead of its blocks
    // anchors its grace where its peers do, on the first opening after the
    // chain turned Active
    #[tokio::test]
    async fn grace_anchor_agrees() {
        let (steady, restarted) = seated_pair(None).await;
        set_phase(&steady, EpochPhase::Snapshot);
        let epoch = steady.state().epoch();
        let before = first_round_block(&steady);
        let turn = block_with(
            SlotNumber(before.slot.as_u64() + 1),
            Hash([0x66; 32]),
            vec![finalize_snapshot(epoch)],
        );
        let after = second_round_block(&steady);

        for ctx in [&steady, &restarted] {
            let mut manager = manager_for(ctx);
            for block in [&before, &turn, &after] {
                manager.on_produced(block.clone()).await.expect("produced");
            }

            assert_eq!(manager.first_round.get(&epoch), Some(&RoundNumber(1)));
        }
    }

    // a skipped opening is void in every group this node holds a spool in, so
    // it counts once per group, the way an opened round does
    #[tokio::test]
    async fn skipped_per_group() {
        let harness = NodeHarness::builder()
            .nodes(NODES)
            .current_group_count(2)
            .no_prev_snapshot_tape()
            .build()
            .await
            .expect("build harness");
        let ctx: TestContext = harness.ctx_for(0);
        seat(&ctx);
        let mut groups: Vec<GroupIndex> = ctx
            .state()
            .member_spools(ctx.node_address())
            .into_iter()
            .map(group_for_spool)
            .collect();
        groups.sort_unstable();
        groups.dedup();
        assert!(groups.len() > 1, "the harness seated this node in one group only");

        let first = schedule(&ctx).first_slot().as_u64();
        let block = Arc::new(ParsedBlock {
            slot: SlotNumber(first + 1),
            parent_slot: SlotNumber(first - 1),
            blockhash: Hash([0x33; 32]),
            ..ParsedBlock::default()
        });
        manager_for(&ctx).on_produced(block).await.expect("produced");

        assert_eq!(
            ctx.challenge_counters.skipped.load(Ordering::Relaxed),
            groups.len() as u64
        );
    }

    // an opening whose block lost counts once, as discarded, when the chain
    // that replaced it produced nothing at that slot
    #[tokio::test]
    async fn rolled_counts_once() {
        let ctx = ready_context().await;
        let mut manager = manager_for(&ctx);
        let opening = first_round_block(&ctx);

        manager.on_produced(opening.clone()).await.expect("produced");
        manager.on_rolled(&[opening.blockhash]);
        let replacement = Arc::new(ParsedBlock {
            slot: SlotNumber(opening.slot.as_u64() + 1),
            parent_slot: SlotNumber(opening.slot.as_u64() - 1),
            blockhash: Hash([0x77; 32]),
            ..ParsedBlock::default()
        });
        manager.on_produced(replacement).await.expect("produced");

        assert_eq!(ctx.challenge_counters.discarded.load(Ordering::Relaxed), 1);
        assert_eq!(ctx.challenge_counters.skipped.load(Ordering::Relaxed), 0);
    }

    // a node one slot short of the settlement deadline behind an opening still
    // takes part, and one a full deadline behind has none left to take part in
    #[tokio::test]
    async fn ready_boundary() {
        let ctx = ready_context().await;
        let manager = manager_for(&ctx);
        let opening = schedule(&ctx).first_slot();
        let progress = ctx.ingest.progress();

        progress.record_tip(opening.as_u64() + SETTLE_DEADLINE_SLOTS - 1);
        assert!(manager.is_ready_at(opening));

        progress.record_tip(opening.as_u64() + SETTLE_DEADLINE_SLOTS);
        assert!(!manager.is_ready_at(opening));
    }

    // a node challenges everyone else holding a position in its group, once
    // each, and never itself
    #[tokio::test]
    async fn group_not_self() {
        let harness = harness().await;
        let ctx: TestContext = harness.ctx_for(0);
        let me = ctx.node_address();
        let mates = group_mates(&ctx.state(), me);

        assert_eq!(mates.len(), ctx.params().n() - 1);
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
            assert_eq!(group_spools(&state, group).len(), ctx.params().n());
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
}
