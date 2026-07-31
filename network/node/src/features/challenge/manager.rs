//! Runs this node's challenge rounds and keeps its record of every group-mate.
//!
//! Rounds sit on a slot grid derived from finalized epoch state, so the whole
//! group reaches the same schedule without coordinating. A round opens a short
//! window; the first finalized block to land inside it is the round's entropy
//! block, and a window that finalizes no block is a void round that counts
//! against nobody.
//!
//! When a round opens this node answers its own challenge and broadcasts, and
//! settles the previous round: any spool whose answer did not certify by then is
//! a local miss. What comes out is a record per peer, not a verdict, so only a
//! sustained pattern proposes anything and the proposal still needs the group and
//! then the network to agree.

use std::collections::HashSet;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};

use rpc::Rpc;
use store::Store;
use tape_core::challenge::elect_splicer;
use tape_core::challenge::schedule::{SLOT_MS, Schedule};
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
use crate::core::types::ChannelName;
use crate::features::block::ingestor::ParsedBlock;
use crate::features::challenge::audition::{
    Round, build_answer, group_members, note_unanswerable, sample_at_cutoff,
};
use crate::features::challenge::fold::fold_outcome;
use crate::features::spool::splice::splice_for_peer;

// What settling needs from a round, captured when the round opened. Settling
// runs epochs later when rounds outpace the boundary, and by then the schedule
// that produced the round is no longer derivable from live state.
struct OpenRound {
    round: Round,
    spools: Vec<SpoolIndex>,
    mine: SpoolIndex,
    base_slot: SlotNumber,
}

pub struct ChallengeManager<Db: Store, Cluster: Api, Blockchain: Rpc> {
    context: Arc<NodeContext<Db, Cluster, Blockchain>>,
    block_rx: mpsc::Receiver<Arc<ParsedBlock>>,
    cancel: CancellationToken,
    // The round this node last opened. A round's window spans several slots and
    // only its first finalized block seeds it, so the rest of the window is
    // ignored, and the previous round is settled when the next one opens.
    last_run: Option<OpenRound>,
    // Group splices in flight, so a repeated miss never doubles the work.
    splices: Arc<Mutex<HashSet<(SpoolIndex, Address)>>>,
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
            splices: Arc::new(Mutex::new(HashSet::new())),
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

    /// Open a round when a finalized block lands in one's entropy window.
    ///
    /// There is no timer here on purpose. The schedule is expressed in slots and
    /// the entropy has to come from a block that finalized, so a block arriving is
    /// the only thing that can open a round.
    async fn on_block(&mut self, block: Arc<ParsedBlock>) -> Result<(), NodeError> {
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
        let Some(mine) = state.member_spools(self.context.node_address()).first().copied() else {
            return Ok(());
        };
        let group = group_for_spool(mine);
        let round = Round {
            epoch,
            group,
            round: number,
            block: block.blockhash,
        };

        // Match on the round, not on the block. A window spans several slots and
        // each carries a different hash, so comparing whole rounds would treat
        // every slot in the window as a new round: it would re-open the round
        // three more times and settle the previous one a slot after it opened,
        // before any attestation could have arrived. The first block in the
        // window seeds the round and the rest of the window is already answered.
        if self
            .last_run
            .as_ref()
            .is_some_and(|open| (open.round.epoch, open.round.round) == (round.epoch, round.round))
        {
            return Ok(());
        }

        self.settle_previous(&state);
        self.last_run = Some(OpenRound {
            round,
            spools: group_spools(&state, group),
            mine,
            base_slot: schedule.base_slot(number),
        });
        self.context
            .challenge_counters
            .opened
            .fetch_add(1, Ordering::Relaxed);

        self.answer_and_broadcast(&state, &round, mine).await;
        self.context
            .round_buffer
            .retire_before(epoch, RoundNumber(number.as_u64().saturating_sub(1)));

        Ok(())
    }

    /// Record how the previous round went for every spool in the group.
    ///
    /// A spool whose answer certified is a success; one that did not is a local
    /// miss. Settling on the next round's opening is what gives a late certificate
    /// the whole interval to arrive and replace a miss before anyone acts on it.
    fn settle_previous(&self, state: &ProtocolState) {
        let Some(open) = self.last_run.as_ref() else {
            return;
        };
        let round = &open.round;

        for spool in &open.spools {
            let Some(owner) = state.spool_owner(*spool) else {
                continue;
            };
            if owner == self.context.node_address() {
                continue;
            }

            let certified = self.context.round_buffer.is_certified(round.key(*spool));
            let counters = &self.context.challenge_counters;
            if certified {
                counters.settled_certified.fetch_add(1, Ordering::Relaxed);
            } else {
                counters.settled_missed.fetch_add(1, Ordering::Relaxed);
                self.maybe_splice(state, round, *spool, owner, open.mine, open.base_slot);
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

    /// How many group splices this node reconstructs at once.
    const MAX_SPLICES_IN_FLIGHT: usize = 2;

    /// Splice for the peer the group just watched fail, if elected.
    ///
    /// Every observer derives the same splicer from the entropy block, so one
    /// node acts and the rest do nothing, with no election and no messages.
    /// The observer's own column names the track the target was asked about.
    fn maybe_splice(
        &self,
        state: &ProtocolState,
        round: &Round,
        failed: SpoolIndex,
        owner: Address,
        mine: SpoolIndex,
        cutoff: SlotNumber,
    ) {
        let candidates: Vec<SpoolIndex> = group_spools(state, round.group)
            .into_iter()
            .filter(|spool| *spool != failed)
            .collect();
        let Some(pick) = elect_splicer(&round.block, failed, candidates.len()) else {
            return;
        };
        let elected = candidates[pick];
        if state.spool_owner(elected) != Some(self.context.node_address()) {
            debug!(
                spool = %failed,
                elected = %elected,
                round = round.round.0,
                "challenge: splice elected elsewhere"
            );
            return;
        }

        let Some(sample) = sample_at_cutoff(&self.context, round, failed, mine, cutoff) else {
            debug!(spool = %failed, "challenge: splice sample underivable");
            return;
        };

        let key = (failed, sample.track);
        {
            let mut splices = self.splices.lock().expect("splice set");
            if splices.len() >= Self::MAX_SPLICES_IN_FLIGHT || !splices.insert(key) {
                debug!(spool = %failed, track = %sample.track, "challenge: splice slots full");
                return;
            }
        }

        info!(
            spool = %failed,
            node = %owner,
            track = %sample.track,
            "challenge: elected splicer, reconstructing"
        );
        let context = self.context.clone();
        let splices = self.splices.clone();
        tokio::spawn(async move {
            splice_for_peer(context, failed, owner, sample.track).await;
            splices.lock().expect("splice set").remove(&key);
        });
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
            note_unanswerable(&self.context, state, round, mine);
            return;
        };

        // Hold our own answer, so a peer relaying it back is a duplicate rather
        // than something to verify again.
        self.context.round_buffer.accept_answer(round.key(mine), answer.clone());

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
        match state.previous.as_ref() {
            Some(previous) if previous.epoch.id == epoch => &previous.epoch,
            _ => return None,
        }
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
