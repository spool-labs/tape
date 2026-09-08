//! The grid each epoch was given, kept as it was first laid.

use std::collections::HashMap;
use std::sync::Mutex;

use tape_core::challenge::schedule::{SETTLE_DEADLINE_SLOTS, Schedule};
use tape_core::types::{EpochNumber, SlotNumber};
use tape_protocol::ProtocolState;

/// The grids this node laid, one epoch each.
///
/// An epoch's grid is sized by the span the epoch before it realized, and that
/// input moves the instant the epoch turns: deriving epoch N's grid again once
/// N+1 is current sizes it off N's own span instead, which shifts the cadence,
/// the offset and every sample cutoff along with them. A round that opens
/// before the turn and settles after it would then be judged against a question
/// nobody was asked, and a node that recomputes would disagree with one that
/// remembers. So an epoch's grid is derived once, while the epoch is current,
/// and kept as it was.
#[derive(Default)]
pub struct Schedules {
    held: Mutex<HashMap<EpochNumber, Schedule>>,
}

impl Schedules {
    /// Lays the current epoch's grid if this node has not laid it yet, and
    /// drops the epochs that can hold no settling round any more.
    ///
    /// Every input is fixed for the epoch's whole life, so a node reaching here
    /// late in the epoch lays the same grid as one that reached it at the start.
    pub fn observe(&self, state: &ProtocolState, now: SlotNumber) {
        let Ok(mut held) = self.held.lock() else { return };

        let epoch = state.epoch();
        if let Some(schedule) = current_schedule(state) {
            held.entry(epoch).or_insert(schedule);
        }

        // The epoch before this one keeps its grid until its last round has run
        // out its deadline: those rounds settle on this side of the turn and are
        // judged against the grid they were asked with.
        let settled =
            now.as_u64() >= state.current.epoch.start_slot.as_u64() + SETTLE_DEADLINE_SLOTS;
        let oldest = if settled {
            epoch.as_u64()
        } else {
            epoch.as_u64().saturating_sub(1)
        };
        held.retain(|epoch, _| epoch.as_u64() >= oldest);
    }

    /// The grid this node laid for an epoch, and nothing if it never laid one.
    ///
    /// A node that restarted or joined after the turn holds no grid for the
    /// epoch before it and takes no part in what is left of that epoch's rounds:
    /// it neither answers them nor judges them, the same as a node with no grid
    /// at all. That costs it the rounds still settling and nothing beyond them.
    pub fn get(&self, epoch: EpochNumber) -> Option<Schedule> {
        self.held.lock().ok()?.get(&epoch).copied()
    }
}

/// The grid for the state's current epoch, laid over the span the epoch before
/// it realized.
///
/// The current epoch only. Once an epoch has turned, the span that sized it is
/// no longer the one state reads out, which is the whole reason its grid is
/// kept rather than derived again.
fn current_schedule(state: &ProtocolState) -> Option<Schedule> {
    let bundle = &state.current.epoch;
    let span = epoch_span_slots(state)?;
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
/// good.
///
/// Nothing when there is no previous epoch to have realized anything. Converting
/// the voted seconds instead would size one node's grid off a different
/// measurement than its peers': the two disagree by whatever the cluster's slot
/// time is not, and the cadence, the slack and the offset all move with it. That
/// node would challenge on slots nobody answers and judge its group on rounds
/// they never opened. A node without an epoch behind it waits for one.
fn epoch_span_slots(state: &ProtocolState) -> Option<u64> {
    let previous = state.previous.as_ref()?;
    state
        .current
        .epoch
        .start_slot
        .as_u64()
        .checked_sub(previous.epoch.start_slot.as_u64())
        .filter(|span| *span > 0)
}

#[cfg(test)]
mod tests {
    use tape_core::challenge::schedule::round_width_slots;
    use tape_core::types::{EpochDuration, RoundNumber};

    use super::*;
    use crate::harness::{NodeHarness, TestContext};

    async fn state() -> ProtocolState {
        let harness = NodeHarness::builder()
            .nodes(25)
            .no_prev_snapshot_tape()
            .build()
            .await
            .expect("build harness");
        let ctx: TestContext = harness.ctx_for(0);
        let mut state = (*ctx.state()).clone();
        state.current.epoch.preferences.epoch_duration = EpochDuration(3_600);
        state
    }

    /// A state whose current epoch starts at `start` and whose previous one
    /// realized `span` slots before it.
    fn realized(state: &mut ProtocolState, start: u64, span: u64) {
        state.current.epoch.start_slot = SlotNumber(start);
        let mut previous = state.current.clone();
        previous.epoch.id = EpochNumber(state.current.epoch.id.as_u64().saturating_sub(1));
        previous.epoch.start_slot = SlotNumber(start - span);
        state.previous = Some(previous);
    }

    /// Turns the epoch over: the current one becomes the previous one and the
    /// next starts at `start`, so what the old one realized is the gap between
    /// the two starts.
    fn turn(state: &mut ProtocolState, start: u64) {
        state.previous = Some(state.current.clone());
        state.current.epoch.id = EpochNumber(state.current.epoch.id.as_u64() + 1);
        state.current.epoch.start_slot = SlotNumber(start);
    }

    // the grid starts at the epoch's own start slot and covers the span the
    // epoch before it realized
    #[tokio::test]
    async fn schedule_from_chain() {
        let mut state = state().await;
        // The 9,000 slots an hour of 400ms slots comes to.
        realized(&mut state, 13_000, 9_000);

        let schedules = Schedules::default();
        schedules.observe(&state, SlotNumber(13_000));

        let schedule = schedules.get(state.epoch()).expect("a usable grid");
        assert_eq!(schedule.epoch_start_slot, SlotNumber(13_000));
        assert_eq!(schedule.rounds(), 1_286);
        assert!(schedule.interval_slots >= round_width_slots());
        assert!(schedule.validate().is_ok());
    }

    // a cluster whose slots are faster than the nominal 400ms realizes more
    // slots per epoch, and the grid has to cover them rather than the count the
    // voted duration converts to
    #[tokio::test]
    async fn grid_follows_the_realized_span() {
        let mut state = state().await;

        realized(&mut state, 30_000, 9_000);
        let schedules = Schedules::default();
        schedules.observe(&state, SlotNumber(30_000));
        let nominal = schedules.get(state.epoch()).expect("a usable grid").rounds();

        // The last epoch ran 21,800 slots for those same 3,600 seconds, which is
        // the ~165ms slot devnet actually produces rather than the nominal 400.
        realized(&mut state, 30_000, 21_800);
        let schedules = Schedules::default();
        schedules.observe(&state, SlotNumber(30_000));
        let realized = schedules.get(state.epoch()).expect("a usable grid");

        assert_eq!(realized.epoch_slots, 21_800, "the span is what elapsed");
        assert!(
            realized.rounds() > nominal * 2,
            "a grid sized off 400ms slots covers under half a 165ms epoch: \
             {} rounds against {nominal}",
            realized.rounds(),
        );
    }

    // nothing has been realized before the first epoch boundary, and a grid
    // guessed from the voted seconds would be one no peer is laying
    #[tokio::test]
    async fn no_epoch_behind_it_no_grid() {
        let mut state = state().await;
        state.current.epoch.start_slot = SlotNumber(13_000);
        state.previous = None;

        let schedules = Schedules::default();
        schedules.observe(&state, SlotNumber(13_000));

        assert!(schedules.get(state.epoch()).is_none());
    }

    // a span too short to hold a round has no grid, whatever was voted for it:
    // one derived from it would challenge on every block
    #[tokio::test]
    async fn a_span_too_short_has_no_grid() {
        let mut state = state().await;
        realized(&mut state, 10_000, 2);

        let schedules = Schedules::default();
        schedules.observe(&state, SlotNumber(10_000));

        assert!(schedules.get(state.epoch()).is_none());
    }

    // two epochs of different realized lengths, and a round that opens in the
    // first and settles in the second: the grid it was asked with is the grid it
    // is judged against, which deriving it again after the turn would not be
    #[tokio::test]
    async fn the_grid_survives_the_turn() {
        let mut state = state().await;
        // Epoch N is current, sized by the 9,000 slots N-1 realized.
        realized(&mut state, 13_000, 9_000);
        let spanning = state.epoch();

        let schedules = Schedules::default();
        schedules.observe(&state, SlotNumber(13_000));
        let asked = schedules.get(spanning).expect("a grid for the current epoch");

        // N ran 21,800 slots, longer than the epoch before it, and N+1 begins.
        turn(&mut state, 34_800);
        schedules.observe(&state, SlotNumber(34_800));

        let judged = schedules.get(spanning).expect("the grid the round was asked with");
        assert_eq!(asked, judged, "the epoch's grid moved when the epoch turned");

        // What deriving it again after the turn would have laid instead, which
        // is a different cadence, a different offset and different cutoffs.
        let recomputed = Schedule::for_epoch(
            SlotNumber(13_000),
            21_800,
            &state.previous.as_ref().expect("previous").epoch.nonce,
        );
        assert_ne!(recomputed, judged, "the two spans would have laid the same grid");
        assert_ne!(
            recomputed.sample_cutoff(RoundNumber(3)),
            judged.sample_cutoff(RoundNumber(3)),
            "a recompute would draw a round's question from a different cut",
        );
    }

    // a node that never saw the epoch has no grid for it and takes no part in
    // what is left of its rounds
    #[tokio::test]
    async fn a_node_that_missed_the_epoch_abstains() {
        let mut state = state().await;
        realized(&mut state, 13_000, 9_000);
        let spanning = state.epoch();
        turn(&mut state, 34_800);

        // Started inside the settle window, with only the new epoch to see.
        let schedules = Schedules::default();
        schedules.observe(&state, SlotNumber(34_810));

        assert!(schedules.get(spanning).is_none(), "a grid it never laid");
        assert!(schedules.get(state.epoch()).is_some(), "no grid for the epoch it did see");
    }

    // the previous epoch's grid stays through the settle window and goes once
    // no round of that epoch can still be waiting, and older epochs go with the
    // turn
    #[tokio::test]
    async fn kept_through_the_settle_window() {
        let mut state = state().await;
        realized(&mut state, 13_000, 9_000);
        let oldest = state.epoch();

        let schedules = Schedules::default();
        schedules.observe(&state, SlotNumber(13_000));

        turn(&mut state, 22_000);
        let previous = state.epoch();
        schedules.observe(&state, SlotNumber(22_000));

        turn(&mut state, 34_800);
        schedules.observe(&state, SlotNumber(34_800));
        assert!(schedules.get(previous).is_some(), "dropped a settling round's grid");
        assert!(schedules.get(oldest).is_none(), "kept an epoch nothing settles in");

        schedules.observe(&state, SlotNumber(34_800 + SETTLE_DEADLINE_SLOTS - 1));
        assert!(schedules.get(previous).is_some(), "dropped it inside the window");

        schedules.observe(&state, SlotNumber(34_800 + SETTLE_DEADLINE_SLOTS));
        assert!(schedules.get(previous).is_none(), "kept it past the window");
        assert!(schedules.get(state.epoch()).is_some(), "dropped the current grid");
    }
}
