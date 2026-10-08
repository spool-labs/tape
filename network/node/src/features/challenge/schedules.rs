//! Retain each observed epoch's grid until its open rounds have settled.

use std::collections::HashMap;
use std::sync::Mutex;
use tape_core::challenge::schedule::{SETTLE_DEADLINE_SLOTS, Schedule, ScheduleError};
use tape_core::types::{EpochNumber, SlotNumber};
use tape_protocol::ProtocolState;

pub struct Schedules {
    interval_slots: u64,
    held: Mutex<HashMap<EpochNumber, Schedule>>,
}

impl Schedules {
    pub fn new(interval_slots: u64) -> Result<Self, ScheduleError> {
        Schedule::for_epoch(SlotNumber(0), interval_slots)?;
        Ok(Self {
            interval_slots,
            held: Mutex::new(HashMap::new()),
        })
    }

    pub fn observe(&self, state: &ProtocolState, now: SlotNumber) {
        let Ok(mut held) = self.held.lock() else {
            return;
        };
        let epoch = state.epoch();
        if let Ok(schedule) =
            Schedule::for_epoch(state.current.epoch.start_slot, self.interval_slots)
        {
            held.entry(epoch).or_insert(schedule);
        }
        let settled = now
            .as_u64()
            .saturating_sub(state.current.epoch.start_slot.as_u64())
            >= SETTLE_DEADLINE_SLOTS;
        let oldest = epoch.as_u64().saturating_sub(u64::from(!settled));
        held.retain(|epoch, _| epoch.as_u64() >= oldest);
    }

    /// An unseen epoch has no local open rounds to answer or judge.
    pub fn get(&self, epoch: EpochNumber) -> Option<Schedule> {
        self.held.lock().ok()?.get(&epoch).copied()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::harness::{NodeHarness, TestContext};
    use tape_core::types::EpochDuration;
    use tape_crypto::hash::Hash;

    async fn state() -> ProtocolState {
        let harness = NodeHarness::builder()
            .nodes(25)
            .no_prev_snapshot_tape()
            .build()
            .await
            .expect("build harness");
        let ctx: TestContext = harness.ctx_for(0);
        (*ctx.state()).clone()
    }

    #[tokio::test]
    async fn duration_previous_epoch_and_nonce_do_not_change_the_grid() {
        let mut state = state().await;
        state.current.epoch.start_slot = SlotNumber(101);
        let expected = Schedule::for_epoch(SlotNumber(101), 25).unwrap();
        for seconds in [1, 20, 100, 3_600, 604_800] {
            state.current.epoch.preferences.epoch_duration = EpochDuration(seconds);
            state.current.epoch.nonce = Hash([seconds as u8; 32]);
            state.previous = None;
            let schedules = Schedules::new(25).unwrap();
            schedules.observe(&state, SlotNumber(200));
            assert_eq!(schedules.get(state.epoch()), Some(expected));
            state.previous = Some(state.current.clone());
            state.previous.as_mut().unwrap().epoch.start_slot = SlotNumber(100);
            let restarted = Schedules::new(25).unwrap();
            restarted.observe(&state, SlotNumber(200));
            assert_eq!(restarted.get(state.epoch()), Some(expected));
        }
    }

    #[tokio::test]
    async fn retains_only_observed_epochs_until_settlement() {
        let mut state = state().await;
        state.current.epoch.start_slot = SlotNumber(101);
        let old_epoch = state.epoch();
        let schedules = Schedules::new(25).unwrap();
        schedules.observe(&state, SlotNumber(101));
        let old = schedules.get(old_epoch).unwrap();
        state.previous = Some(state.current.clone());
        state.current.epoch.id = EpochNumber(old_epoch.as_u64() + 1);
        state.current.epoch.start_slot = SlotNumber(237);
        schedules.observe(&state, SlotNumber(237));
        assert_eq!(schedules.get(old_epoch), Some(old));
        assert_eq!(
            schedules.get(state.epoch()).unwrap().first_slot(),
            SlotNumber(250)
        );

        let restarted = Schedules::new(25).unwrap();
        restarted.observe(&state, SlotNumber(238));
        assert_eq!(restarted.get(state.epoch()), schedules.get(state.epoch()));
        assert!(restarted.get(old_epoch).is_none());

        schedules.observe(&state, SlotNumber(237 + SETTLE_DEADLINE_SLOTS - 1));
        assert!(schedules.get(old_epoch).is_some());
        schedules.observe(&state, SlotNumber(237 + SETTLE_DEADLINE_SLOTS));
        assert!(schedules.get(old_epoch).is_none());
        assert!(schedules.get(state.epoch()).is_some());
    }
}
