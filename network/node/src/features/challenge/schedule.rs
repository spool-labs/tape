//! An epoch's rounds: one slot every `cadence` slots from the slot the epoch advanced in

use tape_core::types::{RoundNumber, SlotNumber};

/// One epoch's round slots, with rounds counted from 1
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Schedule {
    /// Slot of the block that carried the epoch's `AdvanceEpoch`
    pub start_slot: SlotNumber,
    /// Slots between rounds. Zero means the epoch has no rounds.
    pub cadence: u64,
}

impl Schedule {
    pub fn new(start_slot: SlotNumber, cadence: u64) -> Self {
        Self { start_slot, cadence }
    }

    /// Find the round whose seed block is the block at `slot`
    ///
    /// Only a block at a round's own slot opens it. When Solana skips that
    /// slot no block ever asks, so the round never happens.
    pub fn round_at(&self, slot: SlotNumber) -> Option<RoundNumber> {
        if self.cadence == 0 {
            return None;
        }

        let elapsed = slot.checked_sub(self.start_slot)?.as_u64();
        if elapsed == 0 || !elapsed.is_multiple_of(self.cadence) {
            return None;
        }

        Some(RoundNumber(elapsed / self.cadence))
    }

    /// The slot of a round's seed block
    pub fn slot_of(&self, round: RoundNumber) -> Option<SlotNumber> {
        if self.cadence == 0 || round.is_zero() {
            return None;
        }

        let elapsed = round.as_u64().checked_mul(self.cadence)?;
        self.start_slot.checked_add(SlotNumber(elapsed))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const START: SlotNumber = SlotNumber(1_000);
    const CADENCE: u64 = 5;

    // rounds count from 1 at every cadence slots past the start slot
    #[test]
    fn round_slots() {
        let schedule = Schedule::new(START, CADENCE);

        assert_eq!(schedule.round_at(SlotNumber(1_005)), Some(RoundNumber(1)));
        assert_eq!(schedule.round_at(SlotNumber(1_200)), Some(RoundNumber(40)));
        assert_eq!(schedule.slot_of(RoundNumber(1)), Some(SlotNumber(1_005)));
        assert_eq!(schedule.slot_of(RoundNumber(40)), Some(SlotNumber(1_200)));

        assert_eq!(schedule.round_at(START), None);
        assert_eq!(schedule.round_at(SlotNumber(1_004)), None);
        assert_eq!(schedule.round_at(SlotNumber(999)), None);
        assert_eq!(schedule.round_at(SlotNumber(995)), None);
        assert_eq!(schedule.slot_of(RoundNumber(0)), None);
    }

    // a round whose slot has no block never opens, and the next block does not stand in
    #[test]
    fn skipped_slot() {
        let schedule = Schedule::new(START, CADENCE);
        let blocks = [1_001, 1_005, 1_006, 1_009, 1_011, 1_015];

        let mut rounds = Vec::new();
        for slot in blocks {
            if let Some(round) = schedule.round_at(SlotNumber(slot)) {
                rounds.push(round);
            }
        }

        assert_eq!(rounds, vec![RoundNumber(1), RoundNumber(3)]);
    }

    // a cadence of one slot makes every slot after the start a round
    #[test]
    fn every_slot() {
        let schedule = Schedule::new(START, 1);

        assert_eq!(schedule.round_at(START), None);
        assert_eq!(schedule.round_at(SlotNumber(1_001)), Some(RoundNumber(1)));
        assert_eq!(schedule.round_at(SlotNumber(1_002)), Some(RoundNumber(2)));
    }

    // a cadence of zero gives no rounds
    #[test]
    fn no_cadence() {
        let schedule = Schedule::new(START, 0);

        for slot in 995..1_050 {
            assert_eq!(schedule.round_at(SlotNumber(slot)), None);
        }
        assert_eq!(schedule.slot_of(RoundNumber(1)), None);
    }
}
