//! A fixed slot grid, shared by every owner in a fleet.
//!
//! Epochs select membership, not cadence. Round openings remain on multiples of
//! the configured interval across epoch boundaries. A round's entropy is the
//! first produced block in its window, so a public schedule does not reveal its
//! sample in advance. Nodes only open rounds while their epoch is Active.

use crate::types::{RoundNumber, SlotNumber};
use core::ops::Range;

/// Solana slot time. Everything here counts in slots, so this only converts.
pub const SLOT_MS: u64 = 400;

/// Slots a round searches for its entropy block before it voids.
///
/// Wider means fewer void rounds and a longer round. At a 5% miss rate a span of
/// two voids about one round in four hundred, where one slot voids one in twenty.
pub const SPAN_SLOTS: u64 = 2;

/// Slots before a round's window that the sample set is cut at.
///
/// A round opens on a produced block, ahead of what any owner has applied, and
/// owners' frontiers sit a finality window back by differing amounts. Cutting at
/// the window would put writes in that gap on one owner's side and not another's.
/// Cutting behind it puts the whole gap on everyone's side.
///
/// Wide enough for finality plus the spread, which `AT_TIP_THRESHOLD_SLOTS`
/// bounds at five.
pub const SAMPLE_LOOKBACK_SLOTS: u64 = 64;

/// Proof-collection allowance used to size the round and display its timeline.
///
/// Sized to clear honest global propagation, not to catch a fetch. The measured
/// envelope reaches half a globally placed group in about 143 ms, and tightening
/// below one slot is not expressible on the slot clock anyway. This is not an
/// enforced ingress deadline: incoming proofs are admitted against the open
/// round, and settlement is the actual bound.
pub const PROOF_DEADLINE_SLOTS: u64 = 2;

/// Confirmation allowance shown by the observation timeline. Signing does not
/// wait on this allowance; finality determines whether a round can be judged.
pub const CONFIRMATION_SLOTS: u64 = 1;

/// Slots the signature window stays open once confirmation has passed.
pub const ATTESTATION_WINDOW_SLOTS: u64 = 2;

/// Slots allowed for an aggregated certificate to reach the group.
pub const CERTIFICATE_GOSSIP_SLOTS: u64 = 1;

/// Slots a round waits before calling its block lost.
///
/// Sized off TowerBFT rooting, measured at 31 here. It is a timeout for the
/// void case, not a claim about how fast any given chain roots.
pub const FINALITY_SLOTS: u64 = 32;

/// Slots a round waits on its entropy block before it is void.
pub const SETTLE_DEADLINE_SLOTS: u64 = FINALITY_SLOTS + 8;

/// Rounds a group may hold at once: the deadline at the tightest cadence, which
/// is the round width. A cap below that sheds rounds before they can root.
pub const MAX_PENDING_ROUNDS: usize = (SETTLE_DEADLINE_SLOTS / round_width_slots() + 3) as usize;

/// Minimum spacing needed for the round's collection and gossip window.
pub const fn round_width_slots() -> u64 {
    let tail = if PROOF_DEADLINE_SLOTS > ATTESTATION_WINDOW_SLOTS {
        PROOF_DEADLINE_SLOTS
    } else {
        ATTESTATION_WINDOW_SLOTS
    };
    SPAN_SLOTS + tail + CERTIFICATE_GOSSIP_SLOTS
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ScheduleError {
    RoundsOverlap {
        interval: u64,
        width: u64,
    },
    SlotOverflow,
}

/// The portion of the fixed grid belonging to an epoch.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Schedule {
    pub epoch_start_slot: SlotNumber,
    pub interval_slots: u64,
    first_slot: SlotNumber,
}

impl Schedule {
    /// Start at the first grid opening on or after the epoch's start.
    /// No epoch duration, previous epoch, or locally measured clock is involved.
    pub fn for_epoch(
        epoch_start_slot: SlotNumber,
        interval_slots: u64,
    ) -> Result<Self, ScheduleError> {
        let width = round_width_slots();
        if interval_slots < width {
            return Err(ScheduleError::RoundsOverlap {
                interval: interval_slots,
                width,
            });
        }
        let start = epoch_start_slot.as_u64();
        let offset = (interval_slots - start % interval_slots) % interval_slots;
        let first_slot = start
            .checked_add(offset)
            .ok_or(ScheduleError::SlotOverflow)?;
        Ok(Self {
            epoch_start_slot,
            interval_slots,
            first_slot: SlotNumber(first_slot),
        })
    }

    pub fn first_slot(&self) -> SlotNumber {
        self.first_slot
    }

    pub fn base_slot(&self, round: RoundNumber) -> SlotNumber {
        SlotNumber(self.first_slot.as_u64() + round.as_u64() * self.interval_slots)
    }

    /// Cut samples behind finality so owners ask from the same applied data.
    pub fn sample_cutoff(&self, round: RoundNumber) -> SlotNumber {
        SlotNumber(
            self.base_slot(round)
                .as_u64()
                .saturating_sub(SAMPLE_LOOKBACK_SLOTS),
        )
    }

    pub fn entropy_window(&self, round: RoundNumber) -> Range<u64> {
        let base = self.base_slot(round).as_u64();
        base..base + SPAN_SLOTS
    }

    /// Include a partial interval when admitting rounds that can still settle.
    pub fn settling_rounds(&self) -> u64 {
        SETTLE_DEADLINE_SLOTS.div_ceil(self.interval_slots)
    }

    pub fn round_by(&self, slot: SlotNumber) -> Option<RoundNumber> {
        let offset = slot.as_u64().checked_sub(self.first_slot.as_u64())?;
        Some(RoundNumber(offset / self.interval_slots))
    }

    /// The entropy window covering a slot, or None between openings.
    pub fn round_at(&self, slot: SlotNumber) -> Option<RoundNumber> {
        let offset = slot.as_u64().checked_sub(self.first_slot.as_u64())?;
        (offset % self.interval_slots < SPAN_SLOTS)
            .then_some(RoundNumber(offset / self.interval_slots))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn refuses_overlap_and_overflow() {
        for interval in 0..round_width_slots() {
            assert!(Schedule::for_epoch(SlotNumber(0), interval).is_err());
        }
        assert_eq!(
            Schedule::for_epoch(SlotNumber(u64::MAX), 10),
            Err(ScheduleError::SlotOverflow)
        );
    }

    #[test]
    fn epoch_boundary_never_moves_the_grid() {
        for interval in [5, 7, 10, 25, 100] {
            for boundary in 101..201 {
                let old = Schedule::for_epoch(SlotNumber(0), interval).unwrap();
                let new = Schedule::for_epoch(SlotNumber(boundary), interval).unwrap();
                assert!(new.first_slot().as_u64() >= boundary);
                assert!(new.first_slot().as_u64() < boundary + interval);
                for round in 0..10 {
                    let slot = new.base_slot(RoundNumber(round));
                    assert_eq!(slot.as_u64() % interval, 0);
                    assert!(old.round_at(slot).is_some());
                    assert_eq!(new.round_at(slot), Some(RoundNumber(round)));
                }
            }
        }
    }

    #[test]
    fn windows_and_gaps_follow_configured_interval() {
        let schedule = Schedule::for_epoch(SlotNumber(101), 25).unwrap();
        assert_eq!(schedule.first_slot(), SlotNumber(125));
        assert_eq!(schedule.round_at(SlotNumber(124)), None);
        assert_eq!(schedule.round_by(SlotNumber(124)), None);
        for round in [0, 1, 100, 1_000_000] {
            let window = schedule.entropy_window(RoundNumber(round));
            for slot in window.clone() {
                assert_eq!(
                    schedule.round_at(SlotNumber(slot)),
                    Some(RoundNumber(round))
                );
            }
            assert_eq!(schedule.round_at(SlotNumber(window.end)), None);
            assert_eq!(
                schedule.round_by(SlotNumber(window.start + 24)),
                Some(RoundNumber(round))
            );
            assert_eq!(
                schedule.sample_cutoff(RoundNumber(round)).as_u64(),
                window.start - SAMPLE_LOOKBACK_SLOTS
            );
        }
        assert_eq!(schedule.settling_rounds(), 2);
    }

    #[test]
    fn sparse_rounds_still_have_a_settlement_window() {
        let schedule = Schedule::for_epoch(SlotNumber(0), 100).unwrap();
        assert_eq!(schedule.settling_rounds(), 1);
        assert_eq!(schedule.sample_cutoff(RoundNumber(0)), SlotNumber(0));
    }
}
