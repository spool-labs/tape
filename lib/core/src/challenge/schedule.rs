//! A fixed slot grid, shared by every owner in a fleet.
//!
//! An epoch selects membership and leaves the cadence alone: round openings
//! remain on multiples of the configured interval across epoch boundaries. Round
//! r of an epoch opens at one slot every owner knows in advance, and the
//! block the chain produced at that slot is its entropy, so the schedule still
//! reveals no sample ahead of time. A slot the chain skipped leaves its round
//! void for everyone. A round opens only when the blocks before its opening
//! leave the epoch Active.

use crate::types::{RoundNumber, SlotNumber};

/// Fallback slot time before ingest has measured the live rate.
///
/// Solana's default, and no cluster preset plans a slower slot, so a wait it
/// bounds runs long until a rate is observed. Each network's own rate is
/// `GenesisPreset::slot_ms` in the node config, which the challenge cadence and
/// spans convert at. Simnet's chain clock also counts this much per slot.
pub const SLOT_MS: u64 = 400;

/// Milliseconds in a second, for turning a configured span into slot time.
const MILLIS_PER_SECOND: u64 = 1_000;

/// Proof-collection allowance used to size the round and display its timeline.
///
/// Sized to clear honest global propagation, not to catch a fetch. The measured
/// envelope reaches half a globally placed group in about 143 ms, and tightening
/// below one slot is not expressible on the slot clock anyway. This is not an
/// enforced ingress deadline: incoming proofs are admitted against the open
/// round, and settlement is the actual bound.
pub const PROOF_DEADLINE_SLOTS: u64 = 2;

/// Slots the signature window stays open once the proof deadline has passed.
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

/// Slots before a round's opening that the sample set is cut at.
///
/// A round opens on the block at its slot, ahead of what any owner has written
/// to its store. A node takes part in a round up to `SETTLE_DEADLINE_SLOTS`
/// behind the tip, and its store trails its own ingest head, so the cut sits a
/// finality window behind that bound. Cutting at the opening would leave a
/// write inside the gap in one owner's set and outside another's; cutting this
/// far back puts the whole gap inside everyone's. A node whose store has still
/// not reached the cut builds no set for the round until it has.
pub const SAMPLE_LOOKBACK_SLOTS: u64 = SETTLE_DEADLINE_SLOTS + FINALITY_SLOTS;

/// Rounds a group may hold at once: the deadline at the tightest cadence, which
/// is the round width. A cap below that sheds rounds before they can root.
pub const MAX_PENDING_ROUNDS: usize = (SETTLE_DEADLINE_SLOTS / round_width_slots() + 3) as usize;

/// Slots from a round's opening block to the end of its signature window.
///
/// The proof arrives inside `PROOF_DEADLINE_SLOTS` and the signatures over it
/// follow inside `ATTESTATION_WINDOW_SLOTS`, back to back from the block.
pub const fn attest_deadline_slots() -> u64 {
    PROOF_DEADLINE_SLOTS + ATTESTATION_WINDOW_SLOTS
}

/// Slots a round runs from its opening to its verdict, and the floor on the
/// interval, since one group's rounds cannot overlap.
///
/// The signature window closes at `attest_deadline_slots`, and the certificate
/// gossips inside `CERTIFICATE_GOSSIP_SLOTS` after it. Devnet's measured
/// certificate p50 is 384 ms, 2.3 slots at 166 ms, so a shorter span records
/// misses that a later certificate has to upgrade.
pub const fn round_width_slots() -> u64 {
    attest_deadline_slots() + CERTIFICATE_GOSSIP_SLOTS
}

/// The fewest whole rounds that last a wall-clock span at one cadence, and at
/// least one.
///
/// Every threshold the challenge path compares is a round count, so a cadence
/// change would otherwise move the wall-clock meaning of each one by the same
/// factor. Configuration states the span and derives the count here. A span is
/// the least time a threshold grants, so a part round counts as a whole one.
pub fn rounds_in_seconds(seconds: u64, interval_slots: u64, slot_ms: u64) -> u64 {
    let round_ms = interval_slots.saturating_mul(slot_ms).max(1);
    let span_ms = seconds.saturating_mul(MILLIS_PER_SECOND);
    span_ms.div_ceil(round_ms).max(1)
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

    /// Round openings the chain skipped between a block and its parent.
    ///
    /// Two consecutive blocks fix which slots produced nothing, so every node
    /// that ingests the block reads the same openings as skipped and opens none
    /// of them. Both blocks produced, so neither slot's own round is included.
    pub fn skipped_rounds(
        &self,
        parent_slot: SlotNumber,
        slot: SlotNumber,
    ) -> impl Iterator<Item = RoundNumber> {
        let first = self.round_from(SlotNumber(parent_slot.as_u64().saturating_add(1)));
        let end = self.round_from(slot);
        (first.as_u64()..end.as_u64()).map(RoundNumber)
    }

    /// The first round opening at or after a slot.
    pub fn round_from(&self, slot: SlotNumber) -> RoundNumber {
        RoundNumber(
            slot.as_u64()
                .saturating_sub(self.first_slot.as_u64())
                .div_ceil(self.interval_slots),
        )
    }

    /// Include a partial interval when admitting rounds that can still settle.
    pub fn settling_rounds(&self) -> u64 {
        SETTLE_DEADLINE_SLOTS.div_ceil(self.interval_slots)
    }

    pub fn round_by(&self, slot: SlotNumber) -> Option<RoundNumber> {
        let offset = slot.as_u64().checked_sub(self.first_slot.as_u64())?;
        Some(RoundNumber(offset / self.interval_slots))
    }

    /// The round opening at this slot, and nothing at a slot between openings.
    pub fn round_at(&self, slot: SlotNumber) -> Option<RoundNumber> {
        let offset = slot.as_u64().checked_sub(self.first_slot.as_u64())?;
        (offset % self.interval_slots == 0).then_some(RoundNumber(offset / self.interval_slots))
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
    fn openings_and_gaps_follow_configured_interval() {
        let schedule = Schedule::for_epoch(SlotNumber(101), 25).unwrap();
        assert_eq!(schedule.first_slot(), SlotNumber(125));
        assert_eq!(schedule.round_at(SlotNumber(124)), None);
        assert_eq!(schedule.round_by(SlotNumber(124)), None);
        for round in [0, 1, 100, 1_000_000] {
            let base = schedule.base_slot(RoundNumber(round)).as_u64();
            assert_eq!(schedule.round_at(SlotNumber(base)), Some(RoundNumber(round)));
            assert_eq!(
                schedule.round_by(SlotNumber(base + 24)),
                Some(RoundNumber(round))
            );
            assert_eq!(
                schedule.sample_cutoff(RoundNumber(round)).as_u64(),
                base - SAMPLE_LOOKBACK_SLOTS
            );
        }
        assert_eq!(schedule.settling_rounds(), 2);
    }

    // a round opens at one slot, and neither neighbour belongs to it
    #[test]
    fn one_slot_per_round() {
        let schedule = Schedule::for_epoch(SlotNumber(101), 25).expect("a grid");

        for round in [0, 1, 100, 1_000_000] {
            let base = schedule.base_slot(RoundNumber(round)).as_u64();

            assert_eq!(schedule.round_at(SlotNumber(base)), Some(RoundNumber(round)));
            assert_eq!(schedule.round_at(SlotNumber(base + 1)), None);
            assert_eq!(schedule.round_at(SlotNumber(base - 1)), None);
        }
    }

    // the first round from a slot is the one opening there, or the next one
    #[test]
    fn first_round_from() {
        let schedule = Schedule::for_epoch(SlotNumber(100), 25).expect("a grid");
        let first = schedule.first_slot().as_u64();

        assert_eq!(schedule.round_from(SlotNumber(first - 30)), RoundNumber(0));
        assert_eq!(schedule.round_from(SlotNumber(first)), RoundNumber(0));
        assert_eq!(schedule.round_from(SlotNumber(first + 1)), RoundNumber(1));
        assert_eq!(schedule.round_from(SlotNumber(first + 25)), RoundNumber(1));
        assert_eq!(schedule.round_from(SlotNumber(first + 26)), RoundNumber(2));
    }

    // a block and its parent say which openings the chain produced nothing at
    #[test]
    fn skipped_openings() {
        let schedule = Schedule::for_epoch(SlotNumber(100), 25).expect("a grid");
        let first = schedule.first_slot().as_u64();

        let skipped = |from: u64, to: u64| -> Vec<RoundNumber> {
            schedule
                .skipped_rounds(SlotNumber(from), SlotNumber(to))
                .collect()
        };

        assert_eq!(skipped(first, first + 1), Vec::new());
        assert_eq!(skipped(first + 24, first + 25), Vec::new());
        assert_eq!(skipped(first + 25, first + 26), Vec::new());
        assert_eq!(skipped(first + 24, first + 26), vec![RoundNumber(1)]);
        assert_eq!(
            skipped(first, first + 51),
            vec![RoundNumber(1), RoundNumber(2)]
        );
        assert_eq!(skipped(first - 40, first + 1), vec![RoundNumber(0)]);
    }

    // a span in seconds becomes the fewest whole rounds that last it, so a part
    // round counts in full and an exact fit adds nothing
    #[test]
    fn rounds_cover_a_span() {
        assert_eq!(rounds_in_seconds(120, 300, 200), 2);
        assert_eq!(rounds_in_seconds(121, 300, 200), 3);
        assert_eq!(rounds_in_seconds(90, 300, 200), 2);
        assert_eq!(rounds_in_seconds(240, 180, 166), 9);
        assert_eq!(rounds_in_seconds(225, 180, 166), 8);
        assert_eq!(rounds_in_seconds(75, 180, 166), 3);
        assert_eq!(rounds_in_seconds(1, 300, 200), 1);
        assert_eq!(rounds_in_seconds(0, 300, 200), 1);
    }

    #[test]
    fn sparse_rounds_still_have_a_settlement_window() {
        let schedule = Schedule::for_epoch(SlotNumber(0), 100).unwrap();
        assert_eq!(schedule.settling_rounds(), 1);
        assert_eq!(schedule.sample_cutoff(RoundNumber(0)), SlotNumber(0));
    }
}
