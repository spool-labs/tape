//! When a group's challenge rounds fire, derived from finalized epoch state.
//!
//! Round times sit on a fixed slot grid computed from the epoch, so every owner
//! reaches the same schedule with no coordination and nobody can move a round
//! nearer or further from its own convenience. Each round opens a short window of
//! slots; the first block in that window to reach finalized history is the round's
//! entropy block. If no block in the window finalizes the round is void, which
//! counts against nobody.

use core::ops::Range;

use crate::types::{RoundNumber, SlotNumber};

/// Solana slot time. Everything here counts in slots, so this only converts.
pub const SLOT_MS: u64 = 400;

/// Slots a round searches for its entropy block before it voids.
///
/// Wider means fewer void rounds and a longer round. At a 5% miss rate a span of
/// four voids about six rounds per million, where one slot voids one in twenty.
pub const SPAN_SLOTS: u64 = 4;

/// Slots after the entropy block by which a proof must arrive.
///
/// Sized to clear honest global propagation, not to catch a fetch. The measured
/// envelope reaches half a globally placed group in about 143 ms, and tightening
/// below one slot is not expressible on the slot clock anyway.
pub const PROOF_DEADLINE_SLOTS: u64 = 2;

/// Slots after production before signing opens, standing in for the cluster
/// supermajority that confirms the entropy block.
pub const CONFIRMATION_SLOTS: u64 = 1;

/// Slots the signature window stays open once confirmation has passed.
pub const ATTESTATION_WINDOW_SLOTS: u64 = 4;

/// Slots allowed for an aggregated certificate to reach the group.
pub const CERTIFICATE_GOSSIP_SLOTS: u64 = 2;

/// Cadence on an epoch long enough not to constrain it: 60 s at the real slot time.
pub const MAINNET_CADENCE_SLOTS: u64 = 150;

/// Rounds an epoch needs before the eviction rule can engage inside it.
pub const TARGET_ROUNDS_PER_EPOCH: u64 = 4;

/// Slots from a round opening to its certificate having gossiped.
///
/// A round is not instantaneous: it searches for an entropy block, waits out the
/// response deadline, holds a signing window that cannot open before confirmation,
/// and lets the certificate travel. The interval has to clear this or a round's
/// certificate is still moving when its successor opens.
pub const fn round_width_slots() -> u64 {
    let signing = CONFIRMATION_SLOTS + ATTESTATION_WINDOW_SLOTS;
    let tail = if PROOF_DEADLINE_SLOTS > signing {
        PROOF_DEADLINE_SLOTS
    } else {
        signing
    };
    SPAN_SLOTS + tail + CERTIFICATE_GOSSIP_SLOTS
}

/// Slots between rounds for an epoch of the given length.
///
/// The cap holds detection latency steady on long epochs, the divide fits enough
/// rounds into short ones, and the floor keeps rounds from overlapping. Derived
/// rather than tabulated, so an operator voting the epoch duration gets a correct
/// cadence without anyone editing a table.
pub fn cadence_for_epoch(epoch_slots: u64) -> u64 {
    let fitted = epoch_slots / TARGET_ROUNDS_PER_EPOCH;
    MAINNET_CADENCE_SLOTS.min(fitted).max(round_width_slots())
}

/// A schedule that cannot fire a usable round.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ScheduleError {
    /// Rounds would overlap their neighbours.
    RoundsOverlap { interval: u64, width: u64 },
    /// The epoch is too short to hold a single round.
    EpochTooShort { epoch_slots: u64, width: u64 },
}

/// One epoch's challenge grid.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Schedule {
    /// First slot of the epoch these rounds belong to.
    pub epoch_start_slot: SlotNumber,
    /// Slots the epoch spans.
    pub epoch_slots: u64,
    /// Slots between the start of one round and the start of the next.
    pub interval_slots: u64,
}

impl Schedule {
    /// The grid for an epoch, with the cadence derived from its length.
    pub fn for_epoch(epoch_start_slot: SlotNumber, epoch_slots: u64) -> Self {
        Self {
            epoch_start_slot,
            epoch_slots,
            interval_slots: cadence_for_epoch(epoch_slots),
        }
    }

    /// Reject a grid whose rounds would overlap or that holds no round at all.
    pub fn validate(&self) -> Result<(), ScheduleError> {
        let width = round_width_slots();
        if self.interval_slots < width {
            return Err(ScheduleError::RoundsOverlap {
                interval: self.interval_slots,
                width,
            });
        }
        if self.epoch_slots < width {
            return Err(ScheduleError::EpochTooShort {
                epoch_slots: self.epoch_slots,
                width,
            });
        }
        Ok(())
    }

    /// Rounds an epoch of the nominal duration holds.
    ///
    /// Used to size the cadence and to project cost. It is not a limit on the
    /// grid: an epoch that runs past its nominal length keeps challenging.
    pub fn rounds(&self) -> u64 {
        let width = round_width_slots();
        if self.epoch_slots < width {
            return 0;
        }
        (self.epoch_slots - width) / self.interval_slots + 1
    }

    /// First slot of a round's entropy window.
    pub fn base_slot(&self, round: RoundNumber) -> SlotNumber {
        SlotNumber(self.epoch_start_slot.as_u64() + round.as_u64() * self.interval_slots)
    }

    /// Slots a round searches for its entropy block.
    pub fn entropy_window(&self, round: RoundNumber) -> Range<u64> {
        let base = self.base_slot(round).as_u64();
        base..base + SPAN_SLOTS
    }

    /// Slot by which a proof must have arrived, counted from the entropy block.
    pub fn deadline_slot(&self, entropy_slot: SlotNumber) -> SlotNumber {
        SlotNumber(entropy_slot.as_u64() + PROOF_DEADLINE_SLOTS)
    }

    /// The round whose entropy window covers this slot, if any.
    ///
    /// None between rounds, which is most of the grid: the window is four slots
    /// out of an interval that is usually a hundred and fifty.
    ///
    /// Not bounded by `rounds()`. That count is what an epoch of the nominal
    /// duration holds, but an epoch ends when the committee advances it, not when
    /// its nominal length elapses, so one can run long. Capping here would stop
    /// challenging partway through exactly the epochs that ran longest.
    pub fn round_at(&self, slot: SlotNumber) -> Option<RoundNumber> {
        let offset = slot.as_u64().checked_sub(self.epoch_start_slot.as_u64())?;
        if offset % self.interval_slots >= SPAN_SLOTS {
            return None;
        }
        Some(RoundNumber(offset / self.interval_slots))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Epoch durations in seconds, from the genesis presets.
    const MAINNET: u64 = 604_800;
    const DEVNET: u64 = 3_600;
    const LOCALNET: u64 = 100;
    const SIMNET: u64 = 20;

    fn epoch_slots(seconds: u64) -> u64 {
        seconds * 1_000 / SLOT_MS
    }

    #[test]
    fn a_round_is_eleven_slots_wide() {
        // Span, then the signing window which cannot open before confirmation,
        // then certificate gossip. The deadline sits inside the signing window.
        assert_eq!(round_width_slots(), 11);
    }

    #[test]
    fn every_preset_gets_a_workable_cadence() {
        // The table the knobs document publishes, pinned in code so a preset that
        // changes its epoch duration cannot quietly fall off it.
        let expected = [
            (MAINNET, 150u64, 10_080u64),
            (DEVNET, 150, 60),
            (LOCALNET, 62, 4),
            (SIMNET, 12, 4),
        ];

        for (seconds, interval, rounds) in expected {
            let slots = epoch_slots(seconds);
            let schedule = Schedule::for_epoch(SlotNumber(0), slots);

            assert_eq!(schedule.interval_slots, interval, "{seconds}s interval");
            assert_eq!(schedule.rounds(), rounds, "{seconds}s rounds");
            assert!(schedule.validate().is_ok(), "{seconds}s invalid");
        }
    }

    #[test]
    fn the_shortest_votable_epoch_still_holds_rounds() {
        // Simnet's floor is the tight one: the interval falls to the round width
        // and the epoch still has to fit more than one round.
        let schedule = Schedule::for_epoch(SlotNumber(0), epoch_slots(10));
        assert_eq!(schedule.interval_slots, round_width_slots());
        assert_eq!(schedule.rounds(), 2);
        assert!(schedule.validate().is_ok());
    }

    #[test]
    fn an_epoch_too_short_for_one_round_schedules_none() {
        let schedule = Schedule::for_epoch(SlotNumber(0), 10);
        assert_eq!(schedule.rounds(), 0);
        assert_eq!(
            schedule.validate(),
            Err(ScheduleError::EpochTooShort {
                epoch_slots: 10,
                width: round_width_slots(),
            })
        );
    }

    #[test]
    fn rounds_never_overlap_their_neighbours() {
        let schedule = Schedule::for_epoch(SlotNumber(1_000), epoch_slots(SIMNET));
        for round in 0..schedule.rounds() {
            let window = schedule.entropy_window(RoundNumber(round));
            let next = schedule.base_slot(RoundNumber(round + 1)).as_u64();

            assert!(window.start >= schedule.epoch_start_slot.as_u64());
            assert!(window.end + round_width_slots() - SPAN_SLOTS <= next);
        }
    }

    #[test]
    fn the_last_round_finishes_inside_its_epoch() {
        // A round still gossiping when the epoch ends belongs to nobody, so it is
        // not scheduled in the first place.
        for seconds in [MAINNET, DEVNET, LOCALNET, SIMNET, 10, 200] {
            let slots = epoch_slots(seconds);
            let schedule = Schedule::for_epoch(SlotNumber(0), slots);
            let Some(last) = schedule.rounds().checked_sub(1) else {
                continue;
            };

            let end = schedule.base_slot(RoundNumber(last)).as_u64() + round_width_slots();
            assert!(end <= slots, "{seconds}s overruns by {}", end - slots);
        }
    }

    #[test]
    fn an_epoch_that_runs_long_keeps_challenging() {
        // An epoch ends when the committee advances it, not when its nominal
        // length elapses. A grid that stopped at the nominal count would leave
        // the tail of a long epoch unchallenged, which is when a node is most
        // likely to have gone quiet unnoticed.
        let schedule = Schedule::for_epoch(SlotNumber(0), epoch_slots(SIMNET));
        let nominal = schedule.rounds();

        let past = nominal * schedule.interval_slots;
        assert_eq!(
            schedule.round_at(SlotNumber(past)),
            Some(RoundNumber(nominal))
        );
        let far = past + schedule.interval_slots * 10;
        assert_eq!(
            schedule.round_at(SlotNumber(far)),
            Some(RoundNumber(nominal + 10))
        );
    }

    #[test]
    fn a_slot_maps_back_to_the_round_that_owns_it() {
        let start = SlotNumber(9_000);
        let schedule = Schedule::for_epoch(start, epoch_slots(DEVNET));

        for round in [0u64, 1, 30, schedule.rounds() + 5] {
            let window = schedule.entropy_window(RoundNumber(round));
            for slot in window.clone() {
                assert_eq!(schedule.round_at(SlotNumber(slot)), Some(RoundNumber(round)));
            }
            // The gap after a window belongs to no round.
            assert_eq!(schedule.round_at(SlotNumber(window.end)), None);
        }

        // Before the epoch there is no round at all.
        assert_eq!(schedule.round_at(SlotNumber(start.as_u64() - 1)), None);
    }
}
