//! When a group's challenge rounds fire, derived from finalized epoch state.
//!
//! Round times sit on a fixed slot grid computed from the epoch, so every owner
//! reaches the same schedule with no coordination and nobody can move a round
//! nearer or further from its own convenience. Each round opens a short window of
//! slots and the first block produced in it seeds the round, so the request is
//! unpredictable and the answer is due while that branch is live. A candidate
//! that does not survive to finality voids its round, as does a window that
//! produces nothing. Neither counts against anybody.

use core::ops::Range;

use tape_crypto::hash::{Hash, hashv};

use crate::types::{RoundNumber, SlotNumber};

/// Domain tag separating the grid offset from every other hash in the protocol.
const GRID_DOMAIN: &[u8] = b"WHIRLWIND_GRID";

/// Solana slot time. Everything here counts in slots, so this only converts.
pub const SLOT_MS: u64 = 400;

/// Slots a round searches for its entropy block before it voids.
///
/// Wider means fewer void rounds and a longer round. At a 5% miss rate a span of
/// four voids about six rounds per million, where one slot voids one in twenty.
pub const SPAN_SLOTS: u64 = 4;

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

/// Rounds an epoch of this length holds at this cadence, before any shift.
fn rounds_in(epoch_slots: u64, interval_slots: u64) -> u64 {
    let width = round_width_slots();
    if epoch_slots < width {
        return 0;
    }
    (epoch_slots - width) / interval_slots + 1
}

/// Slots the grid may be shifted by without costing a round.
///
/// The rounds an epoch holds never divide its length exactly, and what is left
/// over is room the grid can move within. Shifting further would buy variation by
/// giving up a round, which is the wrong trade on a short epoch where there are
/// only a handful.
pub fn grid_slack(epoch_slots: u64, interval_slots: u64) -> u64 {
    let rounds = rounds_in(epoch_slots, interval_slots);
    if rounds == 0 {
        return 0;
    }

    let used = (rounds - 1) * interval_slots + round_width_slots();
    epoch_slots.saturating_sub(used)
}

/// Slots to shift a grid by, from the epoch nonce.
///
/// Any value within the slack is a valid placement, so the modulo bias across a
/// 64-bit draw is irrelevant here.
pub fn grid_offset(nonce: &Hash, slack: u64) -> u64 {
    let mut head = [0u8; 8];
    head.copy_from_slice(&hashv(&[GRID_DOMAIN, nonce.as_ref()]).as_ref()[..8]);
    u64::from_le_bytes(head) % (slack + 1)
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
    /// Slots the whole grid is shifted by, derived from the epoch nonce.
    ///
    /// Without it every round sits at a fixed offset from the epoch start, so
    /// whichever round lands in a phase that does not challenge is lost in that
    /// position every single epoch. The nonce moves the grid instead, so the
    /// rounds an epoch loses vary. It is knowable the moment the epoch begins,
    /// which is fine: the schedule may be public, only the samples may not be.
    pub grid_offset: u64,
}

impl Schedule {
    /// The grid for an epoch, with the cadence derived from its length and the
    /// placement derived from its nonce.
    pub fn for_epoch(epoch_start_slot: SlotNumber, epoch_slots: u64, nonce: &Hash) -> Self {
        let interval_slots = cadence_for_epoch(epoch_slots);
        Self {
            epoch_start_slot,
            epoch_slots,
            interval_slots,
            grid_offset: grid_offset(nonce, grid_slack(epoch_slots, interval_slots)),
        }
    }

    /// Slot the grid's first round opens at.
    pub fn first_slot(&self) -> SlotNumber {
        SlotNumber(self.epoch_start_slot.as_u64() + self.grid_offset)
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
        rounds_in(self.epoch_slots, self.interval_slots)
    }

    /// Slot the round's sample set is cut at.
    ///
    /// Behind the window by `SAMPLE_LOOKBACK_SLOTS`, so the cut falls inside
    /// the range every owner has applied rather than inside the gap between
    /// their frontiers.
    pub fn sample_cutoff(&self, round: RoundNumber) -> SlotNumber {
        SlotNumber(self.base_slot(round).as_u64().saturating_sub(SAMPLE_LOOKBACK_SLOTS))
    }

    /// First slot of a round's entropy window.
    pub fn base_slot(&self, round: RoundNumber) -> SlotNumber {
        SlotNumber(self.first_slot().as_u64() + round.as_u64() * self.interval_slots)
    }

    /// Slots a round searches for its entropy block.
    pub fn entropy_window(&self, round: RoundNumber) -> Range<u64> {
        let base = self.base_slot(round).as_u64();
        base..base + SPAN_SLOTS
    }

    /// Slot by which a proof must have arrived, counted from the entropy block.
    ///
    /// Sizes the round rather than gating a response: no observer rejects a late
    /// answer today, because a round settles when the next one opens and that is
    /// the bound a response actually runs against.
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
        let offset = slot.as_u64().checked_sub(self.first_slot().as_u64())?;
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

    fn nonce(byte: u8) -> Hash {
        Hash([byte; 32])
    }

    #[test]
    fn round_width() {
        assert_eq!(round_width_slots(), 11);
    }

    #[test]
    fn preset_cadence() {
        let expected = [
            (MAINNET, 150u64, 10_080u64),
            (DEVNET, 150, 60),
            (LOCALNET, 62, 4),
            (SIMNET, 12, 4),
        ];

        for (seconds, interval, rounds) in expected {
            let slots = epoch_slots(seconds);
            let schedule = Schedule::for_epoch(SlotNumber(0), slots, &nonce(0));

            assert_eq!(schedule.interval_slots, interval, "{seconds}s interval");
            assert_eq!(schedule.rounds(), rounds, "{seconds}s rounds");
            assert!(schedule.validate().is_ok(), "{seconds}s invalid");
        }
    }

    #[test]
    fn shortest_epoch() {
        let schedule = Schedule::for_epoch(SlotNumber(0), epoch_slots(10), &nonce(0));
        assert_eq!(schedule.interval_slots, round_width_slots());
        assert_eq!(schedule.rounds(), 2);
        assert!(schedule.validate().is_ok());
    }

    #[test]
    fn epoch_too_short() {
        let schedule = Schedule::for_epoch(SlotNumber(0), 10, &nonce(0));
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
    fn no_overlap() {
        let schedule = Schedule::for_epoch(SlotNumber(1_000), epoch_slots(SIMNET), &nonce(0));
        for round in 0..schedule.rounds() {
            let window = schedule.entropy_window(RoundNumber(round));
            let next = schedule.base_slot(RoundNumber(round + 1)).as_u64();

            assert!(window.start >= schedule.first_slot().as_u64());
            assert!(window.end + round_width_slots() - SPAN_SLOTS <= next);
        }
    }

    #[test]
    fn last_round_fits() {
        for (seconds, byte) in [
            (MAINNET, 0u8), (MAINNET, 200), (DEVNET, 3), (LOCALNET, 7),
            (SIMNET, 11), (10, 13), (200, 17),
        ] {
            let slots = epoch_slots(seconds);
            let schedule = Schedule::for_epoch(SlotNumber(0), slots, &nonce(byte));
            let Some(last) = schedule.rounds().checked_sub(1) else {
                continue;
            };

            let end = schedule.base_slot(RoundNumber(last)).as_u64()
                - schedule.epoch_start_slot.as_u64()
                + round_width_slots();
            assert!(end <= slots, "{seconds}s overruns by {}", end - slots);
        }
    }

    #[test]
    fn long_epoch() {
        let schedule = Schedule::for_epoch(SlotNumber(0), epoch_slots(SIMNET), &nonce(0));
        let nominal = schedule.rounds();

        assert_eq!(
            schedule.round_at(schedule.base_slot(RoundNumber(nominal))),
            Some(RoundNumber(nominal))
        );
        assert_eq!(
            schedule.round_at(schedule.base_slot(RoundNumber(nominal + 10))),
            Some(RoundNumber(nominal + 10))
        );
    }

    #[test]
    fn nonce_moves_grid() {
        let slots = epoch_slots(DEVNET);
        let offsets: Vec<u64> = (0..8u8)
            .map(|byte| Schedule::for_epoch(SlotNumber(0), slots, &nonce(byte)).grid_offset)
            .collect();

        assert!(offsets.iter().all(|offset| *offset <= grid_slack(slots, 150)));
        assert!(
            offsets.iter().collect::<std::collections::HashSet<_>>().len() > 1,
            "the grid sat in the same place for every nonce: {offsets:?}"
        );
    }

    #[test]
    fn nonce_is_pure() {
        let slots = epoch_slots(DEVNET);
        let once = Schedule::for_epoch(SlotNumber(77), slots, &nonce(9));
        let again = Schedule::for_epoch(SlotNumber(77), slots, &nonce(9));

        assert_eq!(once, again);
        assert_eq!(once.base_slot(RoundNumber(3)), again.base_slot(RoundNumber(3)));
    }

    #[test]
    fn grid_inside_epoch() {
        let slots = epoch_slots(SIMNET);
        for byte in 0..32u8 {
            let schedule = Schedule::for_epoch(SlotNumber(500), slots, &nonce(byte));
            assert!(schedule.first_slot().as_u64() >= 500);
            // Moving the grid must never cost a round.
            assert_eq!(
                schedule.rounds(),
                Schedule::for_epoch(SlotNumber(500), slots, &Hash([0; 32])).rounds()
            );
        }
    }

    #[test]
    fn slot_to_round() {
        let start = SlotNumber(9_000);
        let schedule = Schedule::for_epoch(start, epoch_slots(DEVNET), &nonce(0));

        for round in [0u64, 1, 30, schedule.rounds() + 5] {
            let window = schedule.entropy_window(RoundNumber(round));
            for slot in window.clone() {
                assert_eq!(schedule.round_at(SlotNumber(slot)), Some(RoundNumber(round)));
            }
            // The gap after a window belongs to no round.
            assert_eq!(schedule.round_at(SlotNumber(window.end)), None);
        }

        // Before the grid opens there is no round at all.
        assert_eq!(schedule.round_at(SlotNumber(schedule.first_slot().as_u64() - 1)), None);
    }
}
