//! Upkeep decisions for one tape and the payer, from chain state alone

use tape_core::types::coin::{SOL, TAPE};
use tape_core::types::{EpochNumber, StorageUnits, TrackNumber};
use tape_sdk::stream::manifest::MAX_TRACKS_PER_TAPE;

use crate::chain::tape_chain::{PayerBalances, TapeStatus};
use crate::config::settings::FunderConfig;

/// What a tape does for the list, which decides the upkeep it gets
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TapeDuty {
    /// The rack: takes a record for each new blacklist tape
    Rack,
    /// The newest blacklist tape: takes entries
    Newest,
    /// An older blacklist tape, or one not yet named: only kept from expiring
    Kept,
}

/// What one tape needs
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TapeUpkeep {
    pub extend_expiry: Option<EpochNumber>,
    pub extend_capacity: Option<StorageUnits>,
    /// Track slots the tape has left
    pub free_tracks: TrackNumber,
    /// A written tape whose free track slots are down to the headroom
    pub is_short_of_tracks: bool,
}

/// A payer balance under its alert threshold
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LowBalance {
    Sol { balance: SOL, threshold: SOL },
    Tape { balance: TAPE, threshold: TAPE },
}

/// Every tape keeps its expiry funded; only the rack and the newest blacklist tape get capacity
pub fn plan_tape(
    status: &TapeStatus,
    duty: TapeDuty,
    current_epoch: EpochNumber,
    config: &FunderConfig,
) -> TapeUpkeep {
    let remaining = status.expiry_epoch.saturating_sub(current_epoch);
    let is_written = matches!(duty, TapeDuty::Rack | TapeDuty::Newest);
    let free = status.capacity.saturating_sub(status.used);
    let free_tracks = TrackNumber(MAX_TRACKS_PER_TAPE.saturating_sub(status.next_track.0));

    TapeUpkeep {
        extend_expiry: (remaining < config.expiry_horizon).then_some(config.expiry_extension),
        extend_capacity: (is_written && free < config.capacity_reserve).then_some(config.capacity_extension),
        free_tracks,
        is_short_of_tracks: is_written && free_tracks <= config.track_headroom,
    }
}

/// Whether a tape counts for nothing at `current_epoch`, as consumers read expiry
pub fn is_expired(expiry_epoch: EpochNumber, current_epoch: EpochNumber) -> bool {
    expiry_epoch <= current_epoch
}

/// Whether a tape is close enough to expiry that its decisions move to a tape that lasts
pub fn is_expiring(expiry_epoch: EpochNumber, current_epoch: EpochNumber, config: &FunderConfig) -> bool {
    expiry_epoch.saturating_sub(current_epoch) <= config.move_horizon
}

pub fn low_balances(balances: &PayerBalances, config: &FunderConfig) -> Vec<LowBalance> {
    let mut low = Vec::new();
    if balances.sol < config.sol_warn_below {
        low.push(LowBalance::Sol {
            balance: balances.sol,
            threshold: config.sol_warn_below,
        });
    }
    if balances.tape < config.tape_warn_below {
        low.push(LowBalance::Tape {
            balance: balances.tape,
            threshold: config.tape_warn_below,
        });
    }
    low
}

#[cfg(test)]
mod tests {
    use tape_core::types::TrackNumber;

    use super::*;
    use crate::types::ids::TrackCount;

    fn status(expiry: u64, next_track: u64) -> TapeStatus {
        TapeStatus {
            capacity: StorageUnits(1_000_000),
            used: StorageUnits(0),
            expiry_epoch: EpochNumber(expiry),
            next_track: TrackNumber(next_track),
            present_tracks: TrackCount(next_track),
        }
    }

    // expiry extends only inside the horizon, for tapes of any duty
    #[test]
    fn horizon() {
        let config = FunderConfig::default();
        let inside = 100 + config.expiry_horizon.0 - 1;
        let outside = 100 + config.expiry_horizon.0;

        let kept = plan_tape(&status(inside, 0), TapeDuty::Kept, EpochNumber(100), &config);
        let newest = plan_tape(&status(outside, 0), TapeDuty::Newest, EpochNumber(100), &config);

        assert_eq!(kept.extend_expiry, Some(config.expiry_extension));
        assert_eq!(newest.extend_expiry, None);
    }

    // the rack and newest tape run short of tracks once free slots reach the headroom
    #[test]
    fn headroom() {
        let config = FunderConfig::default();
        let at = MAX_TRACKS_PER_TAPE - config.track_headroom.0;

        let before = plan_tape(&status(1_000, at - 1), TapeDuty::Newest, EpochNumber(100), &config);
        let newest = plan_tape(&status(1_000, at), TapeDuty::Newest, EpochNumber(100), &config);
        let rack = plan_tape(&status(1_000, at), TapeDuty::Rack, EpochNumber(100), &config);
        let kept = plan_tape(&status(1_000, at), TapeDuty::Kept, EpochNumber(100), &config);

        assert!(!before.is_short_of_tracks);
        assert!(newest.is_short_of_tracks);
        assert!(rack.is_short_of_tracks);
        assert!(!kept.is_short_of_tracks);
    }

    // a tape expires at its expiry epoch, and is expiring from the move horizon on
    #[test]
    fn expiry() {
        let config = FunderConfig::default();
        let horizon = config.move_horizon.0;

        assert!(!is_expired(EpochNumber(101), EpochNumber(100)));
        assert!(is_expired(EpochNumber(100), EpochNumber(100)));
        assert!(is_expiring(EpochNumber(100 + horizon), EpochNumber(100), &config));
        assert!(!is_expiring(EpochNumber(101 + horizon), EpochNumber(100), &config));
    }
}
