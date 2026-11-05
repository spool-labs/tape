use tape_core::types::{EpochNumber, SpoolIndex};
use tape_crypto::address::Address;

use crate::attempt::{Attempt, AttemptFailure, Method};
use crate::time::WallTime;

/// One track in a spool's missing_list
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MissingTrack {
    /// The stored spool whose missing_list holds the record
    pub spool: SpoolIndex,
    /// The track's address
    pub track: Address,
    /// How the missing track got on the list
    pub origin: TrackOrigin,
    /// When the missing track was added
    pub added_at: WallTime,
    /// What was tried last, nothing for a track never tried
    pub last_attempt: Option<Attempt>,
    /// Failures of the track's own; stops at its largest value
    pub failures: u16,
    /// When a repair first failed for the track
    pub repair_failing_since: Option<WallTime>,
    /// The epoch of the last holder that said it doesn't have our slice
    pub owner_epoch_tried: EpochNumber,
}

/// How a missing track got on the list
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum TrackOrigin {
    /// The scan added it: the track was on the spool when we took it over
    Handover,
    /// The certify hook added it, for a fresh upload
    Upload,
}

impl MissingTrack {
    /// Bytes a missing track's facts take on disk
    pub const SIZE: usize = 36;

    /// A track the scan found missing at `now`, never tried
    pub fn new_handover(
        spool: SpoolIndex,
        track: Address,
        now: WallTime,
    ) -> Self {
        Self {
            spool,
            track,
            origin: TrackOrigin::Handover,
            added_at: now,
            last_attempt: None,
            failures: 0,
            repair_failing_since: None,
            owner_epoch_tried: EpochNumber(0),
        }
    }

    /// A fresh upload's track, certified at `now`, never tried
    pub fn new_upload(
        spool: SpoolIndex,
        track: Address,
        now: WallTime,
    ) -> Self {
        let origin = TrackOrigin::Upload;
        Self { origin, ..Self::new_handover(spool, track, now) }
    }

    /// Record a failed attempt: a holder that said no moves the holders on,
    /// a repair that failed starts the repair clock, and a failure of the
    /// track's own counts
    pub fn record_failure(
        &mut self,
        method: Method,
        failure: AttemptFailure,
        holder_epoch: EpochNumber,
        now: WallTime,
    ) {
        if failure.is_track_failure() {
            self.record_track_failure(method, holder_epoch, now);
        }
        self.last_attempt = Some(Attempt { at: now, method, failure });
    }

    /// Write the facts in their fixed layout; a field with nothing to say is
    /// zero. The store keys them by spool, origin and track.
    pub fn pack(&self) -> [u8; Self::SIZE] {
        let mut bytes = [0u8; Self::SIZE];
        bytes[0..8].copy_from_slice(&self.added_at.0.to_le_bytes());
        if let Some(attempt) = self.last_attempt {
            bytes[8..16].copy_from_slice(&attempt.at.0.to_le_bytes());
            bytes[16] = attempt.method.to_byte();
            bytes[17] = attempt.failure.to_byte();
        }
        bytes[18..20].copy_from_slice(&self.failures.to_le_bytes());
        let since = self.repair_failing_since.unwrap_or_default();
        bytes[20..28].copy_from_slice(&since.0.to_le_bytes());
        let tried = self.owner_epoch_tried.0.to_le_bytes();
        bytes[28..36].copy_from_slice(&tried);
        bytes
    }

    /// Read a missing track back from its key's fields and its facts
    pub fn unpack(
        spool: SpoolIndex,
        origin: TrackOrigin,
        track: Address,
        bytes: [u8; Self::SIZE],
    ) -> Self {
        let since = WallTime(read_u64(&bytes[20..28]));
        Self {
            spool,
            track,
            origin,
            added_at: WallTime(read_u64(&bytes[0..8])),
            last_attempt: unpack_attempt(&bytes),
            failures: u16::from_le_bytes([bytes[18], bytes[19]]),
            repair_failing_since: (since.0 != 0).then_some(since),
            owner_epoch_tried: EpochNumber(read_u64(&bytes[28..36])),
        }
    }

    /// A failure of the track's own, by the method that met it
    fn record_track_failure(
        &mut self,
        method: Method,
        holder_epoch: EpochNumber,
        now: WallTime,
    ) {
        match method {
            Method::Transfer => self.owner_epoch_tried = holder_epoch,
            Method::Repair => {
                self.repair_failing_since.get_or_insert(now);
                self.failures = self.failures.saturating_add(1);
            }
            Method::Sync | Method::Recover => {
                self.failures = self.failures.saturating_add(1);
            }
        }
    }
}

impl TrackOrigin {
    /// The origin's byte in a missing track's key: uploads sort first
    pub fn to_byte(self) -> u8 {
        match self {
            Self::Upload => 0,
            Self::Handover => 1,
        }
    }

    /// The origin a key byte holds, nothing for a byte past them
    pub fn from_byte(byte: u8) -> Option<Self> {
        match byte {
            0 => Some(Self::Upload),
            1 => Some(Self::Handover),
            _ => None,
        }
    }
}

fn unpack_attempt(bytes: &[u8; MissingTrack::SIZE]) -> Option<Attempt> {
    let at = WallTime(read_u64(&bytes[8..16]));
    let method = Method::from_byte(bytes[16])?;
    let failure = AttemptFailure::from_byte(bytes[17])?;
    if at.0 == 0 {
        return None;
    }
    Some(Attempt { at, method, failure })
}

fn read_u64(bytes: &[u8]) -> u64 {
    let mut raw = [0u8; 8];
    raw.copy_from_slice(bytes);
    u64::from_le_bytes(raw)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn address(byte: u8) -> Address {
        Address::new([byte; 32])
    }

    // a missing track's facts come back from their bytes as they went in
    #[test]
    fn pack_roundtrip() {
        let spool = SpoolIndex(41);
        let track = address(3);
        let attempt = Attempt {
            at: WallTime(1_700_000_000_123),
            method: Method::Transfer,
            failure: AttemptFailure::NotHeld,
        };
        let added = WallTime(1_700_000_000_000);
        let tried = MissingTrack {
            last_attempt: Some(attempt),
            failures: u16::MAX,
            repair_failing_since: Some(WallTime(9)),
            owner_epoch_tried: EpochNumber(99),
            ..MissingTrack::new_handover(spool, track, added)
        };
        let uploaded = MissingTrack::new_upload(spool, track, WallTime(5));
        let fresh = MissingTrack::new_handover(spool, track, WallTime(0));

        for missing in [fresh, uploaded, tried] {
            let bytes = missing.pack();
            let origin = missing.origin;
            let read = MissingTrack::unpack(spool, origin, track, bytes);
            assert_eq!(read, missing);
        }
        assert_eq!(fresh.pack(), [0u8; MissingTrack::SIZE]);
    }

    // a holder that said no moves the holders on, a failed repair starts the
    // repair clock, and only a failure of the track's own counts
    #[test]
    fn records_failures() {
        let at = WallTime(1_000);
        let fresh = MissingTrack::new_handover(SpoolIndex(0), address(1), at);
        let (now, later) = (WallTime(5_000), WallTime(9_000));
        let (epoch, none) = (EpochNumber(8), EpochNumber(0));
        let not_held = AttemptFailure::NotHeld;
        let wrong = AttemptFailure::WrongData;
        let down = AttemptFailure::Down;

        let mut transfer = fresh;
        transfer.record_failure(Method::Transfer, not_held, epoch, now);
        let mut repair = fresh;
        repair.record_failure(Method::Repair, wrong, none, now);
        repair.record_failure(Method::Repair, wrong, none, later);
        let mut sync = fresh;
        sync.record_failure(Method::Sync, down, none, now);

        assert_eq!(transfer.owner_epoch_tried, epoch);
        assert_eq!(transfer.failures, 0);
        assert_eq!(repair.repair_failing_since, Some(now));
        assert_eq!(repair.failures, 2);
        assert_eq!(sync.failures, 0);
        let attempt = Attempt { at: now, method: Method::Sync, failure: down };
        assert_eq!(sync.last_attempt, Some(attempt));
    }
}
