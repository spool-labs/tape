//! The missing_list's entries and the fixed layout they take on disk
//!
//! An entry records what happened to one track that still needs something.
//! Nothing in it says when to try next: the spool manager works that out.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tape_core::erasure::MAX_GROUP_SIZE;
use tape_core::spooler::VirtualGroup;
use tape_core::types::{EpochNumber, SpoolIndex};
use tape_crypto::address::Address;

use crate::error::{Result, TapeStoreError};

/// How many times a track failed on its own
pub type FailureCount = u16;

/// An entry's bytes on disk
pub type PackedEntry = [u8; Entry::SIZE];

/// An entry key's bytes on disk
pub type PackedEntryKey = [u8; EntryKey::SIZE];

/// Wall time in milliseconds since the unix epoch, zero for never
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct WallTime(pub u64);

impl WallTime {
    /// The wall clock now
    pub fn now() -> Self {
        let since = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default();
        Self(since.as_millis() as u64)
    }

    /// Time since `earlier`, zero when `earlier` is later
    pub fn since(self, earlier: WallTime) -> Duration {
        Duration::from_millis(self.0.saturating_sub(earlier.0))
    }
}

/// The ways to get what a track is missing, and the upload that counts as
/// a fresh track's first attempt
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum Method {
    Upload,
    Sync,
    Transfer,
    Repair,
    Recovery,
}

/// What happened to an attempt that didn't get the track what it lacks
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum AttemptResult {
    Down,
    TooSlow,
    Refused,
    NotHeld,
    WrongData,
}

/// One try with one method: when, how, and what happened
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Attempt {
    pub at: WallTime,
    pub method: Method,
    pub result: Option<AttemptResult>,
}

/// One track in a missing_list
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Entry {
    /// Nothing for a track that was never tried
    pub last_attempt: Option<Attempt>,
    /// Stops at its largest value
    pub failures: FailureCount,
    pub repair_failing_since: Option<WallTime>,
    /// The epoch of the last holder that said it doesn't have our slice
    pub owner_epoch_tried: EpochNumber,
}

/// Where an entry sits: its stored spool, whether filling added it, its track
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct EntryKey {
    pub spool: SpoolIndex,
    pub is_handover: bool,
    pub track: Address,
}

impl Method {
    fn to_byte(self) -> u8 {
        match self {
            Self::Upload => 1,
            Self::Sync => 2,
            Self::Transfer => 3,
            Self::Repair => 4,
            Self::Recovery => 5,
        }
    }

    fn from_byte(byte: u8) -> Option<Self> {
        match byte {
            1 => Some(Self::Upload),
            2 => Some(Self::Sync),
            3 => Some(Self::Transfer),
            4 => Some(Self::Repair),
            5 => Some(Self::Recovery),
            _ => None,
        }
    }
}

impl AttemptResult {
    fn to_byte(result: Option<Self>) -> u8 {
        match result {
            None => 0,
            Some(Self::Down) => 1,
            Some(Self::TooSlow) => 2,
            Some(Self::Refused) => 3,
            Some(Self::NotHeld) => 4,
            Some(Self::WrongData) => 5,
        }
    }

    fn from_byte(byte: u8) -> Option<Self> {
        match byte {
            1 => Some(Self::Down),
            2 => Some(Self::TooSlow),
            3 => Some(Self::Refused),
            4 => Some(Self::NotHeld),
            5 => Some(Self::WrongData),
            _ => None,
        }
    }
}

impl Entry {
    /// Bytes an entry takes on disk
    pub const SIZE: usize = 28;

    /// An entry for a track nobody has tried yet
    pub fn never_tried() -> Self {
        Self {
            last_attempt: None,
            failures: 0,
            repair_failing_since: None,
            owner_epoch_tried: EpochNumber(0),
        }
    }

    /// An entry whose first attempt is the upload at `at`
    pub fn uploaded(at: WallTime) -> Self {
        let upload = Attempt { at, method: Method::Upload, result: None };
        Self { last_attempt: Some(upload), ..Self::never_tried() }
    }

    /// Write the entry in its fixed layout; a field with nothing to say is zero
    pub fn pack(&self) -> PackedEntry {
        let mut bytes = [0u8; Self::SIZE];
        if let Some(attempt) = self.last_attempt {
            bytes[0..8].copy_from_slice(&attempt.at.0.to_le_bytes());
            bytes[8] = attempt.method.to_byte();
            bytes[9] = AttemptResult::to_byte(attempt.result);
        }
        bytes[10..12].copy_from_slice(&self.failures.to_le_bytes());
        let since = self.repair_failing_since.unwrap_or_default();
        bytes[12..20].copy_from_slice(&since.0.to_le_bytes());
        bytes[20..28].copy_from_slice(&self.owner_epoch_tried.0.to_le_bytes());
        bytes
    }

    /// Read an entry back from its fixed layout
    pub fn unpack(bytes: PackedEntry) -> Self {
        let at = WallTime(read_u64(&bytes[0..8]));
        let last_attempt = match Method::from_byte(bytes[8]) {
            Some(method) if at.0 != 0 => Some(Attempt {
                at,
                method,
                result: AttemptResult::from_byte(bytes[9]),
            }),
            Some(_) | None => None,
        };
        let since = WallTime(read_u64(&bytes[12..20]));
        Self {
            last_attempt,
            failures: FailureCount::from_le_bytes([bytes[10], bytes[11]]),
            repair_failing_since: (since.0 != 0).then_some(since),
            owner_epoch_tried: EpochNumber(read_u64(&bytes[20..28])),
        }
    }
}

impl EntryKey {
    /// Bytes an entry key takes: position, handover, virtual group, track
    pub const SIZE: usize = 36;

    /// Write the key so one position's entries sit together, then the
    /// handover ones apart, then each virtual group's tracks in order
    pub fn pack(&self) -> Result<PackedEntryKey> {
        let mut bytes = [0u8; Self::SIZE];
        bytes[..4].copy_from_slice(&entry_prefix(self.spool, self.is_handover)?);
        bytes[4..].copy_from_slice(&self.track.to_bytes());
        Ok(bytes)
    }

    /// Read a key back from its bytes
    pub fn unpack(bytes: &[u8]) -> Result<Self> {
        if bytes.len() != Self::SIZE {
            return Err(TapeStoreError::Serialization("entry key length".into()));
        }
        let position = u64::from(bytes[0]);
        let group = u64::from(u16::from_be_bytes([bytes[2], bytes[3]]));
        let mut track = [0u8; 32];
        track.copy_from_slice(&bytes[4..]);
        Ok(Self {
            spool: VirtualGroup(group).spool_at(position as usize),
            is_handover: bytes[1] != 0,
            track: Address::new(track),
        })
    }
}

/// The key bytes every entry of one stored spool starts with, by handover
pub fn entry_prefix(spool: SpoolIndex, is_handover: bool) -> Result<[u8; 4]> {
    let position = stored_position(spool);
    let group = stored_group(spool)?;
    let [high, low] = group;
    Ok([position, u8::from(is_handover), high, low])
}

/// A stored spool's position in its group, one byte
pub fn stored_position(spool: SpoolIndex) -> u8 {
    (spool.as_u64() % MAX_GROUP_SIZE as u64) as u8
}

/// A stored spool's virtual group, two bytes big endian
pub fn stored_group(spool: SpoolIndex) -> Result<[u8; 2]> {
    group_bytes(VirtualGroup::containing(spool))
}

/// A virtual group's two key bytes, big endian
pub fn group_bytes(group: VirtualGroup) -> Result<[u8; 2]> {
    u16::try_from(group.as_u64())
        .map(u16::to_be_bytes)
        .map_err(|_| TapeStoreError::Serialization(format!("virtual group {group} too large")))
}

fn read_u64(bytes: &[u8]) -> u64 {
    let mut raw = [0u8; 8];
    raw.copy_from_slice(bytes);
    u64::from_le_bytes(raw)
}

#[cfg(test)]
mod tests {
    use super::*;

    // an entry comes back from its 28 bytes as it went in
    #[test]
    fn entry_roundtrip() {
        let tried = Entry {
            last_attempt: Some(Attempt {
                at: WallTime(1_700_000_000_123),
                method: Method::Transfer,
                result: Some(AttemptResult::NotHeld),
            }),
            failures: FailureCount::MAX,
            repair_failing_since: Some(WallTime(9)),
            owner_epoch_tried: EpochNumber(99),
        };

        for entry in [Entry::never_tried(), Entry::uploaded(WallTime(5)), tried] {
            assert_eq!(Entry::unpack(entry.pack()), entry);
        }
        assert_eq!(Entry::never_tried().pack(), [0u8; Entry::SIZE]);
    }

    // a key comes back from its bytes, and keys sort by position, handover, group, track
    #[test]
    fn key_order() {
        let low = EntryKey {
            spool: VirtualGroup(300).spool_at(4),
            is_handover: false,
            track: Address::new([9; 32]),
        };
        let handover = EntryKey { is_handover: true, ..low };
        let next_group = EntryKey { spool: VirtualGroup(301).spool_at(4), ..low };
        let next_position = EntryKey { spool: VirtualGroup(0).spool_at(5), ..low };

        let packed = low.pack().expect("pack");
        assert_eq!(EntryKey::unpack(&packed).expect("unpack"), low);
        assert!(packed < next_group.pack().expect("pack"));
        assert!(next_group.pack().expect("pack") < handover.pack().expect("pack"));
        assert!(handover.pack().expect("pack") < next_position.pack().expect("pack"));
    }
}
