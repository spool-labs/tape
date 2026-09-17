//! Track and tape targets of reports, checks and decisions

use std::fmt;

use tape_core::system::BlacklistEntry;
use tape_crypto::address::Address;

/// Whether a target is a track or a tape
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum TargetKind {
    Track,
    Tape,
}

impl TargetKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Track => "track",
            Self::Tape => "tape",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "track" => Some(Self::Track),
            "tape" => Some(Self::Tape),
            _ => None,
        }
    }
}

/// A track or tape by address
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct Target {
    pub kind: TargetKind,
    pub address: Address,
}

impl Target {
    pub fn track(address: Address) -> Self {
        Self {
            kind: TargetKind::Track,
            address,
        }
    }

    pub fn tape(address: Address) -> Self {
        Self {
            kind: TargetKind::Tape,
            address,
        }
    }

    /// The target as a blacklist batch names it
    pub fn to_blacklist_entry(self) -> BlacklistEntry {
        match self.kind {
            TargetKind::Track => BlacklistEntry::track(self.address),
            TargetKind::Tape => BlacklistEntry::tape(self.address),
        }
    }

    /// The target a blacklist entry names, if it is a track or tape
    pub fn from_blacklist_entry(entry: &BlacklistEntry) -> Option<Self> {
        match (entry.is_track(), entry.is_tape()) {
            (true, _) => Some(Self::track(entry.target)),
            (false, true) => Some(Self::tape(entry.target)),
            (false, false) => None,
        }
    }
}

impl fmt::Display for Target {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{} {}", self.kind.as_str(), self.address)
    }
}
