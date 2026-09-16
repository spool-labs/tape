//! An action on a track or tape target

use tape_core::system::BlacklistEntry;
use tape_crypto::address::Address;

/// What an entry does to its target
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum Action {
    /// Deny a track
    Remove,
    /// Deny every track on a tape
    Unlist,
}

impl Action {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Remove => "remove",
            Self::Unlist => "unlist",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        let action = match value {
            "remove" => Self::Remove,
            "unlist" => Self::Unlist,
            _ => return None,
        };
        Some(action)
    }
}

/// One line of a blacklist track
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Entry {
    pub action: Action,
    pub target: BlacklistEntry,
}

impl Entry {
    pub fn remove(track: Address) -> Self {
        Self {
            action: Action::Remove,
            target: BlacklistEntry::track(track),
        }
    }

    pub fn unlist(tape: Address) -> Self {
        Self {
            action: Action::Unlist,
            target: BlacklistEntry::tape(tape),
        }
    }

    /// Whether the action applies to the target's kind
    pub fn is_valid(&self) -> bool {
        match self.action {
            Action::Remove => self.target.is_track(),
            Action::Unlist => self.target.is_tape(),
        }
    }
}

/// The word the format uses for a target's kind
pub fn kind_name(target: &BlacklistEntry) -> Option<&'static str> {
    match (target.is_track(), target.is_tape()) {
        (true, _) => Some("track"),
        (false, true) => Some("tape"),
        (false, false) => None,
    }
}
