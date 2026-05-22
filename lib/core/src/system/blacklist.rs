use bytemuck::{bytes_of, Pod, Zeroable};
use num_enum::{IntoPrimitive, TryFromPrimitive};
use serde::{Deserialize, Serialize};
#[cfg(feature = "wincode")]
use wincode_derive::{SchemaRead, SchemaWrite};

use tape_crypto::{hash::hash, Address, Hash};

#[repr(u64)]
#[derive(Clone, Copy, Debug, Eq, PartialEq, IntoPrimitive, TryFromPrimitive)]
pub enum BlacklistKind {
    Unknown = 0,
    Track,
    Tape,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Pod, Zeroable, Serialize, Deserialize)]
#[cfg_attr(feature = "wincode", derive(SchemaRead, SchemaWrite))]
pub struct BlacklistEntry {
    /// Kind of target being blacklisted.
    pub kind: u64,

    /// Address of the blacklisted object.
    pub target: Address,
}

impl BlacklistEntry {
    pub const fn track(target: Address) -> Self {
        Self {
            kind: BlacklistKind::Track as u64,
            target,
        }
    }

    pub const fn tape(target: Address) -> Self {
        Self {
            kind: BlacklistKind::Tape as u64,
            target,
        }
    }

    pub fn kind(&self) -> Option<BlacklistKind> {
        BlacklistKind::try_from(self.kind).ok()
    }

    pub fn is_track(&self) -> bool {
        matches!(self.kind(), Some(BlacklistKind::Track))
            && self.target != Address::default()
    }

    pub fn is_tape(&self) -> bool {
        matches!(self.kind(), Some(BlacklistKind::Tape))
            && self.target != Address::default()
    }

    pub fn is_valid(&self) -> bool {
        self.is_track() || self.is_tape()
    }

    pub fn key(&self) -> Hash {
        hash(bytes_of(self))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validates_entries() {
        let target = Address::new_unique();

        assert!(BlacklistEntry::track(target).is_track());
        assert!(BlacklistEntry::tape(target).is_tape());
        assert!(!BlacklistEntry::track(Address::default()).is_valid());
        assert!(!BlacklistEntry {
            kind: BlacklistKind::Unknown as u64,
            target,
        }
        .is_valid());
    }

    #[test]
    fn key_is_stable() {
        let entry = BlacklistEntry::track(Address::new_unique());

        assert_eq!(entry.key(), entry.key());
        assert_ne!(entry.key(), BlacklistEntry::tape(entry.target).key());
    }
}
