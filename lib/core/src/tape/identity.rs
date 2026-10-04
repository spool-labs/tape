use crate::track::types::TrackFlags;
use crate::types::{EpochNumber, NodeId, TapeNumber};

pub struct TapeFlags;

impl TapeFlags {
    pub const NONE: u64 = 0;
    pub const SYSTEM: u64 = 1;
    pub const HIDDEN: u64 = 1 << 1;
    pub const IMMUTABLE: u64 = 1 << 2;

    const RESERVABLE: u64 = Self::HIDDEN | Self::IMMUTABLE;

    #[inline(always)]
    pub fn is_system(flags: u64) -> bool {
        flags & Self::SYSTEM != 0
    }

    #[inline(always)]
    pub fn is_hidden(flags: u64) -> bool {
        flags & Self::HIDDEN != 0
    }

    #[inline(always)]
    pub fn is_immutable(flags: u64) -> bool {
        flags & Self::IMMUTABLE != 0
    }

    #[inline(always)]
    pub fn is_reservable(flags: u64) -> bool {
        flags & !Self::RESERVABLE == 0
    }

    /// Every track written to a tape with these flags gets these track flags
    #[inline(always)]
    pub fn track_flags(flags: u64) -> u64 {
        let mut track_flags = TrackFlags::NONE;
        if Self::is_hidden(flags) {
            track_flags |= TrackFlags::HIDDEN;
        }
        if Self::is_immutable(flags) {
            track_flags |= TrackFlags::IMMUTABLE;
        }
        track_flags
    }
}

#[repr(u8)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TapeNamespace {
    User = 0x00,
    Snapshot = 0x01,
    History = 0x02,
    Blacklist = 0x03,
}

impl TapeNamespace {
    // Store the namespace in the high byte and leave the lower 56 bits for the tape index.
    const SHIFT: u64 = 56;
    const MASK: u64 = (u8::MAX as u64) << Self::SHIFT;
    const INDEX_MASK: u64 = !Self::MASK;

    #[inline(always)]
    pub fn from_tape_number(tape: TapeNumber) -> Option<Self> {
        match (tape.0 >> Self::SHIFT) as u8 {
            x if x == Self::User as u8 => Some(Self::User),
            x if x == Self::Snapshot as u8 => Some(Self::Snapshot),
            x if x == Self::History as u8 => Some(Self::History),
            x if x == Self::Blacklist as u8 => Some(Self::Blacklist),
            _ => None,
        }
    }

    #[inline(always)]
    pub fn tape_number(self, index: u64) -> TapeNumber {
        TapeNumber(((self as u64) << Self::SHIFT) | (index & Self::INDEX_MASK))
    }

    #[inline(always)]
    pub fn index(tape: TapeNumber) -> u64 {
        tape.0 & Self::INDEX_MASK
    }
}

#[inline(always)]
pub fn tape_namespace(tape: TapeNumber) -> Option<TapeNamespace> {
    TapeNamespace::from_tape_number(tape)
}

#[inline(always)]
pub fn tape_index(tape: TapeNumber) -> u64 {
    TapeNamespace::index(tape)
}

#[inline(always)]
pub fn user_tape_number(index: u64) -> Option<TapeNumber> {
    if index == 0 || index > TapeNamespace::INDEX_MASK {
        return None;
    }
    Some(TapeNamespace::User.tape_number(index))
}

#[inline(always)]
pub fn snapshot_tape_number(epoch: EpochNumber) -> TapeNumber {
    TapeNamespace::Snapshot.tape_number(epoch.0)
}

#[inline(always)]
pub fn history_tape_number(node: NodeId) -> TapeNumber {
    TapeNamespace::History.tape_number(node.0)
}

#[inline(always)]
pub fn blacklist_tape_number(node: NodeId) -> TapeNumber {
    TapeNamespace::Blacklist.tape_number(node.0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn user_numbers_keep_the_user_namespace() {
        assert_eq!(user_tape_number(1), Some(TapeNumber(1)));
        assert_eq!(
            tape_namespace(user_tape_number(9).unwrap()),
            Some(TapeNamespace::User)
        );
        assert_eq!(user_tape_number(0), None);
        assert_eq!(user_tape_number(TapeNamespace::INDEX_MASK + 1), None);
    }

    #[test]
    fn system_numbers_are_namespaced() {
        let snapshot = snapshot_tape_number(EpochNumber(42));
        let history = history_tape_number(NodeId(42));
        let blacklist = blacklist_tape_number(NodeId(42));

        assert_eq!(tape_namespace(snapshot), Some(TapeNamespace::Snapshot));
        assert_eq!(tape_namespace(history), Some(TapeNamespace::History));
        assert_eq!(tape_namespace(blacklist), Some(TapeNamespace::Blacklist));
        assert_eq!(tape_index(snapshot), 42);
        assert_ne!(snapshot, history);
        assert_ne!(history, blacklist);
    }

    // a user can reserve a hidden or immutable tape; system and unknown bits are refused
    #[test]
    fn reservable_flags() {
        assert!(TapeFlags::is_reservable(TapeFlags::NONE));
        assert!(TapeFlags::is_reservable(TapeFlags::HIDDEN | TapeFlags::IMMUTABLE));
        assert!(!TapeFlags::is_reservable(TapeFlags::SYSTEM));
        assert!(!TapeFlags::is_reservable(1 << 40));
    }

    // a tape hands its tracks its hidden and immutable flags only
    #[test]
    fn tape_track_flags() {
        assert_eq!(TapeFlags::track_flags(TapeFlags::NONE), TrackFlags::NONE);
        assert_eq!(TapeFlags::track_flags(TapeFlags::SYSTEM), TrackFlags::NONE);
        assert_eq!(TapeFlags::track_flags(TapeFlags::HIDDEN), TrackFlags::HIDDEN);
        assert_eq!(TapeFlags::track_flags(TapeFlags::IMMUTABLE), TrackFlags::IMMUTABLE);
        assert_eq!(
            TapeFlags::track_flags(TapeFlags::SYSTEM | TapeFlags::HIDDEN | TapeFlags::IMMUTABLE),
            TrackFlags::HIDDEN | TrackFlags::IMMUTABLE,
        );
    }

    #[test]
    fn unknown_namespaces_are_rejected() {
        let tape = TapeNumber(0xff << TapeNamespace::SHIFT);
        assert_eq!(tape_namespace(tape), None);
    }
}
