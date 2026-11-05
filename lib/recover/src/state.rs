use std::collections::HashSet;
use std::ops::Range;

use tape_core::spooler::VirtualGroup;
use tape_core::types::{EpochNumber, SpoolIndex};
use tape_crypto::address::Address;

/// One stored spool this node owns now or owned in the epochs it keeps
/// slices for
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SpoolState {
    /// The scan hasn't read the spool's whole track list yet
    pub is_scanning: bool,
    /// The last track the scan read, zero before its first page
    pub scan_position: Address,
    /// The epoch this node lost the spool, zero while it owns it
    pub lost_at: EpochNumber,
}

/// What a new epoch does to the spool_states
#[derive(Debug, Default, Eq, PartialEq)]
pub struct SpoolChanges {
    /// The spools gained, each with the state it starts with
    pub gained: Vec<(SpoolIndex, SpoolState)>,
    /// The spools lost this epoch, each with its state marked lost
    pub lost: Vec<(SpoolIndex, SpoolState)>,
    /// The spools lost the kept epochs ago, whose slices and missing_list go
    pub purged: Vec<SpoolIndex>,
}

impl SpoolState {
    /// Bytes a spool state takes on disk
    pub const SIZE: usize = 41;

    /// The state of a spool this node just gained, with the scan at the start
    pub fn new_scanning() -> Self {
        Self {
            is_scanning: true,
            scan_position: Address::default(),
            lost_at: EpochNumber(0),
        }
    }

    /// Once lost_at is set we take no new uploads for the spool
    pub fn is_lost(&self) -> bool {
        self.lost_at != EpochNumber(0)
    }

    /// Write the state in its fixed layout: scanning, scan_position, lost_at
    pub fn pack(&self) -> [u8; Self::SIZE] {
        let mut bytes = [0u8; Self::SIZE];
        bytes[0] = u8::from(self.is_scanning);
        bytes[1..33].copy_from_slice(&self.scan_position.to_bytes());
        bytes[33..].copy_from_slice(&self.lost_at.0.to_le_bytes());
        bytes
    }

    /// Read a state back from its fixed layout
    pub fn unpack(bytes: [u8; Self::SIZE]) -> Self {
        let mut position = [0u8; 32];
        position.copy_from_slice(&bytes[1..33]);
        let mut lost = [0u8; 8];
        lost.copy_from_slice(&bytes[33..]);
        Self {
            is_scanning: bytes[0] != 0,
            scan_position: Address::new(position),
            lost_at: EpochNumber(u64::from_le_bytes(lost)),
        }
    }
}

/// The changes a new epoch makes: a spool gained, or one that came back,
/// starts scanning; one lost now is marked lost; one lost the kept epochs
/// ago is purged
pub fn get_spool_changes(
    owned: &HashSet<SpoolIndex>,
    states: &[(SpoolIndex, SpoolState)],
    epoch: EpochNumber,
    keep_epochs: u64,
) -> SpoolChanges {
    let mut changes = SpoolChanges::default();
    let mut has_state = HashSet::with_capacity(states.len());
    for (spool, state) in states {
        has_state.insert(*spool);
        let scanning = (*spool, SpoolState::new_scanning());
        let marked = (*spool, SpoolState { lost_at: epoch, ..*state });
        let is_purged = is_purged(state, epoch, keep_epochs);
        match (owned.contains(spool), state.is_lost()) {
            (true, false) => {}
            (true, true) => changes.gained.push(scanning),
            (false, false) => changes.lost.push(marked),
            (false, true) if is_purged => changes.purged.push(*spool),
            (false, true) => {}
        }
    }
    for spool in owned {
        if !has_state.contains(spool) {
            changes.gained.push((*spool, SpoolState::new_scanning()));
        }
    }
    changes
}

/// The runs of consecutive virtual groups whose states still scan
pub fn get_groups_to_scan(
    states: &[(SpoolIndex, SpoolState)],
) -> Vec<Range<VirtualGroup>> {
    let mut runs: Vec<Range<VirtualGroup>> = Vec::new();
    for (spool, state) in states {
        if !state.is_scanning {
            continue;
        }
        let group = VirtualGroup::containing(*spool);
        let next = VirtualGroup(group.0 + 1);
        match runs.last_mut() {
            Some(run) if run.end == group => run.end = next,
            Some(_) | None => runs.push(group..next),
        }
    }
    runs
}

/// Where the next page starts: after the first scanning state's
/// scan_position, and from the top before its first page
pub fn get_scan_position(
    states: &[(SpoolIndex, SpoolState)],
) -> Option<(VirtualGroup, Address)> {
    for (spool, state) in states {
        if !state.is_scanning {
            continue;
        }
        if state.scan_position == Address::default() {
            return None;
        }
        let group = VirtualGroup::containing(*spool);
        return Some((group, state.scan_position));
    }
    None
}

/// The states a page moves on: a short page ends every scan; a full one
/// ends the scans before its last track's group and moves that group's
/// scan_position to the last track read
pub fn get_states_after_page(
    states: &[(SpoolIndex, SpoolState)],
    page: &[(VirtualGroup, Address)],
    page_size: usize,
) -> Vec<(SpoolIndex, SpoolState)> {
    let last = match page.last() {
        Some(last) if page.len() >= page_size => Some(*last),
        Some(_) | None => None,
    };
    let mut moved = Vec::new();
    for (spool, state) in states {
        if !state.is_scanning {
            continue;
        }
        if let Some(state) = get_state_after_page(*spool, *state, last) {
            moved.push((*spool, state));
        }
    }
    moved
}

/// Ready to attest: the node holds a state for every stored spool the live
/// spool carries, none scans or is lost, and no handover missing track is
/// left
pub fn is_ready_to_attest(
    states: &[(SpoolIndex, SpoolState)],
    carried: u64,
    has_handover_tracks: bool,
) -> bool {
    if states.len() as u64 != carried || has_handover_tracks {
        return false;
    }
    for (_, state) in states {
        if state.is_scanning || state.is_lost() {
            return false;
        }
    }
    true
}

fn is_purged(state: &SpoolState, epoch: EpochNumber, keep_epochs: u64) -> bool {
    epoch.saturating_sub(state.lost_at).as_u64() >= keep_epochs
}

fn get_state_after_page(
    spool: SpoolIndex,
    state: SpoolState,
    last: Option<(VirtualGroup, Address)>,
) -> Option<SpoolState> {
    let group = VirtualGroup::containing(spool);
    let Some((last_group, last_track)) = last else {
        return Some(SpoolState { is_scanning: false, ..state });
    };
    if group < last_group {
        return Some(SpoolState { is_scanning: false, ..state });
    }
    if group == last_group {
        return Some(SpoolState { scan_position: last_track, ..state });
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    fn address(byte: u8) -> Address {
        Address::new([byte; 32])
    }

    fn get_spool(change: &(SpoolIndex, SpoolState)) -> SpoolIndex {
        change.0
    }

    // a spool state comes back from its 41 bytes as it went in
    #[test]
    fn pack_roundtrip() {
        let lost = SpoolState {
            is_scanning: false,
            scan_position: Address::new([7u8; 32]),
            lost_at: EpochNumber(42),
        };

        for state in [SpoolState::new_scanning(), lost] {
            assert_eq!(SpoolState::unpack(state.pack()), state);
        }
        assert!(!SpoolState::new_scanning().is_lost());
        assert!(lost.is_lost());
    }

    // a spool gained starts scanning, one lost is marked at the epoch, and
    // one lost the kept epochs ago is purged
    #[test]
    fn epoch_changes() {
        let spool = SpoolIndex;
        let scanning = SpoolState::new_scanning();
        let lost = |at: u64| SpoolState { lost_at: EpochNumber(at), ..scanning };
        let done = SpoolState { is_scanning: false, ..scanning };
        let states = vec![
            (spool(1), done),
            (spool(2), lost(3)),
            (spool(3), done),
            (spool(4), lost(6)),
            (spool(5), lost(7)),
        ];
        let owned: HashSet<SpoolIndex> = [spool(1), spool(2), spool(6)].into();

        let mut changes = get_spool_changes(&owned, &states, EpochNumber(10), 4);

        changes.gained.sort_by_key(get_spool);
        let marked = SpoolState { lost_at: EpochNumber(10), ..done };
        assert_eq!(changes.gained, vec![(spool(2), scanning), (spool(6), scanning)]);
        assert_eq!(changes.lost, vec![(spool(3), marked)]);
        assert_eq!(changes.purged, vec![spool(4)]);
    }

    // scanning states of consecutive groups make one run
    #[test]
    fn runs_of_groups() {
        let scanning = SpoolState::new_scanning();
        let done = SpoolState { is_scanning: false, ..scanning };
        let states = [
            (VirtualGroup(0).spool_at(2), scanning),
            (VirtualGroup(1).spool_at(2), scanning),
            (VirtualGroup(2).spool_at(2), done),
            (VirtualGroup(5).spool_at(2), scanning),
        ];

        let runs = get_groups_to_scan(&states);

        let first = VirtualGroup(0)..VirtualGroup(2);
        let second = VirtualGroup(5)..VirtualGroup(6);
        assert_eq!(runs, vec![first, second]);
    }

    // a full page moves the scan_position of its last track's group and ends
    // the scans before it; a short page ends every scan
    #[test]
    fn page_moves_states() {
        let scanning = SpoolState::new_scanning();
        let near = VirtualGroup(0).spool_at(1);
        let far = VirtualGroup(3).spool_at(1);
        let states = [(near, scanning), (far, scanning)];
        let page = [(VirtualGroup(0), address(1)), (VirtualGroup(3), address(2))];

        let full = get_states_after_page(&states, &page, 2);
        let short = get_states_after_page(&states, &page, 3);

        let done = SpoolState { is_scanning: false, ..scanning };
        let moved = SpoolState { scan_position: address(2), ..scanning };
        assert_eq!(full, vec![(near, done), (far, moved)]);
        assert_eq!(short, vec![(near, done), (far, done)]);
        assert_eq!(get_scan_position(&full), Some((VirtualGroup(3), address(2))));
    }

    // the node attests once every carried state is done and no handover
    // missing track is left
    #[test]
    fn attest_rules() {
        let done = SpoolState { is_scanning: false, ..SpoolState::new_scanning() };
        let states = [(SpoolIndex(1), done), (SpoolIndex(21), done)];

        assert!(is_ready_to_attest(&states, 2, false));
        assert!(!is_ready_to_attest(&states, 2, true));
        assert!(!is_ready_to_attest(&states, 3, false));
        let scanning = [(SpoolIndex(1), SpoolState::new_scanning())];
        assert!(!is_ready_to_attest(&scanning, 1, false));
    }
}
