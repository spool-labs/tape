//! A new epoch: the owner_history and the spool rows
//!
//! Each new epoch adds its owners to the owner_history and forgets the
//! epochs past the kept ones. A spool gained gets a fresh row, a spool lost
//! is locked at the epoch, and one lost the kept epochs ago is purged. The
//! missing_lists and the running fetches carry on.

use std::collections::HashSet;

use tape_core::encoding::ErasureCode;
use tape_core::erasure::MAX_GROUP_SIZE;
use tape_core::spooler::MAX_LIVE_GROUPS;
use tape_core::system::SpoolState;
use tape_core::types::{EpochNumber, SpoolIndex};
use tape_crypto::address::Address;
use tape_protocol::{EpochBundle, ProtocolState};
use tape_store::ops::{SpoolOps, STORED_SPOOL_COUNT};
use tape_store::TapeStore;
use tape_store_api::Store;

use crate::features::spool::error::SpoolError;
use crate::features::spool::fill::StoredRow;
use crate::features::store::cleanup::purge_spool_local;

/// What an epoch turn wrote to the spool rows, and the spools it purged
#[derive(Debug, Default, Eq, PartialEq)]
pub struct RowChanges {
    pub written: Vec<StoredRow>,
    pub purged: Vec<SpoolIndex>,
}

/// Add this epoch's owners to the owner_history, and last epoch's on the
/// first epoch seen, then forget the epochs past the kept ones
pub fn record_history<Db: Store>(
    store: &TapeStore<Db>,
    state: &ProtocolState,
    profile: ErasureCode,
    keep_epochs: u64,
    is_first: bool,
) -> Result<(), SpoolError> {
    let width = usize::from(profile.n());
    record_owners(store, &state.current, width)?;
    if is_first {
        if let Some(previous) = &state.previous {
            record_owners(store, previous, width)?;
        }
    }
    let oldest = state.epoch().saturating_sub(EpochNumber(keep_epochs));
    store.forget_owners_before(oldest).map_err(store_error)
}

/// Write the rows of the spools gained and lost, and purge the spools lost
/// the kept epochs ago, a page of their missing_list at a time
pub fn change_rows<Db: Store>(
    store: &TapeStore<Db>,
    state: &ProtocolState,
    me: Address,
    keep_epochs: u64,
    page: usize,
) -> Result<RowChanges, SpoolError> {
    let owned = owned_spools(state, me);
    let rows = store.spool_states(None, STORED_SPOOL_COUNT);
    let rows = rows.map_err(store_error)?;
    let changes = row_changes(&rows, &owned, state.epoch(), keep_epochs);
    store.put_spool_states(&changes.written).map_err(store_error)?;
    for spool in &changes.purged {
        purge_spool_local(store, *spool, page).map_err(store_error)?;
    }
    Ok(changes)
}

/// The peers whose peer_score stays: the committee and the owner_history
pub fn kept_peers<Db: Store>(
    store: &TapeStore<Db>,
    state: &ProtocolState,
    keep_epochs: u64,
) -> Result<HashSet<Address>, SpoolError> {
    let limit = history_limit(keep_epochs);
    let owners = store.history_owners(limit).map_err(store_error)?;
    let mut kept: HashSet<Address> = owners.into_iter().collect();
    for member in committee(state) {
        kept.insert(member);
    }
    Ok(kept)
}

/// The rows an epoch turn writes: a spool gained gets a fresh row, a spool
/// lost gets locked at this epoch, and one lost long enough ago is purged
pub fn row_changes(
    rows: &[StoredRow],
    owned: &HashSet<SpoolIndex>,
    epoch: EpochNumber,
    keep_epochs: u64,
) -> RowChanges {
    let mut changes = RowChanges::default();
    let mut has_row = HashSet::with_capacity(rows.len());
    for (spool, row) in rows {
        has_row.insert(*spool);
        let is_owned = owned.contains(spool);
        match row_change(*row, is_owned, epoch, keep_epochs) {
            Some(RowChange::Write(row)) => changes.written.push((*spool, row)),
            Some(RowChange::Purge) => changes.purged.push(*spool),
            None => {}
        }
    }
    for spool in owned {
        if !has_row.contains(spool) {
            changes.written.push((*spool, SpoolState::gained()));
        }
    }
    changes
}

/// What an epoch turn does to one existing row
enum RowChange {
    Write(SpoolState),
    Purge,
}

/// A spool that came back gets a fresh row, one lost now is locked, and
/// one lost the kept epochs ago is purged
fn row_change(
    row: SpoolState,
    is_owned: bool,
    epoch: EpochNumber,
    keep_epochs: u64,
) -> Option<RowChange> {
    let lost_for = epoch.saturating_sub(row.locked_at).as_u64();
    match (is_owned, row.is_locked()) {
        (true, false) => None,
        (true, true) => Some(RowChange::Write(SpoolState::gained())),
        (false, false) => {
            let locked = SpoolState { locked_at: epoch, ..row };
            Some(RowChange::Write(locked))
        }
        (false, true) if lost_for >= keep_epochs => Some(RowChange::Purge),
        (false, true) => None,
    }
}

/// The live spools a node owns this epoch
pub fn owned_live_spools(
    state: &ProtocolState,
    me: Address,
) -> Vec<SpoolIndex> {
    if state.find_member(me).is_none() {
        return Vec::new();
    }
    let mut spools = state.member_live_spools(me);
    spools.sort_unstable();
    spools
}

/// Record one epoch's owners: every position of every live group, zero
/// where nobody holds it
fn record_owners<Db: Store>(
    store: &TapeStore<Db>,
    bundle: &EpochBundle,
    width: usize,
) -> Result<(), SpoolError> {
    let mut owners = Vec::with_capacity(bundle.groups.len() * width);
    for group in &bundle.groups {
        for position in 0..width.min(MAX_GROUP_SIZE) {
            let owner = group.spools[position].node;
            owners.push((group.id.spool_at(position), owner));
        }
    }
    let epoch = bundle.epoch.id;
    store.put_owners(epoch, &owners).map_err(store_error)
}

/// The stored spools a node owns this epoch
fn owned_spools(state: &ProtocolState, me: Address) -> HashSet<SpoolIndex> {
    if state.find_member(me).is_none() {
        return HashSet::new();
    }
    state.member_spools(me).into_iter().collect()
}

/// Every node of the previous, current and next committees
fn committee(state: &ProtocolState) -> Vec<Address> {
    let mut nodes = Vec::new();
    for member in &state.current.committee {
        nodes.push(member.node);
    }
    if let Some(previous) = &state.previous {
        for member in &previous.committee {
            nodes.push(member.node);
        }
    }
    for member in state.next_committee.iter().flatten() {
        nodes.push(member.node);
    }
    nodes
}

/// The most owner_history records the kept epochs can hold
fn history_limit(keep_epochs: u64) -> usize {
    let epochs = keep_epochs.saturating_add(1) as usize;
    let spools = MAX_LIVE_GROUPS as usize * MAX_GROUP_SIZE;
    epochs.saturating_mul(spools)
}

fn store_error(error: impl std::fmt::Display) -> SpoolError {
    SpoolError::Store(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    // a spool gained gets a fresh row, one lost is locked at the epoch, and
    // one lost the kept epochs ago is purged
    #[test]
    fn epoch_rows() {
        let spool = SpoolIndex;
        let gained = SpoolState::gained();
        let locked = |at: u64| SpoolState {
            locked_at: EpochNumber(at),
            ..gained
        };
        let done = SpoolState { is_filling: false, ..gained };
        let rows = vec![
            (spool(1), done),
            (spool(2), locked(3)),
            (spool(3), done),
            (spool(4), locked(6)),
            (spool(5), locked(7)),
        ];
        let owned: HashSet<SpoolIndex> = [spool(1), spool(2), spool(6)].into();

        let changes = row_changes(&rows, &owned, EpochNumber(10), 4);

        let mut written = changes.written;
        let by_spool = |row: &StoredRow| row.0;
        written.sort_by_key(by_spool);
        let relocked = SpoolState { locked_at: EpochNumber(10), ..done };
        let expected = vec![
            (spool(2), gained),
            (spool(3), relocked),
            (spool(6), gained),
        ];
        assert_eq!(written, expected);
        assert_eq!(changes.purged, vec![spool(4)]);
    }
}
