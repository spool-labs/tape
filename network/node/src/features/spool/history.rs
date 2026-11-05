//! The owner_history is our record of who owned each spool in each of the
//! last few epochs. We need it because a node keeps a spool's slices for 4
//! epochs after it loses the spool, so the past owners are the first nodes we
//! ask for our slice.
//!
//! ```text
//! in each epoch, write down the owners of every live group
//! to find who owned a spool in an epoch:
//!   read its group's record, or the record of the group that carried it
//! ```

use tape_core::encoding::ErasureCode;
use tape_core::erasure::MAX_GROUP_SIZE;
use tape_core::spooler::{GroupFold, VirtualGroup};
use tape_core::types::EpochNumber;
use tape_crypto::address::Address;
use tape_protocol::EpochBundle;
use tape_recover::history::{EpochOwners, OwnerHistory};
use tape_recover::limits::SpoolLimits;
use tape_store::ops::SpoolOps;
use tape_store::TapeStore;
use tape_store_api::Store;

use crate::features::spool::error::SpoolError;

/// Record one epoch's owners: every position of every live group, zero
/// where nobody holds it
pub fn record_epoch_owners<Db: Store>(
    store: &TapeStore<Db>,
    bundle: &EpochBundle,
    profile: ErasureCode,
) -> Result<(), SpoolError> {
    let width = usize::from(profile.n()).min(MAX_GROUP_SIZE);
    let mut owners = Vec::with_capacity(bundle.groups.len() * width);
    for group in &bundle.groups {
        for position in 0..width {
            let owner = group.spools[position].node;
            owners.push((group.id.spool_at(position), owner));
        }
    }
    Ok(store.put_owners(bundle.epoch.id, &owners)?)
}

/// Read a virtual group's owners from now back over the kept epochs
pub fn load_owner_history<Db: Store>(
    store: &TapeStore<Db>,
    now: EpochNumber,
    group: VirtualGroup,
    profile: ErasureCode,
    limits: &SpoolLimits,
) -> Result<OwnerHistory, SpoolError> {
    let mut epochs = Vec::new();
    for back in 0..=limits.keep_epochs.min(now.as_u64()) {
        let epoch = EpochNumber(now.as_u64() - back);
        let read = get_carrier_owners(store, epoch, group, profile)?;
        if let Some(owners) = read {
            epochs.push(EpochOwners { epoch, owners });
        }
    }
    Ok(OwnerHistory { now, epochs })
}

/// The owners of the live group that carried `group` in `epoch`
///
/// An epoch's records cover every position of every group live in it. The
/// carrier is the first of `group` and its ancestors with a record.
fn get_carrier_owners<Db: Store>(
    store: &TapeStore<Db>,
    epoch: EpochNumber,
    group: VirtualGroup,
    profile: ErasureCode,
) -> Result<Option<Vec<Address>>, SpoolError> {
    let mut carrier = group;
    loop {
        let owners = store.get_owners(epoch, carrier, profile)?;
        if owners.is_some() {
            return Ok(owners);
        }
        let Some(parent) = get_parent_group(carrier, group) else {
            return Ok(None);
        };
        carrier = parent;
    }
}

/// The ancestor below `carrier` that carries `group`: under a fold of
/// `carrier` groups, the live group `group` folds to
fn get_parent_group(
    carrier: VirtualGroup,
    group: VirtualGroup,
) -> Option<VirtualGroup> {
    let fold = GroupFold::new(carrier.0)?;
    fold.virtual_group(fold.live_group(group))
}

#[cfg(test)]
mod tests {
    use tape_core::encoding::RSParams;
    use tape_core::spooler::GroupIndex;
    use tape_store_memory::MemoryStore;

    use super::*;

    /// Four spools a group, so four owners a record
    const FOUR_WIDE: ErasureCode = ErasureCode::ReedSolomon(RSParams::new(3, 1));

    fn address(byte: u8) -> Address {
        Address::new([byte; 32])
    }

    // a group's owners at an epoch come from the live group that carried it
    // then, and an epoch the history lacks has none
    #[test]
    fn owners_by_carrier() {
        let store = TapeStore::new(MemoryStore::new());
        let width = usize::from(FOUR_WIDE.n());
        let mut owners = Vec::new();
        for group in 0..2u64 {
            for position in 0..width {
                let owner = address((group * 10 + position as u64 + 1) as u8);
                owners.push((GroupIndex(group).spool_at(position), owner));
            }
        }
        store.put_owners(EpochNumber(8), &owners[..width]).expect("one");
        store.put_owners(EpochNumber(9), &owners).expect("two groups");
        let limits = SpoolLimits::default();

        let history = load_owner_history(
            &store,
            EpochNumber(9),
            VirtualGroup(7),
            FOUR_WIDE,
            &limits,
        );

        let history = history.expect("history");
        assert_eq!(history.epochs.len(), 2);
        assert_eq!(history.epochs[0].owners[0], address(11));
        assert_eq!(history.epochs[1].owners[0], address(1));
    }
}
