//! Spools change owners when the epoch advances. The spool manager records
//! the new owners, starts work on the spools we gained, and marks the spools
//! we lost. We keep a lost spool's slices for 4 more epochs, so that its new
//! owner can copy them from us.
//!
//! ```text
//! record who owns each spool in the new epoch
//! for each spool we gained:
//!   start scanning it
//! for each spool we lost:
//!   mark it as lost, and keep its slices for 4 more epochs
//! for each spool we lost 4 epochs ago:
//!   delete its slices and its missing_list
//! forget the scores of peers that we won't ask anymore
//! ```

use std::collections::HashSet;

use tape_core::erasure::MAX_GROUP_SIZE;
use tape_core::spooler::MAX_LIVE_GROUPS;
use tape_core::types::{EpochNumber, SpoolIndex};
use tape_crypto::address::Address;
use tape_protocol::{Api, ProtocolState};
use tape_recover::limits::SpoolLimits;
use tape_recover::peer::PeerScores;
use tape_recover::state::{get_spool_changes, SpoolChanges};
use tape_rpc::Rpc;
use tape_store::ops::{SliceOps, SpoolOps, STORED_SPOOL_COUNT};
use tape_store::TapeStore;
use tape_store_api::Store;
use tracing::info;

use crate::context::NodeContext;
use crate::features::spool::error::SpoolError;
use crate::features::spool::history::record_epoch_owners;

/// Record this epoch's owners and forget the epochs past the kept ones,
/// change the spool_states of the spools gained and lost, purge those lost
/// the kept epochs ago, and forget the peers no longer around
pub fn start_new_epoch<Db: Store, Cluster: Api, Blockchain: Rpc>(
    context: &NodeContext<Db, Cluster, Blockchain>,
    state: &ProtocolState,
    peer_scores: &mut PeerScores,
    limits: &SpoolLimits,
) -> Result<(), SpoolError> {
    let store = &context.store;
    let profile = context.params().profile;
    let epoch = state.epoch();
    record_epoch_owners(store, &state.current, profile)?;
    let oldest = epoch.saturating_sub(EpochNumber(limits.keep_epochs));
    store.delete_owners_before(oldest)?;

    let owned = get_owned_spools(state, context.node_address());
    let states = store.get_spool_states(None, STORED_SPOOL_COUNT)?;
    let keep = limits.keep_epochs;
    let changes = get_spool_changes(&owned, &states, epoch, keep);
    write_spool_changes(store, &changes, limits)?;

    let kept = get_kept_peers(store, state, limits)?;
    peer_scores.forget_peers_except(&kept);

    let written = changes.gained.len() + changes.lost.len();
    let purged = changes.purged.len();
    info!(%epoch, written, purged, "spool: new epoch");
    Ok(())
}

/// The live spools a node owns this epoch
pub fn get_owned_live_spools(
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

/// The stored spools a node owns this epoch
fn get_owned_spools(
    state: &ProtocolState,
    me: Address,
) -> HashSet<SpoolIndex> {
    if state.find_member(me).is_none() {
        return HashSet::new();
    }
    state.member_spools(me).into_iter().collect()
}

/// Write the states of the spools gained and lost, and purge the spools lost
/// the kept epochs ago, a page of their missing_list at a time
fn write_spool_changes<Db: Store>(
    store: &TapeStore<Db>,
    changes: &SpoolChanges,
    limits: &SpoolLimits,
) -> Result<(), SpoolError> {
    let written = [changes.gained.as_slice(), changes.lost.as_slice()];
    store.put_spool_states(&written.concat())?;
    for spool in &changes.purged {
        purge_spool(store, *spool, limits.scan_page)?;
    }
    Ok(())
}

/// Delete a stored spool's slices and its missing_list, a page at a time,
/// then its state
fn purge_spool<Db: Store>(
    store: &TapeStore<Db>,
    spool: SpoolIndex,
    page: usize,
) -> Result<(), SpoolError> {
    store.delete_all_slices_for_spool(spool)?;
    while store.delete_missing_tracks(spool, page)? > 0 {}
    Ok(store.delete_spool_state(spool)?)
}

/// The peers whose peer_score stays: the committees and the owner_history
fn get_kept_peers<Db: Store>(
    store: &TapeStore<Db>,
    state: &ProtocolState,
    limits: &SpoolLimits,
) -> Result<HashSet<Address>, SpoolError> {
    let limit = get_history_limit(limits.keep_epochs);
    let owners = store.get_recorded_owners(limit)?;
    let mut kept: HashSet<Address> = owners.into_iter().collect();
    for node in get_committee_nodes(state) {
        kept.insert(node);
    }
    Ok(kept)
}

/// The most owner_history records the kept epochs can hold
fn get_history_limit(keep_epochs: u64) -> usize {
    let epochs = keep_epochs.saturating_add(1) as usize;
    let spools = MAX_LIVE_GROUPS as usize * MAX_GROUP_SIZE;
    epochs.saturating_mul(spools)
}

/// Every node of the previous, current and next committees
fn get_committee_nodes(state: &ProtocolState) -> Vec<Address> {
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
