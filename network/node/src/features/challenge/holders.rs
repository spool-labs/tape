//! Reads who holds which live spool from one epoch's assignment

use tape_core::spooler::GroupIndex;
use tape_core::types::{EpochNumber, SpoolIndex};
use tape_crypto::Address;
use tape_protocol::{EpochBundle, ProtocolState};

use crate::features::challenge::round::Batch;

/// Find the assignment of `epoch` in the state, current or previous
pub fn bundle_at(state: &ProtocolState, epoch: EpochNumber) -> Option<&EpochBundle> {
    if state.epoch() == epoch {
        return Some(&state.current);
    }

    state
        .previous
        .as_ref()
        .filter(|previous| previous.epoch.id == epoch)
}

/// Find the node that holds a live spool
pub fn holder(bundle: &EpochBundle, spool: SpoolIndex) -> Option<Address> {
    let group = GroupIndex::containing(spool);
    let position = group.position_of(spool)?;

    for candidate in &bundle.groups {
        if candidate.id == group {
            return assigned(candidate.spools[position].node);
        }
    }
    None
}

/// Whether `sender` holds every spool a batch carries a proof for, under the
/// current assignment or the previous one
///
/// Which of the two answers in the batch's round is checked when its proofs
/// are recorded. This only turns away a node that holds none of them.
pub fn answers_for(state: &ProtocolState, sender: Address, batch: &Batch) -> bool {
    for proof in &batch.proofs {
        let is_current = holder(&state.current, proof.spool) == Some(sender);
        let is_previous = state
            .previous
            .as_ref()
            .is_some_and(|previous| holder(previous, proof.spool) == Some(sender));

        if !is_current && !is_previous {
            return false;
        }
    }
    true
}

/// List the live spools a node holds
pub fn own_spools(bundle: &EpochBundle, node: Address) -> Vec<SpoolIndex> {
    let mut spools = Vec::new();
    for group in &bundle.groups {
        for (position, spool) in group.spools.iter().enumerate() {
            if spool.node == node {
                spools.push(group.id.spool_at(position));
            }
        }
    }
    spools
}

/// List the other nodes' live spools, with their holders, in every group the node holds a spool of
pub fn peer_spools(bundle: &EpochBundle, node: Address) -> Vec<(SpoolIndex, Address)> {
    let mut peers = Vec::new();
    for group in &bundle.groups {
        if !group.spools.iter().any(|spool| spool.node == node) {
            continue;
        }

        for (position, spool) in group.spools.iter().enumerate() {
            match assigned(spool.node) {
                Some(holder) if holder != node => {
                    peers.push((group.id.spool_at(position), holder));
                }
                Some(_) | None => {}
            }
        }
    }
    peers
}

/// A network with n spools a group leaves the positions past n unassigned
fn assigned(node: Address) -> Option<Address> {
    (node != Address::default()).then_some(node)
}
