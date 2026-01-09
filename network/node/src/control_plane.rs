//! Control plane cache - in-memory cache of on-chain state.
//!
//! The control plane maintains a synchronized view of on-chain state
//! that is updated by Thread A (live updates) as blocks are processed.

use std::sync::RwLock;

use tape_api::state::{Epoch, Node, System};
use tape_core::prelude::*;
use tape_core::spooler::SpoolIndex;

/// In-memory cache of on-chain control plane state.
///
/// Protected by RwLock for concurrent read access with occasional writes.
/// Thread A updates this cache as it processes blocks.
/// Other threads read from it to make decisions.
pub struct ControlPlane {
    inner: RwLock<ControlPlaneInner>,
}

struct ControlPlaneInner {
    /// System account state (committees, spool assignments).
    system: System,
    /// Current epoch account state.
    epoch: Epoch,
    /// This node's on-chain state.
    node: Node,
    /// Last processed Solana slot.
    last_processed_slot: SlotNumber,
    /// Cached list of spools we own (derived from system state).
    our_spools: Vec<SpoolIndex>,
    /// Whether we're in the current committee.
    in_committee: bool,
}

impl ControlPlane {
    /// Create a new control plane cache with initial state.
    pub fn new(system: System, epoch: Epoch, node: Node) -> Self {
        let (our_spools, in_committee) = compute_our_spools(&system, &node);

        Self {
            inner: RwLock::new(ControlPlaneInner {
                system,
                epoch,
                node,
                last_processed_slot: SlotNumber(0),
                our_spools,
                in_committee,
            }),
        }
    }

    // -------------------------------------------------------------------------
    // Getters (read-only access)
    // -------------------------------------------------------------------------

    /// Get a clone of the current system state.
    pub fn get_system(&self) -> System {
        self.inner.read().unwrap().system.clone()
    }

    /// Get a clone of the current epoch state.
    pub fn get_epoch(&self) -> Epoch {
        self.inner.read().unwrap().epoch.clone()
    }

    /// Get a clone of this node's state.
    pub fn get_node(&self) -> Node {
        self.inner.read().unwrap().node.clone()
    }

    /// Get the last processed Solana slot.
    pub fn get_last_processed_slot(&self) -> SlotNumber {
        self.inner.read().unwrap().last_processed_slot
    }

    /// Get the list of spools this node owns.
    pub fn get_our_spools(&self) -> Vec<SpoolIndex> {
        self.inner.read().unwrap().our_spools.clone()
    }

    /// Check if this node is in the current committee.
    pub fn is_in_committee(&self) -> bool {
        self.inner.read().unwrap().in_committee
    }

    /// Get the current epoch number.
    pub fn current_epoch(&self) -> EpochNumber {
        self.inner.read().unwrap().epoch.id
    }

    /// Get this node's ID.
    pub fn our_node_id(&self) -> NodeId {
        self.inner.read().unwrap().node.id
    }

    // -------------------------------------------------------------------------
    // Setters (called by Thread A when processing blocks)
    // -------------------------------------------------------------------------

    /// Update the system state and recompute spool assignments.
    pub fn update_system(&self, system: System) {
        let mut inner = self.inner.write().unwrap();
        let (our_spools, in_committee) = compute_our_spools(&system, &inner.node);
        inner.system = system;
        inner.our_spools = our_spools;
        inner.in_committee = in_committee;
    }

    /// Update the epoch state.
    pub fn update_epoch(&self, epoch: Epoch) {
        let mut inner = self.inner.write().unwrap();
        inner.epoch = epoch;
    }

    /// Update this node's state.
    pub fn update_node(&self, node: Node) {
        let mut inner = self.inner.write().unwrap();
        // Recompute spools in case node ID changed (shouldn't happen, but be safe)
        let (our_spools, in_committee) = compute_our_spools(&inner.system, &node);
        inner.node = node;
        inner.our_spools = our_spools;
        inner.in_committee = in_committee;
    }

    /// Update the last processed slot.
    pub fn set_last_processed_slot(&self, slot: SlotNumber) {
        let mut inner = self.inner.write().unwrap();
        inner.last_processed_slot = slot;
    }

    /// Update just the epoch number (used during catch-up when we don't have
    /// full epoch account data, only the event log).
    pub fn set_current_epoch(&self, epoch: EpochNumber) {
        let mut inner = self.inner.write().unwrap();
        inner.epoch.id = epoch;
    }

    // -------------------------------------------------------------------------
    // Query helpers
    // -------------------------------------------------------------------------

    /// Check if a given spool is assigned to this node.
    pub fn owns_spool(&self, spool: SpoolIndex) -> bool {
        self.inner.read().unwrap().our_spools.contains(&spool)
    }

    /// Get the number of spools this node owns.
    pub fn spool_count(&self) -> usize {
        self.inner.read().unwrap().our_spools.len()
    }
}

/// Compute which spools this node owns based on system state.
fn compute_our_spools(system: &System, node: &Node) -> (Vec<SpoolIndex>, bool) {
    // Find our index in the current committee
    let our_node_id = node.id;

    match system.committee.index_of(&our_node_id) {
        Some(member_idx) => {
            let spools = system.spools.spools_for_member(member_idx);
            (spools, true)
        }
        None => {
            // Not in committee
            (Vec::new(), false)
        }
    }
}

#[cfg(test)]
mod tests {
    // Tests would require mock System/Epoch/Node structures
}
