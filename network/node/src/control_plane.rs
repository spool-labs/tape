//! Control plane cache - in-memory cache of on-chain state.
//!
//! The control plane maintains a synchronized view of on-chain state
//! that is updated by Thread A (live updates) as blocks are processed.

use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::RwLock;
use tokio::sync::Notify;

use tape_api::fsm::{NodeAction, NodeStateMachine};
use tape_api::state::{Epoch, Node, System};
use tape_core::bft::is_supermajority;
use tape_core::erasure::SPOOL_COUNT;
use tape_core::prelude::*;
use tape_core::spooler::SpoolIndex;
use tape_store::types::NodeStatus;

/// In-memory cache of on-chain control plane state.
///
/// Protected by RwLock for concurrent read access with occasional writes.
/// Thread A updates this cache as it processes blocks.
/// Other threads read from it to make decisions.
pub struct ControlPlane {
    inner: RwLock<ControlPlaneInner>,
    /// Block processor pause flag (atomic for lock-free hot-path check).
    paused: AtomicBool,
    /// Notifies the block processor to resume after a pause.
    resume_notify: Notify,
    /// Notifies the requester that the block processor has acknowledged the pause.
    pause_ack: Notify,
}

/// Tracks sync progress for an epoch transition.
///
/// Monitors which nodes have submitted SyncEpoch transactions and their
/// cumulative spool weight. When supermajority (2/3+) of spools are
/// covered by synced nodes, we consider the epoch ready for activation.
#[derive(Clone, Debug)]
pub struct EpochSyncTracker {
    /// The epoch being tracked.
    pub epoch: EpochNumber,
    /// Set of nodes that have synced for this epoch.
    synced_nodes: HashSet<NodeId>,
    /// Cumulative spool weight from synced nodes.
    cumulative_weight: u64,
    /// Total weight (SPOOL_COUNT).
    total_weight: u64,
}

impl EpochSyncTracker {
    /// Create a new tracker for an epoch.
    pub fn new(epoch: EpochNumber) -> Self {
        Self {
            epoch,
            synced_nodes: HashSet::new(),
            cumulative_weight: 0,
            total_weight: SPOOL_COUNT as u64,
        }
    }

    /// Record that a node has synced, returning true if quorum was just reached.
    ///
    /// If the node was already recorded, this is a no-op and returns false.
    pub fn record_sync(&mut self, node_id: NodeId, spool_count: u64) -> bool {
        if self.synced_nodes.insert(node_id) {
            let was_quorum = self.is_quorum_reached();
            self.cumulative_weight += spool_count;
            // Return true only if we just crossed the quorum threshold
            !was_quorum && self.is_quorum_reached()
        } else {
            false
        }
    }

    /// Check if supermajority of spools are covered by synced nodes.
    pub fn is_quorum_reached(&self) -> bool {
        is_supermajority(self.cumulative_weight, self.total_weight)
    }

    /// Get the number of synced nodes.
    pub fn synced_count(&self) -> usize {
        self.synced_nodes.len()
    }

    /// Get the current cumulative weight.
    pub fn cumulative_weight(&self) -> u64 {
        self.cumulative_weight
    }
}

struct ControlPlaneInner {
    /// System account state (committees, spool assignments).
    system: System,
    /// Current epoch account state (from event processing).
    epoch: Epoch,
    /// This node's on-chain state.
    node: Node,
    /// Last processed Solana slot.
    last_processed_slot: SlotNumber,
    /// Cached list of spools we own (derived from system state).
    our_spools: Vec<SpoolIndex>,
    /// Whether we're in the current committee.
    in_committee: bool,
    /// Tracks sync progress for the current epoch transition.
    sync_tracker: Option<EpochSyncTracker>,
    /// Epoch for which we've completed local sync (our own spools).
    local_sync_complete: Option<EpochNumber>,
    /// Latest epoch known from the chain (from RPC).
    /// Used to detect catch-up mode: if epoch.id < chain_epoch, we're catching up.
    chain_epoch: EpochNumber,
    /// Recovery lifecycle status (persisted via MetaOps on every transition).
    node_status: NodeStatus,
}

impl ControlPlane {
    /// Create a new control plane cache with initial state.
    ///
    /// Initially assumes we're caught up (chain_epoch = epoch.id).
    /// Call `set_chain_epoch` after fetching from RPC to update.
    /// `node_status` should be loaded from `MetaOps::get_node_status()` on startup.
    pub fn new(system: System, epoch: Epoch, node: Node, node_status: NodeStatus) -> Self {
        let (our_spools, in_committee) = compute_our_spools(&system, &node);
        let chain_epoch = epoch.id;

        Self {
            inner: RwLock::new(ControlPlaneInner {
                system,
                epoch,
                node,
                last_processed_slot: SlotNumber(0),
                our_spools,
                in_committee,
                sync_tracker: None,
                local_sync_complete: None,
                chain_epoch,
                node_status,
            }),
            paused: AtomicBool::new(false),
            resume_notify: Notify::new(),
            pause_ack: Notify::new(),
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

    /// Get the current recovery lifecycle status.
    pub fn get_node_status(&self) -> NodeStatus {
        self.inner.read().unwrap().node_status.clone()
    }

    /// Update the recovery lifecycle status.
    pub fn set_node_status(&self, status: NodeStatus) {
        self.inner.write().unwrap().node_status = status;
    }

    /// Check if the node is in a replay state (RecoveryReplay or PartialReplay).
    pub fn is_replaying(&self) -> bool {
        let inner = self.inner.read().unwrap();
        matches!(
            inner.node_status,
            NodeStatus::RecoveryReplay | NodeStatus::PartialReplay { .. }
        )
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
    // Block processor pause/resume (for snapshot bootstrap)
    // -------------------------------------------------------------------------

    /// Request the block processor to pause. Waits for acknowledgment.
    pub async fn request_block_processor_pause(&self) {
        self.paused.store(true, Ordering::Release);
        self.pause_ack.notified().await;
    }

    /// Resume the block processor after a pause.
    pub fn resume_block_processor(&self) {
        self.paused.store(false, Ordering::Release);
        self.resume_notify.notify_one();
    }

    /// Check if the block processor is paused (lock-free).
    pub fn is_paused(&self) -> bool {
        self.paused.load(Ordering::Acquire)
    }

    /// Acknowledge a pause request and wait for resume.
    /// Called by the block processor when it detects is_paused().
    pub async fn wait_for_resume(&self) {
        self.pause_ack.notify_one();
        self.resume_notify.notified().await;
    }

    // -------------------------------------------------------------------------
    // Epoch sync quorum tracking
    // -------------------------------------------------------------------------

    /// Start tracking sync progress for a new epoch.
    ///
    /// Called when an epoch advances and we need to track node syncs.
    /// Replaces any existing tracker.
    pub fn start_epoch_sync(&self, epoch: EpochNumber) {
        let mut inner = self.inner.write().unwrap();
        inner.sync_tracker = Some(EpochSyncTracker::new(epoch));
        inner.local_sync_complete = None;
    }

    /// Record that a node has synced for the current epoch.
    ///
    /// Returns true if this sync caused the quorum threshold to be reached.
    /// Returns false if already recorded, wrong epoch, or quorum already reached.
    pub fn record_node_sync(&self, epoch: EpochNumber, node_id: NodeId, spool_count: u64) -> bool {
        let mut inner = self.inner.write().unwrap();
        if let Some(ref mut tracker) = inner.sync_tracker {
            if tracker.epoch == epoch {
                return tracker.record_sync(node_id, spool_count);
            }
        }
        false
    }

    /// Check if sync quorum has been reached for the current epoch.
    pub fn is_sync_quorum_reached(&self) -> bool {
        let inner = self.inner.read().unwrap();
        inner
            .sync_tracker
            .as_ref()
            .map(|t| t.is_quorum_reached())
            .unwrap_or(false)
    }

    /// Get the current sync tracker state (for logging/debugging).
    pub fn get_sync_tracker(&self) -> Option<EpochSyncTracker> {
        self.inner.read().unwrap().sync_tracker.clone()
    }

    /// Mark that we've completed our local sync for an epoch.
    ///
    /// This means we've synced all spools we're assigned to from previous owners.
    pub fn mark_local_sync_complete(&self, epoch: EpochNumber) {
        let mut inner = self.inner.write().unwrap();
        inner.local_sync_complete = Some(epoch);
    }

    /// Check if we've completed our local sync for a given epoch.
    pub fn is_local_sync_complete(&self, epoch: EpochNumber) -> bool {
        let inner = self.inner.read().unwrap();
        inner.local_sync_complete == Some(epoch)
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

    // -------------------------------------------------------------------------
    // Catch-up state tracking
    // -------------------------------------------------------------------------

    /// Check if we're in catch-up mode (processing historical blocks).
    ///
    /// Returns true if our locally-processed epoch is behind the chain's
    /// current epoch. In catch-up mode, nodes should skip submitting
    /// transactions (SyncEpoch, AdvancePool, etc.) since those epochs
    /// have already passed.
    pub fn is_catching_up(&self) -> bool {
        let inner = self.inner.read().unwrap();
        inner.epoch.id < inner.chain_epoch
    }

    /// Check if we're caught up with the chain.
    ///
    /// Returns true if our locally-processed epoch matches the chain's
    /// current epoch. In real-time mode, the node should participate
    /// in epoch sync and other consensus activities.
    pub fn is_caught_up(&self) -> bool {
        let inner = self.inner.read().unwrap();
        inner.epoch.id >= inner.chain_epoch
    }

    /// Get the chain's current epoch (latest known from RPC).
    pub fn chain_epoch(&self) -> EpochNumber {
        self.inner.read().unwrap().chain_epoch
    }

    /// Update the chain epoch (called after fetching from RPC).
    ///
    /// This should be called periodically or when fetching epoch state
    /// from the chain to keep catch-up detection accurate.
    pub fn set_chain_epoch(&self, epoch: EpochNumber) {
        let mut inner = self.inner.write().unwrap();
        inner.chain_epoch = epoch;
    }

    /// Check if a specific epoch is stale (behind the chain).
    ///
    /// Returns true if the epoch has already passed on-chain. Event handlers
    /// should skip submissions for stale epochs since those consensus
    /// activities have already completed.
    pub fn is_stale_epoch(&self, epoch: EpochNumber) -> bool {
        let inner = self.inner.read().unwrap();
        epoch < inner.chain_epoch
    }

    // -------------------------------------------------------------------------
    // FSM integration
    // -------------------------------------------------------------------------

    /// Determine what action this node should take based on cached on-chain state.
    ///
    /// Returns the FSM action along with whether we're catching up (stale).
    /// If catching_up is true, the action should be logged but NOT executed.
    pub fn determine_action(&self, current_time: i64) -> (NodeAction, bool) {
        let inner = self.inner.read().unwrap();
        let action = NodeStateMachine::determine_action(
            &inner.system,
            &inner.epoch,
            &inner.node,
            current_time,
        );
        let catching_up = inner.epoch.id < inner.chain_epoch;
        (action, catching_up)
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
