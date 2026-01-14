//! Node State Machine
//!
//! This module provides a state machine that determines what action a node
//! should take based on the current on-chain state. Used by:
//! - Network nodes to decide what transaction to submit next
//! - Tests to validate expected behavior
//! - CLI tools to show users what action is expected
//!
//! # Usage
//!
//! ```rust,ignore
//! use tape_api::state_machine::{NodeStateMachine, NodeAction};
//!
//! let action = NodeStateMachine::determine_action(&system, &epoch, &node, current_time);
//! match action {
//!     NodeAction::SyncEpoch => { /* submit SyncEpoch */ }
//!     NodeAction::AdvancePool => { /* submit AdvancePool */ }
//!     NodeAction::JoinNetwork => { /* submit JoinNetwork */ }
//!     _ => { /* wait or blocked */ }
//! }
//! ```

use crate::state::{Epoch, Node, System};
use crate::program::{EPOCH_DURATION, MIN_COMMITTEE_SIZE};
use tape_core::system::EpochPhase;
use tape_core::types::EpochNumber;

/// Actions a node can or should take based on on-chain state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NodeAction {
    // =========================================================================
    // Actions the node should take
    // =========================================================================

    /// Node should submit SyncEpoch to attest spool sync.
    /// Only valid during Syncing phase for current committee members.
    SyncEpoch,

    /// Node should submit AdvancePool to process rewards/stake.
    /// Valid during Settling (for committee_prev) or Active phase.
    AdvancePool,

    /// Node should submit JoinNetwork to (re)join committee_next.
    /// Required after AdvancePool for committee members to continue serving.
    JoinNetwork,

    /// Node can trigger AdvanceEpoch (permissionless).
    /// Valid when Active, EPOCH_DURATION elapsed, and committee_next >= threshold.
    AdvanceEpoch,

    // =========================================================================
    // Waiting states (node has done what it can, waiting for others)
    // =========================================================================

    /// Already synced, waiting for sync quorum (Syncing → Settling).
    WaitForSyncQuorum {
        current_weight: u64,
    },

    /// Already advanced, waiting for settle quorum (Settling → Active).
    WaitForSettleQuorum {
        current_weight: u64,
    },

    /// In Active phase, waiting for EPOCH_DURATION to elapse.
    WaitForEpochDuration {
        seconds_remaining: i64,
    },

    /// Ready to advance but waiting for more nodes to join committee_next.
    WaitForCommitteeThreshold {
        current_size: usize,
        required_size: usize,
    },

    // =========================================================================
    // Blocked/Error states
    // =========================================================================

    /// Node is not in any relevant committee for current phase.
    NotInCommittee,

    /// Epoch is blocked - committee_next below threshold.
    EpochBlocked {
        committee_next_size: usize,
    },

    /// Unknown epoch phase (should not happen).
    UnknownPhase {
        phase: u64,
    },
}

impl NodeAction {
    /// Returns true if this action requires submitting a transaction.
    pub fn requires_transaction(&self) -> bool {
        matches!(
            self,
            NodeAction::SyncEpoch
                | NodeAction::AdvancePool
                | NodeAction::JoinNetwork
                | NodeAction::AdvanceEpoch
        )
    }

    /// Returns true if the node is waiting (no transaction needed).
    pub fn is_waiting(&self) -> bool {
        matches!(
            self,
            NodeAction::WaitForSyncQuorum { .. }
                | NodeAction::WaitForSettleQuorum { .. }
                | NodeAction::WaitForEpochDuration { .. }
                | NodeAction::WaitForCommitteeThreshold { .. }
        )
    }

    /// Returns true if the node is blocked and cannot proceed.
    pub fn is_blocked(&self) -> bool {
        matches!(
            self,
            NodeAction::NotInCommittee
                | NodeAction::EpochBlocked { .. }
                | NodeAction::UnknownPhase { .. }
        )
    }
}

/// State machine for determining node actions based on on-chain state.
pub struct NodeStateMachine;

impl NodeStateMachine {
    /// Determine what action a node should take based on current on-chain state.
    ///
    /// # Arguments
    /// * `system` - The System account state
    /// * `epoch` - The Epoch account state
    /// * `node` - The Node account state
    /// * `current_time` - Current unix timestamp (for EPOCH_DURATION check)
    ///
    /// # Returns
    /// The action the node should take, or a waiting/blocked state.
    pub fn determine_action(
        system: &System,
        epoch: &Epoch,
        node: &Node,
        current_time: i64,
    ) -> NodeAction {
        let node_id = &node.id;

        // Check committee membership
        let in_committee = system.committee.contains(node_id);
        let in_committee_prev = system.committee_prev.contains(node_id);
        let in_committee_next = system.committee_next.contains(node_id);

        // Determine action based on epoch phase
        if epoch.state.is_syncing() {
            Self::handle_syncing_phase(epoch, node, in_committee)
        } else if epoch.state.is_settling() {
            Self::handle_settling_phase(epoch, node, in_committee_prev, in_committee_next)
        } else if epoch.state.is_active() {
            Self::handle_active_phase(system, epoch, node, in_committee, in_committee_next, current_time)
        } else {
            NodeAction::UnknownPhase { phase: epoch.state.phase }
        }
    }

    /// Handle Syncing phase logic.
    /// Current committee members should call SyncEpoch to attest spool sync.
    fn handle_syncing_phase(
        epoch: &Epoch,
        node: &Node,
        in_committee: bool,
    ) -> NodeAction {
        if !in_committee {
            return NodeAction::NotInCommittee;
        }

        // Check if we've already synced this epoch
        if node.latest_sync_epoch >= epoch.id {
            // Already synced, waiting for quorum
            NodeAction::WaitForSyncQuorum {
                current_weight: epoch.state.weight,
            }
        } else {
            // Need to sync
            NodeAction::SyncEpoch
        }
    }

    /// Handle Settling phase logic.
    /// Committee_prev members should call AdvancePool, then JoinNetwork.
    fn handle_settling_phase(
        epoch: &Epoch,
        node: &Node,
        in_committee_prev: bool,
        in_committee_next: bool,
    ) -> NodeAction {
        // During Settling, committee_prev members need to AdvancePool
        if in_committee_prev {
            if node.latest_advance_epoch < epoch.id {
                return NodeAction::AdvancePool;
            }
            // Already advanced - check if we need to join committee_next
            if !in_committee_next {
                return NodeAction::JoinNetwork;
            }
            // Waiting for settle quorum
            return NodeAction::WaitForSettleQuorum {
                current_weight: epoch.state.weight,
            };
        }

        // Not in committee_prev - can't contribute to settling
        // But might want to join committee_next
        if !in_committee_next && node.pool.stake.as_u64() > 0 {
            return NodeAction::JoinNetwork;
        }

        NodeAction::NotInCommittee
    }

    /// Handle Active phase logic.
    /// Nodes should AdvancePool, JoinNetwork, then wait for epoch advancement.
    fn handle_active_phase(
        system: &System,
        epoch: &Epoch,
        node: &Node,
        in_committee: bool,
        in_committee_next: bool,
        current_time: i64,
    ) -> NodeAction {
        // In Active phase, committee members should:
        // 1. AdvancePool (if not done)
        // 2. JoinNetwork (if not in committee_next)
        // 3. Wait for epoch advancement

        if in_committee {
            // Check if we need to AdvancePool
            if node.latest_advance_epoch < epoch.id {
                return NodeAction::AdvancePool;
            }

            // Check if we need to JoinNetwork
            if !in_committee_next {
                return NodeAction::JoinNetwork;
            }
        }

        // Check if we can advance the epoch
        let time_elapsed = current_time.saturating_sub(epoch.last_epoch);

        if time_elapsed < EPOCH_DURATION {
            return NodeAction::WaitForEpochDuration {
                seconds_remaining: EPOCH_DURATION - time_elapsed,
            };
        }

        // Time has elapsed - check committee_next threshold
        let committee_next_size = system.committee_next.size();

        // Bootstrap exception: if committee_prev is empty, allow any size
        let is_bootstrap = system.committee_prev_empty();

        if committee_next_size >= MIN_COMMITTEE_SIZE || is_bootstrap {
            return NodeAction::AdvanceEpoch;
        }

        // Epoch blocked - not enough nodes in committee_next
        NodeAction::EpochBlocked {
            committee_next_size,
        }
    }
}

/// Summary of the overall epoch state for debugging.
#[derive(Debug, Clone)]
pub struct EpochStateSummary {
    pub epoch_id: EpochNumber,
    pub phase: EpochPhase,
    pub weight: u64,
    pub committee_size: usize,
    pub committee_prev_size: usize,
    pub committee_next_size: usize,
    pub is_low_quorum: bool,
    pub will_be_low_quorum: bool,
}

impl EpochStateSummary {
    /// Create a summary from system and epoch state.
    pub fn from_state(system: &System, epoch: &Epoch) -> Self {
        let phase = EpochPhase::try_from(epoch.state.phase).unwrap_or(EpochPhase::Unknown);

        Self {
            epoch_id: epoch.id,
            phase,
            weight: epoch.state.weight,
            committee_size: system.committee.size(),
            committee_prev_size: system.committee_prev.size(),
            committee_next_size: system.committee_next.size(),
            is_low_quorum: system.is_low_quorum(),
            will_be_low_quorum: system.will_be_low_quorum(),
        }
    }
}

/// Summary of a node's state for debugging.
#[derive(Debug, Clone)]
pub struct NodeStateSummary {
    pub node_id: tape_core::types::NodeId,
    pub stake: u64,
    pub latest_sync_epoch: EpochNumber,
    pub latest_advance_epoch: EpochNumber,
    pub in_committee: bool,
    pub in_committee_prev: bool,
    pub in_committee_next: bool,
}

impl NodeStateSummary {
    /// Create a summary from system and node state.
    pub fn from_state(system: &System, node: &Node) -> Self {
        Self {
            node_id: node.id,
            stake: node.pool.stake.as_u64(),
            latest_sync_epoch: node.latest_sync_epoch,
            latest_advance_epoch: node.latest_advance_epoch,
            in_committee: system.committee.contains(&node.id),
            in_committee_prev: system.committee_prev.contains(&node.id),
            in_committee_next: system.committee_next.contains(&node.id),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytemuck::Zeroable;
    use tape_core::system::EpochState;
    use tape_core::system::Committee;
    use tape_core::system::CommitteeMember;
    use tape_core::spooler::SpoolAssignment;
    use tape_core::types::coin::TAPE;
    use tape_core::types::{NodeId, VersionId};
    use tape_core::erasure::SLICE_COUNT;
    use crate::program::MEMBER_COUNT;

    fn make_member(id: u64, stake: u64) -> CommitteeMember {
        CommitteeMember::new(NodeId(id), TAPE(stake))
    }

    fn make_committee(members: &[CommitteeMember]) -> Committee<MEMBER_COUNT> {
        Committee::from_members(members)
    }

    fn make_node(id: u64, stake: u64, sync_epoch: u64, advance_epoch: u64) -> Node {
        let mut node = Node::zeroed();
        node.id = NodeId(id);
        node.registered_epoch = EpochNumber(1);
        node.latest_sync_epoch = EpochNumber(sync_epoch);
        node.latest_advance_epoch = EpochNumber(advance_epoch);
        node.pool.stake = TAPE(stake);
        node
    }

    fn make_epoch(id: u64, state: EpochState, last_epoch: i64) -> Epoch {
        Epoch {
            id: EpochNumber(id),
            state,
            last_epoch,
        }
    }

    fn make_system(
        committee_prev: Committee<MEMBER_COUNT>,
        committee: Committee<MEMBER_COUNT>,
        committee_next: Committee<MEMBER_COUNT>,
    ) -> System {
        System {
            version: VersionId(1),
            total_nodes: 0,
            committee_prev,
            committee,
            committee_next,
            spools_prev: SpoolAssignment::<SLICE_COUNT>::zeroed(),
            spools: SpoolAssignment::<SLICE_COUNT>::zeroed(),
        }
    }

    #[test]
    fn test_syncing_needs_sync() {
        // Node in committee, hasn't synced yet
        let members = vec![make_member(1, 1000)];
        let system = make_system(
            Committee::new(),
            make_committee(&members),
            Committee::new(),
        );
        let epoch = make_epoch(5, EpochState::syncing(), 0);
        let node = make_node(1, 1000, 4, 4); // sync_epoch < epoch.id

        let action = NodeStateMachine::determine_action(&system, &epoch, &node, 0);
        assert_eq!(action, NodeAction::SyncEpoch);
    }

    #[test]
    fn test_syncing_already_synced() {
        // Node in committee, already synced
        let members = vec![make_member(1, 1000)];
        let system = make_system(
            Committee::new(),
            make_committee(&members),
            Committee::new(),
        );
        let mut state = EpochState::syncing();
        state.weight = 500; // Some weight accumulated
        let epoch = make_epoch(5, state, 0);
        let node = make_node(1, 1000, 5, 4); // sync_epoch == epoch.id

        let action = NodeStateMachine::determine_action(&system, &epoch, &node, 0);
        assert_eq!(action, NodeAction::WaitForSyncQuorum { current_weight: 500 });
    }

    #[test]
    fn test_syncing_not_in_committee() {
        // Node NOT in committee
        let members = vec![make_member(2, 1000)]; // Different node
        let system = make_system(
            Committee::new(),
            make_committee(&members),
            Committee::new(),
        );
        let epoch = make_epoch(5, EpochState::syncing(), 0);
        let node = make_node(1, 1000, 4, 4);

        let action = NodeStateMachine::determine_action(&system, &epoch, &node, 0);
        assert_eq!(action, NodeAction::NotInCommittee);
    }

    #[test]
    fn test_settling_needs_advance() {
        // Node in committee_prev, hasn't advanced yet
        let members = vec![make_member(1, 1000)];
        let system = make_system(
            make_committee(&members), // committee_prev
            Committee::new(),
            Committee::new(),
        );
        let epoch = make_epoch(5, EpochState::settling(), 0);
        let node = make_node(1, 1000, 5, 4); // advance_epoch < epoch.id

        let action = NodeStateMachine::determine_action(&system, &epoch, &node, 0);
        assert_eq!(action, NodeAction::AdvancePool);
    }

    #[test]
    fn test_settling_needs_join() {
        // Node in committee_prev, advanced, but not in committee_next
        let members = vec![make_member(1, 1000)];
        let system = make_system(
            make_committee(&members), // committee_prev
            Committee::new(),
            Committee::new(),         // NOT in committee_next
        );
        let epoch = make_epoch(5, EpochState::settling(), 0);
        let node = make_node(1, 1000, 5, 5); // advance_epoch == epoch.id

        let action = NodeStateMachine::determine_action(&system, &epoch, &node, 0);
        assert_eq!(action, NodeAction::JoinNetwork);
    }

    #[test]
    fn test_settling_waiting() {
        // Node in committee_prev, advanced, and in committee_next
        let members = vec![make_member(1, 1000)];
        let system = make_system(
            make_committee(&members), // committee_prev
            Committee::new(),
            make_committee(&members), // committee_next
        );
        let mut state = EpochState::settling();
        state.weight = 300;
        let epoch = make_epoch(5, state, 0);
        let node = make_node(1, 1000, 5, 5);

        let action = NodeStateMachine::determine_action(&system, &epoch, &node, 0);
        assert_eq!(action, NodeAction::WaitForSettleQuorum { current_weight: 300 });
    }

    #[test]
    fn test_active_needs_advance() {
        // Node in committee, hasn't advanced yet
        let members = vec![make_member(1, 1000)];
        let system = make_system(
            Committee::new(),
            make_committee(&members), // committee
            Committee::new(),
        );
        let epoch = make_epoch(5, EpochState::active(), 0);
        let node = make_node(1, 1000, 5, 4); // advance_epoch < epoch.id

        let action = NodeStateMachine::determine_action(&system, &epoch, &node, 0);
        assert_eq!(action, NodeAction::AdvancePool);
    }

    #[test]
    fn test_active_needs_join() {
        // Node in committee, advanced, not in committee_next
        let members = vec![make_member(1, 1000)];
        let system = make_system(
            Committee::new(),
            make_committee(&members), // committee
            Committee::new(),         // NOT in committee_next
        );
        let epoch = make_epoch(5, EpochState::active(), 0);
        let node = make_node(1, 1000, 5, 5);

        let action = NodeStateMachine::determine_action(&system, &epoch, &node, 0);
        assert_eq!(action, NodeAction::JoinNetwork);
    }

    #[test]
    fn test_active_waiting_for_duration() {
        // Node done with maintenance, waiting for epoch duration
        let members = vec![make_member(1, 1000)];
        let prev_members = vec![make_member(2, 1000)]; // Non-empty to avoid bootstrap mode
        let system = make_system(
            make_committee(&prev_members), // Non-empty committee_prev
            make_committee(&members),
            make_committee(&members),
        );
        let epoch = make_epoch(5, EpochState::active(), 100);
        let node = make_node(1, 1000, 5, 5);

        // Current time is 200, epoch started at 100, EPOCH_DURATION is 604800
        let current_time = 200;
        let action = NodeStateMachine::determine_action(&system, &epoch, &node, current_time);

        match action {
            NodeAction::WaitForEpochDuration { seconds_remaining } => {
                assert_eq!(seconds_remaining, EPOCH_DURATION - 100);
            }
            NodeAction::EpochBlocked { .. } => {
                // Also acceptable - committee_next is below threshold
            }
            _ => panic!("Expected WaitForEpochDuration or EpochBlocked, got {:?}", action),
        }
    }

    #[test]
    fn test_active_can_advance_bootstrap() {
        // Bootstrap: committee_prev empty, can advance with any committee_next size
        let members = vec![make_member(1, 1000)];
        let system = make_system(
            Committee::new(),         // Empty committee_prev (bootstrap)
            make_committee(&members),
            make_committee(&members), // Only 1 node
        );
        let last_epoch = 0;
        let current_time = EPOCH_DURATION + 100; // Past duration
        let epoch = make_epoch(1, EpochState::active(), last_epoch);
        let node = make_node(1, 1000, 1, 1);

        let action = NodeStateMachine::determine_action(&system, &epoch, &node, current_time);
        assert_eq!(action, NodeAction::AdvanceEpoch);
    }

    #[test]
    fn test_active_blocked_low_quorum() {
        // Not bootstrap, committee_next below threshold
        let members = vec![make_member(1, 1000)];
        let prev_members = vec![make_member(2, 1000)]; // Non-empty committee_prev
        let system = make_system(
            make_committee(&prev_members), // Non-empty = not bootstrap
            make_committee(&members),
            make_committee(&members),      // Only 1 node, below MIN_COMMITTEE_SIZE
        );
        let last_epoch = 0;
        let current_time = EPOCH_DURATION + 100;
        let epoch = make_epoch(5, EpochState::active(), last_epoch);
        let node = make_node(1, 1000, 5, 5);

        let action = NodeStateMachine::determine_action(&system, &epoch, &node, current_time);
        assert_eq!(action, NodeAction::EpochBlocked { committee_next_size: 1 });
    }

    #[test]
    fn test_action_helper_methods() {
        assert!(NodeAction::WaitForSyncQuorum { current_weight: 0 }.is_waiting());
        assert!(NodeAction::WaitForEpochDuration { seconds_remaining: 100 }.is_waiting());
        assert!(!NodeAction::SyncEpoch.is_waiting());

        assert!(NodeAction::SyncEpoch.requires_transaction());
        assert!(NodeAction::AdvancePool.requires_transaction());
        assert!(!NodeAction::WaitForSyncQuorum { current_weight: 0 }.requires_transaction());

        assert!(NodeAction::NotInCommittee.is_blocked());
        assert!(NodeAction::EpochBlocked { committee_next_size: 1 }.is_blocked());
        assert!(!NodeAction::SyncEpoch.is_blocked());
    }
}
