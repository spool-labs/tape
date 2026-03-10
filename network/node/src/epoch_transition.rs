//! Epoch-transition logic extracted from the FSM loop.
//!
//! Runs spool cleanup and ownership reconciliation when the committee rotates.

use std::collections::HashSet;

use store::Store;
use tape_core::types::NodeId;
use tape_protocol::state::ProtocolState;
use tape_store::TapeStore;

use crate::fsm::StateChange;
use crate::scheduler::SpoolPlanner;

/// Apply spool-planning work that belongs at epoch boundaries.
///
/// Cleans up locked spools from prior epochs and reconciles local spool
/// ownership against the on-chain assignment, pushing
/// `SpoolAssignmentChanged` into `changes` when ownership differs.
pub fn apply<S: Store>(
    store: &TapeStore<S>,
    my_spools: &HashSet<u16>,
    state: &ProtocolState,
    node_id: NodeId,
    changes: &mut Vec<StateChange>,
) {
    SpoolPlanner::cleanup_locked(store, state.epoch);

    if SpoolPlanner::reconcile_ownership(
        store,
        my_spools,
        state.epoch,
        node_id,
        &state.spools_prev,
        &state.committee_prev,
    ) {
        changes.push(StateChange::SpoolAssignmentChanged);
    }
}
