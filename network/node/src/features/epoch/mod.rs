//! Epoch synchronization feature.
//!
//! Handles epoch state management and synchronization:
//! - FSM extension (node-specific states)
//! - Sync handler (FSM loop and transaction submission)

mod fsm;
mod worker;

pub use fsm::LocalNodeAction;
pub use worker::{
    run, execute_action, refresh_state,
    FsmSignal, HandlerOutcome, NetworkSyncError,
    EPOCH_ADVANCE_POLL_INTERVAL, ADVANCE_EPOCH_COMPUTE_UNITS, ADVANCE_POOL_COMPUTE_UNITS,
};
