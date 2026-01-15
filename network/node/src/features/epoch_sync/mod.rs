//! Epoch synchronization feature.
//!
//! Handles epoch state management and synchronization:
//! - Control plane cache (in-memory on-chain state)
//! - FSM extension (node-specific states)
//! - Sync handler (FSM loop and transaction submission)

mod control_plane;
mod fsm;
mod sync_handler;

pub use control_plane::{ControlPlane, EpochSyncTracker};
pub use fsm::ExtendedNodeAction;
pub use sync_handler::{
    run, execute_action, refresh_state,
    FsmSignal, HandlerOutcome, NetworkSyncError,
    EPOCH_ADVANCE_POLL_INTERVAL, ADVANCE_EPOCH_COMPUTE_UNITS, ADVANCE_POOL_COMPUTE_UNITS,
};
