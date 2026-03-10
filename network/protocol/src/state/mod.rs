mod types;

use std::sync::Arc;

use arc_swap::ArcSwap;

pub use types::ProtocolState;

/// Thread-safe shared protocol state. Clone the `Arc` to share across components.
pub type SharedState = Arc<ArcSwap<ProtocolState>>;

/// Create a new `SharedState` seeded with the given initial state.
pub fn new_shared_state(initial: ProtocolState) -> SharedState {
    Arc::new(ArcSwap::from_pointee(initial))
}
