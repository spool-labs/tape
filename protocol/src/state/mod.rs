//! Protocol state: on-chain state snapshot, caching, and fetching.

mod fetch;
mod handle;
mod types;

pub use fetch::fetch_state;
pub use handle::StateHandle;
pub use types::ProtocolState;
