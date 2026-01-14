//! Tapedrive storage node REST API server.

// Core modules
pub mod block;
pub mod config;
pub mod context;
pub mod control_plane;
pub mod error;
pub mod events;
pub mod fsm;
pub mod metrics;

// Submodules
pub mod server;
pub mod storage;
pub mod sync;
pub mod workers;

// Re-exports for convenience
pub use config::{ConfigError, NodeConfig};
pub use context::{ContextError, NodeContext};
pub use control_plane::ControlPlane;
pub use error::ApiError;
pub use events::NodeEvent;
pub use metrics::NodeMetrics;
pub use server::{Server, ServerHandle};
pub use storage::{StorageError, StorageService};
pub use sync::{SignedSyncRequest, SpoolSyncHandler, SyncSpoolRequest, SyncSpoolResponse};
pub use workers::orchestrator;
