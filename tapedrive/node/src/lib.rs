//! Tapedrive storage node REST API server.

// Core modules
pub mod config;
pub mod context;
pub mod control_plane;
pub mod error;
pub mod events;
pub mod metrics;
pub mod server;
pub mod storage_service;
pub mod sync_types;

// Worker threads
pub mod challenges;
pub mod live_updates;
pub mod network_sync;
pub mod orchestrator;
pub mod spool_sync;
pub mod tx_parser;

// Re-exports for convenience
pub use config::{ConfigError, NodeConfig};
pub use context::{ContextError, NodeContext};
pub use control_plane::ControlPlane;
pub use error::ApiError;
pub use events::NodeEvent;
pub use metrics::NodeMetrics;
pub use server::{Server, ServerHandle};
pub use spool_sync::SpoolSyncHandler;
pub use storage_service::{StorageError, StorageService};
pub use sync_types::{SignedSyncRequest, SyncSpoolRequest, SyncSpoolResponse};
