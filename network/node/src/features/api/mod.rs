//! API feature module.
//!
//! This module provides the HTTP API server and route handlers for the storage node.
//!
//! ## Structure
//!
//! - `server.rs` - Server setup and lifecycle management
//! - `routes/` - Route handlers organized by resource type
//!   - `slices.rs` - Slice upload/download operations
//!   - `metadata.rs` - Track metadata operations
//!   - `status.rs` - Health and status endpoints
//!   - `sign.rs` - BLS signature endpoints
//!   - `info.rs` - Node info and stats
//!   - `sync.rs` - Spool synchronization

pub mod errors;
pub mod routes;
pub mod server;

pub use errors::ApiError;
pub use routes::{create_router, ApiState};
pub use server::{Server, ServerHandle};
