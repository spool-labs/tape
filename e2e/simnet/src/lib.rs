//! Simnet harness scaffold for multi-node Tapedrive e2e tests.
//!
//! This crate intentionally starts as a lightweight skeleton:
//! - network builder + fixture APIs
//! - LiteSVM chain helper utilities
//! - in-memory node fixtures
//! - runtime lifecycle controls

pub mod chain;
pub mod config;
pub mod fixtures;
pub mod log;
pub mod node;
pub mod scenario;
pub mod simnet;
pub mod tls;

pub use chain::ChainFixture;
pub use config::{NodeRuntimeMode, SimnetConfig};
pub use node::NodeFixture;
pub use scenario::{JoinResult, SimnetScenario};
pub use simnet::{SimnetBuilder, SimnetHarness};
