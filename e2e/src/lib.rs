//! End-to-end testing framework for Tapedrive.
//!
//! This crate provides utilities for running integration tests against
//! the Tapedrive storage network, including:
//!
//! - CLI wrapper for calling the `tape` binary
//! - Validator lifecycle management
//! - Node management helpers
//! - Wait/polling utilities
//! - Test fixtures and data generators
//!
//! # Example
//!
//! ```ignore
//! use tape_e2e::{Tapedrive, Validator, TestNode};
//!
//! #[tokio::test]
//! async fn test_basic_flow() {
//!     let validator = Validator::spawn().await.unwrap();
//!     let cli = Tapedrive::new_localnet();
//!
//!     cli.admin_init().await.unwrap();
//!     // ... rest of test
//! }
//! ```

pub mod cli;
pub mod consts;
pub mod context;
pub mod fixtures;
pub mod fsm;
pub mod node;
pub mod rpc;
pub mod validator;
pub mod wait;

pub use cli::{Tapedrive, ArchiveAccount, NodeStatus, CommitteeInfo, CommitteeMember};
pub use consts::*;
pub use context::{TestContext, TestContextBuilder, VARYING_STAKES};
pub use fixtures::*;
pub use fsm::*;
pub use node::{TestCluster, TestNode};
pub use rpc::*;
pub use validator::{Validator, ValidatorOptions};
pub use wait::*;
