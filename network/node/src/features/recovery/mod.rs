//! Recovery feature module.
//!
//! Handles recovery of slices that failed to sync from previous owners.
//! Uses erasure coding to reconstruct slices from the committee.

pub mod worker;

pub use worker::{run, RecoveryError};
