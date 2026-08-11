//! Targeted measurements of the Whirlwind mechanism against the real code.
//!
//! The probes measure commitment structure, the cost of faking a sampled unit,
//! detection of partial loss, entropy grinding, and certificate-threshold margin.

pub mod commitment;
pub mod detection;
pub mod grinding;
pub mod lifecycle;
pub mod quorum;
pub mod reconstruct;
