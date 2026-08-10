//! Targeted measurements of the Whirlwind mechanism against the real code.
//!
//! Each probe maps to one claim in the draft or issue #80: the commitment
//! structure, the cost of faking a sampled unit, detection of partial loss,
//! entropy grinding, and certificate-threshold margin.

pub mod commitment;
pub mod detection;
pub mod grinding;
pub mod lifecycle;
pub mod quorum;
pub mod reconstruct;
