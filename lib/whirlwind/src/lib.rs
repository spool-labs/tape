//! Whirlwind: the TAPEDRIVE storage challenge mechanism.
//!
//! Each node independently samples a peer's assigned data, proves possession,
//! and keeps a local score of every peer. Certificates provide monotonic evidence
//! that a possession signal was witnessed. This makes the mechanism asynchronous:
//! rounds proceed without global agreement, and a missing certificate is
//! inconclusive.
//!
//! This crate models the mechanism against the real slicer, merkle, and BLS code:
//! the honest round and its measured cost, the same round under attack, and the
//! threshold edge cases. The probes module holds the targeted measurements; the
//! simulations feature adds the latency histogram driven by a real ping dataset.

pub mod crypto;
#[cfg(feature = "simulations")]
pub mod network;
pub mod probes;
pub mod report;
#[cfg(feature = "simulations")]
pub mod sim;
pub mod spool;
pub mod types;
