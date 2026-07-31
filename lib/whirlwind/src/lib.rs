//! Whirlwind: the TAPEDRIVE storage challenge mechanism.
//!
//! Whirlwind is a signal, not consensus. Each node independently samples a peer's
//! assigned data, proves possession, and keeps its own local score of every peer.
//! Certificates are monotonic positive evidence that a possession signal was
//! witnessed, never an agreement, and that is what makes the mechanism
//! asynchronous: no round blocks on global agreement, and a round with no
//! certificate is a gap, not a dispute.
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
