//! The storage challenge: when a round fires, and what it asks for.
//!
//! A round is a slot window derived from finalized epoch state, so every owner
//! computes the same schedule without coordinating. The first block to finalize
//! inside that window seeds the round, and the seed picks one sample leaf out of
//! the challenged spool's holdings. Nothing here reads storage or the network.

pub mod proof;
pub mod record;
pub mod sample;
pub mod schedule;

pub use proof::{ProofOfAccess, ProofRejection};
pub use record::PeerRecord;
pub use sample::{Sample, SampleEntry, draw, round_seed, sort_entries};
pub use schedule::{Schedule, ScheduleError};
