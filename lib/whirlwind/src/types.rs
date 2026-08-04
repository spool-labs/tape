//! Semantic newtypes for the challenge simulation.
//!
//! Epoch number, slot number, and track number come from tape-core and are
//! re-exported here so this module is the single type surface. The round index,
//! the schedule slot counts, and a spool's group position are defined alongside
//! them.

mod group_position;
mod round;
mod slot_count;

pub use group_position::GroupPosition;
pub use round::RoundNumber;
pub use slot_count::SlotCount;

pub use tape_core::types::{EpochNumber, SlotNumber, TrackNumber};
