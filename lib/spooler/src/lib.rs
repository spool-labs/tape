//! Byte-aware spool assignment across the next committee.

use tape_core::types::BasisPoints;

use crate::score::Score;

pub mod assign;
pub mod score;
pub mod target;

/// Assigned bytes count at this multiple against a node's capacity, so every spool can grow
pub const GROWTH_MARGIN: u64 = 2;

/// A rebalancing move must cut the byte objective by at least a quarter per byte moved
pub const MIN_MOVE_GAIN: Score = Score(1 << (score::SCORE_BITS - 2));

/// Rebalancing stops once an epoch has moved this share of stored bytes
pub const MOVE_BUDGET: BasisPoints = BasisPoints(500);
