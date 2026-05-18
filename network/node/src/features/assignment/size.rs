//! Assignment size calculation.
//!
//! The assignment root commits to per-group spool sizes. That value is
//! consensus-critical and must be derived from active track metadata at a
//! deterministic epoch cutoff. Until that rule is implemented, assignment
//! candidate construction fails closed.

use thiserror::Error;

use tape_core::spooler::GroupIndex;
use tape_core::types::{EpochNumber, StorageUnits};
use tape_protocol::ProtocolState;

#[derive(Debug, Error)]
pub enum AssignmentSizeError {
    #[error("assignment size calculation is not implemented")]
    Unavailable,
}

pub fn group_size(
    _state: &ProtocolState,
    _target_epoch: EpochNumber,
    _group: GroupIndex,
) -> Result<StorageUnits, AssignmentSizeError> {
    Err(AssignmentSizeError::Unavailable)
}
