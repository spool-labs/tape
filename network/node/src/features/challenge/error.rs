//! Errors of the challenge feature

use thiserror::Error;

use crate::features::assignment::size::AssignmentSizeError;

#[derive(Debug, Error)]
pub enum ChallengeError {
    #[error("store error: {0}")]
    Store(String),
    #[error("track set: {0}")]
    TrackSet(#[from] AssignmentSizeError),
}
