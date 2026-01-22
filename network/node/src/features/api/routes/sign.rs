//! BLS signature handlers for track certification.
//!
//! NOTE: This handler is currently a stub pending API redesign.

use axum::{
    extract::{Path, State},
    response::Response,
};
use store::Store;
use tracing::debug;

use crate::features::api::ApiError;

use super::{parse_track_id, ApiState};

/// GET /v1/tracks/:track_id/sign
///
/// Returns a BLS signature over the track address for certification.
pub async fn get_sign<S: Store>(
    State(state): State<ApiState<S>>,
    Path(track_id): Path<String>,
) -> Result<Response, ApiError> {
    let track_address = parse_track_id(&track_id)?;

    // Check if node is in committee
    if !state.control_plane.is_in_committee() {
        return Err(ApiError::Unauthorized);
    }

    debug!(
        track = %track_address,
        "get_sign (stub)"
    );

    // Stub: return not found (no slice data)
    Err(ApiError::TrackNotFound)
}
