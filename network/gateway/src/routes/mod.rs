mod health;
mod tapes;
mod tracks;

use axum::{routing::get, Router};

pub fn create_router() -> Router {
    Router::new()
        // Blob operations
        .route("/v1/tracks/{address}", get(tracks::get_track).put(tracks::put_track))
        .route("/v1/tracks/{address}/byte-range", get(tracks::get_byte_range))
        // Tape operations
        .route("/v1/tapes/{address}", get(tapes::get_tape))
        .route(
            "/v1/tapes/{address}/by-track-number/{number}",
            get(tapes::get_track_by_number),
        )
        .route("/v1/tapes/{address}/tracks", get(tapes::list_tracks))
        // Health
        .route("/status", get(health::get_status))
}
