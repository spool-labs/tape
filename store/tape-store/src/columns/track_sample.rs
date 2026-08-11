use store::Column;

use crate::types::{TrackSample, TrackSampleKey};

/// Stores the replay-derived challenge sample set by group and track.
pub struct TrackSampleCol;

impl Column for TrackSampleCol {
    const CF_NAME: &'static str = "track_sample";
    type Key = TrackSampleKey;
    type Value = TrackSample;
}
