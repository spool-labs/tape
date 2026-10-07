//! Log of tapes closed or rewound in the current epoch

use tape_core::types::TrackNumber;
use tape_store_api::Column;

use crate::types::RemovalKey;

/// One row per tape closed or rewound in an epoch, kept until that epoch's
/// assignment round is over
///
/// Key: RemovalKey (tape, tape_number, epoch)
/// Value: the tape's tree start before its first removal in that epoch. The
/// tracks numbered at or above it were on the tape when the epoch began.
pub struct RemovalLogCol;

impl Column for RemovalLogCol {
    const CF_NAME: &'static str = "removal_log";
    type Key = RemovalKey;
    type Value = TrackNumber;
}
