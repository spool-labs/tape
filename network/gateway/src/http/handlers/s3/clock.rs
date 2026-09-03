//! Wall-clock helpers and time constants shared across the S3 subsystem

use std::time::{SystemTime, UNIX_EPOCH};

/// Seconds in a minute.
pub const SECONDS_PER_MINUTE: i64 = 60;
/// Seconds in an hour.
pub const SECONDS_PER_HOUR: i64 = 3_600;
/// Seconds in a day.
pub const SECONDS_PER_DAY: i64 = 86_400;

/// Current wall-clock time as unix seconds, saturating to 0 before the epoch
pub fn now_unix() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_secs() as i64)
        .unwrap_or(0)
}
