//! Wall-clock helpers and time constants shared across the S3 subsystem

use std::time::{SystemTime, UNIX_EPOCH};

/// Seconds in a minute.
pub const SECONDS_PER_MINUTE: i64 = 60;
/// Seconds in an hour.
pub const SECONDS_PER_HOUR: i64 = 3_600;
/// Seconds in a day.
pub const SECONDS_PER_DAY: i64 = 86_400;

/// Weekday abbreviations, indexed `0 = Sunday`
pub const WEEKDAYS: [&str; 7] = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"];

/// Month abbreviations, indexed `0 = January`
pub const MONTHS: [&str; 12] = [
    "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
];

/// Current wall-clock time as unix seconds, saturating to 0 before the epoch
pub fn now_unix() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_secs() as i64)
        .unwrap_or(0)
}

/// Days since the Unix epoch (1970-01-01) for a proleptic-Gregorian date, via
/// Howard Hinnant's branch-free `days_from_civil`. Valid for any in-range date.
pub fn days_from_civil(year: i64, month: i64, day: i64) -> i64 {
    let year = if month <= 2 { year - 1 } else { year };
    let era = (if year >= 0 { year } else { year - 399 }) / 400;
    let year_of_era = year - era * 400; // [0, 399]
    let day_of_year =
        (153 * (if month > 2 { month - 3 } else { month + 9 }) + 2) / 5 + day - 1; // [0, 365]
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year; // [0, 146096]
    era * 146_097 + day_of_era - 719_468
}

/// The 1-based month number for an abbreviation like `Nov`
pub fn month_number(name: &str) -> Option<i64> {
    for (index, month) in MONTHS.iter().enumerate() {
        if *month == name {
            return Some(index as i64 + 1);
        }
    }
    None
}
