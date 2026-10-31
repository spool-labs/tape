//! Conversions between slots and SQLite's signed integers

/// A u64 as SQLite's signed integer. Slots stay far below `i64::MAX`, and a larger value clamps to it.
pub fn sql_integer(value: u64) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

/// A stored integer back as a u64. Nothing stores a negative value, so one reads as zero.
pub fn stored_u64(value: i64) -> u64 {
    u64::try_from(value).unwrap_or(0)
}
