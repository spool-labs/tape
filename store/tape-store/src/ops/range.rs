//! Ranged reads in key order, for the columns read a page at a time

use tape_store_api::{Direction, KeyValue, Store};

use crate::error::Result;
use crate::TapeStore;

/// The keys from `start` up to `end`
pub struct KeyRange {
    pub start: Vec<u8>,
    pub end: Vec<u8>,
}

/// Up to `limit` rows of `range` that sort after `after`, read in key order
pub fn read_range<S: Store>(
    store: &TapeStore<S>,
    cf: &str,
    range: &KeyRange,
    after: &[u8],
    limit: usize,
) -> Result<Vec<KeyValue>> {
    let mut rows = Vec::new();
    let start = range.start.as_slice().max(after);
    if limit == 0 || start >= range.end.as_slice() {
        return Ok(rows);
    }

    for (key, value) in store.inner().inner().iter_from(cf, start, Direction::Asc)? {
        if key.as_slice() >= range.end.as_slice() {
            break;
        }
        if key.as_slice() == after {
            continue;
        }
        rows.push((key, value));
        if rows.len() == limit {
            break;
        }
    }
    Ok(rows)
}
