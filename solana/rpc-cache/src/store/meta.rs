//! Named values the block file keeps beside its blocks

use rusqlite::{Connection, OptionalExtension, params};

use crate::store::error::StoreResult;

/// This meta row records the chain, filter version and programs of the stored blocks
pub const FINGERPRINT: &str = "fingerprint";

/// Read a meta value
pub fn read_meta(connection: &Connection, name: &str) -> StoreResult<Option<String>> {
    let value = connection
        .query_row("SELECT value FROM meta WHERE name = ?1", params![name], |row| row.get(0))
        .optional()?;
    Ok(value)
}

/// Write a meta value
pub fn write_meta(connection: &Connection, name: &str, value: &str) -> StoreResult<()> {
    connection.execute(
        "INSERT INTO meta(name, value) VALUES (?1, ?2)
         ON CONFLICT(name) DO UPDATE SET value = excluded.value",
        params![name, value],
    )?;
    Ok(())
}
