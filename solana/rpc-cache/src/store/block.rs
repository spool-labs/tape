//! Stored blocks: a filtered block's result bodies, or a tombstone for a skipped slot

use std::collections::HashSet;

use bytes::Bytes;
use rusqlite::{Connection, OptionalExtension, params};

use crate::store::error::StoreResult;
use crate::store::meta::{FINGERPRINT, read_meta, write_meta};
use crate::store::sql::{sql_integer, stored_u64};

/// What the store holds for a slot
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CachedBlock {
    /// Pre-serialized JSON-RPC `result` bodies for a confirmed, filtered
    /// block, one per encoding. Wrapped in an envelope at serve time.
    Present { base64: Bytes, json: Bytes },
    /// The slot was skipped or never produced. Replays the standard
    /// upstream error envelope.
    Skipped,
}

/// Read the block stored at `slot`
pub fn read_block(connection: &Connection, slot: u64) -> StoreResult<Option<CachedBlock>> {
    let mut statement = connection.prepare_cached("SELECT base64, json FROM blocks WHERE slot = ?1")?;
    let bodies = statement
        .query_row(params![sql_integer(slot)], |row| Ok((row.get(0)?, row.get(1)?)))
        .optional()?;
    Ok(bodies.map(stored_block))
}

/// Store a block, replacing whatever the slot held
pub fn write_block(connection: &Connection, slot: u64, block: &CachedBlock) -> StoreResult<()> {
    let (base64, json) = match block {
        CachedBlock::Present { base64, json } => (Some(base64.as_ref()), Some(json.as_ref())),
        CachedBlock::Skipped => (None, None),
    };
    connection
        .prepare_cached("INSERT OR REPLACE INTO blocks (slot, base64, json) VALUES (?1, ?2, ?3)")?
        .execute(params![sql_integer(slot), base64, json])?;
    Ok(())
}

/// Store these slots as skipped in one transaction. A slot that already holds a block keeps it.
pub fn write_skipped(connection: &mut Connection, slots: &[u64]) -> StoreResult<()> {
    let transaction = connection.transaction()?;
    for slot in slots {
        transaction
            .prepare_cached("INSERT OR IGNORE INTO blocks (slot) VALUES (?1)")?
            .execute(params![sql_integer(*slot)])?;
    }
    transaction.commit()?;
    Ok(())
}

/// List the stored slots in `from..=to`
pub fn stored_slots(connection: &Connection, from: u64, to: u64) -> StoreResult<HashSet<u64>> {
    let mut statement = connection.prepare_cached("SELECT slot FROM blocks WHERE slot BETWEEN ?1 AND ?2")?;
    let mut rows = statement.query(params![sql_integer(from), sql_integer(to)])?;
    let mut slots = HashSet::new();
    while let Some(row) = rows.next()? {
        slots.insert(stored_u64(row.get(0)?));
    }
    Ok(slots)
}

/// Delete up to `limit` blocks below `floor` and return how many went
pub fn delete_below(connection: &Connection, floor: u64, limit: u64) -> StoreResult<u64> {
    let deleted = connection
        .prepare_cached("DELETE FROM blocks WHERE slot IN (SELECT slot FROM blocks WHERE slot < ?1 LIMIT ?2)")?
        .execute(params![sql_integer(floor), sql_integer(limit)])?;
    Ok(deleted as u64)
}

/// Delete every block when the file was filled under another fingerprint, and
/// record this one, in one transaction. Returns how many blocks went.
pub fn claim(connection: &mut Connection, fingerprint: &str) -> StoreResult<u64> {
    let transaction = connection.transaction()?;
    if read_meta(&transaction, FINGERPRINT)?.as_deref() == Some(fingerprint) {
        return Ok(0);
    }
    let deleted = transaction.execute("DELETE FROM blocks", [])?;
    write_meta(&transaction, FINGERPRINT, fingerprint)?;
    transaction.commit()?;
    Ok(deleted as u64)
}

/// A row's bodies as a block. The schema keeps both bodies or neither.
fn stored_block(bodies: (Option<Vec<u8>>, Option<Vec<u8>>)) -> CachedBlock {
    match bodies {
        (Some(base64), Some(json)) => CachedBlock::Present {
            base64: Bytes::from(base64),
            json: Bytes::from(json),
        },
        (None, _) | (_, None) => CachedBlock::Skipped,
    }
}

#[cfg(test)]
mod tests {
    use crate::store::SCHEMA;

    use super::*;

    fn open_in_memory() -> Connection {
        let connection = Connection::open_in_memory().expect("open database");
        connection.execute_batch(SCHEMA).expect("schema");
        connection
    }

    fn present(tag: &str) -> CachedBlock {
        CachedBlock::Present {
            base64: Bytes::from(format!("base64-{tag}")),
            json: Bytes::from(format!("json-{tag}")),
        }
    }

    // a block and a skip read back as they were written
    #[test]
    fn round_trip() {
        let mut connection = open_in_memory();

        write_block(&connection, 10, &present("10")).expect("write block");
        write_skipped(&mut connection, &[11]).expect("write skip");

        assert_eq!(read_block(&connection, 10).expect("read"), Some(present("10")));
        assert_eq!(read_block(&connection, 11).expect("read"), Some(CachedBlock::Skipped));
        assert_eq!(read_block(&connection, 12).expect("read"), None);
    }

    // a skip leaves a stored block alone
    #[test]
    fn skip_keeps_block() {
        let mut connection = open_in_memory();
        write_block(&connection, 5, &present("5")).expect("write block");

        write_skipped(&mut connection, &[4, 5, 6]).expect("write skips");

        assert_eq!(read_block(&connection, 5).expect("read"), Some(present("5")));
        assert_eq!(stored_slots(&connection, 0, 10).expect("list"), HashSet::from([4, 5, 6]));
    }

    // a row with one body is refused
    #[test]
    fn one_body() {
        let connection = open_in_memory();

        let written = connection.execute("INSERT INTO blocks (slot, base64) VALUES (1, x'00')", []);

        assert!(written.is_err());
    }

    // a delete stops at its limit and never reaches the floor
    #[test]
    fn delete_limit() {
        let mut connection = open_in_memory();
        let slots: Vec<u64> = (0..10).collect();
        write_skipped(&mut connection, &slots).expect("write skips");

        let first = delete_below(&connection, 8, 5).expect("delete");
        let second = delete_below(&connection, 8, 5).expect("delete");

        assert_eq!((first, second), (5, 3));
        assert_eq!(stored_slots(&connection, 0, 10).expect("list"), HashSet::from([8, 9]));
    }

    // the same fingerprint keeps the blocks, and another one deletes them
    #[test]
    fn claim_file() {
        let mut connection = open_in_memory();
        assert_eq!(claim(&mut connection, "devnet").expect("claim"), 0);
        write_block(&connection, 1, &present("1")).expect("write block");

        let kept = claim(&mut connection, "devnet").expect("claim");
        let deleted = claim(&mut connection, "localnet").expect("claim");

        assert_eq!((kept, deleted), (0, 1));
        assert_eq!(read_block(&connection, 1).expect("read"), None);
        assert_eq!(read_meta(&connection, FINGERPRINT).expect("meta").as_deref(), Some("localnet"));
    }
}
