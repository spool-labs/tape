//! The list's rack and blacklist tapes

use std::path::{Path, PathBuf};

use rusqlite::{params, Connection, OptionalExtension, Row};
use tape_core::types::{EpochNumber, StorageUnits, TrackNumber};
use tape_crypto::address::Address;

use crate::records::columns;
use crate::records::error::RecordsError;
use crate::records::rows::{ListTape, ListTapes, TapeObservation};
use crate::records::states::{TapeRole, TapeState};
use crate::types::ids::TrackCount;
use crate::types::timestamp::Timestamp;

/// A tape's columns, then what its tracks say about it
const TAPE_COLUMNS: &str = "p.address, p.role, p.key_path, p.state, p.next_track, p.expiry_epoch, p.capacity, p.used,
     p.observed_at,
     EXISTS (SELECT 1 FROM tracks h WHERE h.tape = p.address AND h.kind = 'header' AND h.state = 'present'
             AND h.track_number = 0),
     EXISTS (SELECT 1 FROM rack_records r JOIN tracks t ON t.id = r.track
             WHERE r.tape = p.address AND t.state = 'present'),
     (SELECT COUNT(*) FROM tracks t WHERE t.tape = p.address AND t.track_number IS NOT NULL),
     (SELECT COUNT(*) FROM tracks t WHERE t.tape = p.address AND t.state = 'writing'),
     (SELECT COUNT(*) FROM tracks t WHERE t.tape = p.address AND t.state = 'deleting'
      AND t.delete_sent_at IS NOT NULL)";

struct TapeColumns {
    address: String,
    role: String,
    key_path: String,
    state: String,
    next_track: u64,
    expiry_epoch: Option<u64>,
    capacity: Option<u64>,
    used: Option<u64>,
    observed_at: Option<i64>,
    is_declared: bool,
    is_named: bool,
    landed_tracks: u64,
    writing_tracks: u64,
    sent_deletes: u64,
}

/// The rack, then blacklist tapes oldest first
pub fn list(connection: &Connection) -> Result<ListTapes, RecordsError> {
    let mut statement = connection.prepare(&format!(
        "SELECT {TAPE_COLUMNS} FROM tapes p ORDER BY p.role = 'blacklist', p.id"
    ))?;
    let rows = statement.query_map([], read_columns)?;
    let mut tapes = ListTapes::default();
    for row in rows {
        let tape = to_tape(row?)?;
        match tape.role {
            TapeRole::Rack => tapes.rack = Some(tape),
            TapeRole::Blacklist => tapes.blacklists.push(tape),
        }
    }
    Ok(tapes)
}

pub fn find(connection: &Connection, address: Address) -> Result<Option<ListTape>, RecordsError> {
    let row = connection
        .query_row(
            &format!("SELECT {TAPE_COLUMNS} FROM tapes p WHERE p.address = ?1"),
            params![address.to_string()],
            read_columns,
        )
        .optional()?;
    match row {
        Some(values) => to_tape(values).map(Some),
        None => Ok(None),
    }
}

/// Record a rack or blacklist tape, its key persisted and the tape not yet reserved
pub fn insert(
    connection: &Connection,
    address: Address,
    role: TapeRole,
    key_path: &Path,
    now: Timestamp,
) -> Result<ListTape, RecordsError> {
    connection.execute(
        "INSERT INTO tapes (address, role, key_path, state, created_at, updated_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?5)",
        params![
            address.to_string(),
            role.as_str(),
            key_path.to_string_lossy(),
            TapeState::Reserving.as_str(),
            now.0,
        ],
    )?;
    find(connection, address)?.ok_or(RecordsError::UnknownTape(address))
}

pub fn set_state(
    connection: &Connection,
    address: Address,
    state: TapeState,
    now: Timestamp,
) -> Result<(), RecordsError> {
    let updated = connection.execute(
        "UPDATE tapes SET state = ?2, updated_at = ?3 WHERE address = ?1",
        params![address.to_string(), state.as_str(), now.0],
    )?;
    match updated {
        0 => Err(RecordsError::UnknownTape(address)),
        _ => Ok(()),
    }
}

pub fn observe(
    connection: &Connection,
    address: Address,
    observation: &TapeObservation,
    now: Timestamp,
) -> Result<(), RecordsError> {
    connection.execute(
        "UPDATE tapes SET
             next_track = MAX(next_track, ?2), expiry_epoch = ?3, capacity = ?4, used = ?5,
             observed_at = ?6, updated_at = ?6
         WHERE address = ?1",
        params![
            address.to_string(),
            observation.next_track.0,
            observation.expiry_epoch.0,
            observation.capacity.0,
            observation.used.0,
            now.0,
        ],
    )?;
    Ok(())
}

/// Move a tape's known next track forward past a landed track
pub fn advance(
    connection: &Connection,
    address: Address,
    landed: TrackNumber,
    now: Timestamp,
) -> Result<(), RecordsError> {
    connection.execute(
        "UPDATE tapes SET next_track = MAX(next_track, ?2), updated_at = ?3 WHERE address = ?1",
        params![address.to_string(), landed.0.saturating_add(1), now.0],
    )?;
    Ok(())
}

/// Blacklist tapes to name in the rack: declared, in use, and named by no present or planned rack track
pub fn to_name(connection: &Connection) -> Result<Vec<Address>, RecordsError> {
    let mut statement = connection.prepare(
        "SELECT p.address FROM tapes p
         WHERE p.role = 'blacklist' AND p.state IN ('reserved', 'vacating')
           AND EXISTS (SELECT 1 FROM tracks h WHERE h.tape = p.address AND h.kind = 'header'
                       AND h.state = 'present' AND h.track_number = 0)
           AND NOT EXISTS (SELECT 1 FROM rack_records r JOIN tracks t ON t.id = r.track
                           WHERE r.tape = p.address AND t.state IN ('writing', 'present'))
         ORDER BY p.id",
    )?;
    let rows = statement.query_map([], |row| row.get::<_, String>(0))?;
    let mut tapes = Vec::new();
    for row in rows {
        tapes.push(columns::address("address", &row?)?);
    }
    Ok(tapes)
}

fn read_columns(row: &Row<'_>) -> rusqlite::Result<TapeColumns> {
    Ok(TapeColumns {
        address: row.get(0)?,
        role: row.get(1)?,
        key_path: row.get(2)?,
        state: row.get(3)?,
        next_track: row.get(4)?,
        expiry_epoch: row.get(5)?,
        capacity: row.get(6)?,
        used: row.get(7)?,
        observed_at: row.get(8)?,
        is_declared: row.get(9)?,
        is_named: row.get(10)?,
        landed_tracks: row.get(11)?,
        writing_tracks: row.get(12)?,
        sent_deletes: row.get(13)?,
    })
}

fn to_tape(values: TapeColumns) -> Result<ListTape, RecordsError> {
    Ok(ListTape {
        address: columns::address("address", &values.address)?,
        role: columns::parsed("role", &values.role, TapeRole::parse)?,
        key_path: PathBuf::from(values.key_path),
        state: columns::parsed("state", &values.state, TapeState::parse)?,
        next_track: TrackNumber(values.next_track),
        expiry_epoch: values.expiry_epoch.map(EpochNumber),
        capacity: values.capacity.map(StorageUnits),
        used: values.used.map(StorageUnits),
        observed_at: values.observed_at.map(Timestamp),
        is_declared: values.is_declared,
        is_named: values.is_named,
        landed_tracks: TrackCount(values.landed_tracks),
        writing_tracks: TrackCount(values.writing_tracks),
        sent_deletes: TrackCount(values.sent_deletes),
    })
}
