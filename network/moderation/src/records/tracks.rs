//! Tracks the producer writes on its tapes, and what each one names

use rusqlite::{params, Connection, OptionalExtension, Params, Row};
use tape_core::types::TrackNumber;
use tape_crypto::address::Address;
use tape_crypto::Hash;

use crate::publisher::writer::{inline_key, WrittenTrack};
use crate::records::columns;
use crate::records::error::RecordsError;
use crate::records::rows::{Adopted, TrackContent, TrackRow};
use crate::records::states::{TrackKind, TrackState};
use crate::records::tapes;
use crate::types::ids::{DecisionId, TrackCount, TrackId};
use crate::types::timestamp::Timestamp;

const TRACK_COLUMNS: &str =
    "t.id, t.tape, t.kind, t.bytes, t.state, t.track_number, t.address, t.delete_sent_at IS NOT NULL";

/// Tracks to delete whose kept contents are present on other tracks, sent deletes first
///
/// A live decision the track names must be held by another present track, and a
/// blacklist tape it names that stays in the rack must be named by another present
/// rack track.
const DELETABLE: &str = "JOIN tapes p ON p.address = t.tape
    WHERE t.state = 'deleting' AND p.state IN ('reserved', 'vacating')
      AND NOT EXISTS (
          SELECT 1 FROM entries e
          JOIN decisions d ON d.id = e.decision
          LEFT JOIN tracks holder ON holder.id = d.track
          WHERE e.track = t.id AND d.undone_at IS NULL
            AND (holder.id IS NULL OR holder.id = t.id OR holder.state != 'present'))
      AND NOT EXISTS (
          SELECT 1 FROM rack_records r
          JOIN tapes named ON named.address = r.tape
          WHERE r.track = t.id AND named.state IN ('reserved', 'vacating')
            AND NOT EXISTS (
                SELECT 1 FROM rack_records other
                JOIN tracks holder ON holder.id = other.track
                WHERE other.tape = r.tape AND holder.id != t.id AND holder.state = 'present'))
    ORDER BY t.delete_sent_at IS NULL, t.id";

type TrackColumns = (i64, String, String, Vec<u8>, String, Option<u64>, Option<String>, bool);

/// Record a track about to be written, and point what it holds at it
pub fn plan(
    connection: &Connection,
    tape: Address,
    content: &TrackContent,
    bytes: &[u8],
    now: Timestamp,
) -> Result<TrackId, RecordsError> {
    connection.execute(
        "INSERT INTO tracks (tape, kind, bytes, key, state, created_at, updated_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?6)",
        params![
            tape.to_string(),
            content.kind().as_str(),
            bytes,
            inline_key(bytes).0.as_slice(),
            TrackState::Writing.as_str(),
            now.0,
        ],
    )?;
    let track = TrackId(connection.last_insert_rowid());
    match content {
        TrackContent::Header => {}
        TrackContent::Entries(decisions) => place_entries(connection, track, decisions)?,
        TrackContent::Records(named) => place_records(connection, track, named)?,
    }
    Ok(track)
}

/// Record where a planned track landed
pub fn land(
    connection: &Connection,
    track: TrackId,
    written: &WrittenTrack,
    now: Timestamp,
) -> Result<(), RecordsError> {
    let tape: Option<String> = connection
        .query_row(
            "SELECT tape FROM tracks WHERE id = ?1 AND state = ?2",
            params![track.0, TrackState::Writing.as_str()],
            |row| row.get(0),
        )
        .optional()?;
    let tape = tape.ok_or(RecordsError::NotWriting(track))?;
    connection.execute(
        "UPDATE tracks SET state = ?2, track_number = ?3, address = ?4, updated_at = ?5 WHERE id = ?1",
        params![
            track.0,
            TrackState::Present.as_str(),
            written.track_number.0,
            written.track.to_string(),
            now.0,
        ],
    )?;
    tapes::advance(connection, columns::address("tape", &tape)?, written.track_number, now)
}

/// Record a landed track records lacked: the planned track it is, a copy of a landed one, or unknown bytes
pub fn adopt(
    connection: &Connection,
    tape: Address,
    written: &WrittenTrack,
    key: Hash,
    now: Timestamp,
) -> Result<Adopted, RecordsError> {
    let key_bytes = key.0.as_slice();
    let planned: Option<i64> = connection
        .query_row(
            "SELECT id FROM tracks WHERE tape = ?1 AND key = ?2 AND state = 'writing' ORDER BY id LIMIT 1",
            params![tape.to_string(), key_bytes],
            |row| row.get(0),
        )
        .optional()?;
    if let Some(track) = planned {
        land(connection, TrackId(track), written, now)?;
        return Ok(Adopted::Planned(TrackId(track)));
    }

    let landed: Option<i64> = connection
        .query_row(
            "SELECT id FROM tracks WHERE tape = ?1 AND key = ?2 AND track_number IS NOT NULL
             ORDER BY id LIMIT 1",
            params![tape.to_string(), key_bytes],
            |row| row.get(0),
        )
        .optional()?;
    let adopted = match landed {
        Some(original) => Adopted::Duplicate(insert_copy(connection, TrackId(original), written, now)?),
        None => Adopted::Unknown(insert_unknown(connection, tape, written, key, now)?),
    };
    tapes::advance(connection, tape, written.track_number, now)?;
    Ok(adopted)
}

/// Numbers of the tape's recorded tracks that landed, lowest first
pub fn landed_numbers(connection: &Connection, tape: Address) -> Result<Vec<TrackNumber>, RecordsError> {
    let mut statement = connection.prepare(
        "SELECT track_number FROM tracks WHERE tape = ?1 AND track_number IS NOT NULL ORDER BY track_number",
    )?;
    let rows = statement.query_map(params![tape.to_string()], |row| row.get::<_, u64>(0))?;
    let mut numbers = Vec::new();
    for row in rows {
        numbers.push(TrackNumber(row?));
    }
    Ok(numbers)
}

/// Planned tracks on a tape not yet known to have landed, oldest first
pub fn writing(connection: &Connection, tape: Address) -> Result<Vec<TrackRow>, RecordsError> {
    select(
        connection,
        "WHERE t.tape = ?1 AND t.state = 'writing' ORDER BY t.id",
        params![tape.to_string()],
    )
}

/// Recorded tracks on a tape the chain still holds
pub fn on_chain(connection: &Connection, tape: Address) -> Result<TrackCount, RecordsError> {
    let count: u64 = connection.query_row(
        "SELECT COUNT(*) FROM tracks WHERE tape = ?1 AND state IN ('present', 'lapsing', 'deleting')",
        params![tape.to_string()],
        |row| row.get(0),
    )?;
    Ok(TrackCount(count))
}

/// Tracks on a tape whose delete was sent and not yet settled
pub fn sent_deletes(connection: &Connection, tape: Address) -> Result<Vec<TrackId>, RecordsError> {
    let mut statement = connection.prepare(
        "SELECT id FROM tracks WHERE tape = ?1 AND state = 'deleting' AND delete_sent_at IS NOT NULL ORDER BY id",
    )?;
    let rows = statement.query_map(params![tape.to_string()], |row| row.get::<_, i64>(0))?;
    let mut tracks = Vec::new();
    for row in rows {
        tracks.push(TrackId(row?));
    }
    Ok(tracks)
}

/// Give up planned tracks on a tape that can take no more, queueing their decisions again
pub fn abandon(connection: &Connection, tape: Address, now: Timestamp) -> Result<usize, RecordsError> {
    connection.execute(
        "UPDATE decisions SET track = NULL
         WHERE track IN (SELECT id FROM tracks WHERE tape = ?1 AND state = 'writing')",
        params![tape.to_string()],
    )?;
    let abandoned = connection.execute(
        "UPDATE tracks SET state = 'abandoned', updated_at = ?2 WHERE tape = ?1 AND state = 'writing'",
        params![tape.to_string(), now.0],
    )?;
    Ok(abandoned)
}

/// Record that a delete is about to be sent
pub fn mark_delete_sent(connection: &Connection, track: TrackId, now: Timestamp) -> Result<(), RecordsError> {
    connection.execute(
        "UPDATE tracks SET delete_sent_at = ?2, updated_at = ?2 WHERE id = ?1 AND state = 'deleting'",
        params![track.0, now.0],
    )?;
    Ok(())
}

pub fn mark_deleted(connection: &Connection, track: TrackId, now: Timestamp) -> Result<(), RecordsError> {
    connection.execute(
        "UPDATE tracks SET state = 'deleted', updated_at = ?2 WHERE id = ?1 AND state = 'deleting'",
        params![track.0, now.0],
    )?;
    Ok(())
}

pub fn deletable(connection: &Connection) -> Result<Vec<TrackRow>, RecordsError> {
    select(connection, DELETABLE, [])
}

fn place_entries(connection: &Connection, track: TrackId, decisions: &[DecisionId]) -> Result<(), RecordsError> {
    let mut name = connection.prepare("INSERT INTO entries (track, decision) VALUES (?1, ?2)")?;
    let mut hold = connection.prepare("UPDATE decisions SET track = ?1 WHERE id = ?2")?;
    for decision in decisions {
        name.execute(params![track.0, decision.0])?;
        hold.execute(params![track.0, decision.0])?;
    }
    Ok(())
}

fn place_records(connection: &Connection, track: TrackId, named: &[Address]) -> Result<(), RecordsError> {
    let mut name = connection.prepare("INSERT INTO rack_records (track, tape) VALUES (?1, ?2)")?;
    for tape in named {
        name.execute(params![track.0, tape.to_string()])?;
    }
    Ok(())
}

/// A second landing of a recorded track, naming what the original names, to be deleted
fn insert_copy(
    connection: &Connection,
    original: TrackId,
    written: &WrittenTrack,
    now: Timestamp,
) -> Result<TrackId, RecordsError> {
    connection.execute(
        "INSERT INTO tracks (tape, kind, bytes, key, state, track_number, address, created_at, updated_at)
         SELECT tape, kind, bytes, key, ?2, ?3, ?4, ?5, ?5 FROM tracks WHERE id = ?1",
        params![
            original.0,
            TrackState::Deleting.as_str(),
            written.track_number.0,
            written.track.to_string(),
            now.0,
        ],
    )?;
    let copy = TrackId(connection.last_insert_rowid());
    connection.execute(
        "INSERT INTO entries (track, decision) SELECT ?1, decision FROM entries WHERE track = ?2",
        params![copy.0, original.0],
    )?;
    connection.execute(
        "INSERT INTO rack_records (track, tape) SELECT ?1, tape FROM rack_records WHERE track = ?2",
        params![copy.0, original.0],
    )?;
    Ok(copy)
}

fn insert_unknown(
    connection: &Connection,
    tape: Address,
    written: &WrittenTrack,
    key: Hash,
    now: Timestamp,
) -> Result<TrackId, RecordsError> {
    connection.execute(
        "INSERT INTO tracks (tape, kind, bytes, key, state, track_number, address, created_at, updated_at)
         VALUES (?1, ?2, X'', ?3, ?4, ?5, ?6, ?7, ?7)",
        params![
            tape.to_string(),
            TrackKind::Unknown.as_str(),
            key.0.as_slice(),
            TrackState::Present.as_str(),
            written.track_number.0,
            written.track.to_string(),
            now.0,
        ],
    )?;
    Ok(TrackId(connection.last_insert_rowid()))
}

fn select<Parameters: Params>(
    connection: &Connection,
    filter: &str,
    parameters: Parameters,
) -> Result<Vec<TrackRow>, RecordsError> {
    let mut statement = connection.prepare(&format!("SELECT {TRACK_COLUMNS} FROM tracks t {filter}"))?;
    let rows = statement.query_map(parameters, read_columns)?;
    let mut tracks = Vec::new();
    for row in rows {
        tracks.push(to_track(row?)?);
    }
    Ok(tracks)
}

fn read_columns(row: &Row<'_>) -> rusqlite::Result<TrackColumns> {
    Ok((
        row.get(0)?,
        row.get(1)?,
        row.get(2)?,
        row.get(3)?,
        row.get(4)?,
        row.get(5)?,
        row.get(6)?,
        row.get(7)?,
    ))
}

fn to_track(values: TrackColumns) -> Result<TrackRow, RecordsError> {
    let (id, tape, kind, bytes, state, track_number, address, is_delete_sent) = values;
    Ok(TrackRow {
        id: TrackId(id),
        tape: columns::address("tape", &tape)?,
        kind: columns::parsed("kind", &kind, TrackKind::parse)?,
        bytes,
        state: columns::parsed("state", &state, TrackState::parse)?,
        track_number: track_number.map(TrackNumber),
        address: columns::optional_address("address", address)?,
        is_delete_sent,
    })
}
