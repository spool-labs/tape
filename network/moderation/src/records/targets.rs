//! Per-target moderation state

use rusqlite::{params, Connection, OptionalExtension};
use tape_crypto::address::Address;

use crate::records::columns;
use crate::records::error::RecordsError;
use crate::records::states::{DecisionSource, TargetState};
use crate::types::ids::RemovalCount;
use crate::types::target::Target;
use crate::types::timestamp::Timestamp;

/// A target's stored state and what decided it
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TargetRow {
    pub state: TargetState,
    pub decided_by: Option<DecisionSource>,
}

/// Count a report against its target, creating the target when new
pub fn note_report(
    connection: &Connection,
    target: Target,
    now: Timestamp,
) -> Result<(), RecordsError> {
    connection.execute(
        "INSERT INTO targets (kind, address, state, report_count, created_at, updated_at)
         VALUES (?1, ?2, ?3, 1, ?4, ?4)
         ON CONFLICT (kind, address) DO UPDATE SET
             report_count = targets.report_count + 1,
             updated_at = excluded.updated_at",
        params![
            target.kind.as_str(),
            target.address.to_string(),
            TargetState::Pending.as_str(),
            now.0,
        ],
    )?;
    Ok(())
}

pub fn find(connection: &Connection, target: Target) -> Result<Option<TargetRow>, RecordsError> {
    let row: Option<(String, Option<String>)> = connection
        .query_row(
            "SELECT state, decided_by FROM targets WHERE kind = ?1 AND address = ?2",
            params![target.kind.as_str(), target.address.to_string()],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?;
    let Some((state, decided_by)) = row else {
        return Ok(None);
    };

    let decided_by = match decided_by {
        Some(source) => Some(columns::parsed("decided_by", &source, DecisionSource::parse)?),
        None => None,
    };
    Ok(Some(TargetRow {
        state: columns::parsed("state", &state, TargetState::parse)?,
        decided_by,
    }))
}

/// Set a target's state, keeping a known tape when none is given
pub fn set_state(
    connection: &Connection,
    target: Target,
    state: TargetState,
    tape: Option<Address>,
    decided_by: Option<DecisionSource>,
    now: Timestamp,
) -> Result<(), RecordsError> {
    connection.execute(
        "INSERT INTO targets (kind, address, tape, state, decided_by, report_count, created_at, updated_at)
         VALUES (?1, ?2, ?3, ?4, ?5, 0, ?6, ?6)
         ON CONFLICT (kind, address) DO UPDATE SET
             state = excluded.state,
             tape = COALESCE(excluded.tape, targets.tape),
             decided_by = excluded.decided_by,
             updated_at = excluded.updated_at",
        params![
            target.kind.as_str(),
            target.address.to_string(),
            tape.map(|tape| tape.to_string()),
            state.as_str(),
            decided_by.map(DecisionSource::as_str),
            now.0,
        ],
    )?;
    Ok(())
}

/// Set a state from a check, unless a decision already holds the target
pub fn settle_state(
    connection: &Connection,
    target: Target,
    state: TargetState,
    tape: Option<Address>,
    now: Timestamp,
) -> Result<(), RecordsError> {
    let is_decided = find(connection, target)?.is_some_and(|row| row.state.is_decided());
    match is_decided {
        true => Ok(()),
        false => set_state(connection, target, state, tape, None, now),
    }
}

/// Tracks on a tape that checks removed and nothing restored since
pub fn removals_on_tape(
    connection: &Connection,
    tape: Address,
) -> Result<RemovalCount, RecordsError> {
    let count: u64 = connection.query_row(
        "SELECT COUNT(*) FROM targets
         WHERE kind = 'track' AND tape = ?1 AND state = ?2 AND decided_by = ?3",
        params![
            tape.to_string(),
            TargetState::Removed.as_str(),
            DecisionSource::Check.as_str(),
        ],
        |row| row.get(0),
    )?;
    Ok(RemovalCount(count))
}
