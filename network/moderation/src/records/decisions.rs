//! Decisions, the track holding each, and undoing them

use rusqlite::{params, Connection, OptionalExtension};
use tape_blacklist::entry::{kind_name, Entry};

use crate::records::columns;
use crate::records::error::RecordsError;
use crate::records::rows::Decision;
use crate::records::states::DecisionSource;
use crate::types::ids::{CheckId, DecisionId};
use crate::types::target::Target;
use crate::types::timestamp::Timestamp;

/// Record a decision, or return the live one already making it
pub fn insert(
    connection: &Connection,
    entry: &Entry,
    source: DecisionSource,
    check: Option<CheckId>,
    now: Timestamp,
) -> Result<DecisionId, RecordsError> {
    let kind = kind_name(&entry.target).ok_or(RecordsError::InvalidEntry)?;
    if !entry.is_valid() {
        return Err(RecordsError::InvalidEntry);
    }
    let target = entry.target.target.to_string();
    let live: Option<i64> = connection
        .query_row(
            "SELECT id FROM decisions
             WHERE action = ?1 AND target_kind = ?2 AND target = ?3 AND undone_at IS NULL
             ORDER BY id LIMIT 1",
            params![entry.action.as_str(), kind, target],
            |row| row.get(0),
        )
        .optional()?;
    if let Some(id) = live {
        return Ok(DecisionId(id));
    }

    connection.execute(
        "INSERT INTO decisions (action, target_kind, target, source, check_id, created_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
        params![
            entry.action.as_str(),
            kind,
            target,
            source.as_str(),
            check.map(|check| check.0),
            now.0,
        ],
    )?;
    Ok(DecisionId(connection.last_insert_rowid()))
}

/// Live decisions no track holds, in the order they were decided
pub fn queued(connection: &Connection, limit: usize) -> Result<Vec<Decision>, RecordsError> {
    let mut statement = connection.prepare(
        "SELECT id, action, target_kind, target, source, created_at FROM decisions
         WHERE track IS NULL AND undone_at IS NULL
         ORDER BY id
         LIMIT ?1",
    )?;
    let rows = statement.query_map(params![i64::try_from(limit).unwrap_or(i64::MAX)], |row| {
        let columns: (i64, String, String, String, String, i64) = (
            row.get(0)?,
            row.get(1)?,
            row.get(2)?,
            row.get(3)?,
            row.get(4)?,
            row.get(5)?,
        );
        Ok(columns)
    })?;

    let mut decisions = Vec::new();
    for row in rows {
        let (id, action, kind, target, source, created_at) = row?;
        decisions.push(Decision {
            id: DecisionId(id),
            entry: columns::entry(&action, &kind, &target)?,
            source: columns::parsed("source", &source, DecisionSource::parse)?,
            created_at: Timestamp(created_at),
        });
    }
    Ok(decisions)
}

/// Undo every live decision on a target, returning how many there were
pub fn undo(connection: &Connection, target: Target, now: Timestamp) -> Result<usize, RecordsError> {
    let undone = connection.execute(
        "UPDATE decisions SET undone_at = ?3 WHERE target_kind = ?1 AND target = ?2 AND undone_at IS NULL",
        params![target.kind.as_str(), target.address.to_string(), now.0],
    )?;
    Ok(undone)
}
