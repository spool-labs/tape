//! The summary `status` prints

use rusqlite::{params, Connection};

use crate::records::error::RecordsError;
use crate::records::rows::StatusSummary;
use crate::records::states::{JobState, TrackState};
use crate::records::tapes;
use crate::types::timestamp::Timestamp;

pub fn summary(connection: &Connection, now: Timestamp) -> Result<StatusSummary, RecordsError> {
    let (pending_jobs, due_jobs): (u64, u64) = connection.query_row(
        "SELECT COUNT(*), COALESCE(SUM(next_attempt_at <= ?2), 0) FROM jobs WHERE state = ?1",
        params![JobState::Pending.as_str(), now.0],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;
    let failed_jobs: u64 = connection.query_row(
        "SELECT COUNT(*) FROM jobs WHERE state = ?1",
        params![JobState::Failed.as_str()],
        |row| row.get(0),
    )?;
    let (queued_decisions, oldest): (u64, Option<i64>) = connection.query_row(
        "SELECT COUNT(*), MIN(created_at) FROM decisions WHERE track IS NULL AND undone_at IS NULL",
        [],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;
    let deleting_tracks: u64 = connection.query_row(
        "SELECT COUNT(*) FROM tracks WHERE state = ?1",
        params![TrackState::Deleting.as_str()],
        |row| row.get(0),
    )?;

    Ok(StatusSummary {
        pending_jobs,
        due_jobs,
        failed_jobs,
        queued_decisions,
        oldest_queued: oldest.map(Timestamp),
        deleting_tracks,
        tapes: tapes::list(connection)?,
    })
}
