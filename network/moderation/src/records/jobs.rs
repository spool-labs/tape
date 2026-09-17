//! Check and listing jobs with retry scheduling

use rusqlite::{params, Connection, OptionalExtension};
use tape_crypto::address::Address;

use crate::records::checks;
use crate::records::columns;
use crate::records::error::RecordsError;
use crate::records::rows::{CheckAttempt, Job, JobFailure};
use crate::records::states::{CheckOutcome, JobState, TargetState};
use crate::records::targets::{self, TargetRow};
use crate::types::ids::{AttemptCount, JobId};
use crate::types::target::{Target, TargetKind};
use crate::types::timestamp::Timestamp;

/// Queue a job for the target unless one is pending or its outcome stands
pub fn queue_check(
    connection: &Connection,
    target: Target,
    tape: Option<Address>,
    now: Timestamp,
) -> Result<bool, RecordsError> {
    let is_pending: bool = connection.query_row(
        "SELECT EXISTS (
             SELECT 1 FROM jobs WHERE target_kind = ?1 AND target = ?2 AND state = ?3
         )",
        params![
            target.kind.as_str(),
            target.address.to_string(),
            JobState::Pending.as_str(),
        ],
        |row| row.get(0),
    )?;
    let existing = targets::find(connection, target)?;
    if is_pending || !is_checkable(target.kind, existing) {
        return Ok(false);
    }

    targets::set_state(connection, target, TargetState::Pending, tape, None, now)?;
    connection.execute(
        "INSERT INTO jobs
             (target_kind, target, tape, state, attempts, next_attempt_at, created_at, updated_at)
         VALUES (?1, ?2, ?3, ?4, 0, ?5, ?5, ?5)",
        params![
            target.kind.as_str(),
            target.address.to_string(),
            tape.map(|tape| tape.to_string()),
            JobState::Pending.as_str(),
            now.0,
        ],
    )?;
    Ok(true)
}

/// The pending job due soonest, if any is due
pub fn next_due(connection: &Connection, now: Timestamp) -> Result<Option<Job>, RecordsError> {
    let row: Option<(i64, String, String, Option<String>, u32)> = connection
        .query_row(
            "SELECT id, target_kind, target, tape, attempts FROM jobs
             WHERE state = ?1 AND next_attempt_at <= ?2
             ORDER BY next_attempt_at, id
             LIMIT 1",
            params![JobState::Pending.as_str(), now.0],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?)),
        )
        .optional()?;
    let Some((id, kind, target, tape, attempts)) = row else {
        return Ok(None);
    };

    Ok(Some(Job {
        id: JobId(id),
        target: columns::target(&kind, &target)?,
        tape: columns::optional_address("tape", tape)?,
        attempts: AttemptCount(attempts),
    }))
}

pub fn finish(connection: &Connection, job: JobId, now: Timestamp) -> Result<(), RecordsError> {
    connection.execute(
        "UPDATE jobs SET state = ?2, attempts = attempts + 1, last_error = NULL, updated_at = ?3
         WHERE id = ?1",
        params![job.0, JobState::Done.as_str(), now.0],
    )?;
    Ok(())
}

/// Count a failed attempt: reschedule it, or fail the job and its target
pub fn record_failure(
    connection: &mut Connection,
    failure: &JobFailure,
) -> Result<(), RecordsError> {
    let transaction = connection.transaction()?;
    let job = failure.job;
    match failure.retry_at {
        Some(retry_at) => reschedule(&transaction, &failure.error, job.id, retry_at, failure.failed_at)?,
        None => fail(&transaction, &failure.error, job, failure.failed_at)?,
    }

    if job.target.kind == TargetKind::Track {
        let mut attempt = CheckAttempt::new(job.id, job.target.address, CheckOutcome::Error, failure.failed_at);
        attempt.detail = Some(failure.error.clone());
        checks::insert(&transaction, &attempt)?;
    }
    transaction.commit()?;
    Ok(())
}

fn reschedule(
    connection: &Connection,
    error: &str,
    job: JobId,
    retry_at: Timestamp,
    now: Timestamp,
) -> Result<(), RecordsError> {
    connection.execute(
        "UPDATE jobs SET attempts = attempts + 1, next_attempt_at = ?2, last_error = ?3, updated_at = ?4
         WHERE id = ?1",
        params![job.0, retry_at.0, error, now.0],
    )?;
    Ok(())
}

fn fail(connection: &Connection, error: &str, job: Job, now: Timestamp) -> Result<(), RecordsError> {
    connection.execute(
        "UPDATE jobs SET state = ?2, attempts = attempts + 1, last_error = ?3, updated_at = ?4
         WHERE id = ?1",
        params![job.id.0, JobState::Failed.as_str(), error, now.0],
    )?;
    targets::settle_state(connection, job.target, TargetState::Failed, job.tape, now)
}

fn is_checkable(kind: TargetKind, existing: Option<TargetRow>) -> bool {
    let Some(existing) = existing else {
        return true;
    };
    match existing.state {
        // Unresolved is not a decision: a later report deserves another look, since
        // the policy or the API's answer may have changed since
        TargetState::Pending | TargetState::Failed | TargetState::Unresolved => true,
        TargetState::Listed => kind == TargetKind::Tape,
        TargetState::Clean
        | TargetState::Removed
        | TargetState::Gone
        | TargetState::Unlisted
        | TargetState::Restored => false,
    }
}
