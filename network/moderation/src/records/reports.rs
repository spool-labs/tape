//! Committing reports

use rusqlite::{params, Connection};

use crate::records::error::RecordsError;
use crate::records::rows::{NewReport, SubmitOutcome};
use crate::records::{jobs, targets};
use crate::types::timestamp::Timestamp;

/// Commit a report and queue its check, ignoring a repeated idempotency key
pub fn submit(
    connection: &mut Connection,
    report: &NewReport,
    now: Timestamp,
) -> Result<SubmitOutcome, RecordsError> {
    let transaction = connection.transaction()?;
    let inserted = transaction.execute(
        "INSERT INTO reports
             (received_at, target_kind, target, reason, note, idempotency_key, reporter_hash)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
         ON CONFLICT (idempotency_key) DO NOTHING",
        params![
            now.0,
            report.target.kind.as_str(),
            report.target.address.to_string(),
            report.reason.as_str(),
            report.note,
            report.idempotency_key,
            report.reporter_hash,
        ],
    )?;

    let outcome = match inserted {
        0 => SubmitOutcome::Duplicate,
        _ => {
            targets::note_report(&transaction, report.target, now)?;
            jobs::queue_check(&transaction, report.target, None, now)?;
            SubmitOutcome::Accepted
        }
    };
    transaction.commit()?;
    Ok(outcome)
}
