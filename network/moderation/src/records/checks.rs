//! Check attempts and every category the moderation API returned

use rusqlite::{params, Connection};

use crate::records::error::RecordsError;
use crate::records::rows::CheckAttempt;
use crate::types::ids::CheckId;

/// Separates applied input types in one text column
const INPUT_TYPE_SEPARATOR: &str = ",";

pub fn insert(connection: &Connection, attempt: &CheckAttempt) -> Result<CheckId, RecordsError> {
    connection.execute(
        "INSERT INTO checks
             (job_id, track, checked_at, outcome, detail, content_type, input_type,
              read_bytes, total_bytes, is_complete, response_id, model)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)",
        params![
            attempt.job.0,
            attempt.track.to_string(),
            attempt.checked_at.0,
            attempt.outcome.as_str(),
            attempt.detail,
            attempt.content_type,
            attempt.input_type.map(|input_type| input_type.as_str()),
            attempt.read_bytes,
            attempt.total_bytes,
            attempt.is_complete,
            attempt.response_id,
            attempt.model,
        ],
    )?;
    let check = CheckId(connection.last_insert_rowid());

    let mut statement = connection.prepare(
        "INSERT INTO check_categories
             (check_id, result_index, category, flagged, score, applied_input_types)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
    )?;
    for score in &attempt.categories {
        let applied = score
            .applied_input_types
            .as_ref()
            .map(|types| types.join(INPUT_TYPE_SEPARATOR));
        statement.execute(params![
            check.0,
            score.result_index,
            score.category,
            score.flagged,
            score.score,
            applied,
        ])?;
    }
    Ok(check)
}
