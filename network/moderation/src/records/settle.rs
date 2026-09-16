//! Multi-table changes committed in one transaction each

use rusqlite::{params, Connection};
use tape_blacklist::entry::{Action, Entry};
use tape_crypto::address::Address;
use tape_crypto::Hash;

use crate::publisher::writer::WrittenTrack;
use crate::records::error::RecordsError;
use crate::records::rows::{Adopted, CheckSettlement, Job, Removal, SettledCheck, TrackContent};
use crate::records::states::{DecisionSource, TapeState, TargetState};
use crate::records::{checks, decisions, jobs, tapes, targets, tracks};
use crate::types::ids::{CheckId, DecisionId, TrackId};
use crate::types::target::Target;
use crate::types::timestamp::Timestamp;
use crate::worker::tape_rule::TapeRule;

/// Decisions held by present entries tracks on vacating tapes queue again
const LAPSE_HOLDERS: &str = "UPDATE decisions SET track = NULL WHERE track IN (
    SELECT t.id FROM tracks t JOIN tapes p ON p.address = t.tape
    WHERE p.state = 'vacating' AND t.kind = 'entries' AND t.state = 'present')";

/// Those tracks keep counting until their tape leaves the rack, but hold nothing
const LAPSE_TRACKS: &str = "UPDATE tracks SET state = 'lapsing', updated_at = ?1
    WHERE kind = 'entries' AND state = 'present'
      AND tape IN (SELECT address FROM tapes WHERE state = 'vacating')";

/// Decisions held by a track that names an undone decision queue again
const UNDO_HOLDERS: &str = "UPDATE decisions SET track = NULL WHERE track IN (
    SELECT e.track FROM decisions undone
    JOIN entries e ON e.decision = undone.id
    JOIN tracks t ON t.id = e.track
    WHERE undone.undone_at IS NOT NULL AND t.state IN ('present', 'lapsing'))";

/// Every landed track still counting that names an undone decision is to be deleted
const UNDO_TRACKS: &str = "UPDATE tracks SET state = 'deleting', updated_at = ?1
    WHERE state IN ('present', 'lapsing') AND id IN (
        SELECT e.track FROM decisions undone JOIN entries e ON e.decision = undone.id
        WHERE undone.undone_at IS NOT NULL)";

/// A vacating tape retires once every live decision is held by a present track, none of them its own
const RETIRE_TAPES: &str = "UPDATE tapes SET state = 'retired', updated_at = ?1
    WHERE state = 'vacating'
      AND NOT EXISTS (SELECT 1 FROM tracks WHERE state = 'writing' AND kind = 'entries')
      AND NOT EXISTS (SELECT 1 FROM decisions WHERE track IS NULL AND undone_at IS NULL)";

/// Present rack tracks naming a retired or expired tape are to be deleted
const UNNAME_TRACKS: &str = "UPDATE tracks SET state = 'deleting', updated_at = ?1
    WHERE kind = 'records' AND state = 'present' AND id IN (
        SELECT r.track FROM rack_records r JOIN tapes p ON p.address = r.tape
        WHERE p.state IN ('retired', 'expired'))";

/// Tracks on a retired tape count for nothing once no rack track names it
const DROP_UNREACHED: &str = "UPDATE tracks SET state = 'dropped', updated_at = ?1
    WHERE state IN ('present', 'lapsing', 'deleting') AND tape IN (
        SELECT p.address FROM tapes p WHERE p.state = 'retired' AND NOT EXISTS (
            SELECT 1 FROM rack_records r JOIN tracks t ON t.id = r.track
            WHERE r.tape = p.address AND t.state IN ('writing', 'present', 'deleting')))";

/// Record a check, its verdict's decisions and the tape rule, then finish the job
pub fn check(
    connection: &mut Connection,
    settlement: &CheckSettlement,
    rule: &TapeRule,
) -> Result<SettledCheck, RecordsError> {
    let transaction = connection.transaction()?;
    let now = settlement.attempt.checked_at;
    let check = checks::insert(&transaction, &settlement.attempt)?;

    let mut settled = SettledCheck::default();
    match &settlement.removal {
        Some(removal) => remove(&transaction, settlement, removal, check, rule, &mut settled)?,
        None => {
            let track = Target::track(settlement.attempt.track);
            targets::settle_state(&transaction, track, settlement.state, settlement.tape, now)?;
        }
    }

    jobs::finish(&transaction, settlement.job.id, now)?;
    transaction.commit()?;
    Ok(settled)
}

/// Queue a check of each listed track, then finish the tape's job
pub fn listing(
    connection: &mut Connection,
    job: &Job,
    tracks: &[Address],
    now: Timestamp,
) -> Result<usize, RecordsError> {
    let transaction = connection.transaction()?;
    let mut queued = 0;
    for track in tracks {
        if jobs::queue_check(&transaction, Target::track(*track), Some(job.target.address), now)? {
            queued += 1;
        }
    }
    targets::settle_state(&transaction, job.target, TargetState::Listed, None, now)?;
    jobs::finish(&transaction, job.id, now)?;
    transaction.commit()?;
    Ok(queued)
}

/// An operator's remove or unlist
pub fn operator(
    connection: &mut Connection,
    entry: &Entry,
    now: Timestamp,
) -> Result<DecisionId, RecordsError> {
    let target = Target::from_blacklist_entry(&entry.target).ok_or(RecordsError::InvalidEntry)?;
    let state = match entry.action {
        Action::Remove => TargetState::Removed,
        Action::Unlist => TargetState::Unlisted,
    };

    let transaction = connection.transaction()?;
    let source = DecisionSource::Operator;
    let id = decisions::insert(&transaction, entry, source, None, now)?;
    targets::set_state(&transaction, target, state, None, Some(source), now)?;
    transaction.commit()?;
    Ok(id)
}

/// An operator's restore: undo this producer's live decisions on a target
pub fn restore(connection: &mut Connection, target: Target, now: Timestamp) -> Result<usize, RecordsError> {
    let transaction = connection.transaction()?;
    let undone = decisions::undo(&transaction, target, now)?;
    let source = Some(DecisionSource::Operator);
    targets::set_state(&transaction, target, TargetState::Restored, None, source, now)?;
    transaction.commit()?;
    Ok(undone)
}

/// Record a track about to be written and what it holds
pub fn planned(
    connection: &mut Connection,
    tape: Address,
    content: &TrackContent,
    bytes: &[u8],
    now: Timestamp,
) -> Result<TrackId, RecordsError> {
    let transaction = connection.transaction()?;
    let track = tracks::plan(&transaction, tape, content, bytes, now)?;
    transaction.commit()?;
    Ok(track)
}

pub fn landed(
    connection: &mut Connection,
    track: TrackId,
    written: &WrittenTrack,
    now: Timestamp,
) -> Result<(), RecordsError> {
    let transaction = connection.transaction()?;
    tracks::land(&transaction, track, written, now)?;
    transaction.commit()?;
    Ok(())
}

pub fn adopted(
    connection: &mut Connection,
    tape: Address,
    written: &WrittenTrack,
    key: Hash,
    now: Timestamp,
) -> Result<Adopted, RecordsError> {
    let transaction = connection.transaction()?;
    let adopted = tracks::adopt(&transaction, tape, written, key, now)?;
    transaction.commit()?;
    Ok(adopted)
}

pub fn abandoned(connection: &mut Connection, tape: Address, now: Timestamp) -> Result<usize, RecordsError> {
    let transaction = connection.transaction()?;
    let abandoned = tracks::abandon(&transaction, tape, now)?;
    transaction.commit()?;
    Ok(abandoned)
}

pub fn deleted(connection: &mut Connection, deleted: &[TrackId], now: Timestamp) -> Result<(), RecordsError> {
    let transaction = connection.transaction()?;
    for track in deleted {
        tracks::mark_deleted(&transaction, *track, now)?;
    }
    transaction.commit()?;
    Ok(())
}

/// Record an expired blacklist tape: nothing on it counts, and its decisions queue again
pub fn expire(connection: &mut Connection, tape: Address, now: Timestamp) -> Result<(), RecordsError> {
    let transaction = connection.transaction()?;
    tracks::abandon(&transaction, tape, now)?;
    transaction.execute(
        "UPDATE decisions SET track = NULL
         WHERE track IN (SELECT id FROM tracks WHERE tape = ?1 AND state = 'present')",
        params![tape.to_string()],
    )?;
    transaction.execute(
        "UPDATE tracks SET state = 'dropped', updated_at = ?2
         WHERE tape = ?1 AND state IN ('present', 'lapsing', 'deleting')",
        params![tape.to_string(), now.0],
    )?;
    tapes::set_state(&transaction, tape, TapeState::Expired, now)?;
    transaction.commit()?;
    Ok(())
}

/// Move tracks and tapes along once what each waits on is recorded
///
/// Vacating tapes lapse their entries tracks, tracks naming undone decisions are
/// marked for deletion, vacating tapes retire once everything they held is held
/// elsewhere, and retired or expired tapes lose their rack records.
pub fn sweep(connection: &mut Connection, now: Timestamp) -> Result<(), RecordsError> {
    let transaction = connection.transaction()?;
    transaction.execute(LAPSE_HOLDERS, [])?;
    transaction.execute(LAPSE_TRACKS, params![now.0])?;
    transaction.execute(UNDO_HOLDERS, [])?;
    transaction.execute(UNDO_TRACKS, params![now.0])?;
    transaction.execute(RETIRE_TAPES, params![now.0])?;
    transaction.execute(UNNAME_TRACKS, params![now.0])?;
    transaction.execute(DROP_UNREACHED, params![now.0])?;
    transaction.commit()?;
    Ok(())
}

fn remove(
    connection: &Connection,
    settlement: &CheckSettlement,
    removal: &Removal,
    check: CheckId,
    rule: &TapeRule,
    settled: &mut SettledCheck,
) -> Result<(), RecordsError> {
    let now = settlement.attempt.checked_at;
    let mut tracks = vec![(settlement.attempt.track, DecisionSource::Check)];
    for chunk in &removal.chunks {
        tracks.push((*chunk, DecisionSource::Stream));
    }

    for (address, source) in tracks {
        let target = Target::track(address);
        if targets::find(connection, target)?.is_some_and(|row| row.state.is_decided()) {
            continue;
        }
        let entry = Entry::remove(address);
        settled.decisions.push(decisions::insert(connection, &entry, source, Some(check), now)?);
        targets::set_state(connection, target, TargetState::Removed, settlement.tape, Some(source), now)?;
    }

    match settlement.tape {
        Some(tape) => apply_tape_rule(connection, tape, removal, check, rule, now, settled),
        None => Ok(()),
    }
}

fn apply_tape_rule(
    connection: &Connection,
    tape: Address,
    removal: &Removal,
    check: CheckId,
    rule: &TapeRule,
    now: Timestamp,
    settled: &mut SettledCheck,
) -> Result<(), RecordsError> {
    let target = Target::tape(tape);
    if targets::find(connection, target)?.is_some_and(|row| row.state.is_decided()) {
        return Ok(());
    }
    let removals = targets::removals_on_tape(connection, tape)?;
    if !rule.should_unlist(&removal.categories, removals) {
        return Ok(());
    }

    let source = DecisionSource::TapeRule;
    let entry = Entry::unlist(tape);
    settled.decisions.push(decisions::insert(connection, &entry, source, Some(check), now)?);
    targets::set_state(connection, target, TargetState::Unlisted, None, Some(source), now)?;
    settled.is_tape_unlisted = true;
    Ok(())
}
