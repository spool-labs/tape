//! The records handle: every query runs on a blocking thread

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use rusqlite::Connection;
use tape_blacklist::entry::Entry;
use tape_core::types::TrackNumber;
use tape_crypto::address::Address;
use tape_crypto::Hash;
use tokio::task::spawn_blocking;

use crate::publisher::writer::WrittenTrack;
use crate::records::error::RecordsError;
use crate::records::rows::{
    Adopted, CheckSettlement, Decision, Job, JobFailure, ListTape, ListTapes, NewReport, SettledCheck,
    StatusSummary, SubmitOutcome, TapeObservation, TrackContent, TrackRow,
};
use crate::records::schema::SCHEMA;
use crate::records::states::{TapeRole, TapeState, TrackState};
use crate::records::{decisions, jobs, reports, settle, status, tapes, tracks};
use crate::types::ids::{DecisionId, TrackCount, TrackId};
use crate::types::target::Target;
use crate::types::timestamp::Timestamp;
use crate::worker::tape_rule::TapeRule;

/// How long a write waits on another process, such as an operator command
const BUSY_TIMEOUT: Duration = Duration::from_secs(5);

const WAL_JOURNAL: &str = "wal";

/// Shared access to the records database
#[derive(Clone)]
pub struct Records {
    connection: Arc<Mutex<Connection>>,
}

impl Records {
    /// Open or create the database, with WAL and full sync so commits are durable
    pub fn open(path: &Path) -> Result<Self, RecordsError> {
        let connection = Connection::open(path).map_err(|source| RecordsError::Open {
            path: path.to_path_buf(),
            source,
        })?;
        connection.busy_timeout(BUSY_TIMEOUT)?;
        let journal: String =
            connection.pragma_update_and_check(None, "journal_mode", "WAL", |row| row.get(0))?;
        if !journal.eq_ignore_ascii_case(WAL_JOURNAL) {
            return Err(RecordsError::Journal(journal));
        }
        connection.pragma_update(None, "synchronous", "FULL")?;
        connection.pragma_update(None, "foreign_keys", "ON")?;
        connection.execute_batch(SCHEMA)?;

        Ok(Self {
            connection: Arc::new(Mutex::new(connection)),
        })
    }

    pub async fn submit_report(
        &self,
        report: NewReport,
        now: Timestamp,
    ) -> Result<SubmitOutcome, RecordsError> {
        self.call(move |connection| reports::submit(connection, &report, now))
            .await
    }

    pub async fn next_due_job(&self, now: Timestamp) -> Result<Option<Job>, RecordsError> {
        self.call(move |connection| jobs::next_due(connection, now))
            .await
    }

    pub async fn settle_check(
        &self,
        settlement: CheckSettlement,
        rule: TapeRule,
    ) -> Result<SettledCheck, RecordsError> {
        self.call(move |connection| settle::check(connection, &settlement, &rule))
            .await
    }

    pub async fn settle_listing(
        &self,
        job: Job,
        tracks: Vec<Address>,
        now: Timestamp,
    ) -> Result<usize, RecordsError> {
        self.call(move |connection| settle::listing(connection, &job, &tracks, now))
            .await
    }

    pub async fn record_job_failure(&self, failure: JobFailure) -> Result<(), RecordsError> {
        self.call(move |connection| jobs::record_failure(connection, &failure))
            .await
    }

    /// Queue an operator's remove or unlist
    pub async fn queue_operator_decision(
        &self,
        entry: Entry,
        now: Timestamp,
    ) -> Result<DecisionId, RecordsError> {
        self.call(move |connection| settle::operator(connection, &entry, now))
            .await
    }

    /// Undo this producer's live decisions on a target, returning how many there were
    pub async fn restore_target(&self, target: Target, now: Timestamp) -> Result<usize, RecordsError> {
        self.call(move |connection| settle::restore(connection, target, now))
            .await
    }

    pub async fn queued_decisions(&self, limit: usize) -> Result<Vec<Decision>, RecordsError> {
        self.call(move |connection| decisions::queued(connection, limit))
            .await
    }

    pub async fn list_tapes(&self) -> Result<ListTapes, RecordsError> {
        self.call(|connection| tapes::list(connection)).await
    }

    pub async fn insert_tape(
        &self,
        address: Address,
        role: TapeRole,
        key_path: PathBuf,
        now: Timestamp,
    ) -> Result<ListTape, RecordsError> {
        self.call(move |connection| tapes::insert(connection, address, role, &key_path, now))
            .await
    }

    pub async fn set_tape_state(
        &self,
        address: Address,
        state: TapeState,
        now: Timestamp,
    ) -> Result<(), RecordsError> {
        self.call(move |connection| tapes::set_state(connection, address, state, now))
            .await
    }

    pub async fn expire_tape(&self, address: Address, now: Timestamp) -> Result<(), RecordsError> {
        self.call(move |connection| settle::expire(connection, address, now))
            .await
    }

    pub async fn observe_tape(
        &self,
        address: Address,
        observation: TapeObservation,
        now: Timestamp,
    ) -> Result<(), RecordsError> {
        self.call(move |connection| tapes::observe(connection, address, &observation, now))
            .await
    }

    pub async fn tapes_to_name(&self) -> Result<Vec<Address>, RecordsError> {
        self.call(|connection| tapes::to_name(connection)).await
    }

    pub async fn sweep(&self, now: Timestamp) -> Result<(), RecordsError> {
        self.call(move |connection| settle::sweep(connection, now))
            .await
    }

    /// Record a track before it is written, returning it as recorded
    pub async fn plan_track(
        &self,
        tape: Address,
        content: TrackContent,
        bytes: Vec<u8>,
        now: Timestamp,
    ) -> Result<TrackRow, RecordsError> {
        self.call(move |connection| {
            let id = settle::planned(connection, tape, &content, &bytes, now)?;
            Ok(TrackRow {
                id,
                tape,
                kind: content.kind(),
                bytes,
                state: TrackState::Writing,
                track_number: None,
                address: None,
                is_delete_sent: false,
            })
        })
        .await
    }

    pub async fn land_track(
        &self,
        track: TrackId,
        written: WrittenTrack,
        now: Timestamp,
    ) -> Result<(), RecordsError> {
        self.call(move |connection| settle::landed(connection, track, &written, now))
            .await
    }

    pub async fn adopt_track(
        &self,
        tape: Address,
        written: WrittenTrack,
        key: Hash,
        now: Timestamp,
    ) -> Result<Adopted, RecordsError> {
        self.call(move |connection| settle::adopted(connection, tape, &written, key, now))
            .await
    }

    pub async fn landed_numbers(&self, tape: Address) -> Result<Vec<TrackNumber>, RecordsError> {
        self.call(move |connection| tracks::landed_numbers(connection, tape))
            .await
    }

    pub async fn writing_tracks(&self, tape: Address) -> Result<Vec<TrackRow>, RecordsError> {
        self.call(move |connection| tracks::writing(connection, tape))
            .await
    }

    pub async fn on_chain_tracks(&self, tape: Address) -> Result<TrackCount, RecordsError> {
        self.call(move |connection| tracks::on_chain(connection, tape))
            .await
    }

    pub async fn sent_deletes(&self, tape: Address) -> Result<Vec<TrackId>, RecordsError> {
        self.call(move |connection| tracks::sent_deletes(connection, tape))
            .await
    }

    pub async fn abandon_writing(&self, tape: Address, now: Timestamp) -> Result<usize, RecordsError> {
        self.call(move |connection| settle::abandoned(connection, tape, now))
            .await
    }

    pub async fn mark_delete_sent(&self, track: TrackId, now: Timestamp) -> Result<(), RecordsError> {
        self.call(move |connection| tracks::mark_delete_sent(connection, track, now))
            .await
    }

    pub async fn mark_deleted(&self, deleted: Vec<TrackId>, now: Timestamp) -> Result<(), RecordsError> {
        self.call(move |connection| settle::deleted(connection, &deleted, now))
            .await
    }

    pub async fn deletable_tracks(&self) -> Result<Vec<TrackRow>, RecordsError> {
        self.call(|connection| tracks::deletable(connection)).await
    }

    pub async fn status(&self, now: Timestamp) -> Result<StatusSummary, RecordsError> {
        self.call(move |connection| status::summary(connection, now))
            .await
    }

    /// Take a write, then read the summary: what readiness and metrics answer from
    pub async fn observe(&self, now: Timestamp) -> Result<StatusSummary, RecordsError> {
        self.call(move |connection| {
            status::probe(connection, now)?;
            status::summary(connection, now)
        })
        .await
    }

    async fn call<Output, Operation>(&self, operation: Operation) -> Result<Output, RecordsError>
    where
        Output: Send + 'static,
        Operation: FnOnce(&mut Connection) -> Result<Output, RecordsError> + Send + 'static,
    {
        let connection = Arc::clone(&self.connection);
        let task = spawn_blocking(move || {
            let mut guard = connection.lock().map_err(|_| RecordsError::Poisoned)?;
            operation(&mut guard)
        });
        task.await
            .map_err(|error| RecordsError::Task(error.to_string()))?
    }
}
