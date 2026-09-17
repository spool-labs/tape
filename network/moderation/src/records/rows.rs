//! Values read from and written to records

use std::path::PathBuf;

use tape_blacklist::entry::Entry;
use tape_core::types::{EpochNumber, StorageUnits, TrackNumber};
use tape_crypto::address::Address;

use crate::records::states::{
    CheckOutcome, DecisionSource, TapeRole, TapeState, TargetState, TrackKind, TrackState,
};
use crate::types::category::Category;
use crate::types::ids::{AttemptCount, DecisionId, JobId, TrackCount, TrackId};
use crate::types::reason::Reason;
use crate::types::target::Target;
use crate::types::timestamp::Timestamp;
use crate::worker::content::InputType;

/// A validated report ready to commit
#[derive(Clone, Debug)]
pub struct NewReport {
    pub target: Target,
    pub reason: Reason,
    pub note: Option<String>,
    pub idempotency_key: String,
    pub reporter_hash: String,
}

/// Whether a report was new
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SubmitOutcome {
    Accepted,
    Duplicate,
}

/// A due check or tape listing
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Job {
    pub id: JobId,
    pub target: Target,
    /// The track's tape, when a tape listing queued it
    pub tape: Option<Address>,
    pub attempts: AttemptCount,
}

/// One check attempt, kept with everything the moderation API returned
#[derive(Clone, Debug)]
pub struct CheckAttempt {
    pub job: JobId,
    pub track: Address,
    pub checked_at: Timestamp,
    pub outcome: CheckOutcome,
    pub detail: Option<String>,
    pub content_type: Option<String>,
    pub input_type: Option<InputType>,
    pub read_bytes: Option<u64>,
    pub total_bytes: Option<u64>,
    pub is_complete: Option<bool>,
    pub response_id: Option<String>,
    pub model: Option<String>,
    pub categories: Vec<CategoryScore>,
}

/// One category of one moderation result, as returned
#[derive(Clone, Debug, PartialEq)]
pub struct CategoryScore {
    pub result_index: u32,
    pub category: String,
    pub flagged: Option<bool>,
    pub score: Option<f64>,
    pub applied_input_types: Option<Vec<String>>,
}

/// What a removal verdict takes down besides the checked track
#[derive(Clone, Debug, Default)]
pub struct Removal {
    /// Chunk tracks of a stream whose manifest was checked
    pub chunks: Vec<Address>,
    /// Removal categories the API flagged
    pub categories: Vec<Category>,
}

/// A finished check to commit in one transaction
#[derive(Clone, Debug)]
pub struct CheckSettlement {
    pub job: Job,
    pub attempt: CheckAttempt,
    pub state: TargetState,
    pub tape: Option<Address>,
    pub removal: Option<Removal>,
}

/// Decisions a settled check queued
#[derive(Clone, Debug, Default)]
pub struct SettledCheck {
    pub decisions: Vec<DecisionId>,
    pub is_tape_unlisted: bool,
}

/// A failed job attempt, retried at `retry_at` or failed for good without it
#[derive(Clone, Debug)]
pub struct JobFailure {
    pub job: Job,
    pub error: String,
    pub retry_at: Option<Timestamp>,
    pub failed_at: Timestamp,
}

/// A decision waiting for a track to hold it
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Decision {
    pub id: DecisionId,
    pub entry: Entry,
    pub source: DecisionSource,
    pub created_at: Timestamp,
}

/// The list's rack or one of its blacklist tapes
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ListTape {
    pub address: Address,
    pub role: TapeRole,
    pub key_path: PathBuf,
    pub state: TapeState,
    /// Every track known to have landed on the tape is below this number
    pub next_track: TrackNumber,
    pub expiry_epoch: Option<EpochNumber>,
    pub capacity: Option<StorageUnits>,
    pub used: Option<StorageUnits>,
    pub observed_at: Option<Timestamp>,

    /// Its track 0 is its present header
    pub is_declared: bool,
    /// A present rack track names it
    pub is_named: bool,

    /// Recorded tracks that landed, deleted ones included
    pub landed_tracks: TrackCount,
    /// Recorded tracks not yet known to have landed
    pub writing_tracks: TrackCount,
    /// Deletes sent whose outcome is not recorded
    pub sent_deletes: TrackCount,
}

/// The list's rack and its blacklist tapes, oldest first
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ListTapes {
    pub rack: Option<ListTape>,
    pub blacklists: Vec<ListTape>,
}

/// A tape as last read from chain
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TapeObservation {
    pub next_track: TrackNumber,
    pub expiry_epoch: EpochNumber,
    pub capacity: StorageUnits,
    pub used: StorageUnits,
}

/// A track the producer planned or found on one of its tapes
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TrackRow {
    pub id: TrackId,
    pub tape: Address,
    pub kind: TrackKind,
    pub bytes: Vec<u8>,
    pub state: TrackState,
    pub track_number: Option<TrackNumber>,
    pub address: Option<Address>,
    pub is_delete_sent: bool,
}

/// What a track about to be written holds
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TrackContent {
    Header,
    Entries(Vec<DecisionId>),
    /// Blacklist tapes the rack track names
    Records(Vec<Address>),
}

/// What a landed track records did not know turned out to be
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Adopted {
    /// A planned track whose write landed
    Planned(TrackId),
    /// A second landing of a recorded track, to delete
    Duplicate(TrackId),
    /// Bytes that match nothing recorded, left alone
    Unknown(TrackId),
}

/// What `status` prints
#[derive(Clone, Debug)]
pub struct StatusSummary {
    pub pending_jobs: u64,
    pub due_jobs: u64,
    pub failed_jobs: u64,
    /// Live decisions no track holds yet
    pub queued_decisions: u64,
    pub oldest_queued: Option<Timestamp>,
    /// Tracks waiting to be deleted
    pub deleting_tracks: u64,
    pub tapes: ListTapes,
}

impl ListTape {
    /// Whether records may lag the chain: a write or delete of unknown outcome, or a landed track not recorded
    pub fn is_uncertain(&self) -> bool {
        self.writing_tracks.0 > 0
            || self.sent_deletes.0 > 0
            || self.next_track.0 > self.landed_tracks.0
    }

    /// Whether the publisher writes or deletes on it
    pub fn is_active(&self) -> bool {
        matches!(self.state, TapeState::Reserved | TapeState::Vacating)
    }

    /// Whether its header is still to be written: it is reserved and nothing landed or is being written
    pub fn needs_header(&self) -> bool {
        self.state == TapeState::Reserved
            && self.next_track.0 == 0
            && self.landed_tracks.0 == 0
            && self.writing_tracks.0 == 0
    }
}

impl ListTapes {
    /// The blacklist tape entries go to: the newest one declared and named
    pub fn newest(&self) -> Option<&ListTape> {
        self.blacklists
            .iter()
            .rev()
            .find(|tape| tape.state == TapeState::Reserved && tape.is_declared && tape.is_named)
    }

    /// A blacklist tape being added: not reserved yet, or reserved and not yet named
    pub fn pending(&self) -> Option<&ListTape> {
        self.blacklists.iter().find(|tape| {
            tape.state == TapeState::Reserving || (tape.state == TapeState::Reserved && !tape.is_named)
        })
    }

    /// The rack, then every blacklist tape oldest first
    pub fn all(&self) -> impl Iterator<Item = &ListTape> + '_ {
        self.rack.iter().chain(&self.blacklists)
    }

    pub fn find(&self, address: Address) -> Option<&ListTape> {
        self.all().find(|tape| tape.address == address)
    }
}

impl TrackContent {
    pub fn kind(&self) -> TrackKind {
        match self {
            Self::Header => TrackKind::Header,
            Self::Entries(_) => TrackKind::Entries,
            Self::Records(_) => TrackKind::Records,
        }
    }
}

impl CheckAttempt {
    pub fn new(job: JobId, track: Address, outcome: CheckOutcome, checked_at: Timestamp) -> Self {
        Self {
            job,
            track,
            checked_at,
            outcome,
            detail: None,
            content_type: None,
            input_type: None,
            read_bytes: None,
            total_bytes: None,
            is_complete: None,
            response_id: None,
            model: None,
            categories: Vec::new(),
        }
    }
}
