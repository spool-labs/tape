//! States and labels records store as text

/// Where a target stands with this producer
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum TargetState {
    /// A check is queued
    Pending,
    Clean,
    Removed,
    /// Checked, but not provably clean and nothing flagged
    Unresolved,
    /// Checks kept failing until the attempts ran out
    Failed,
    /// The gateway already refuses it
    Gone,
    /// A tape whose tracks were queued for checks
    Listed,
    Unlisted,
    /// An operator undid this producer's decisions on it
    Restored,
}

/// Whether a job still needs running
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum JobState {
    Pending,
    Done,
    Failed,
}

/// What one check attempt found
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum CheckOutcome {
    Remove,
    Clean,
    Unresolved,
    Unsupported,
    Gone,
    Error,
}

/// What produced a decision or the state a decision set
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum DecisionSource {
    Check,
    /// A chunk track removed along with its stream's manifest
    Stream,
    TapeRule,
    Operator,
}

/// What a tape is to the list
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum TapeRole {
    /// The rack consumers subscribe to
    Rack,
    /// A tape holding blacklist entries, named by the rack
    Blacklist,
}

/// A rack or blacklist tape's place in its lifecycle
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum TapeState {
    /// Key persisted, reservation not yet confirmed
    Reserving,
    /// Reserved and in use
    Reserved,
    /// Near expiry: its decisions are written again on the newest tape, then it leaves the rack
    Vacating,
    /// Leaving or out of the rack; nothing on it counts once no rack track names it
    Retired,
    /// Expired on chain; nothing on it counts
    Expired,
}

/// What a track holds
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum TrackKind {
    /// A tape's header, alone in track 0
    Header,
    /// Blacklist entries
    Entries,
    /// Rack records naming blacklist tapes
    Records,
    /// A landed track whose bytes match nothing recorded
    Unknown,
}

/// Where a track stands on chain
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum TrackState {
    /// Recorded before writing, not yet known to have landed
    Writing,
    /// Landed and counting, holding the decisions that point at it
    Present,
    /// Landed on a vacating tape: counts until its tape leaves the rack, holds nothing
    Lapsing,
    /// Landed, and deleted once what it names that should stay is present on another track
    Deleting,
    Deleted,
    /// Counts for nothing: its tape expired or left the rack
    Dropped,
    /// Never landed, on a tape that can take no more tracks
    Abandoned,
}

impl TargetState {
    const ALL: [Self; 9] = [
        Self::Pending,
        Self::Clean,
        Self::Removed,
        Self::Unresolved,
        Self::Failed,
        Self::Gone,
        Self::Listed,
        Self::Unlisted,
        Self::Restored,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Clean => "clean",
            Self::Removed => "removed",
            Self::Unresolved => "unresolved",
            Self::Failed => "failed",
            Self::Gone => "gone",
            Self::Listed => "listed",
            Self::Unlisted => "unlisted",
            Self::Restored => "restored",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|state| state.as_str() == value)
    }

    /// A decision holds this state; later checks never overwrite it
    pub fn is_decided(self) -> bool {
        matches!(self, Self::Removed | Self::Unlisted | Self::Restored)
    }
}

impl JobState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Done => "done",
            Self::Failed => "failed",
        }
    }
}

impl CheckOutcome {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Remove => "remove",
            Self::Clean => "clean",
            Self::Unresolved => "unresolved",
            Self::Unsupported => "unsupported",
            Self::Gone => "gone",
            Self::Error => "error",
        }
    }
}

impl DecisionSource {
    const ALL: [Self; 4] = [Self::Check, Self::Stream, Self::TapeRule, Self::Operator];

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Check => "check",
            Self::Stream => "stream",
            Self::TapeRule => "tape-rule",
            Self::Operator => "operator",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|source| source.as_str() == value)
    }
}

impl TapeRole {
    const ALL: [Self; 2] = [Self::Rack, Self::Blacklist];

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Rack => "rack",
            Self::Blacklist => "blacklist",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|role| role.as_str() == value)
    }
}

impl TapeState {
    const ALL: [Self; 5] = [
        Self::Reserving,
        Self::Reserved,
        Self::Vacating,
        Self::Retired,
        Self::Expired,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Reserving => "reserving",
            Self::Reserved => "reserved",
            Self::Vacating => "vacating",
            Self::Retired => "retired",
            Self::Expired => "expired",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|state| state.as_str() == value)
    }
}

impl TrackKind {
    const ALL: [Self; 4] = [Self::Header, Self::Entries, Self::Records, Self::Unknown];

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Header => "header",
            Self::Entries => "entries",
            Self::Records => "records",
            Self::Unknown => "unknown",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|kind| kind.as_str() == value)
    }
}

impl TrackState {
    const ALL: [Self; 7] = [
        Self::Writing,
        Self::Present,
        Self::Lapsing,
        Self::Deleting,
        Self::Deleted,
        Self::Dropped,
        Self::Abandoned,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Writing => "writing",
            Self::Present => "present",
            Self::Lapsing => "lapsing",
            Self::Deleting => "deleting",
            Self::Deleted => "deleted",
            Self::Dropped => "dropped",
            Self::Abandoned => "abandoned",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|state| state.as_str() == value)
    }
}
