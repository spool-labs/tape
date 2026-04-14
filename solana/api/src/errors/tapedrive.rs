use tape_solana::{Error, IntoPrimitive, TryFromPrimitive};

/// Tapedrive program errors (range: 0x10-0x7F).
///
/// Used both on-chain (in the program) and off-chain (in clients).
#[repr(u32)]
#[derive(Debug, Error, Clone, Copy, PartialEq, Eq, IntoPrimitive, TryFromPrimitive)]
pub enum TapeError {
    // General (0x10)
    #[error("unexpected state")]
    UnexpectedState = 0x10,

    // Crypto (0x11-0x12)
    #[error("bad bls proof")]
    BadBlsProof = 0x11,
    #[error("bad signature")]
    BadSignature = 0x12,

    // Archive (0x20-0x25)
    #[error("no capacity")]
    NoCapacity = 0x20,
    #[error("no space")]
    NoSpace = 0x21,
    #[error("tape expired")]
    TapeExpired = 0x22,
    #[error("not expired")]
    NotExpired = 0x23,
    #[error("not empty")]
    NotEmpty = 0x24,
    #[error("cannot merge")]
    CannotMerge = 0x25,

    // Epoch (0x30-0x33)
    #[error("bad epoch state")]
    BadEpochState = 0x30,
    #[error("too soon")]
    TooSoon = 0x31,
    #[error("bad schedule")]
    BadSchedule = 0x32,
    #[error("bad epoch id")]
    BadEpochId = 0x33,
    #[error("snapshot incomplete")]
    SnapshotIncomplete = 0x34,

    // Committee (0x40-0x45)
    #[error("no quorum")]
    NoQuorum = 0x40,
    #[error("no signers")]
    NoSigners = 0x41,
    #[error("bad member")]
    BadMember = 0x42,
    #[error("not in committee")]
    NotInCommittee = 0x43,
    #[error("bad spool hash")]
    BadSpoolHash = 0x44,
    #[error("insufficient committee")]
    InsufficientCommittee = 0x45,

    // Node (0x50-0x55)
    #[error("node stale")]
    NodeStale = 0x50,
    #[error("already synced")]
    AlreadySynced = 0x51,
    #[error("already advanced")]
    AlreadyAdvanced = 0x52,
    #[error("no rewards")]
    NoRewards = 0x53,
    #[error("rewards overflow")]
    RewardsOverflow = 0x54,
    #[error("no commission")]
    NoCommission = 0x55,

    // Staking (0x60-0x67)
    #[error("staking failed")]
    StakingFailed = 0x60,
    #[error("bad stake state")]
    BadStakeState = 0x61,
    #[error("not staked")]
    NotStaked = 0x62,
    #[error("epoch mismatch")]
    EpochMismatch = 0x63,
    #[error("rate missing")]
    RateMissing = 0x64,
    #[error("epoch not reached")]
    EpochNotReached = 0x65,
    #[error("zero shares")]
    ZeroShares = 0x66,
    #[error("pool accounting failed")]
    PoolAccountingFailed = 0x67,

    // Commitment (0x70-0x74)
    #[error("bad proof")]
    BadProof = 0x70,
    #[error("list full")]
    ListFull = 0x71,
    #[error("invalid commitment")]
    InvalidCommitment = 0x72,
    #[error("already invalidated")]
    AlreadyInvalidated = 0x73,
    #[error("already certified")]
    AlreadyCertified = 0x74,
}

impl From<TapeError> for solana_program::program_error::ProgramError {
    fn from(e: TapeError) -> Self {
        solana_program::program_error::ProgramError::Custom(e as u32)
    }
}

// Client-only code (not compiled for BPF)
#[cfg(not(target_os = "solana"))]
impl TapeError {
    /// Decode from a raw error code
    pub fn from_code(code: u32) -> Option<Self> {
        Self::try_from(code).ok()
    }

    /// Parse from error string like "Custom(0x30)"
    pub fn from_error_string(s: &str) -> Option<Self> {
        let code = parse_error_code(s)?;
        Self::from_code(code)
    }

    /// Errors that indicate the operation already completed
    pub fn is_already_done(&self) -> bool {
        matches!(
            self,
            Self::BadEpochState
                | Self::AlreadyAdvanced
                | Self::AlreadySynced
                | Self::AlreadyInvalidated
                | Self::AlreadyCertified
                | Self::UnexpectedState
        )
    }

    /// Errors that indicate retry later
    pub fn is_retriable(&self) -> bool {
        matches!(self, Self::TooSoon | Self::InsufficientCommittee | Self::SnapshotIncomplete)
    }

    /// Action required before retrying
    pub fn required_action(&self) -> Option<RequiredAction> {
        match self {
            Self::NodeStale => Some(RequiredAction::AdvancePool),
            _ => None,
        }
    }

    /// User-friendly message for CLI/UI
    pub fn user_message(&self) -> &'static str {
        match self {
            Self::UnexpectedState => "The account is in an unexpected state",
            Self::BadBlsProof => "Invalid BLS signature provided",
            Self::BadSignature => "Invalid signature",
            Self::NoCapacity => "No storage capacity available",
            Self::NoSpace => "Insufficient space on tape",
            Self::TapeExpired => "Tape has expired",
            Self::NotExpired => "Tape has not expired yet",
            Self::NotEmpty => "Tape is not empty",
            Self::CannotMerge => "Tapes cannot be merged",
            Self::BadEpochState => "Epoch is not in the expected phase",
            Self::TooSoon => "Please wait - epoch duration has not elapsed",
            Self::BadSchedule => "Invalid schedule",
            Self::BadEpochId => "Invalid epoch ID",
            Self::SnapshotIncomplete => "Previous epoch snapshot not yet complete",
            Self::NoQuorum => "Quorum not reached",
            Self::NoSigners => "No signers provided",
            Self::BadMember => "Invalid committee member",
            Self::NotInCommittee => "Node is not in the committee",
            Self::BadSpoolHash => "Spool hash mismatch",
            Self::InsufficientCommittee => "Not enough committee members",
            Self::NodeStale => "Node is behind - run advance-pool first",
            Self::AlreadySynced => "Node has already synced",
            Self::AlreadyAdvanced => "Already advanced",
            Self::NoRewards => "No rewards to claim",
            Self::RewardsOverflow => "Rewards calculation overflow",
            Self::NoCommission => "No commission to claim",
            Self::StakingFailed => "Staking operation failed",
            Self::BadStakeState => "Invalid stake state",
            Self::NotStaked => "Not staked",
            Self::EpochMismatch => "Epoch mismatch",
            Self::RateMissing => "Rate not found",
            Self::EpochNotReached => "Target epoch not reached",
            Self::ZeroShares => "Cannot operate on zero shares",
            Self::PoolAccountingFailed => "Pool accounting failed during advance",
            Self::BadProof => "Invalid proof",
            Self::ListFull => "Blacklist is full",
            Self::InvalidCommitment => "Leaf hashes do not match commitment root",
            Self::AlreadyInvalidated => "Track already invalidated",
            Self::AlreadyCertified => "Track already certified",
        }
    }
}

#[cfg(not(target_os = "solana"))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RequiredAction {
    AdvancePool,
}

#[cfg(not(target_os = "solana"))]
pub(crate) fn parse_error_code(s: &str) -> Option<u32> {
    // Try hex format: "0x30" or "Custom(0x30)"
    if let Some(start) = s.find("0x") {
        let hex: String = s[start + 2..]
            .chars()
            .take_while(|c| c.is_ascii_hexdigit())
            .collect();
        if !hex.is_empty() {
            return u32::from_str_radix(&hex, 16).ok();
        }
    }
    // Try decimal format: "Custom(48)"
    if let Some(start) = s.find("Custom(") {
        let num: String = s[start + 7..]
            .chars()
            .take_while(|c| c.is_ascii_digit())
            .collect();
        if !num.is_empty() {
            return num.parse().ok();
        }
    }
    None
}
