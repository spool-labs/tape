use tape_solana::*;

#[repr(u32)]
#[derive(Debug, Error, Clone, Copy, PartialEq, Eq, IntoPrimitive)]
pub enum TapeError {
    // General
    #[error("unexpected state")]
    UnexpectedState     = 0x0010,

    // Crypto
    #[error("bad bls proof")]
    BadBlsProof         = 0x0020,
    #[error("bad signature")]
    BadSignature        = 0x0021,

    // Archive
    #[error("no capacity")]
    NoCapacity          = 0x0030,
    #[error("no space")]
    NoSpace             = 0x0031,
    #[error("tape expired")]
    TapeExpired         = 0x0032,
    #[error("not expired")]
    NotExpired          = 0x0033,
    #[error("not empty")]
    NotEmpty            = 0x0034,
    #[error("cannot merge")]
    CannotMerge         = 0x0035,

    // Epoch
    #[error("bad epoch state")]
    BadEpochState       = 0x0040,
    #[error("too soon")]
    TooSoon             = 0x0041,
    #[error("bad schedule")]
    BadSchedule         = 0x0042,
    #[error("bad epoch id")]
    BadEpochId          = 0x0043,

    // Committee
    #[error("no quorum")]
    NoQuorum            = 0x0050,
    #[error("no signers")]
    NoSigners           = 0x0051,
    #[error("bad member")]
    BadMember           = 0x0052,
    #[error("not in committee")]
    NotInCommittee      = 0x0053,
    #[error("bad spool hash")]
    BadSpoolHash        = 0x0054,

    // Node
    #[error("node stale")]
    NodeStale           = 0x0060,
    #[error("already synced")]
    AlreadySynced       = 0x0061,
    #[error("already advanced")]
    AlreadyAdvanced     = 0x0062,
    #[error("no rewards")]
    NoRewards           = 0x0063,
    #[error("rewards overflow")]
    RewardsOverflow     = 0x0064,
    #[error("no commission")]
    NoCommission        = 0x0065,

    // Staking
    #[error("staking failed")]
    StakingFailed       = 0x0070,
    #[error("bad stake state")]
    BadStakeState       = 0x0071,
    #[error("not staked")]
    NotStaked           = 0x0072,
    #[error("epoch mismatch")]
    EpochMismatch       = 0x0073,
    #[error("rate missing")]
    RateMissing         = 0x0074,
    #[error("epoch not reached")]
    EpochNotReached     = 0x0075,
    #[error("zero shares")]
    ZeroShares          = 0x0076,

    // Blacklist
    #[error("bad proof")]
    BadProof            = 0x0080,
    #[error("list full")]
    ListFull            = 0x0081,
}

error!(TapeError);
