use steel::*;

#[repr(u32)]
#[derive(Debug, Error, Clone, Copy, PartialEq, Eq, IntoPrimitive)]
pub enum TapeError {
    #[error("Unknown error")]
    UnknownError = 0,

    #[error("The provided tape is in an unexpected state")]
    UnexpectedState         = 0x10,
    #[error("The tape write failed")]
    WriteFailed             = 0x11,
    #[error("The tape is too long")]
    TapeTooLong             = 0x12,
    #[error("The tape does not have enough rent")]
    InsufficientRent        = 0x13,

    #[error("The provided hash is invalid")]
    SolutionInvalid         = 0x20,
    #[error("The provided tape doesn't match the expected tape")]
    UnexpectedTape          = 0x21,
    #[error("The provided hash did not satisfy the minimum required difficulty")]
    SolutionTooEasy         = 0x22,
    #[error("The provided solution is too early")]
    SolutionTooEarly        = 0x23,
    #[error("The provided claim is too large")]
    ClaimTooLarge           = 0x24,
    #[error("Computed commitment does not match the miner commitment")]
    CommitmentMismatch      = 0x25,

    #[error("Faild to pack the tape into the spool")]
    SpoolPackFailed         = 0x30,
    #[error("Failed to unpack the tape from the spool")]
    SpoolUnpackFailed       = 0x31,
    #[error("Too many tapes in the spool")]
    SpoolTooManyTapes       = 0x32,
    #[error("Spool commit failed")]
    SpoolCommitFailed       = 0x33,
}

error!(TapeError);
