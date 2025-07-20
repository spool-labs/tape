use steel::*;

#[repr(u32)]
#[derive(Debug, Error, Clone, Copy, PartialEq, Eq, IntoPrimitive)]
pub enum TapeError {
    #[error("Unknown error")]
    UnknownError = 0,

    #[error("The provided tape is in an unexpected state")]
    UnexpectedState = 10,
    #[error("The tape write failed")]
    WriteFailed = 11,
    #[error("The tape is too long")]
    TapeTooLong = 12,

    #[error("The provided hash is invalid")]
    SolutionInvalid = 20,
    #[error("The provided hash did not satisfy the minimum required difficulty")]
    SolutionTooEasy = 21,
    #[error("The provided claim is too large")]
    ClaimTooLarge = 22,
}

error!(TapeError);
