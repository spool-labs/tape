use steel::*;

#[repr(u32)]
#[derive(Debug, Error, Clone, Copy, PartialEq, Eq, IntoPrimitive)]
pub enum TapeError {
    #[error("Unknown error")]
    UnknownError = 0,

    #[error("The provided account is in an unexpected state")]
    UnexpectedState         = 0x10,


    #[error("The provided account has insufficient funds")]
    InsufficientFunds       = 0x20,

}

error!(TapeError);
