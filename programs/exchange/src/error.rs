use steel::*;

#[repr(u32)]
#[derive(Debug, Error, Clone, Copy, PartialEq, Eq, IntoPrimitive)]
pub enum ExchangeError {
    #[error("Unknown error")]
    UnknownError = 0,

    #[error("Arithmetic overflow occurred")]
    Overflow                = 0x01,
    #[error("Arithmetic underflow occurred")]
    Underflow               = 0x02,

    #[error("The provided account has insufficient funds")]
    InsufficientFunds       = 0x10,
    #[error("The provided account is in an unexpected state")]
    UnexpectedState         = 0x20,
}

error!(ExchangeError);
