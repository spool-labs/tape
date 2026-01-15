use tape_solana::{Error, IntoPrimitive, TryFromPrimitive};

/// Exchange program errors (range: 0x80-0x8F).
#[repr(u32)]
#[derive(Debug, Error, Clone, Copy, PartialEq, Eq, IntoPrimitive, TryFromPrimitive)]
pub enum ExchangeError {
    #[error("unknown error")]
    UnknownError = 0x80,

    #[error("arithmetic overflow")]
    Overflow = 0x81,

    #[error("arithmetic underflow")]
    Underflow = 0x82,

    #[error("insufficient funds")]
    InsufficientFunds = 0x83,

    #[error("unexpected state")]
    UnexpectedState = 0x84,
}

impl From<ExchangeError> for solana_program::program_error::ProgramError {
    fn from(e: ExchangeError) -> Self {
        solana_program::program_error::ProgramError::Custom(e as u32)
    }
}

#[cfg(not(target_os = "solana"))]
impl ExchangeError {
    /// Decode from a raw error code
    pub fn from_code(code: u32) -> Option<Self> {
        Self::try_from(code).ok()
    }

    /// Parse from error string like "Custom(0x81)"
    pub fn from_error_string(s: &str) -> Option<Self> {
        let code = super::tapedrive::parse_error_code(s)?;
        Self::from_code(code)
    }

    /// Exchange errors are not retriable
    pub fn is_retriable(&self) -> bool {
        false
    }

    /// User-friendly message for CLI/UI
    pub fn user_message(&self) -> &'static str {
        match self {
            Self::UnknownError => "Unknown error",
            Self::Overflow => "Amount too large",
            Self::Underflow => "Amount too small",
            Self::InsufficientFunds => "Insufficient balance",
            Self::UnexpectedState => "Unexpected account state",
        }
    }
}
