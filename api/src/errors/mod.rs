mod exchange;
mod tapedrive;

pub use exchange::ExchangeError;
pub use tapedrive::TapeError;

#[cfg(not(target_os = "solana"))]
pub use tapedrive::RequiredAction;

/// Unified error type for all tapedrive programs (client-only).
///
/// Error code ranges:
/// - TapeError: 0x10-0x7F
/// - ExchangeError: 0x80-0x8F
#[cfg(not(target_os = "solana"))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProgramError {
    Tape(TapeError),
    Exchange(ExchangeError),
}

#[cfg(not(target_os = "solana"))]
impl ProgramError {
    /// Try to decode from a raw error code.
    pub fn from_code(code: u32) -> Option<Self> {
        // Try tapedrive first (0x10-0x7F)
        if let Some(e) = TapeError::from_code(code) {
            return Some(Self::Tape(e));
        }
        // Then exchange (0x80-0x8F)
        if let Some(e) = ExchangeError::from_code(code) {
            return Some(Self::Exchange(e));
        }
        None
    }

    /// Parse from error string like "Custom(0x40)"
    pub fn from_error_string(s: &str) -> Option<Self> {
        if let Some(e) = TapeError::from_error_string(s) {
            return Some(Self::Tape(e));
        }
        if let Some(e) = ExchangeError::from_error_string(s) {
            return Some(Self::Exchange(e));
        }
        for code in parse_codes(s) {
            if let Some(err) = Self::from_code(code) {
                return Some(err);
            }
        }
        None
    }

    /// Whether this error indicates the operation already completed
    pub fn is_already_done(&self) -> bool {
        match self {
            Self::Tape(e) => e.is_already_done(),
            Self::Exchange(_) => false,
        }
    }

    /// Whether this error indicates retry later
    pub fn is_retriable(&self) -> bool {
        match self {
            Self::Tape(e) => e.is_retriable(),
            Self::Exchange(e) => e.is_retriable(),
        }
    }

    /// Action required before retrying
    pub fn required_action(&self) -> Option<RequiredAction> {
        match self {
            Self::Tape(e) => e.required_action(),
            Self::Exchange(_) => None,
        }
    }

    /// User-friendly message for CLI/UI
    pub fn user_message(&self) -> &'static str {
        match self {
            Self::Tape(e) => e.user_message(),
            Self::Exchange(e) => e.user_message(),
        }
    }
}

#[cfg(not(target_os = "solana"))]
impl std::fmt::Display for ProgramError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Tape(e) => write!(f, "{}", e),
            Self::Exchange(e) => write!(f, "{}", e),
        }
    }
}

#[cfg(not(target_os = "solana"))]
impl std::error::Error for ProgramError {}

#[cfg(not(target_os = "solana"))]
fn parse_codes(s: &str) -> Vec<u32> {
    let mut out = Vec::new();

    let mut scan = s;
    while let Some(idx) = scan.find("0x") {
        let start = idx + 2;
        let hex: String = scan[start..]
            .chars()
            .take_while(|c| c.is_ascii_hexdigit())
            .collect();
        if !hex.is_empty() {
            if let Ok(code) = u32::from_str_radix(&hex, 16) {
                out.push(code);
            }
        }
        scan = &scan[start..];
    }

    let mut scan = s;
    while let Some(idx) = scan.find("Custom(") {
        let start = idx + 7;
        let dec: String = scan[start..]
            .chars()
            .take_while(|c| c.is_ascii_digit())
            .collect();
        if !dec.is_empty() {
            if let Ok(code) = dec.parse::<u32>() {
                out.push(code);
            }
        }
        scan = &scan[start..];
    }

    out
}

#[cfg(test)]
mod tests {
    use super::{parse_codes, ProgramError};
    use crate::errors::TapeError;

    #[test]
    fn parse_many() {
        let got = parse_codes("a 0x1 b 0x34 c Custom(81)");
        assert_eq!(got, vec![1, 52, 81]);
    }

    #[test]
    fn parse_chain() {
        let msg = "x 0xdead y custom program error: 0x34";
        let got = ProgramError::from_error_string(msg);
        assert_eq!(got, Some(ProgramError::Tape(TapeError::SnapshotIncomplete)));
    }
}
