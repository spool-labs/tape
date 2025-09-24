use core::fmt;
use core::num::ParseIntError;
use crate::define_u64_type;

/// A type alias for coin amounts.
pub type Coin<T> = T;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CoinError {
    InvalidFormat,
    Overflow,
    DivisionByZero,
    Int(ParseIntError),
}

impl From<ParseIntError> for CoinError {
    fn from(e: ParseIntError) -> Self { Self::Int(e) }
}

define_u64_type!(TAPE);

impl TAPE {
    /// Number of decimal places for TAPE (of units: "flux").
    pub const DECIMALS: u32 = 6;

    /// Scaling factor for converting to TAPE (10^6).
    pub const SCALE: u64 = 1_000_000;

    /// Raw units accessor (flux).
    #[inline]
    pub fn flux(&self) -> u64 { self.0 }

    /// Whole string, trimmed fractional part.
    #[inline]
    pub fn as_string(&self) -> String {
        to_string_trimmed(self.flux(), Self::DECIMALS)
    }

    /// Whole string, fixed-width fractional part.
    #[inline]
    pub fn as_string_fixed(&self) -> String {
        to_string_fixed(self.flux(), Self::DECIMALS)
    }

    /// Display-only convenience; don't use for logic.
    #[inline]
    pub fn as_f64(&self) -> f64 {
        (self.flux() as f64) / (10u64.pow(Self::DECIMALS) as f64)
    }

    /// Integer-only constructor from whole & fractional parts.
    /// `fraction` is in flux and must be `< SCALE`.
    #[inline]
    pub fn from_fixed(whole: u64, fraction: u64) -> Self {
        assert!(fraction < Self::SCALE, "fraction must be < SCALE");
        TAPE(
            whole.checked_mul(Self::SCALE).unwrap()
                 .checked_add(fraction).unwrap()
        )
    }

    /// Infallible string constructor (panics on invalid input).
    #[inline]
    pub fn from<S: AsRef<str>>(s: S) -> Self {
        Self::parse(s.as_ref()).expect("invalid fixed-point amount")
    }

    /// Fallible fixed-point parser.
    #[inline]
    pub fn parse(s: &str) -> Result<Self, CoinError> {
        parse_fixed_str(s, Self::DECIMALS).map(TAPE)
    }
}

impl core::fmt::Debug for TAPE {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        fmt_fixed(self.0, Self::DECIMALS, "TAPE", f)?;
        write!(f, " ({})", self.0)
    }
}

impl core::fmt::Display for TAPE {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        fmt_fixed(self.0, Self::DECIMALS, "TAPE", f)
    }
}

define_u64_type!(SOL);

impl SOL {
    /// Number of decimal places for SOL (lamports).
    pub const DECIMALS: u32 = 9;

    /// Scaling factor for converting to SOL (10^9).
    pub const SCALE: u64 = 1_000_000_000;

    /// Raw units accessor (lamports).
    #[inline]
    pub fn lamports(&self) -> u64 { self.0 }

    /// Whole string, trimmed fractional part.
    #[inline]
    pub fn as_string(&self) -> String {
        to_string_trimmed(self.lamports(), Self::DECIMALS)
    }

    /// Whole string, fixed-width fractional part.
    #[inline]
    pub fn as_string_fixed(&self) -> String {
        to_string_fixed(self.lamports(), Self::DECIMALS)
    }

    /// Display-only convenience; don't use for logic.
    #[inline]
    pub fn as_f64(&self) -> f64 {
        (self.lamports() as f64) / (10u64.pow(Self::DECIMALS) as f64)
    }

    /// Integer-only constructor from whole & fractional parts.
    /// `fraction` is in lamports and must be `< SCALE`.
    #[inline]
    pub fn from_fixed(whole: u64, fraction: u64) -> Self {
        assert!(fraction < Self::SCALE, "fraction must be < SCALE");
        SOL(
            whole.checked_mul(Self::SCALE).unwrap()
                 .checked_add(fraction).unwrap()
        )
    }

    /// Infallible string constructor (panics on invalid input).
    #[inline]
    pub fn from<S: AsRef<str>>(s: S) -> Self {
        Self::parse(s.as_ref()).expect("invalid fixed-point amount")
    }

    /// Fallible fixed-point parser.
    #[inline]
    pub fn parse(s: &str) -> Result<Self, CoinError> {
        parse_fixed_str(s, Self::DECIMALS).map(SOL)
    }
}

impl core::fmt::Debug for SOL {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        fmt_fixed(self.0, Self::DECIMALS, "SOL", f)?;
        write!(f, " ({})", self.0)
    }
}

impl core::fmt::Display for SOL {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        fmt_fixed(self.0, Self::DECIMALS, "SOL", f)
    }
}

/// Helper for Display/Debug impls.
#[inline]
fn fmt_fixed(u: u64, decimals: u32, label: &str, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    write!(f, "{}:{}", label, to_string_trimmed(u, decimals))
}

/// Integer-only string "W.FFFFFF".
#[inline]
fn to_string_fixed(u: u64, decimals: u32) -> String {
    if decimals == 0 {
        return u.to_string();
    }
    let scale = 10u64.pow(decimals);
    let whole = u / scale;
    let frac  = u % scale;
    format!("{}.{:0width$}", whole, frac, width = decimals as usize)
}

/// Integer-only string "W.F[trimmed]" with at least one fractional digit (e.g., "1.0").
#[inline]
fn to_string_trimmed(u: u64, decimals: u32) -> String {
    if decimals == 0 {
        return u.to_string();
    }
    let scale = 10u64.pow(decimals);
    let whole = u / scale;
    let frac  = u % scale;

    // Build a padded fractional string, then trim trailing zeros.
    // For exact-whole values, keep a single '0' fractional digit.
    let frac_str = if frac == 0 {
        String::from("0")
    } else {
        let mut s = format!("{:0width$}", frac, width = decimals as usize);
        while s.ends_with('0') {
            s.pop();
        }
        if s.is_empty() { s.push('0'); } // safety, though not expected
        s
    };

    format!("{}.{}", whole, frac_str)
}

/// Parse a fixed-point decimal string into scaled units (integer-only).
/// Accepts "1", "1.0", ".5", "0.000001"; up to `decimals` fractional digits.
/// Underscores and surrounding whitespace are ignored. No exponents, no negatives.
#[inline]
fn parse_fixed_str(s: &str, decimals: u32) -> Result<u64, CoinError> {
    let s = s.trim().replace('_', "");
    if s.is_empty() || s.starts_with('-') {
        return Err(CoinError::InvalidFormat);
    }

    let mut parts = s.split('.');
    let whole_str = parts.next().unwrap_or("");
    let frac_str  = parts.next().unwrap_or("");
    if parts.next().is_some() {
        return Err(CoinError::InvalidFormat); // more than one '.'
    }

    // whole part
    let whole = if whole_str.is_empty() { 0 } else { whole_str.parse::<u64>()? };

    // fractional part
    if frac_str.len() > decimals as usize {
        return Err(CoinError::InvalidFormat);
    }
    let mut frac_scaled = 0u64;
    if !frac_str.is_empty() {
        let frac = frac_str.parse::<u64>()?;
        let pad = decimals as usize - frac_str.len();
        frac_scaled = frac
            .checked_mul(10u64.pow(pad as u32))
            .ok_or(CoinError::Overflow)?;
    }

    let scale = 10u64.pow(decimals);
    let base = whole.checked_mul(scale).ok_or(CoinError::Overflow)?;
    base.checked_add(frac_scaled).ok_or(CoinError::Overflow)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sol() {
        let amount = SOL::new(1_000_000_000); // 1 SOL
        assert_eq!(amount.as_u64(), 1_000_000_000);
        assert_eq!(format!("{}", amount), "SOL:1.0");
        assert_eq!(SOL::from("1.5"), SOL::new(1_500_000_000));
        assert_eq!(SOL::from_fixed(2, 0).as_f64(), 2.0);
        assert_eq!(SOL::from_fixed(3, 250_000_000).as_string(), "3.25");
        assert_eq!(SOL::from_fixed(0, 999_999_999).lamports(), 999_999_999);
        assert_eq!(SOL::from_fixed(1, 999_999_999).lamports(), 1_999_999_999);
        assert_eq!(SOL::from("7.999999999").lamports(), 7_999_999_999);

        // parse (fallible)
        assert_eq!(SOL::parse("1.000000000").unwrap().lamports(), 1_000_000_000);
        assert!(matches!(SOL::parse("1.0000000001"), Err(CoinError::InvalidFormat)));
    }

    #[test]
    fn test_tape() {
        let amount = TAPE::new(1_000_000); // 1 TAPE
        assert_eq!(amount.as_u64(), 1_000_000);
        assert_eq!(format!("{}", amount), "TAPE:1.0");
        assert_eq!(TAPE::from("1.5"), TAPE::new(1_500_000));
        assert_eq!(TAPE::from_fixed(2, 0).as_f64(), 2.0);
        assert_eq!(TAPE::from_fixed(3, 250_000).as_string(), "3.25");
        assert_eq!(TAPE::from_fixed(0, 999_999).flux(), 999_999);
        assert_eq!(TAPE::from_fixed(1, 999_999).flux(), 1_999_999);
        assert_eq!(TAPE::from("7.999999").flux(), 7_999_999);

        // parse (fallible)
        assert_eq!(TAPE::parse("1.000000").unwrap().flux(), 1_000_000);
        assert!(matches!(TAPE::parse("1.0000001"), Err(CoinError::InvalidFormat)));
    }

    #[test]
    fn test_padding_tape() {
        // whole numbers (no dot)
        assert_eq!(TAPE::parse("2").unwrap().flux(), 2_000_000);
        assert_eq!(TAPE::from("2").flux(), 2_000_000);

        // single fractional digit -> pad to 6
        assert_eq!(TAPE::parse("2.1").unwrap().flux(), 2_100_000);
        assert_eq!(TAPE::from("2.1").flux(), 2_100_000);

        // trailing dot means zero fractional part
        assert_eq!(TAPE::parse("3.").unwrap().flux(), 3_000_000);

        // fewer than 6 fractional digits
        assert_eq!(TAPE::parse("0.0001").unwrap().flux(), 100); // 0.000100
        assert_eq!(TAPE::parse(".5").unwrap().flux(), 500_000);  // 0.500000

        // underscores & spaces
        assert_eq!(TAPE::parse(" 1_234.0567 ").unwrap().flux(), 1_234_056_700);
    }

    #[test]
    fn test_padding_sol() {
        // whole numbers
        assert_eq!(SOL::parse("2").unwrap().lamports(), 2_000_000_000);
        assert_eq!(SOL::from("2").lamports(), 2_000_000_000);

        // single fractional digit -> pad to 9
        assert_eq!(SOL::parse("2.1").unwrap().lamports(), 2_100_000_000);

        // trailing dot
        assert_eq!(SOL::parse("3.").unwrap().lamports(), 3_000_000_000);

        // fewer than 9 fractional digits
        assert_eq!(SOL::parse("0.0001").unwrap().lamports(), 100_000); // 0.000100000
        assert_eq!(SOL::parse(".5").unwrap().lamports(), 500_000_000);

        // underscores & spaces
        assert_eq!(SOL::parse(" 1_234.0567 ").unwrap().lamports(), 1_234_056_700_000);
    }

    #[test]
    fn test_to_string() {
        // zero decimals
        assert_eq!(to_string_fixed(42, 0), "42");

        // 6 decimals (TAPE)
        assert_eq!(to_string_fixed(1_000_000, 6), "1.000000");
        assert_eq!(to_string_fixed(1, 6), "0.000001");
        assert_eq!(to_string_fixed(12_345_678, 6), "12.345678");

        // 9 decimals (SOL)
        assert_eq!(to_string_fixed(1_000_000_000, 9), "1.000000000");
        assert_eq!(to_string_fixed(42, 9), "0.000000042");

        // No decimals -> identity
        assert_eq!(to_string_trimmed(42, 0), "42");

        // Exact whole amounts -> include ".0"
        assert_eq!(to_string_trimmed(1_000_000, 6), "1.0");
        assert_eq!(to_string_trimmed(2_000_000_000, 9), "2.0");

        // Minimal fractional digits (trim trailing zeros, keep leading zeros)
        assert_eq!(to_string_trimmed(1, 6), "0.000001");
        assert_eq!(to_string_trimmed(10, 6), "0.00001");
        assert_eq!(to_string_trimmed(100, 6), "0.0001");
        assert_eq!(to_string_trimmed(1_500_000, 6), "1.5");
        assert_eq!(to_string_trimmed(1_230_000, 6), "1.23");
        assert_eq!(to_string_trimmed(12_345_678, 6), "12.345678");

        // 9-decimal variant
        assert_eq!(to_string_trimmed(1_000_000_000, 9), "1.0");
        assert_eq!(to_string_trimmed(500_000_000, 9), "0.5");
        assert_eq!(to_string_trimmed(42, 9), "0.000000042");
        assert_eq!(to_string_trimmed(123_456_789, 9), "0.123456789");
    }


    #[test]
    fn test_parse_string() {
        // valid forms
        assert_eq!(parse_fixed_str("1", 6).unwrap(), 1_000_000);
        assert_eq!(parse_fixed_str("1.0", 6).unwrap(), 1_000_000);
        assert_eq!(parse_fixed_str(".5", 6).unwrap(), 500_000);
        assert_eq!(parse_fixed_str("0.000001", 6).unwrap(), 1);
        assert_eq!(parse_fixed_str("  1_234.056_700 ", 6).unwrap(), 1_234_056_700);

        // too many fractional digits
        assert!(matches!(parse_fixed_str("1.0000001", 6), Err(CoinError::InvalidFormat)));

        // invalid formats
        assert!(matches!(parse_fixed_str("", 6), Err(CoinError::InvalidFormat)));
        assert!(matches!(parse_fixed_str("-", 6), Err(CoinError::InvalidFormat)));
        assert!(matches!(parse_fixed_str("1.2.3", 6), Err(CoinError::InvalidFormat)));

        // overflow checks (decimals=0 simplifies to identity)
        assert!(matches!(parse_fixed_str("18446744073709551616", 0), Err(CoinError::Int(_))));
    }
}
