use crate::define_u64_type;

define_u64_type!(TAPE);
define_u64_type!(SOL);

/// A type alias for coin amounts.
pub type Coin<T> = T;

impl TAPE {
    /// Number of decimal places for TAPE (micro-units).
    pub const DECIMALS: u32 = 6;

    /// Scaling factor for converting to TAPE (10^6).
    pub const SCALE: u64 = 1_000_000;

    /// Creates a TAPE from a value in standard units (e.g., 1.5 TAPE).
    #[inline]
    pub fn from_whole(value: f64) -> Self {
        TAPE((value * Self::SCALE as f64).round() as u64)
    }

    /// Converts to standard TAPE units as a float (e.g., 1,000,000 micro-units = 1.0 TAPE).
    #[inline]
    pub fn to_whole(&self) -> f64 {
        self.0 as f64 / Self::SCALE as f64
    }
}

impl core::fmt::Debug for TAPE {
    /// Formats TAPE in standard units with up to 6 decimal places.
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        let value = self.to_whole();
        write!(f, "TAPE:{:.6} ({})", value, self.0)
    }
}

impl core::fmt::Display for TAPE {
    /// Formats TAPE in standard units with up to 6 decimal places.
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        let value = self.to_whole();
        write!(f, "TAPE:{:.6}", value)
    }
}

impl SOL {
    /// Number of decimal places for SOL (lamports).
    pub const DECIMALS: u32 = 9;

    /// Scaling factor for converting to SOL (10^9).
    pub const SCALE: u64 = 1_000_000_000;

    /// Creates a SOL from a value in standard units (e.g., 1.5 SOL).
    #[inline]
    pub fn from_whole(value: f64) -> Self {
        SOL((value * Self::SCALE as f64).round() as u64)
    }

    /// Converts to standard SOL units as a float (e.g., 1,000,000,000 lamports = 1.0 SOL).
    #[inline]
    pub fn to_whole(&self) -> f64 {
        self.0 as f64 / Self::SCALE as f64
    }
}

impl core::fmt::Debug for SOL {
    /// Formats SOL in standard units with up to 6 decimal places for debug output.
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        let value = self.to_whole();
        write!(f, "SOL:{:.9} ({})", value, self.0)
    }
}

impl core::fmt::Display for SOL {
    /// Formats SOL in standard units with up to 9 decimal places.
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        let value = self.to_whole();
        write!(f, "SOL:{:.9}", value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tape() {
        let amount = TAPE::from_whole(1.0); // 1 TAPE = 1,000,000 micro-units
        assert_eq!(amount.as_u64(), 1_000_000);
        assert_eq!(format!("{}", amount), "TAPE:1.000000");
        assert_eq!(TAPE::from_whole(1.5), TAPE::new(1_500_000));
        assert_eq!(TAPE::new(2_000_000).to_whole(), 2.0);
    }

    #[test]
    fn test_sol() {
        let amount = SOL::from_whole(1.0); // 1 SOL = 1,000,000,000 lamports
        assert_eq!(amount.as_u64(), 1_000_000_000);
        assert_eq!(format!("{}", amount), "SOL:1.000000000");
        assert_eq!(SOL::from_whole(1.5), SOL::new(1_500_000_000));
        assert_eq!(SOL::new(2_000_000_000).to_whole(), 2.0);
    }
}
