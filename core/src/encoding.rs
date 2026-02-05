//! Encoding profile types for erasure-coded track data.
//!
//! Defines `EncodingType`, `ClayParams`, and `EncodingProfile` for
//! configuring erasure coding parameters per-track.

use bytemuck::{Pod, Zeroable};
use num_enum::{IntoPrimitive, TryFromPrimitive};

use crate::erasure::{DATA_SLICES, SPOOL_GROUP_SIZE};

/// Encoding type for erasure-coded track data.
///
/// Determines how blob data is split into stripes and mapped to slices.
#[repr(u64)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, IntoPrimitive, TryFromPrimitive)]
pub enum EncodingType {
    /// Unknown encoding (default for uninitialized tracks).
    #[default]
    Unknown = 0,
    /// Basic encoding - single RS pass, testing only.
    Basic = 1,
    /// Clay encoding - Clay codes with striping and rotation.
    Clay = 2,
}

/// Clay erasure code parameters, packed into u64.
///
/// Supports Clay codes with parameters:
/// - k: data slices (1-255)
/// - m: parity slices (1-255), where n = k + m
/// - d: helper count for repair (k+1 <= d <= n-1)
///
/// Constraints (from clay-codes 0.1.1):
/// - k >= 1, m >= 1, k + m = n
/// - d >= k + 1, d <= n - 1
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Pod, Zeroable)]
pub struct ClayParams {
    /// Packed parameters: byte 0 = n, byte 1 = k, byte 2 = d
    packed: u64,
}

impl ClayParams {
    /// Create new Clay parameters.
    ///
    /// # Arguments
    /// - `n`: total slices (k + m)
    /// - `k`: data slices needed for reconstruction
    /// - `d`: helper count for repair
    #[inline]
    pub const fn new(n: u8, k: u8, d: u8) -> Self {
        Self {
            packed: (n as u64) | ((k as u64) << 8) | ((d as u64) << 16),
        }
    }

    /// Total slices (n = k + m).
    #[inline]
    pub const fn n(&self) -> u8 {
        (self.packed & 0xFF) as u8
    }

    /// Data slices needed for reconstruction.
    #[inline]
    pub const fn k(&self) -> u8 {
        ((self.packed >> 8) & 0xFF) as u8
    }

    /// Helper count for repair.
    #[inline]
    pub const fn d(&self) -> u8 {
        ((self.packed >> 16) & 0xFF) as u8
    }

    /// Parity slices (m = n - k).
    #[inline]
    pub const fn m(&self) -> u8 {
        self.n().saturating_sub(self.k())
    }

    /// Convert to raw u64 for storage.
    #[inline]
    pub const fn as_u64(&self) -> u64 {
        self.packed
    }

    /// Create from raw u64.
    #[inline]
    pub const fn from_u64(v: u64) -> Self {
        Self { packed: v }
    }
}

impl Default for ClayParams {
    fn default() -> Self {
        Self::new(
            SPOOL_GROUP_SIZE as u8,
            DATA_SLICES as u8,
            (SPOOL_GROUP_SIZE - 1) as u8,
        )
    }
}

/// Encoding configuration: type + params.
///
/// Follows the EpochState pattern (discriminant + payload as u64 pair).
/// This is a 16-byte Pod struct suitable for on-chain storage.
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Pod, Zeroable)]
pub struct EncodingProfile {
    /// Encoding type discriminant (EncodingType as u64).
    pub encoding: u64,
    /// Encoding-specific parameters (e.g., ClayParams packed).
    pub params: u64,
}

impl EncodingProfile {
    /// Get the encoding type.
    #[inline]
    pub fn encoding_type(&self) -> Option<EncodingType> {
        EncodingType::try_from(self.encoding).ok()
    }

    /// Check if this is Clay encoding.
    #[inline]
    pub fn is_clay(&self) -> bool {
        matches!(self.encoding_type(), Some(EncodingType::Clay))
    }

    /// Check if this is Basic encoding.
    #[inline]
    pub fn is_basic(&self) -> bool {
        matches!(self.encoding_type(), Some(EncodingType::Basic))
    }

    /// Create a Clay encoding profile with the given parameters.
    #[inline]
    pub const fn clay(params: ClayParams) -> Self {
        Self {
            encoding: EncodingType::Clay as u64,
            params: params.as_u64(),
        }
    }

    /// Create a Clay encoding profile with default parameters.
    pub fn clay_default() -> Self {
        Self::clay(ClayParams::default())
    }

    /// Get the Clay parameters (only valid if is_clay()).
    #[inline]
    pub const fn clay_params(&self) -> ClayParams {
        ClayParams::from_u64(self.params)
    }

    /// Create a Basic encoding profile.
    #[inline]
    pub const fn basic() -> Self {
        Self {
            encoding: EncodingType::Basic as u64,
            params: 0,
        }
    }

    /// Create an Unknown encoding profile (zeroed).
    #[inline]
    pub const fn unknown() -> Self {
        Self {
            encoding: EncodingType::Unknown as u64,
            params: 0,
        }
    }
}

impl Default for EncodingProfile {
    fn default() -> Self {
        Self::clay_default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_clay_params_new() {
        let params = ClayParams::new(20, 10, 19);
        assert_eq!(params.n(), 20);
        assert_eq!(params.k(), 10);
        assert_eq!(params.d(), 19);
        assert_eq!(params.m(), 10);
    }

    #[test]
    fn test_clay_params_default() {
        use crate::erasure::PARITY_SLICES;
        let params = ClayParams::default();
        assert_eq!(params.n(), SPOOL_GROUP_SIZE as u8);
        assert_eq!(params.k(), DATA_SLICES as u8);
        assert_eq!(params.d(), (SPOOL_GROUP_SIZE - 1) as u8);
        assert_eq!(params.m(), PARITY_SLICES as u8);
    }

    #[test]
    fn test_clay_params_roundtrip() {
        let params = ClayParams::new(20, 10, 19);
        let packed = params.as_u64();
        let recovered = ClayParams::from_u64(packed);
        assert_eq!(params, recovered);
    }

    #[test]
    fn test_encoding_profile_clay() {
        let params = ClayParams::new(20, 10, 19);
        let profile = EncodingProfile::clay(params);

        assert!(profile.is_clay());
        assert!(!profile.is_basic());
        assert_eq!(profile.encoding_type(), Some(EncodingType::Clay));
        assert_eq!(profile.clay_params(), params);
    }

    #[test]
    fn test_encoding_profile_clay_default() {
        let profile = EncodingProfile::clay_default();
        assert!(profile.is_clay());
        assert_eq!(profile.clay_params(), ClayParams::default());
    }

    #[test]
    fn test_encoding_profile_basic() {
        let profile = EncodingProfile::basic();

        assert!(profile.is_basic());
        assert!(!profile.is_clay());
        assert_eq!(profile.encoding_type(), Some(EncodingType::Basic));
        assert_eq!(profile.params, 0);
    }

    #[test]
    fn test_encoding_profile_unknown() {
        let profile = EncodingProfile::unknown();
        assert_eq!(profile.encoding_type(), Some(EncodingType::Unknown));
    }

    #[test]
    fn test_encoding_profile_zeroed() {
        let profile = EncodingProfile::zeroed();
        assert_eq!(profile.encoding_type(), Some(EncodingType::Unknown));
    }

    #[test]
    fn test_encoding_profile_size() {
        assert_eq!(std::mem::size_of::<EncodingProfile>(), 16);
    }

    #[test]
    fn test_clay_params_size() {
        assert_eq!(std::mem::size_of::<ClayParams>(), 8);
    }

    #[test]
    fn test_encoding_type_values() {
        assert_eq!(EncodingType::Unknown as u64, 0);
        assert_eq!(EncodingType::Basic as u64, 1);
        assert_eq!(EncodingType::Clay as u64, 2);
    }

    #[test]
    fn test_different_clay_params() {
        // Test with k=6, m=14 (n=20, d=19)
        let params1 = ClayParams::new(20, 6, 19);
        assert_eq!(params1.k(), 6);
        assert_eq!(params1.m(), 14);

        // Test with k=14, m=6 (n=20, d=19)
        let params2 = ClayParams::new(20, 14, 19);
        assert_eq!(params2.k(), 14);
        assert_eq!(params2.m(), 6);

        // They should be different
        assert_ne!(params1, params2);
    }
}
