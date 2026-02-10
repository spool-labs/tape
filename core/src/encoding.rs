//! Encoding profile types for erasure-coded track data.
//!
//! Defines `EncodingType`, `ClayParams`, and `EncodingProfile` for
//! configuring erasure coding parameters per-track.

use bytemuck::{Pod, Zeroable};
use num_enum::{IntoPrimitive, TryFromPrimitive};


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

    /// Create a Basic (RS) encoding profile with given parameters.
    #[inline]
    pub const fn basic(params: RSParams) -> Self {
        Self {
            encoding: EncodingType::Basic as u64,
            params: params.as_u64(),
        }
    }

    /// Create a Basic encoding profile with default parameters (k=10, m=10).
    pub fn basic_default() -> Self {
        Self::basic(RSParams::default())
    }

    /// Get the RS parameters (only valid if is_basic()).
    #[inline]
    pub const fn rs_params(&self) -> RSParams {
        RSParams::from_u64(self.params)
    }

    /// Get k (data slices) for any encoding type.
    ///
    /// # Panics
    /// Panics if encoding type is Unknown.
    #[inline]
    pub fn k(&self) -> u8 {
        match self.encoding_type() {
            Some(EncodingType::Clay) => self.clay_params().k(),
            Some(EncodingType::Basic) => self.rs_params().k(),
            Some(EncodingType::Unknown) | None => panic!("cannot get k from Unknown encoding"),
        }
    }

    /// Get m (parity slices) for any encoding type.
    ///
    /// # Panics
    /// Panics if encoding type is Unknown.
    #[inline]
    pub fn m(&self) -> u8 {
        match self.encoding_type() {
            Some(EncodingType::Clay) => self.clay_params().m(),
            Some(EncodingType::Basic) => self.rs_params().m(),
            Some(EncodingType::Unknown) | None => panic!("cannot get m from Unknown encoding"),
        }
    }

    /// Get n (total slices) for any encoding type.
    ///
    /// # Panics
    /// Panics if encoding type is Unknown.
    #[inline]
    pub fn n(&self) -> u8 {
        self.k() + self.m()
    }

    /// Pack into a byte array (for unaligned instruction data).
    pub fn pack(&self) -> [u8; 16] {
        let mut out = [0u8; 16];
        out[..8].copy_from_slice(&self.encoding.to_le_bytes());
        out[8..16].copy_from_slice(&self.params.to_le_bytes());
        out
    }

    /// Unpack from a byte array.
    pub fn unpack(data: [u8; 16]) -> Self {
        Self {
            encoding: u64::from_le_bytes(data[..8].try_into().unwrap()),
            params: u64::from_le_bytes(data[8..16].try_into().unwrap()),
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

/// Default Clay parameters: n=20, k=7, d=16 → m=13 parity (35% of slices needed for recovery).
impl ClayParams {
    pub const DEFAULT: Self = Self::new(20, 7, 16);
}

impl Default for ClayParams {
    fn default() -> Self {
        Self::DEFAULT
    }
}

/// Reed-Solomon erasure code parameters, packed into u64.
///
/// Basic RS encoding with parameters:
/// - k: data slices (1-255)
/// - m: parity slices (1-255), where n = k + m
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Pod, Zeroable)]
pub struct RSParams {
    /// Packed parameters: byte 0 = n, byte 1 = k
    packed: u64,
}

impl RSParams {
    /// Create new RS parameters.
    ///
    /// # Arguments
    /// - `k`: data slices needed for reconstruction
    /// - `m`: parity slices
    #[inline]
    pub const fn new(k: u8, m: u8) -> Self {
        let n = k + m;
        Self {
            packed: (n as u64) | ((k as u64) << 8),
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

impl Default for RSParams {
    fn default() -> Self {
        // k=10, m=10 (standard default)
        Self::new(10, 10)
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
        let params = ClayParams::default();
        assert_eq!(params.n(), 20);
        assert_eq!(params.k(), 7);
        assert_eq!(params.d(), 16);
        assert_eq!(params.m(), 13);
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
        let params = RSParams::new(10, 10);
        let profile = EncodingProfile::basic(params);

        assert!(profile.is_basic());
        assert!(!profile.is_clay());
        assert_eq!(profile.encoding_type(), Some(EncodingType::Basic));
        assert_eq!(profile.rs_params(), params);
    }

    #[test]
    fn test_encoding_profile_basic_default() {
        let profile = EncodingProfile::basic_default();
        assert!(profile.is_basic());
        assert_eq!(profile.rs_params(), RSParams::default());
        assert_eq!(profile.k(), 10);
        assert_eq!(profile.m(), 10);
        assert_eq!(profile.n(), 20);
    }

    #[test]
    fn test_rs_params_new() {
        let params = RSParams::new(10, 10);
        assert_eq!(params.n(), 20);
        assert_eq!(params.k(), 10);
        assert_eq!(params.m(), 10);
    }

    #[test]
    fn test_rs_params_default() {
        let params = RSParams::default();
        assert_eq!(params.n(), 20);
        assert_eq!(params.k(), 10);
        assert_eq!(params.m(), 10);
    }

    #[test]
    fn test_rs_params_roundtrip() {
        let params = RSParams::new(8, 4);
        let packed = params.as_u64();
        let recovered = RSParams::from_u64(packed);
        assert_eq!(params, recovered);
    }

    #[test]
    fn test_rs_params_size() {
        assert_eq!(std::mem::size_of::<RSParams>(), 8);
    }

    #[test]
    fn test_profile_k_m_n_clay() {
        let profile = EncodingProfile::clay_default();
        assert_eq!(profile.k(), 7);
        assert_eq!(profile.m(), 13);
        assert_eq!(profile.n(), 20);
    }

    #[test]
    fn test_profile_k_m_n_basic() {
        let profile = EncodingProfile::basic(RSParams::new(8, 4));
        assert_eq!(profile.k(), 8);
        assert_eq!(profile.m(), 4);
        assert_eq!(profile.n(), 12);
    }

    #[test]
    #[should_panic(expected = "cannot get k from Unknown encoding")]
    fn test_profile_k_unknown_panics() {
        let profile = EncodingProfile::unknown();
        let _ = profile.k();
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
