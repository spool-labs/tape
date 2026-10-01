//! Encoding profile types for erasure-coded track data.

use core::fmt::{Display, Formatter};

use bytemuck::{Pod, Zeroable};
use num_enum::{IntoPrimitive, TryFromPrimitive};

#[cfg(feature = "wincode")]
use serde::{Deserialize, Serialize};
#[cfg(feature = "wincode")]
use wincode_derive::{SchemaRead, SchemaWrite};

use crate::erasure::MAX_GROUP_SIZE;

/// Tapenet's Clay parameters: any 7 of 20 slices recover, and a repair reads from 16.
pub const TAPENET_CLAY: ClayParams = ClayParams::new(20, 7, 16);

/// Tapenet's profile. The only place its numbers are written down.
pub const TAPENET_PROFILE: ErasureCode = ErasureCode::Clay(TAPENET_CLAY);

/// Why a profile is no code a network can run
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProfileError {
    /// The encoding discriminant is no known code
    UnknownEncoding,

    /// n is zero or wider than a group's arrays
    Width,

    /// k is zero or leaves no parity slice
    Recovery,

    /// d is outside k + 1 ..= n - 1
    Helpers,
}

impl Display for ProfileError {
    fn fmt(&self, f: &mut Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::UnknownEncoding => write!(f, "unknown encoding type"),
            Self::Width => write!(f, "n is outside 1..={MAX_GROUP_SIZE}"),
            Self::Recovery => write!(f, "k is outside 1..n"),
            Self::Helpers => write!(f, "d is outside k + 1..n"),
        }
    }
}

impl std::error::Error for ProfileError {}

/// Encoding type for erasure-coded track data.
#[repr(u64)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, IntoPrimitive, TryFromPrimitive)]
pub enum EncodingType {

    /// Unknown encoding (default for uninitialized profiles).
    #[default]
    Unknown = 0,

    /// Reed-Solomon with striping and rotation.
    ReedSolomon = 1,

    /// Clay encoding - Clay codes with striping and rotation.
    Clay = 2,

    /// Replication - n full copies.
    Replication = 3,
}

/// Encoding configuration as stored: type + params, unchecked.
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Pod, Zeroable)]
#[cfg_attr(feature = "wincode", derive(Serialize, Deserialize, SchemaRead, SchemaWrite))]
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

    /// Create a Clay encoding profile with the given parameters.
    #[inline]
    pub const fn clay(params: ClayParams) -> Self {
        Self {
            encoding: EncodingType::Clay as u64,
            params: params.as_u64(),
        }
    }

    /// Create a Reed-Solomon encoding profile with the given parameters.
    #[inline]
    pub const fn reed_solomon(params: RSParams) -> Self {
        Self {
            encoding: EncodingType::ReedSolomon as u64,
            params: params.as_u64(),
        }
    }

    /// Create a Replication profile holding n full copies.
    #[inline]
    pub const fn replication(params: RepParams) -> Self {
        Self {
            encoding: EncodingType::Replication as u64,
            params: params.as_u64(),
        }
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

/// The code a network runs, with its parameters
///
/// Every accessor is total. A value parsed from an `EncodingProfile` has also
/// passed `validate`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ErasureCode {
    /// Clay code: any k of n recover, and a repair reads part of d helpers.
    Clay(ClayParams),

    /// Reed-Solomon: any k of n recover.
    ReedSolomon(RSParams),

    /// n full copies: any one recovers.
    Replication(RepParams),
}

impl ErasureCode {
    /// Total slices, one per spool of a group.
    #[inline]
    pub const fn n(&self) -> u8 {
        match self {
            Self::Clay(params) => params.n(),
            Self::ReedSolomon(params) => params.n(),
            Self::Replication(params) => params.n(),
        }
    }

    /// Slices needed to reconstruct.
    #[inline]
    pub const fn k(&self) -> u8 {
        match self {
            Self::Clay(params) => params.k(),
            Self::ReedSolomon(params) => params.k(),
            Self::Replication(params) => params.k(),
        }
    }

    /// Parity slices (m = n - k).
    #[inline]
    pub const fn m(&self) -> u8 {
        match self {
            Self::Clay(params) => params.m(),
            Self::ReedSolomon(params) => params.m(),
            Self::Replication(params) => params.m(),
        }
    }

    /// Check the parameters against the code's own rules.
    pub const fn validate(&self) -> Result<(), ProfileError> {
        match self {
            Self::Clay(params) => params.validate(),
            Self::ReedSolomon(params) => params.validate(),
            Self::Replication(params) => params.validate(),
        }
    }
}

impl TryFrom<EncodingProfile> for ErasureCode {
    type Error = ProfileError;

    fn try_from(profile: EncodingProfile) -> Result<Self, ProfileError> {
        let code = match profile.encoding_type() {
            Some(EncodingType::Clay) => Self::Clay(ClayParams::from_u64(profile.params)),
            Some(EncodingType::ReedSolomon) => Self::ReedSolomon(RSParams::from_u64(profile.params)),
            Some(EncodingType::Replication) => Self::Replication(RepParams::from_u64(profile.params)),
            Some(EncodingType::Unknown) | None => return Err(ProfileError::UnknownEncoding),
        };
        code.validate()?;

        Ok(code)
    }
}

impl From<ErasureCode> for EncodingProfile {
    fn from(code: ErasureCode) -> Self {
        match code {
            ErasureCode::Clay(params) => Self::clay(params),
            ErasureCode::ReedSolomon(params) => Self::reed_solomon(params),
            ErasureCode::Replication(params) => Self::replication(params),
        }
    }
}

// Shared by every code: a group's arrays hold at most MAX_GROUP_SIZE slices.
const fn is_width(n: u8) -> bool {
    n >= 1 && n as usize <= MAX_GROUP_SIZE
}

/// Clay erasure code parameters, packed into u64.
///
/// Supports Clay codes with parameters:
/// - k: data slices (1-255)
/// - m: parity slices (1-255), where n = k + m
/// - d: helper count for repair (k+1 <= d <= n-1)
///
/// Constraints (from clay-codes):
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

    /// Sub-chunks per chunk: alpha = q^t with q = d - k + 1 and t = ceil(n/q).
    ///
    /// Total over every packed byte pattern.
    #[inline]
    pub const fn alpha(&self) -> u64 {
        let q = self.d().saturating_sub(self.k()) as u64 + 1;
        let n = self.n() as u64;
        let t = (n + q - 1) / q;
        let mut out = 1u64;
        let mut i = 0;
        while i < t {
            out = out.saturating_mul(q);
            i += 1;
        }
        out
    }

    /// Encode granularity in bytes: stripe inputs pad to k * alpha * 2.
    #[inline]
    pub const fn stripe_alignment(&self) -> u64 {
        (self.k() as u64).saturating_mul(self.alpha()).saturating_mul(2)
    }

    /// Check the parameters are a Clay code that fits a group.
    ///
    /// The helper bounds are the ones `clay_codes::ClayCode::new` needs.
    pub const fn validate(&self) -> Result<(), ProfileError> {
        let (n, k, d) = (self.n(), self.k(), self.d());
        if !is_width(n) {
            return Err(ProfileError::Width);
        }
        if k == 0 || k >= n {
            return Err(ProfileError::Recovery);
        }
        if d <= k || d >= n {
            return Err(ProfileError::Helpers);
        }
        Ok(())
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

/// Reed-Solomon erasure code parameters, packed into u64.
///
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
        let n = k.saturating_add(m);
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

    /// Check the parameters are a Reed-Solomon code that fits a group.
    pub const fn validate(&self) -> Result<(), ProfileError> {
        let (n, k) = (self.n(), self.k());
        if !is_width(n) {
            return Err(ProfileError::Width);
        }
        if k == 0 || k >= n {
            return Err(ProfileError::Recovery);
        }
        Ok(())
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

/// Replication parameters, packed into u64: n full copies of the blob.
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Pod, Zeroable)]
pub struct RepParams {
    /// Packed parameters: byte 0 = n
    packed: u64,
}

impl RepParams {
    /// Create new replication parameters for n copies.
    #[inline]
    pub const fn new(n: u8) -> Self {
        Self { packed: n as u64 }
    }

    /// Total slices (n copies).
    #[inline]
    pub const fn n(&self) -> u8 {
        (self.packed & 0xFF) as u8
    }

    /// Copies needed to reconstruct: any one is the whole blob.
    #[inline]
    pub const fn k(&self) -> u8 {
        1
    }

    /// Redundant copies (m = n - 1).
    #[inline]
    pub const fn m(&self) -> u8 {
        self.n().saturating_sub(1)
    }

    /// Check the copy count fits a group.
    pub const fn validate(&self) -> Result<(), ProfileError> {
        if !is_width(self.n()) {
            return Err(ProfileError::Width);
        }
        Ok(())
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
    fn test_clay_params_roundtrip() {
        let params = ClayParams::new(20, 10, 19);
        let packed = params.as_u64();
        let recovered = ClayParams::from_u64(packed);
        assert_eq!(params, recovered);
    }

    #[test]
    fn test_rs_params_new() {
        let params = RSParams::new(10, 10);
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
        assert_eq!(EncodingType::ReedSolomon as u64, 1);
        assert_eq!(EncodingType::Clay as u64, 2);
        assert_eq!(EncodingType::Replication as u64, 3);
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

    // tapenet's profile is Clay (20, 7, 16) with alpha 100, and it parses back
    #[test]
    fn tapenet() {
        let code = ErasureCode::try_from(EncodingProfile::from(TAPENET_PROFILE)).expect("tapenet");

        assert_eq!(code, TAPENET_PROFILE);
        assert_eq!((code.n(), code.k(), code.m()), (20, 7, 13));
        assert_eq!(TAPENET_CLAY.d(), 16);
        assert_eq!(TAPENET_CLAY.alpha(), 100);
        assert_eq!(TAPENET_CLAY.stripe_alignment(), 1_400);
    }

    // every code survives the trip through its stored form
    #[test]
    fn stored_form() {
        let codes = [
            ErasureCode::Clay(ClayParams::new(10, 7, 9)),
            ErasureCode::ReedSolomon(RSParams::new(4, 2)),
            ErasureCode::Replication(RepParams::new(2)),
        ];

        for code in codes {
            let profile = EncodingProfile::from(code);
            let unpacked = EncodingProfile::unpack(profile.pack());

            assert_eq!(ErasureCode::try_from(unpacked), Ok(code));
        }
    }

    // each code reports n, k and m from its own parameters
    #[test]
    fn counts() {
        let reed_solomon = ErasureCode::ReedSolomon(RSParams::new(4, 2));
        let copies = ErasureCode::Replication(RepParams::new(3));

        assert_eq!((reed_solomon.n(), reed_solomon.k(), reed_solomon.m()), (6, 4, 2));
        assert_eq!((copies.n(), copies.k(), copies.m()), (3, 1, 2));
    }

    // a zeroed or unassigned discriminant is no code
    #[test]
    fn unknown_encoding() {
        let unassigned = EncodingProfile { encoding: 9, params: TAPENET_CLAY.as_u64() };

        assert_eq!(ErasureCode::try_from(EncodingProfile::unknown()), Err(ProfileError::UnknownEncoding));
        assert_eq!(ErasureCode::try_from(EncodingProfile::zeroed()), Err(ProfileError::UnknownEncoding));
        assert_eq!(ErasureCode::try_from(unassigned), Err(ProfileError::UnknownEncoding));
    }

    // Clay needs two parity slices and helpers strictly between k and n
    #[test]
    fn clay_rules() {
        assert_eq!(TAPENET_CLAY.validate(), Ok(()));
        assert_eq!(ClayParams::new(4, 2, 3).validate(), Ok(()));
        assert_eq!(ClayParams::new(20, 10, 12).validate(), Ok(()));

        assert_eq!(ClayParams::new(0, 0, 0).validate(), Err(ProfileError::Width));
        assert_eq!(ClayParams::new(21, 7, 16).validate(), Err(ProfileError::Width));
        assert_eq!(ClayParams::new(20, 0, 16).validate(), Err(ProfileError::Recovery));
        assert_eq!(ClayParams::new(20, 20, 16).validate(), Err(ProfileError::Recovery));
        assert_eq!(ClayParams::new(20, 7, 7).validate(), Err(ProfileError::Helpers));
        assert_eq!(ClayParams::new(20, 7, 20).validate(), Err(ProfileError::Helpers));
        assert_eq!(ClayParams::new(4, 3, 3).validate(), Err(ProfileError::Helpers));
    }

    // Reed-Solomon takes any parity count from one up
    #[test]
    fn reed_solomon_rules() {
        assert_eq!(RSParams::new(3, 1).validate(), Ok(()));
        assert_eq!(RSParams::new(4, 2).validate(), Ok(()));
        assert_eq!(RSParams::new(1, 19).validate(), Ok(()));

        assert_eq!(RSParams::new(0, 0).validate(), Err(ProfileError::Width));
        assert_eq!(RSParams::new(10, 11).validate(), Err(ProfileError::Width));
        assert_eq!(RSParams::new(0, 4).validate(), Err(ProfileError::Recovery));
        assert_eq!(RSParams::new(4, 0).validate(), Err(ProfileError::Recovery));
    }

    // copies run from a single drive up to a full group
    #[test]
    fn replication_rules() {
        assert_eq!(RepParams::new(1).validate(), Ok(()));
        assert_eq!(RepParams::new(20).validate(), Ok(()));

        assert_eq!(RepParams::new(0).validate(), Err(ProfileError::Width));
        assert_eq!(RepParams::new(21).validate(), Err(ProfileError::Width));
    }

    // a stored profile with bad parameters is refused at parse
    #[test]
    fn parse_validates() {
        let wide = EncodingProfile::replication(RepParams::new(255));
        let no_parity = EncodingProfile::reed_solomon(RSParams::new(4, 0));

        assert_eq!(ErasureCode::try_from(wide), Err(ProfileError::Width));
        assert_eq!(ErasureCode::try_from(no_parity), Err(ProfileError::Recovery));
    }

    // alpha and the alignment stay total over every packed byte pattern
    #[test]
    fn alpha_total() {
        for packed in [0u64, u64::MAX, ClayParams::new(255, 255, 0).as_u64(), ClayParams::new(255, 0, 1).as_u64()] {
            let params = ClayParams::from_u64(packed);

            let _ = params.alpha();
            let _ = params.stripe_alignment();
        }
        assert_eq!(ClayParams::new(20, 10, 12).alpha(), 2_187);
    }
}
