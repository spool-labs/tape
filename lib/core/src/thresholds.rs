use core::fmt::{Display, Formatter};
use core::str::FromStr;

use num_enum::{IntoPrimitive, TryFromPrimitive};

use crate::encoding::{EncodingProfile, ErasureCode, ProfileError};
use crate::erasure::MAX_GROUP_SIZE;

/// What a network assumes about its nodes, fixed at genesis
#[repr(u64)]
#[derive(Clone, Copy, Debug, Eq, IntoPrimitive, PartialEq, TryFromPrimitive)]
pub enum TrustModel {
    /// Nodes may lie: tapenet
    Permissionless = 1,
    /// Nodes only fail: one owner runs every node
    Trusted = 2,
}

/// Writes `permissionless` or `trusted`, the form a flag or config takes.
impl Display for TrustModel {
    fn fmt(&self, f: &mut Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Permissionless => write!(f, "permissionless"),
            Self::Trusted => write!(f, "trusted"),
        }
    }
}

impl FromStr for TrustModel {
    type Err = ThresholdError;

    fn from_str(text: &str) -> Result<Self, ThresholdError> {
        match text.trim() {
            "permissionless" => Ok(Self::Permissionless),
            "trusted" => Ok(Self::Trusted),
            _ => Err(ThresholdError::TrustModel),
        }
    }
}

/// A group size or recovery count that no network can have
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ThresholdError {
    Width(u64),
    Recovery { n: u64, k: u64 },
    Profile(ProfileError),
    PermissionlessRecovery { n: u64, k: u64, limit: u64 },
    TrustModel,
}

impl Display for ThresholdError {
    fn fmt(&self, f: &mut Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Width(n) => write!(f, "group size {n} is outside 1..={MAX_GROUP_SIZE}"),
            Self::Recovery { n, k } => write!(f, "recovery count {k} is outside 1..={n}"),
            Self::Profile(error) => write!(f, "{error}"),
            Self::PermissionlessRecovery { n, k, limit } => write!(
                f,
                "a permissionless network of {n} recovers from at most {limit}, got {k}"
            ),
            Self::TrustModel => write!(f, "expected permissionless or trusted"),
        }
    }
}

impl std::error::Error for ThresholdError {}

impl From<ProfileError> for ThresholdError {
    fn from(error: ProfileError) -> Self {
        Self::Profile(error)
    }
}

/// How many signatures each check over a group's members needs
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Thresholds {
    /// Group signatures that certify a track
    pub certify: u64,
    /// Group signatures that invalidate a track
    pub invalidate: u64,
    /// Group signatures on an eviction
    pub evict: u64,
    /// Member attestations on a challenge certificate
    pub challenge: u64,
    /// Group signatures on a snapshot hash
    pub snapshot: u64,
    /// Group signatures on an assignment root
    pub assignment: u64,
    /// Synced spools that make a group ready
    pub synced: u64,
}

impl Thresholds {
    /// Compute every threshold for groups of n spools that recover from k
    pub fn try_new(trust: TrustModel, n: u64, k: u64) -> Result<Self, ThresholdError> {
        if n == 0 || n > MAX_GROUP_SIZE as u64 {
            return Err(ThresholdError::Width(n));
        }
        if k == 0 || k > n {
            return Err(ThresholdError::Recovery { n, k });
        }

        match trust {
            TrustModel::Permissionless => Ok(Self::permissionless(n, k)),
            TrustModel::Trusted => Ok(Self::trusted(k)),
        }
    }

    // Up to f nodes may lie. A vote that picks between values needs the
    // supermajority, and one that confirms a value every node can recompute
    // needs f + 1, which always includes an honest node.
    fn permissionless(n: u64, k: u64) -> Self {
        let quorum = supermajority(n);
        let honest = max_faulty(n) + 1;

        Self {
            certify: quorum,
            invalidate: quorum,
            evict: quorum,
            challenge: quorum,
            snapshot: honest,
            assignment: honest,
            synced: k.max(honest),
        }
    }

    // Nodes don't lie, so a check only has to prove k of them hold the data.
    fn trusted(k: u64) -> Self {
        Self {
            certify: k,
            invalidate: k,
            evict: k,
            challenge: k,
            snapshot: k,
            assignment: k,
            synced: k,
        }
    }
}

/// A network's fixed settings, set at genesis and read from `System`
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct NetworkParams {
    /// Every track is coded with this erasure code
    pub profile: ErasureCode,
    /// What the network assumes about its nodes
    pub trust: TrustModel,
    /// Signature counts for every check over a group's members
    pub thresholds: Thresholds,
}

impl NetworkParams {
    /// The settings of a network on this profile and trust model
    pub fn try_new(profile: ErasureCode, trust: TrustModel) -> Result<Self, ThresholdError> {
        let thresholds =
            Thresholds::try_new(trust, u64::from(profile.n()), u64::from(profile.k()))?;

        Ok(Self { profile, trust, thresholds })
    }

    /// Spools in a group, which is also the slices in a coded track
    pub fn n(&self) -> usize {
        usize::from(self.profile.n())
    }

    /// Slices that recover a track
    pub fn k(&self) -> usize {
        usize::from(self.profile.k())
    }
}

/// Parse a stored profile and check it against the trust model's rules
///
/// A permissionless network caps k at the nodes that may lie plus one, which a
/// certified track's honest holders always cover.
pub fn validate_profile(
    trust: TrustModel,
    profile: EncodingProfile,
) -> Result<ErasureCode, ThresholdError> {
    let code = ErasureCode::try_from(profile)?;
    let n = u64::from(code.n());
    let k = u64::from(code.k());

    let limit = max_faulty(n) + 1;
    if trust == TrustModel::Permissionless && k > limit {
        return Err(ThresholdError::PermissionlessRecovery { n, k, limit });
    }

    Ok(code)
}

/// f, the malicious nodes a group of n withstands: floor((n - 1) / 3)
const fn max_faulty(n: u64) -> u64 {
    (n - 1) / 3
}

/// Smallest count above two thirds of n
const fn supermajority(n: u64) -> u64 {
    (2 * n + 3) / 3
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::encoding::{ClayParams, RSParams, RepParams, TAPENET_PROFILE};

    const WIDTHS: std::ops::RangeInclusive<u64> = 1..=MAX_GROUP_SIZE as u64;

    // The tapenet network uses spool groups of 20 nodes and can recover from 7 failures within a
    // group. We assume 6 of them may lie, so the supermajority is 14 and the honest count is 7.
    // Local or private netowrks can use any profile they like.
    #[test]
    fn tapenet() {
        let thresholds = Thresholds::try_new(TrustModel::Permissionless, 20, 7).expect("tapenet");

        assert_eq!(
            thresholds,
            Thresholds {
                certify: 14,
                invalidate: 14,
                evict: 14,
                challenge: 14,
                snapshot: 7,
                assignment: 7,
                synced: 7,
            },
        );
    }

    // the quorum is the smallest count above two thirds of n, f is the nodes
    // it can do without, and every other count follows from those two and k
    #[test]
    fn permissionless_counts() {
        for n in WIDTHS {
            for k in 1..=n {
                let thresholds = Thresholds::try_new(TrustModel::Permissionless, n, k).expect("valid");
                let quorum = thresholds.certify;
                let faulty = n - quorum;

                assert!(3 * quorum > 2 * n);
                assert!(3 * (quorum - 1) <= 2 * n);
                assert_eq!(faulty, max_faulty(n));
                assert_eq!(
                    thresholds,
                    Thresholds {
                        certify: quorum,
                        invalidate: quorum,
                        evict: quorum,
                        challenge: quorum,
                        snapshot: faulty + 1,
                        assignment: faulty + 1,
                        synced: k.max(faulty + 1),
                    },
                );
            }
        }
    }

    // every Trusted count is k
    #[test]
    fn trusted_counts() {
        for n in WIDTHS {
            for k in 1..=n {
                let thresholds = Thresholds::try_new(TrustModel::Trusted, n, k).expect("valid");

                assert_eq!(
                    thresholds,
                    Thresholds {
                        certify: k,
                        invalidate: k,
                        evict: k,
                        challenge: k,
                        snapshot: k,
                        assignment: k,
                        synced: k,
                    },
                );
            }
        }
    }

    // a group size of zero or above the array width is refused
    #[test]
    fn bad_width() {
        let wide = MAX_GROUP_SIZE as u64 + 1;

        for trust in [TrustModel::Permissionless, TrustModel::Trusted] {
            assert_eq!(Thresholds::try_new(trust, 0, 1), Err(ThresholdError::Width(0)));
            assert_eq!(Thresholds::try_new(trust, wide, 7), Err(ThresholdError::Width(wide)));
        }
    }

    // a recovery count of zero or above the group size is refused
    #[test]
    fn bad_recovery() {
        for trust in [TrustModel::Permissionless, TrustModel::Trusted] {
            assert_eq!(
                Thresholds::try_new(trust, 4, 0),
                Err(ThresholdError::Recovery { n: 4, k: 0 }),
            );
            assert_eq!(
                Thresholds::try_new(trust, 4, 5),
                Err(ThresholdError::Recovery { n: 4, k: 5 }),
            );
        }
    }

    // a zeroed trust model has no value, so an unset System can't yield thresholds
    #[test]
    fn zero_model() {
        assert!(TrustModel::try_from(0u64).is_err());
        assert_eq!(TrustModel::try_from(1u64), Ok(TrustModel::Permissionless));
        assert_eq!(TrustModel::try_from(2u64), Ok(TrustModel::Trusted));
        assert!(TrustModel::try_from(3u64).is_err());
    }

    // each trust model reads back from the text it writes
    #[test]
    fn model_text() {
        for trust in [TrustModel::Permissionless, TrustModel::Trusted] {
            assert_eq!(trust.to_string().parse(), Ok(trust));
        }
        assert_eq!("open".parse::<TrustModel>(), Err(ThresholdError::TrustModel));
    }

    // tapenet's profile sits exactly at the permissionless limit
    #[test]
    fn tapenet_profile() {
        let above = EncodingProfile::clay(ClayParams::new(20, 8, 16));

        assert_eq!(
            validate_profile(TrustModel::Permissionless, TAPENET_PROFILE.into()),
            Ok(TAPENET_PROFILE),
        );
        assert_eq!(
            validate_profile(TrustModel::Permissionless, above),
            Err(ThresholdError::PermissionlessRecovery { n: 20, k: 8, limit: 7 }),
        );
    }

    // a permissionless network accepts k up to n + 1 - certify and nothing above
    #[test]
    fn recovery_rule() {
        for n in 2..=MAX_GROUP_SIZE as u8 {
            let certify = Thresholds::try_new(TrustModel::Permissionless, u64::from(n), 1)
                .expect("valid")
                .certify;
            let limit = u64::from(n) + 1 - certify;

            for k in 1..n {
                let profile = EncodingProfile::reed_solomon(RSParams::new(k, n - k));
                let is_accepted = validate_profile(TrustModel::Permissionless, profile).is_ok();

                assert_eq!(is_accepted, u64::from(k) <= limit, "n {n} k {k}");
            }
        }
    }

    // a trusted cluster takes any profile its code allows
    #[test]
    fn trusted_profiles() {
        let profiles = [
            EncodingProfile::reed_solomon(RSParams::new(3, 1)),
            EncodingProfile::reed_solomon(RSParams::new(4, 2)),
            EncodingProfile::clay(ClayParams::new(10, 7, 9)),
            EncodingProfile::replication(RepParams::new(1)),
            EncodingProfile::replication(RepParams::new(2)),
        ];

        for profile in profiles {
            assert!(validate_profile(TrustModel::Trusted, profile).is_ok());
        }
        assert!(validate_profile(TrustModel::Permissionless, profiles[0]).is_err());
    }

    // a profile its code refuses is refused under either trust model
    #[test]
    fn bad_profile() {
        let cases = [
            (EncodingProfile::unknown(), ProfileError::UnknownEncoding),
            (EncodingProfile::reed_solomon(RSParams::new(4, 0)), ProfileError::Recovery),
            (EncodingProfile::clay(ClayParams::new(20, 7, 7)), ProfileError::Helpers),
            (EncodingProfile::replication(RepParams::new(21)), ProfileError::Width),
        ];

        for trust in [TrustModel::Permissionless, TrustModel::Trusted] {
            for (profile, error) in cases {
                assert_eq!(validate_profile(trust, profile), Err(ThresholdError::Profile(error)));
            }
        }
    }
}
