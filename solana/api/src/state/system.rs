use tape_solana::*;
use tape_core::encoding::{EncodingProfile, ErasureCode};
use tape_core::thresholds::{NetworkParams, Thresholds, TrustModel};
use tape_core::types::{BitmapRead, EpochDuration, EpochNumber, SpoolBitmap};
use super::AccountType;
use crate::errors::TapeError;

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Pod, Zeroable)]
pub struct System {
    /// Current epoch number.
    pub current_epoch: EpochNumber,

    /// Total registered storage nodes.
    pub total_nodes: u64,

    /// Member capacity of newly created committees.
    pub committee_size: u64,

    /// Target number of spool groups for future epoch creation.
    pub target_group_count: u64,

    /// Number of spool groups live in the current epoch.
    pub live_group_count: u64,

    /// Lower bound on aggregated NodePreferences::epoch_duration. Set once at
    /// StartNetwork; immutable thereafter.
    pub min_epoch_duration: EpochDuration,

    /// Upper bound on aggregated NodePreferences::epoch_duration. Set once at
    /// StartNetwork; immutable thereafter.
    pub max_epoch_duration: EpochDuration,

    /// Erasure code every track uses. Set once at CreateSystem; immutable
    /// thereafter.
    pub profile: EncodingProfile,

    /// What the network assumes about its nodes (TrustModel as u64). Set once
    /// at CreateSystem; immutable thereafter.
    pub trust_model: u64,
}

impl System {
    /// The network's erasure code, parsed and checked.
    pub fn code(&self) -> Result<ErasureCode, ProgramError> {
        ErasureCode::try_from(self.profile).map_err(|_| TapeError::UnexpectedState.into())
    }

    /// What the network assumes about its nodes.
    pub fn trust(&self) -> Result<TrustModel, ProgramError> {
        TrustModel::try_from(self.trust_model).map_err(|_| TapeError::UnexpectedState.into())
    }

    /// Signature counts for every check over a group's members.
    pub fn thresholds(&self) -> Result<Thresholds, ProgramError> {
        let trust = self.trust()?;
        let code = self.code()?;

        Thresholds::try_new(trust, u64::from(code.n()), u64::from(code.k()))
            .map_err(|_| TapeError::UnexpectedState.into())
    }

    /// The profile, trust model and thresholds together, for a client or node
    /// that reads them once and keeps them.
    pub fn params(&self) -> Result<NetworkParams, ProgramError> {
        Ok(NetworkParams {
            profile: self.code()?,
            trust: self.trust()?,
            thresholds: self.thresholds()?,
        })
    }

    /// Group positions set in a signer bitmap.
    ///
    /// A bit at or past n is refused: those spools are empty, and counting
    /// one would add signer weight nobody signed for.
    pub fn signers(&self, bitmap: &SpoolBitmap) -> Result<Vec<usize>, ProgramError> {
        let width = usize::from(self.code()?.n());
        let positions = bitmap.indices();
        if positions.iter().any(|position| *position >= width) {
            return Err(TapeError::BadMember.into());
        }

        Ok(positions)
    }
}

tape_solana::state!(AccountType, System);

#[cfg(test)]
mod tests {
    use super::*;
    use tape_core::encoding::{RSParams, TAPENET_PROFILE};

    // a zeroed System has no code, trust model or thresholds
    #[test]
    fn unset() {
        let system = System::zeroed();

        assert!(system.code().is_err());
        assert!(system.trust().is_err());
        assert!(system.thresholds().is_err());
        assert!(system.params().is_err());
    }

    // a tapenet System yields tapenet's counts
    #[test]
    fn tapenet() {
        let system = System {
            profile: TAPENET_PROFILE.into(),
            trust_model: TrustModel::Permissionless.into(),
            ..System::zeroed()
        };

        let thresholds = system.thresholds().expect("tapenet");

        assert_eq!(system.code(), Ok(TAPENET_PROFILE));
        assert_eq!((thresholds.certify, thresholds.snapshot, thresholds.synced), (14, 7, 7));
    }

    // a trusted System counts k for everything
    #[test]
    fn trusted() {
        let system = System {
            profile: EncodingProfile::reed_solomon(RSParams::new(3, 1)),
            trust_model: TrustModel::Trusted.into(),
            ..System::zeroed()
        };

        let thresholds = system.thresholds().expect("3 of 4");

        assert_eq!((thresholds.certify, thresholds.snapshot, thresholds.synced), (3, 3, 3));

        let params = system.params().expect("3 of 4");

        assert_eq!((params.n(), params.k()), (4, 3));
        assert_eq!(params.trust, TrustModel::Trusted);
        assert_eq!(params.thresholds, thresholds);
    }

    // a set profile with an unset trust model yields no thresholds
    #[test]
    fn missing_trust() {
        let system = System {
            profile: TAPENET_PROFILE.into(),
            ..System::zeroed()
        };

        assert!(system.thresholds().is_err());
    }

    // a signer bit at or past n is refused, so empty spools add no weight
    #[test]
    fn signer_bounds() {
        let system = System {
            profile: EncodingProfile::reed_solomon(RSParams::new(3, 1)),
            trust_model: TrustModel::Trusted.into(),
            ..System::zeroed()
        };

        assert_eq!(system.signers(&SpoolBitmap::from_indices(&[0, 3])), Ok(vec![0, 3]));
        assert!(system.signers(&SpoolBitmap::from_indices(&[0, 4])).is_err());
        assert!(system.signers(&SpoolBitmap::from_indices(&[19])).is_err());
        assert!(System::zeroed().signers(&SpoolBitmap::from_indices(&[0])).is_err());
    }
}
