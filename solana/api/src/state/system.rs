use tape_solana::*;
use tape_core::encoding::{EncodingProfile, ErasureCode};
use tape_core::spooler::{GroupFold, GroupIndex, TrackGroup};
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

    /// Number of spool groups live in the previous epoch.
    pub previous_group_count: u64,

    /// Number of track groups: the most spool groups ever live. A leaf's group is below it.
    pub track_group_count: u64,

    /// Lower bound on aggregated NodePreferences::epoch_duration.
    pub min_epoch_duration: EpochDuration,

    /// Upper bound on aggregated NodePreferences::epoch_duration.
    pub max_epoch_duration: EpochDuration,

    /// Erasure code profile every track uses.
    pub profile: EncodingProfile,

    /// Whether this is a permissionless or trusted network.
    pub trust_model: u64,
}

impl System {
    /// The network's erasure code, parsed and checked.
    pub fn code(&self) -> Result<ErasureCode, ProgramError> {
        ErasureCode::try_from(self.profile)
            .map_err(|_| TapeError::UnexpectedState.into())
    }

    /// What the network assumes about its nodes.
    pub fn trust(&self) -> Result<TrustModel, ProgramError> {
        TrustModel::try_from(self.trust_model)
            .map_err(|_| TapeError::UnexpectedState.into())
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
        NetworkParams::try_new(self.code()?, self.trust()?)
            .map_err(|_| TapeError::UnexpectedState.into())
    }

    /// Where each track group lives in the current epoch.
    pub fn fold(&self) -> Result<GroupFold, ProgramError> {
        GroupFold::new(self.live_group_count, self.track_group_count)
            .ok_or_else(|| TapeError::UnexpectedState.into())
    }

    /// The live group that carries a track group.
    pub fn live_group(&self, group: TrackGroup) -> Result<GroupIndex, ProgramError> {
        Ok(self.fold()?.live_group(group))
    }

    /// How many groups take new tracks: the live ones that stay live next epoch.
    pub fn writable_group_count(&self) -> u64 {
        self.live_group_count.min(self.target_group_count)
    }

    /// Synced spools that make a group ready in the current epoch.
    ///
    /// After the group count drops, every holder of a retired group's data is
    /// new, so a group waits for as many synced spools as certify a track.
    pub fn synced_threshold(&self) -> Result<u64, ProgramError> {
        let thresholds = self.thresholds()?;

        if self.live_group_count < self.previous_group_count {
            return Ok(thresholds.certify);
        }

        Ok(thresholds.synced)
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

    // a retired track group certifies under the live group that carries it
    #[test]
    fn live_group() {
        let system = System {
            target_group_count: 5,
            live_group_count: 5,
            track_group_count: 10,
            ..System::zeroed()
        };

        assert_eq!(system.live_group(TrackGroup(3)), Ok(GroupIndex(3)));
        assert_eq!(system.live_group(TrackGroup(7)), Ok(GroupIndex(3)));
        assert_eq!(system.live_group(TrackGroup(8)), Ok(GroupIndex(0)));
        assert!(System::zeroed().live_group(TrackGroup(0)).is_err());
    }

    // a group about to retire takes no new tracks
    #[test]
    fn writable_groups() {
        let shrinking = System { target_group_count: 5, live_group_count: 10, ..System::zeroed() };
        let growing = System { target_group_count: 10, live_group_count: 5, ..System::zeroed() };

        assert_eq!(shrinking.writable_group_count(), 5);
        assert_eq!(growing.writable_group_count(), 5);
    }

    // the epoch after the count drops waits for the certify count
    #[test]
    fn synced_after_shrink() {
        let steady = System {
            profile: TAPENET_PROFILE.into(),
            trust_model: TrustModel::Permissionless.into(),
            live_group_count: 10,
            previous_group_count: 10,
            ..System::zeroed()
        };
        let shrunk = System { live_group_count: 5, ..steady };
        let grown = System { live_group_count: 20, ..steady };

        assert_eq!(steady.synced_threshold(), Ok(7));
        assert_eq!(shrunk.synced_threshold(), Ok(14));
        assert_eq!(grown.synced_threshold(), Ok(7));
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
