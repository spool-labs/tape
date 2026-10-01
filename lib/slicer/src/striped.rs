//! The coder a network's profile selects, so every code shares the striped path.

use tape_core::encoding::{ErasureCode, ProfileError};

use crate::clay::ClayCoder;
use crate::reed_solomon::ReedSolomonCoder;
use crate::replication::ReplicationCoder;
use crate::{DecodeError, EncodeError, ErasureCoder};

/// One of the three codes a network can run.
pub enum StripedCoder {
    Clay(ClayCoder),
    ReedSolomon(ReedSolomonCoder),
    Replication(ReplicationCoder),
}

impl StripedCoder {
    /// Build the coder for a network's profile, refusing parameters its code
    /// can't run.
    pub fn try_new(profile: ErasureCode) -> Result<Self, ProfileError> {
        profile.validate()?;

        Ok(match profile {
            ErasureCode::Clay(params) => Self::Clay(ClayCoder::from_params(params)),
            ErasureCode::ReedSolomon(params) => {
                Self::ReedSolomon(ReedSolomonCoder::new(params.k() as usize, params.m() as usize))
            }
            ErasureCode::Replication(params) => {
                Self::Replication(ReplicationCoder::from_params(params))
            }
        })
    }
}

impl ErasureCoder for StripedCoder {
    #[inline]
    fn k(&self) -> usize {
        match self {
            Self::Clay(coder) => coder.k(),
            Self::ReedSolomon(coder) => coder.k(),
            Self::Replication(coder) => coder.k(),
        }
    }

    #[inline]
    fn m(&self) -> usize {
        match self {
            Self::Clay(coder) => coder.m(),
            Self::ReedSolomon(coder) => coder.m(),
            Self::Replication(coder) => coder.m(),
        }
    }

    #[inline]
    fn stripe_alignment(&self) -> usize {
        match self {
            Self::Clay(coder) => coder.stripe_alignment(),
            Self::ReedSolomon(coder) => coder.stripe_alignment(),
            Self::Replication(coder) => coder.stripe_alignment(),
        }
    }

    fn encode(&mut self, data: &[u8]) -> Result<Vec<Vec<u8>>, EncodeError> {
        match self {
            Self::Clay(coder) => coder.encode(data),
            Self::ReedSolomon(coder) => coder.encode(data),
            Self::Replication(coder) => coder.encode(data),
        }
    }

    fn decode(&self, chunks: &[(usize, &[u8])]) -> Result<Vec<u8>, DecodeError> {
        match self {
            Self::Clay(coder) => coder.decode(chunks),
            Self::ReedSolomon(coder) => coder.decode(chunks),
            Self::Replication(coder) => coder.decode(chunks),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tape_core::encoding::{ClayParams, RSParams, RepParams, TAPENET_PROFILE};

    // each profile builds its own coder with the profile's n and k
    #[test]
    fn selects_coder() {
        let profiles = [
            TAPENET_PROFILE,
            ErasureCode::Clay(ClayParams::new(4, 2, 3)),
            ErasureCode::ReedSolomon(RSParams::new(3, 1)),
            ErasureCode::ReedSolomon(RSParams::new(4, 2)),
            ErasureCode::Replication(RepParams::new(2)),
        ];

        for profile in profiles {
            let coder = StripedCoder::try_new(profile).expect("valid profile");

            assert_eq!(coder.n(), profile.n() as usize);
            assert_eq!(coder.k(), profile.k() as usize);
        }
    }

    // parameters a code can't run build no coder
    #[test]
    fn bad_profile() {
        let no_parity = ErasureCode::ReedSolomon(RSParams::new(4, 0));
        let bad_helpers = ErasureCode::Clay(ClayParams::new(20, 7, 7));

        assert_eq!(StripedCoder::try_new(no_parity).err(), Some(ProfileError::Recovery));
        assert_eq!(StripedCoder::try_new(bad_helpers).err(), Some(ProfileError::Helpers));
    }
}
