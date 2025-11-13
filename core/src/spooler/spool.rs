use bytemuck::{Pod, Zeroable};
use crate::system::Committee;
use super::assignment::*;
use super::dhondt::*;
use tape_crypto::hash::{hashv, Hash};

pub type SpoolIndex = u16;
pub type SpoolCount = u16;
pub type SpoolMapping = u8;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SpoolerError {
    CountMismatch,
    MemberLimit,
    TotalMismatch,
    BalanceMismatch,
    InsufficientFree,
    BadIndex,
    NotNext,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct SpoolAssignment<const SPOOLS: usize>(pub [SpoolMapping; SPOOLS]);

unsafe impl<const SPOOLS: usize> Zeroable for SpoolAssignment<SPOOLS> {}
unsafe impl<const SPOOLS: usize> Pod for SpoolAssignment<SPOOLS> {}

impl <const SPOOLS: usize> SpoolAssignment<SPOOLS> {

    /// Create a new spool map from an array.
    pub fn new(spools: [SpoolMapping; SPOOLS]) -> Self {
        Self(spools)
    }

    /// Create a spool map from a slice.
    pub fn try_from(spool_map: &[SpoolMapping]) -> Result<Self, SpoolerError> {
        if spool_map.len() != SPOOLS {
            return Err(SpoolerError::TotalMismatch);
        }

        let mut spools = [0u8; SPOOLS];
        for i in 0..SPOOLS {
            spools[i] = spool_map[i];
        }

        Ok(Self(spools))
    }

    /// Create an initial spool map from spool counts, assigning spools contiguously.
    pub fn try_from_counts(
        spool_counts: &[SpoolCount],
    ) -> Result<Self, SpoolerError> {
        let spool_map = to_spool_map(spool_counts);
        Self::try_from(&spool_map)
    }

    /// Reassign spools from current committee to next committee with minimal disruption.
    pub fn reassign<const N:usize>(
        &mut self,
        current: &Committee<N>,
        next: &Committee<N>,
    ) -> Result<(), SpoolerError> {

        let members_current = current.active_members();
        let members_next    = next.active_members();
        let stakes_next     = next.active_stakes();

        // Figure out how many spools each member should get.
        let spool_counts = dhondt_allocate(
            &stakes_next,
            SPOOLS as u16,
        );

        // Distribute spools with minimal disruption.
        let spools = reassign_spools(
            &self.0,
            &members_current,
            &members_next,
            &spool_counts,
        )?;

        // Update spool mapping
        for i in 0..SPOOLS {
            self.0[i] = spools[i];
        }

        Ok(())
    }

    /// Returns the voting weight for a given member based on how many spools they hold.
    pub fn weight(&self, member_index: usize) -> u16 {
        debug_assert!(member_index <= u8::MAX as usize);

        let mut count = 0u16;
        for i in 0..SPOOLS {
            if self.0[i] as usize == member_index {
                count += 1;
            }
        }
        count
    }

    /// Returns a slice of the spool mappings for a given member.
    pub fn spools_for_member(&self, member_index: usize) -> Vec<SpoolIndex> {
        debug_assert!(member_index <= u8::MAX as usize);

        let mut spool_indices = Vec::new();
        for i in 0..SPOOLS {
            if self.0[i] as usize == member_index {
                spool_indices.push(i as SpoolIndex);
            }
        }
        spool_indices
    }

    /// Returns an iterator over the spool mappings.
    pub fn iter(&self) -> impl Iterator<Item = &SpoolMapping> {
        self.0.iter()
    }
}

pub fn get_spool_hash(spools: &[SpoolIndex]) -> Hash {
    let data: &[&[u8]] = &[bytemuck::cast_slice(spools)];
    hashv(data)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn from_slice_okay() {
        let s = SpoolAssignment::<4>::try_from(&[0u8, 1, 1, 0]).unwrap();
        assert_eq!(s.0, [0, 1, 1, 0]);
    }

    #[test]
    fn from_slice_bad_length() {
        let err = SpoolAssignment::<3>::try_from(&[0u8, 1, 2, 3]).unwrap_err();
        assert_eq!(err, SpoolerError::TotalMismatch);
    }

    #[test]
    fn from_counts_weight() {
        let counts: &[SpoolCount] = &[2, 1, 3];
        let spools = SpoolAssignment::<6>::try_from_counts(counts).unwrap();

        assert_eq!(spools.0, [0, 0, 1, 2, 2, 2]);
        assert_eq!(spools.weight(0), 2);
        assert_eq!(spools.weight(1), 1);
        assert_eq!(spools.weight(2), 3);
        assert_eq!(spools.weight(3), 0);
    }

    #[test]
    fn from_counts_bad_length() {
        let counts: &[SpoolCount] = &[2, 1, 1]; // total 4
        let res = SpoolAssignment::<3>::try_from_counts(counts);
        assert_eq!(res.unwrap_err(), SpoolerError::TotalMismatch);
    }

    #[test]
    fn weight_count() {
        let spools = SpoolAssignment::new([3, 3, 3, 2, 1]);
        assert_eq!(spools.weight(3), 3);
        assert_eq!(spools.weight(2), 1);
        assert_eq!(spools.weight(1), 1);
        assert_eq!(spools.weight(0), 0);
    }

    #[test]
    fn empty_weight() {
        let spools = SpoolAssignment::new([]);
        assert_eq!(spools.weight(0), 0);
        assert_eq!(SpoolAssignment::try_from(&[]).unwrap().0, []);
    }

    #[test]
    fn spools_slice() {
        let spools = SpoolAssignment::new([1, 0, 1, 2, 1, 0]);
        let member_spools = spools.spools_for_member(1);
        assert_eq!(member_spools, vec![0, 2, 4]);
    }

    #[test]
    fn spool_hash() {
        let spools: &[SpoolIndex] = &[42, 1, 2, 3, 4, 5, 99];
        let hash = get_spool_hash(spools);
        let expected_bytes: [u8; 32] = [
            0x41, 0x03, 0xab, 0xff, 0x9f, 0xac, 0xfc, 0x32,
            0x5a, 0xa0, 0x2c, 0x99, 0x23, 0x6b, 0xfc, 0xc9,
            0xea, 0x56, 0xdc, 0x08, 0x41, 0xf3, 0x04, 0xab,
            0x79, 0xd4, 0x5d, 0x3e, 0xe4, 0x0f, 0xbe, 0xcf,
        ];
        assert_eq!(hash.to_bytes(), expected_bytes);
    }
}
