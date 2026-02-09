use bytemuck::{Pod, Zeroable};
use crate::erasure::SPOOL_GROUP_SIZE;
use crate::system::Committee;
use crate::types::Bitmap;
use super::{Spooler, SpoolerError};
use super::dhondt::DhondtSpooler;
use super::sainte_lague::SainteLagueSpooler;
use super::migrate::to_spool_map;
use super::{SpoolGroup, SpoolIndex, SpoolCount, SpoolMapping};
use tape_crypto::hash::{hashv, Hash};

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct SpoolAssignment<const SPOOLS: usize>(pub [SpoolMapping; SPOOLS]);

unsafe impl<const SPOOLS: usize> Zeroable for SpoolAssignment<SPOOLS> {}
unsafe impl<const SPOOLS: usize> Pod for SpoolAssignment<SPOOLS> {}

impl <const SPOOLS: usize> SpoolAssignment<SPOOLS> {
    pub fn new(spools: [SpoolMapping; SPOOLS]) -> Self {
        Self(spools)
    }

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

    pub fn try_from_counts(spool_counts: &[SpoolCount]) -> Result<Self, SpoolerError> {
        let spool_map = to_spool_map(spool_counts);
        Self::try_from(&spool_map)
    }

    /// Migrate spools from current committee to next committee using a policy `Spooler`,
    /// with minimal disruption of existing placements.
    pub fn migrate_with<S: Spooler, const N:usize>(
        &mut self,
        spooler: &mut S,
        current: &Committee<N>,
        next: &Committee<N>,
    ) -> Result<(), SpoolerError> {
        let members_current = current.active_members();
        let members_next    = next.active_members();
        let stakes_next     = next.active_stakes();

        let spool_counts = spooler.allocate(&stakes_next, SPOOLS as u16)?;

        let spools = super::migrate_spools(&self.0, &members_current, &members_next, &spool_counts)?;
        for i in 0..SPOOLS {
            self.0[i] = spools[i];
        }
        Ok(())
    }

    /// Convenience: use DhondtSpooler.
    pub fn migrate_dhondt<const N:usize>(
        &mut self,
        current: &Committee<N>,
        next: &Committee<N>,
    ) -> Result<(), SpoolerError> {
        let mut dh = DhondtSpooler::default();
        self.migrate_with(&mut dh, current, next)
    }

    /// Convenience: use SainteLagueSpooler.
    pub fn migrate_sainte_lague<const N:usize>(
        &mut self,
        current: &Committee<N>,
        next: &Committee<N>,
    ) -> Result<(), SpoolerError> {
        let mut sl = SainteLagueSpooler::default();
        self.migrate_with(&mut sl, current, next)
    }

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

    pub fn iter(&self) -> impl Iterator<Item = &SpoolMapping> {
        self.0.iter()
    }

    /// Get the member mappings for a spool group (SPOOL_GROUP_SIZE entries).
    pub fn members_in_group(&self, group: SpoolGroup) -> &[SpoolMapping] {
        let start = group as usize * SPOOL_GROUP_SIZE;
        let end = start + SPOOL_GROUP_SIZE;
        &self.0[start..end]
    }

    /// Count how many spools in a group are owned by members in the bitmap.
    pub fn group_weight<const BYTES: usize>(&self, group: SpoolGroup, bitmap: &Bitmap<BYTES>) -> u64 {
        let start = group as usize * SPOOL_GROUP_SIZE;
        let end = start + SPOOL_GROUP_SIZE;
        let mut weight = 0u64;
        for i in start..end {
            if bitmap.is_set(self.0[i] as usize) {
                weight += 1;
            }
        }
        weight
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
        assert_eq!(format!("{:?}", err), format!("{:?}", SpoolerError::TotalMismatch));
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
        assert_eq!(format!("{:?}", res.unwrap_err()), format!("{:?}", SpoolerError::TotalMismatch));
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
    fn members_in_group() {
        // 40 spools, group size 20 → 2 groups
        let mut arr = [0u8; 40];
        for i in 0..20 { arr[i] = (i % 4) as u8; }       // group 0: members 0-3
        for i in 20..40 { arr[i] = ((i - 20) % 3) as u8; } // group 1: members 0-2
        let sa = SpoolAssignment::new(arr);

        let g0 = sa.members_in_group(0);
        assert_eq!(g0.len(), 20);
        assert_eq!(g0[0], 0);
        assert_eq!(g0[1], 1);

        let g1 = sa.members_in_group(1);
        assert_eq!(g1.len(), 20);
        assert_eq!(g1[0], 0);
    }

    #[test]
    fn group_weight_with_bitmap() {
        use crate::types::Bitmap;
        // 40 spools, 2 groups
        let mut arr = [0u8; 40];
        // Group 0: all owned by member 0
        for i in 0..20 { arr[i] = 0; }
        // Group 1: split between members 1 and 2
        for i in 20..40 { arr[i] = if i % 2 == 0 { 1 } else { 2 }; }
        let sa = SpoolAssignment::new(arr);

        // Bitmap with members 0 and 1 set
        let bm = Bitmap::<1>::from_indices(&[0, 1], 8);
        assert_eq!(sa.group_weight(0, &bm), 20); // all 20 owned by member 0
        assert_eq!(sa.group_weight(1, &bm), 10); // 10 owned by member 1
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
