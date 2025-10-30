use bytemuck::{Pod, Zeroable};
use crate::types::*;
use tape_crypto::{
    merkle::{MerkleTree, TreeError},
    Hash,
};

const BLACKLIST: &[u8] = b"blacklist";

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BlacklistError {
    BadArgument,
    BadLength,
    BadProof,
    TreeFull,
    Overflow,
    Underflow,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Blacklist<const N: usize> {
    pub state: MerkleTree<N>,
    pub size: StorageUnits,
    pub count: u64,
}

unsafe impl<const N: usize> Zeroable for Blacklist<N> {}
unsafe impl<const N: usize> Pod for Blacklist<N> {}

impl<const N: usize> Blacklist<N> {
    pub fn new() -> Self {
        Self {
            state: MerkleTree::new(&[BLACKLIST]),
            size: StorageUnits::zero(),
            count: 0,
        }
    }

    pub fn root(&self) -> Hash {
        self.state.get_root()
    }

    pub fn capacity(&self) -> u64 {
        self.state.get_capacity()
    }

    pub fn items(&self) -> u64 {
        self.count
    }

    pub fn total_size(&self) -> StorageUnits {
        self.size
    }

    /// Adds a new blacklist entry (blob_hash, units).
    pub fn add(&mut self, blob_hash: Hash, units: StorageUnits) -> Result<(), BlacklistError> {
        let units_bytes = units.pack();
        let parts: [&[u8]; 2] = [
            blob_hash.as_ref(), 
            units_bytes.as_ref()
        ];

        self.state.try_add(&parts).map_err(BlacklistError::from)?;

        self.count = self
            .count
            .checked_add(1)
            .ok_or(BlacklistError::Overflow)?;

        self.size = self
            .size
            .checked_add(units)
            .ok_or(BlacklistError::Overflow)?;

        Ok(())
    }

    /// Removes an existing blacklist entry using a Merkle proof.
    /// The proof must correspond to the (blob_hash, units) pair.
    pub fn remove<P>(
        &mut self, proof: &[P], blob_hash: Hash, units: StorageUnits
    ) -> Result<(), BlacklistError>
    where
        P: Into<Hash> + Copy,
    {
        let units_bytes = units.pack();
        let parts: [&[u8]; 2] = [
            blob_hash.as_ref(), 
            units_bytes.as_ref()
        ];

        self.state
            .try_remove(proof, &parts)
            .map_err(BlacklistError::from)?;

        self.count = self
            .count
            .checked_sub(1)
            .ok_or(BlacklistError::Underflow)?;

        self.size = self
            .size
            .checked_sub(units)
            .ok_or(BlacklistError::Underflow)?;

        Ok(())
    }

    /// Checks membership using a provided Merkle proof.
    pub fn contains<P>(&self, proof: &[P], blob_hash: Hash, units: StorageUnits) -> bool
    where
        P: Into<Hash> + Copy,
    {
        let units_bytes = units.pack();
        let parts: [&[u8]; 2] = [
            blob_hash.as_ref(), 
            units_bytes.as_ref()
        ];
        self.state.contains(proof, &parts)
    }
}

impl From<TreeError> for BlacklistError {
    fn from(e: TreeError) -> Self {
        match e {
            TreeError::InvalidArgument => BlacklistError::BadArgument,
            TreeError::TreeFull => BlacklistError::TreeFull,
            TreeError::InvalidProof => BlacklistError::BadProof,
            TreeError::ProofLength => BlacklistError::BadLength,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tape_crypto::merkle::Leaf;

    #[test]
    fn test_blacklist() {
        let mut bl = Blacklist::<3>::new();

        let blob = Hash::from([7u8; 32]);
        let units = StorageUnits::from_bytes(1); // 1 byte => 1 unit (ceiling)

        // Add
        bl.add(blob, units).unwrap();
        assert_eq!(bl.items(), 1);
        assert_eq!(bl.total_size(), units);

        // Build leaf and proof for index 0
        let bytes = units.pack();
        let leaf = Leaf::new(&[blob.as_ref(), bytes.as_ref()]);
        let leaves = [leaf];
        let proof = bl.state.get_proof(&leaves, 0);

        // Contains
        assert!(bl.contains(&proof, blob, units));

        // Remove
        bl.remove(&proof, blob, units).unwrap();
        assert_eq!(bl.items(), 0);
        assert_eq!(bl.total_size(), StorageUnits::zero());

        // No longer contained
        assert!(!bl.contains(&proof, blob, units));
    }
}
