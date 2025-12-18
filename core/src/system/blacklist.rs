use bytemuck::{Pod, Zeroable};
use crate::types::*;
use tape_crypto::{
    merkle::{MerkleTree, MerkleError},
    Hash,
};

pub const BLACKLIST: &[u8] = b"blacklist";

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
            state: MerkleTree::default(),
            size: StorageUnits::zero(),
            count: 0,
        }
    }

    pub fn root(&self) -> Hash {
        self.state.root()
    }

    pub fn capacity(&self) -> u64 {
        MerkleTree::<N>::capacity()
    }

    pub fn items(&self) -> u64 {
        self.count
    }

    pub fn total_size(&self) -> StorageUnits {
        self.size
    }

    /// Adds a new blacklist entry (blob_hash, units).
    pub fn add(&mut self, blob_hash: Hash, units: StorageUnits) -> Result<(), BlacklistError> {
        let leaf = blacklist_entry(blob_hash, units);

        self.state.add_leaf(&leaf)
            .map_err(BlacklistError::from)?;

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
        &mut self, 
        index: u64, 
        proof: &[P], 
        blob_hash: Hash, 
        units: StorageUnits
    ) -> Result<(), BlacklistError>
    where
        P: Into<Hash> + Copy,
    {
        let leaf = blacklist_entry(blob_hash, units);

        self.state
            .remove_leaf(index, proof, &leaf)
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
    pub fn contains<P>(
        &self, 
        index: u64,
        proof: &[P], 
        blob_hash: Hash, 
        units: StorageUnits
        ) -> bool
    where
        P: Into<Hash> + Copy,
    {
        let leaf = blacklist_entry(blob_hash, units);

        self.state.contains(index, proof, &leaf)
    }
}

impl From<MerkleError> for BlacklistError {
    fn from(e: MerkleError) -> Self {
        match e {
            MerkleError::TreeFull => BlacklistError::TreeFull,
            MerkleError::InvalidProof => BlacklistError::BadProof,
            MerkleError::InvalidIndex => BlacklistError::BadArgument,
            MerkleError::ProofLength => BlacklistError::BadLength,
        }
    }
}

/// Creates a leaf node from (blob_hash, units).
pub fn blacklist_entry(blob_hash: Hash, units: StorageUnits) -> Vec<u8> {
    let units_bytes = units.pack();
    [
        blob_hash.as_ref(), 
        units_bytes.as_ref()
    ].concat()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_blacklist() {
        let mut bl = Blacklist::<3>::new();

        let blob_hash = Hash::from([7u8; 32]);
        let units = StorageUnits::from_bytes(1); // 1 byte => 1 unit (ceiling)

        // Add
        bl.add(blob_hash, units).unwrap();
        assert_eq!(bl.items(), 1);
        assert_eq!(bl.total_size(), units);

        // Build leaf and proof for index 0
        let leaf = blacklist_entry(blob_hash, units);
        let leaves = [leaf];
        let proof = bl.state.create_proof(&leaves, 0).unwrap();

        // Contains
        assert!(bl.contains(0, &proof, blob_hash, units));

        // Remove
        bl.remove(0, &proof, blob_hash, units).unwrap();
        assert_eq!(bl.items(), 0);
        assert_eq!(bl.total_size(), StorageUnits::zero());

        // No longer contained
        assert!(!bl.contains(0, &proof, blob_hash, units));
    }
}
