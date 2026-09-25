//! A node signs each track write once per epoch and reuses the signature

use std::collections::HashMap;
use std::sync::{Mutex, PoisonError};

use tape_core::bls::{BlsPrivateKey, BlsSignature};
use tape_core::types::EpochNumber;
use tape_crypto::bls12254::errors::BLSError;
use tape_crypto::Hash;
use tape_core::cert::track::TrackWriteMessage;

/// The cache starts over once it holds this many tracks
const MAX_SIGNATURES: usize = 4096;

/// Signs each track write once per epoch, however many of its slices the node stores
#[derive(Default)]
pub struct WriteSignatures {
    signed: Mutex<Signed>,
}

#[derive(Default)]
struct Signed {
    epoch: EpochNumber,
    signatures: HashMap<Hash, BlsSignature>,
}

impl WriteSignatures {
    /// An empty cache
    pub fn new() -> Self {
        Self::default()
    }

    /// The node's signature over this track write, made on first use
    pub fn sign(
        &self,
        key: &BlsPrivateKey,
        epoch: EpochNumber,
        track_hash: Hash,
    ) -> Result<BlsSignature, BLSError> {
        if let Some(signature) = self.cached(epoch, &track_hash) {
            return Ok(signature);
        }
        let signature = key.sign(TrackWriteMessage::new(epoch, track_hash).to_bytes())?;
        let mut signed = self.signed.lock().unwrap_or_else(PoisonError::into_inner);
        if signed.epoch != epoch || signed.signatures.len() >= MAX_SIGNATURES {
            signed.epoch = epoch;
            signed.signatures.clear();
        }
        signed.signatures.insert(track_hash, signature);
        Ok(signature)
    }

    fn cached(&self, epoch: EpochNumber, track_hash: &Hash) -> Option<BlsSignature> {
        let signed = self.signed.lock().unwrap_or_else(PoisonError::into_inner);
        if signed.epoch != epoch {
            return None;
        }
        signed.signatures.get(track_hash).copied()
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.signed.lock().unwrap_or_else(PoisonError::into_inner).signatures.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // a second slice of the same track reuses the first signature
    #[test]
    fn reused() {
        let key = BlsPrivateKey::from_random();
        let signatures = WriteSignatures::new();
        let track = Hash([4u8; 32]);

        let first = signatures.sign(&key, EpochNumber(3), track).expect("sign");
        let second = signatures.sign(&key, EpochNumber(3), track).expect("sign");

        assert_eq!(first, second);
        assert_eq!(signatures.len(), 1);
        assert_eq!(
            first,
            key.sign(TrackWriteMessage::new(EpochNumber(3), track).to_bytes()).expect("sign")
        );
    }

    // a new epoch signs afresh and drops the old epoch's signatures
    #[test]
    fn new_epoch() {
        let key = BlsPrivateKey::from_random();
        let signatures = WriteSignatures::new();
        let track = Hash([4u8; 32]);

        let old = signatures.sign(&key, EpochNumber(3), track).expect("sign");
        let new = signatures.sign(&key, EpochNumber(4), track).expect("sign");

        assert_ne!(old, new);
        assert_eq!(signatures.len(), 1);
    }
}
