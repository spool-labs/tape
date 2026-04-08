use bytemuck::{Pod, Zeroable};
use crate::types::{Bitmap, EpochNumber};
use tape_crypto::Hash;
use crate::bls::{BlsSignature, BlsPubkey};
use tape_crypto::bls12254::min_sig::G1Point;
use super::CertificateError;

/// BLS certificate that tracks one-bit-per-member progress for an exact epoch.
/// The message is a 32-byte value (commonly a hash or address) to sign.
/// Each call verifies a single-signer BLS signature and sets the bit for that member.
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct BlsCertificate<const BITS: usize, const BYTES: usize> {
    /// The 32-byte message to be signed (e.g., a hash or address).
    pub message: Hash,

    /// Exact epoch for which signatures are valid (committee rotates every epoch).
    pub epoch: EpochNumber,

    /// Bitmap of committee members that have signed.
    pub signers: Bitmap<BITS, BYTES>,
}

unsafe impl<const BITS: usize, const BYTES: usize> Zeroable for BlsCertificate<BITS, BYTES> {}
unsafe impl<const BITS: usize, const BYTES: usize> Pod for BlsCertificate<BITS, BYTES> {}

impl<const BITS: usize, const BYTES: usize> BlsCertificate<BITS, BYTES> {
    /// Create a new certificate for an exact epoch and 32-byte message.
    #[inline]
    pub fn new(message: Hash, epoch: EpochNumber) -> Self {
        Self {
            message,
            epoch,
            signers: Bitmap::<BITS, BYTES>::zeroed(),
        }
    }

    /// Return the message as bytes for signing/verification.
    #[inline]
    pub fn message_bytes(&self) -> &[u8] {
        self.message.as_ref()
    }

    /// True if the bit for `committee_index` is already set.
    #[inline]
    pub fn has_signed(&self, committee_index: usize) -> bool {
        self.signers.is_set(committee_index)
    }

    /// Verify a single-signer BLS signature over this certificate's message and set the signer bit.
    pub fn try_add_signature(
        &mut self,
        committee_epoch: EpochNumber,
        committee_index: usize,
        signer_pubkey: BlsPubkey,
        signature: BlsSignature,
    ) -> Result<(), CertificateError> {
        if committee_epoch != self.epoch {
            return Err(CertificateError::EpochMismatch);
        }

        if self.signers.is_set(committee_index) {
            return Err(CertificateError::AlreadySigned);
        }

        // Decompress signature then verify with this certificate's message
        let sig_point = G1Point::try_from(&signature.0)
            .map_err(|_| CertificateError::SignatureInvalid)?;

        signer_pubkey.0
            .verify(&sig_point, self.message.as_ref())
            .map_err(|_| CertificateError::SignatureInvalid)?;

        self.signers.set(committee_index);
        Ok(())
    }

    /// Verify an aggregated BLS signature over this certificate's message and set multiple signer
    /// bits.
    pub fn try_add_aggregate(
        &mut self,
        committee_epoch: EpochNumber,
        new_indices: &[usize],
        signer_pubkeys: &[BlsPubkey],
        aggregated: BlsSignature,
    ) -> Result<(), CertificateError> {

        if committee_epoch != self.epoch {
            return Err(CertificateError::EpochMismatch);
        }

        if new_indices.is_empty() || new_indices.len() != signer_pubkeys.len() {
            return Err(CertificateError::BadIndex);
        }

        // Verify once
        aggregated
            .verify_aggregate(self.message_bytes(), signer_pubkeys)
            .map_err(|_| CertificateError::SignatureInvalid)?;

        // Set bits
        for &i in new_indices {
            self.signers.set(i);
        }
        Ok(())
    }

    /// Count of set bits (number of signatures recorded).
    #[inline]
    pub fn signer_count(&self) -> usize {
        self.signers.count_ones()
    }

    /// Merge signers from another certificate with the same (message, epoch).
    /// This ORs the bitmaps in place.
    pub fn merge_signers_from(&mut self, other: &Self) {
        debug_assert!(self.message == other.message && self.epoch == other.epoch, "certificate mismatch");
        let self_bytes = self.signers.as_bytes_mut();
        let other_bytes = other.signers.as_bytes();

        for i in 0..BYTES {
            self_bytes[i] |= other_bytes[i];
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bls::BlsPrivateKey;
    use crate::types::EpochNumber;
    use tape_crypto::hash::Hash;

    #[test]
    fn sign_single() {
        // Create a signer
        let sk = BlsPrivateKey::from_random();
        let pk = sk.public_key().unwrap();

        // Message is exactly the 32 bytes included in the certificate
        let message = Hash::from([9u8; 32]);
        let epoch = EpochNumber(42);

        // Sign the 32-byte message
        let sig = sk.sign(message.as_ref()).unwrap();

        // Create cert and verify/mark
        let mut cert = BlsCertificate::<16, 2>::new(message, epoch);
        assert_eq!(cert.signer_count(), 0);
        cert.try_add_signature(epoch, 1, pk, sig).expect("mark ok");

        assert!(cert.has_signed(1));
        assert_eq!(cert.signer_count(), 1);
    }

    #[test]
    fn epoch_mismatch() {
        let sk = BlsPrivateKey::from_random();
        let pk = sk.public_key().unwrap();
        let msg = Hash::from([7u8; 32]);
        let sig = sk.sign(msg.as_ref()).unwrap();

        let mut cert = BlsCertificate::<8, 1>::new(msg, EpochNumber(5));
        let err = cert.try_add_signature(EpochNumber(6), 0, pk, sig).unwrap_err();
        assert_eq!(err, CertificateError::EpochMismatch);
    }

    #[test]
    fn merge_signers() {
        let message = Hash::new_unique();
        let epoch = EpochNumber(1);

        // Two certs for same (message, epoch)
        let mut a = BlsCertificate::<16, 2>::new(message, epoch);
        let mut b = BlsCertificate::<16, 2>::new(message, epoch);

        // Set disjoint bits
        a.signers.set(3);
        b.signers.set(13);

        // Merge signers
        a.merge_signers_from(&b);

        assert!(a.has_signed(3));
        assert!(a.has_signed(13));
        assert_eq!(a.signer_count(), 2);
    }
}
