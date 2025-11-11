use bytemuck::{Pod, Zeroable};
use crate::types::{Bitmap, EpochNumber};
use crate::prelude::Hash;
use tape_crypto::ed25519::sig_verify;
use super::CertificateError;

/// Ed25519 certificate that tracks one-bit-per-member progress for a specific epoch.
/// The message is a 32-byte value the signer signs (e.g., a hash or address).
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct EdwardsCertificate<const BYTES: usize> {
    /// The message bytes to be signed (32 bytes). Conventionally a hash or address.
    pub message: Hash,

    /// Exact epoch for which this certificate is valid. Committee rotates each epoch.
    pub epoch: EpochNumber,

    /// Bitmap of committee members that have signed.
    pub signers: Bitmap<BYTES>,
}

unsafe impl<const BYTES: usize> Zeroable for EdwardsCertificate<BYTES> {}
unsafe impl<const BYTES: usize> Pod for EdwardsCertificate<BYTES> {}

impl<const BYTES: usize> EdwardsCertificate<BYTES> {
    /// Create a new certificate for an exact epoch and 32-byte message.
    #[inline]
    pub fn new(message: Hash, epoch: EpochNumber) -> Self {
        Self {
            message,
            epoch,
            signers: Bitmap::<BYTES>::zeroed(),
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

    /// Verify an ed25519 signature over this certificate's message and mark the signer bit.
    pub fn try_add_signature(
        &mut self,
        committee_epoch: EpochNumber,
        committee_index: usize,
        pubkey: &[u8; 32],
        sig: &[u8; 64],
    ) -> Result<(), CertificateError> {

        if committee_epoch != self.epoch {
            return Err(CertificateError::EpochMismatch);
        }

        if self.signers.is_set(committee_index) {
            return Err(CertificateError::AlreadySigned);
        }

        // Verify signature against this certificate's message bytes
        sig_verify(pubkey, sig, self.message.as_ref())
            .map_err(|_| CertificateError::SignatureInvalid)?;

        self.signers.set(committee_index);

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

        let self_bytes = unsafe { &mut *(&mut self.signers as *mut Bitmap<BYTES> as *mut [u8; BYTES]) };
        let other_bytes = unsafe { &*(&other.signers as *const Bitmap<BYTES> as *const [u8; BYTES]) };

        for i in 0..BYTES {
            self_bytes[i] |= other_bytes[i];
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use solana_keypair::{ Keypair, Signer };

    #[test]
    fn sign_single() {
        // Random keypair
        let kp = Keypair::new();
        let pubkey = kp.pubkey().to_bytes();

        // Random 32-byte message (hash)
        let message = Hash::new_unique();

        // Sign exact message bytes
        let signature = kp.sign_message(message.as_ref());
        let mut sig64 = [0u8; 64];
        sig64.copy_from_slice(signature.as_ref());

        // Cert at exact epoch
        let epoch = EpochNumber(10);
        let mut cert = EdwardsCertificate::<2>::new(message, epoch);

        cert.try_add_signature(epoch, 0, &pubkey, &sig64)
            .expect("valid ed25519 signature should mark bit");

        assert!(cert.has_signed(0));
        assert_eq!(cert.signer_count(), 1);
    }

    #[test]
    fn epoch_mismatch() {
        let message = Hash::from([7u8; 32]);
        let epoch = EpochNumber(5);
        let mut cert = EdwardsCertificate::<1>::new(message, epoch);

        let dummy_pk = [0u8; 32];
        let dummy_sig = [0u8; 64];

        let err = cert.try_add_signature(EpochNumber(6), 0, &dummy_pk, &dummy_sig).unwrap_err();
        assert_eq!(err, CertificateError::EpochMismatch);
    }

    #[test]
    fn merge_signers() {
        let message = Hash::new_unique();
        let epoch = EpochNumber(1);

        // Two certs for same (message, epoch)
        let mut a = EdwardsCertificate::<2>::new(message, epoch);
        let mut b = EdwardsCertificate::<2>::new(message, epoch);

        // Set disjoint bits
        a.signers.set(3);
        b.signers.set(13);

        // Merge → OR
        a.merge_signers_from(&b);

        assert!(a.has_signed(3));
        assert!(a.has_signed(13));
        assert_eq!(a.signer_count(), 2);
    }
}
