//! Authority signature for slice uploads.
//!
//! Proves the uploader owns the tape and has registered the track.
//! This signature authorizes a storage node to accept and store a slice.
//!
//! # Message Format
//!
//! The authority message is 34 bytes:
//! - track_address: 32 bytes (Solana pubkey of the Track account)
//! - slice_index: 2 bytes (little-endian u16, 0..SPOOL_GROUP_SIZE)
//!
//! # Usage
//!
//! **Client-side (signing):**
//! ```ignore
//! let msg = AuthorityMessage::new(track_address, slice_index);
//! let signature = keypair.sign_message(&msg.to_bytes());
//! ```
//!
//! **Node-side (verification):**
//! ```ignore
//! verify_authority_signature(
//!     &tape_authority_pubkey,
//!     track_address,
//!     slice_index,
//!     &signature,
//! )?;
//! ```

use tape_crypto::ed25519::sig_verify;

use super::CertificateError;

/// Message signed by tape authority to authorize slice uploads.
///
/// The message format is:
/// - 32 bytes: track account address (Pubkey)
/// - 2 bytes: slice index (little-endian u16)
///
/// Total: 34 bytes
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AuthorityMessage {
    /// The Solana address of the Track account being uploaded to.
    pub track_address: [u8; 32],

    /// The index of the slice (0..SPOOL_GROUP_SIZE).
    pub slice_index: u16,
}

impl AuthorityMessage {
    /// Create a new authority message.
    #[inline]
    pub fn new(track_address: [u8; 32], slice_index: u16) -> Self {
        Self {
            track_address,
            slice_index,
        }
    }

    /// Serialize the message to bytes for signing/verification.
    #[inline]
    pub fn to_bytes(&self) -> [u8; 34] {
        let mut msg = [0u8; 34];
        msg[0..32].copy_from_slice(&self.track_address);
        msg[32..34].copy_from_slice(&self.slice_index.to_le_bytes());
        msg
    }

    /// Deserialize from bytes.
    #[inline]
    pub fn from_bytes(bytes: &[u8; 34]) -> Self {
        let mut track_address = [0u8; 32];
        track_address.copy_from_slice(&bytes[0..32]);
        let slice_index = u16::from_le_bytes([bytes[32], bytes[33]]);
        Self {
            track_address,
            slice_index,
        }
    }
}

/// Verify an authority signature over a slice upload message.
///
/// This verifies that the tape authority (owner of the Tape account)
/// has authorized uploading slice `slice_index` to the track at `track_address`.
///
/// # Arguments
/// * `authority_pubkey` - The Ed25519 public key of the tape authority (32 bytes)
/// * `track_address` - The Solana address of the Track account (32 bytes)
/// * `slice_index` - The index of the slice being uploaded (0..SPOOL_GROUP_SIZE)
/// * `signature` - The Ed25519 signature to verify (64 bytes)
///
/// # Returns
/// `Ok(())` if the signature is valid, `Err(CertificateError::SignatureInvalid)` otherwise.
pub fn verify_authority_signature(
    authority_pubkey: &[u8; 32],
    track_address: [u8; 32],
    slice_index: u16,
    signature: &[u8; 64],
) -> Result<(), CertificateError> {
    let msg = AuthorityMessage::new(track_address, slice_index);
    sig_verify(authority_pubkey, signature, &msg.to_bytes())
        .map_err(|_| CertificateError::SignatureInvalid)
}

/// Sign a slice upload authorization message.
///
/// This is a convenience function that creates and signs an authority message.
/// In practice, you may want to use your signing library directly with
/// `AuthorityMessage::to_bytes()`.
///
/// # Arguments
/// * `signing_fn` - Function that signs bytes and returns a 64-byte signature
/// * `track_address` - The Solana address of the Track account (32 bytes)
/// * `slice_index` - The index of the slice being uploaded (0..SPOOL_GROUP_SIZE)
///
/// # Returns
/// The 64-byte Ed25519 signature.
pub fn sign_authority_message<F>(
    signing_fn: F,
    track_address: [u8; 32],
    slice_index: u16,
) -> [u8; 64]
where
    F: FnOnce(&[u8]) -> [u8; 64],
{
    let msg = AuthorityMessage::new(track_address, slice_index);
    signing_fn(&msg.to_bytes())
}

#[cfg(test)]
mod tests {
    use super::*;
    use solana_keypair::{Keypair, Signer};

    #[test]
    fn test_message_roundtrip() {
        let track_address = [42u8; 32];
        let slice_index = 512u16;

        let msg = AuthorityMessage::new(track_address, slice_index);
        let bytes = msg.to_bytes();
        let recovered = AuthorityMessage::from_bytes(&bytes);

        assert_eq!(msg, recovered);
        assert_eq!(recovered.track_address, track_address);
        assert_eq!(recovered.slice_index, slice_index);
    }

    #[test]
    fn test_message_byte_layout() {
        let track_address = [0xAB; 32];
        let slice_index = 0x1234u16;

        let msg = AuthorityMessage::new(track_address, slice_index);
        let bytes = msg.to_bytes();

        // Track address should be first 32 bytes
        assert_eq!(&bytes[0..32], &track_address);

        // Slice index should be little-endian u16 at bytes 32-33
        assert_eq!(bytes[32], 0x34); // low byte
        assert_eq!(bytes[33], 0x12); // high byte
    }

    #[test]
    fn test_sign_and_verify() {
        let keypair = Keypair::new();
        let pubkey = keypair.pubkey().to_bytes();

        let track_address = [99u8; 32];
        let slice_index = 100u16;

        // Sign the message
        let msg = AuthorityMessage::new(track_address, slice_index);
        let signature = keypair.sign_message(&msg.to_bytes());
        let mut sig64 = [0u8; 64];
        sig64.copy_from_slice(signature.as_ref());

        // Verify should succeed
        let result = verify_authority_signature(&pubkey, track_address, slice_index, &sig64);
        assert!(result.is_ok());
    }

    #[test]
    fn test_verify_wrong_track_address() {
        let keypair = Keypair::new();
        let pubkey = keypair.pubkey().to_bytes();

        let track_address = [99u8; 32];
        let wrong_track_address = [100u8; 32];
        let slice_index = 100u16;

        // Sign with correct track address
        let msg = AuthorityMessage::new(track_address, slice_index);
        let signature = keypair.sign_message(&msg.to_bytes());
        let mut sig64 = [0u8; 64];
        sig64.copy_from_slice(signature.as_ref());

        // Verify with wrong track address should fail
        let result =
            verify_authority_signature(&pubkey, wrong_track_address, slice_index, &sig64);
        assert!(matches!(result, Err(CertificateError::SignatureInvalid)));
    }

    #[test]
    fn test_verify_wrong_slice_index() {
        let keypair = Keypair::new();
        let pubkey = keypair.pubkey().to_bytes();

        let track_address = [99u8; 32];
        let slice_index = 100u16;
        let wrong_slice_index = 200u16;

        // Sign with correct slice index
        let msg = AuthorityMessage::new(track_address, slice_index);
        let signature = keypair.sign_message(&msg.to_bytes());
        let mut sig64 = [0u8; 64];
        sig64.copy_from_slice(signature.as_ref());

        // Verify with wrong slice index should fail
        let result =
            verify_authority_signature(&pubkey, track_address, wrong_slice_index, &sig64);
        assert!(matches!(result, Err(CertificateError::SignatureInvalid)));
    }

    #[test]
    fn test_verify_wrong_pubkey() {
        let keypair = Keypair::new();
        let other_keypair = Keypair::new();
        let wrong_pubkey = other_keypair.pubkey().to_bytes();

        let track_address = [99u8; 32];
        let slice_index = 100u16;

        // Sign with correct keypair
        let msg = AuthorityMessage::new(track_address, slice_index);
        let signature = keypair.sign_message(&msg.to_bytes());
        let mut sig64 = [0u8; 64];
        sig64.copy_from_slice(signature.as_ref());

        // Verify with wrong pubkey should fail
        let result =
            verify_authority_signature(&wrong_pubkey, track_address, slice_index, &sig64);
        assert!(matches!(result, Err(CertificateError::SignatureInvalid)));
    }

    #[test]
    fn test_sign_helper() {
        let keypair = Keypair::new();
        let pubkey = keypair.pubkey().to_bytes();

        let track_address = [77u8; 32];
        let slice_index = 500u16;

        // Use the helper function
        let sig64 = sign_authority_message(
            |msg| {
                let sig = keypair.sign_message(msg);
                let mut out = [0u8; 64];
                out.copy_from_slice(sig.as_ref());
                out
            },
            track_address,
            slice_index,
        );

        // Verify should succeed
        let result = verify_authority_signature(&pubkey, track_address, slice_index, &sig64);
        assert!(result.is_ok());
    }
}
