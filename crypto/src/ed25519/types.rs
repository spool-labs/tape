//! Ed25519 signature types for off-chain operations.
//!
//! This module provides wrapper types around `ed25519_consensus` for:
//! - `SecretKey` - wrapper around `SigningKey`
//! - `PublicKey` - wrapper around `VerificationKey` with Solana Pubkey conversions
//! - `Signature` - wrapper around `ed25519_consensus::Signature`
//! - `Keypair` - combines SecretKey and PublicKey

#![allow(unexpected_cfgs)]

use ed25519_consensus::{SigningKey, VerificationKey};
use rand::CryptoRng;
use serde::{Deserialize, Serialize};

#[cfg(feature = "wincode")]
use core::mem::MaybeUninit;
#[cfg(feature = "wincode")]
use wincode::{
    io::{Reader, Writer},
    ReadResult, SchemaRead, SchemaWrite, WriteResult,
};

use super::consts::{ED25519_PUBKEY_LEN, ED25519_SIG_LEN};
use super::errors::SignatureError;

/// Constant for public key length (32 bytes).
pub const PUBLIC_KEY_LEN: usize = ED25519_PUBKEY_LEN;

/// Constant for signature length (64 bytes).
pub const SIGNATURE_LEN: usize = ED25519_SIG_LEN;

/// Ed25519 secret key wrapper around `SigningKey`.
///
/// Used for signing messages off-chain.
pub struct SecretKey(SigningKey);

impl SecretKey {
    /// Generate a new random secret key.
    pub fn new<R: CryptoRng + rand::RngCore>(rng: &mut R) -> Self {
        Self(SigningKey::new(rng))
    }

    /// Create a secret key from raw bytes.
    pub fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(SigningKey::from(bytes))
    }

    /// Derive the public key from this secret key.
    pub fn public_key(&self) -> PublicKey {
        PublicKey(self.0.verification_key())
    }

    /// Sign a message with this secret key.
    pub fn sign(&self, msg: &[u8]) -> Signature {
        Signature(self.0.sign(msg))
    }

    /// Get the raw bytes of this secret key.
    pub fn as_bytes(&self) -> &[u8; 32] {
        self.0.as_bytes()
    }
}

/// Ed25519 public key wrapper around `VerificationKey`.
///
/// Provides verification and Solana Pubkey conversions.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PublicKey(VerificationKey);

impl PublicKey {
    /// The length of a public key in bytes.
    pub const LEN: usize = ED25519_PUBKEY_LEN;

    /// Create a public key from raw bytes.
    pub fn from_bytes(bytes: [u8; 32]) -> Result<Self, SignatureError> {
        VerificationKey::try_from(bytes)
            .map(Self)
            .map_err(|_| SignatureError::InvalidPublicKey)
    }

    /// Get a reference to the raw bytes of this public key.
    pub fn as_bytes(&self) -> &[u8; 32] {
        self.0.as_bytes()
    }

    /// Convert this public key to raw bytes.
    pub fn to_bytes(self) -> [u8; 32] {
        self.0.to_bytes()
    }

    /// Verify a signature on a message.
    pub fn verify(&self, msg: &[u8], sig: &Signature) -> Result<(), SignatureError> {
        self.0
            .verify(&sig.0, msg)
            .map_err(|_| SignatureError::VerificationFailed)
    }
}

// Solana Pubkey conversions

impl From<PublicKey> for solana_program::pubkey::Pubkey {
    fn from(pk: PublicKey) -> Self {
        solana_program::pubkey::Pubkey::from(pk.to_bytes())
    }
}

impl From<&PublicKey> for solana_program::pubkey::Pubkey {
    fn from(pk: &PublicKey) -> Self {
        solana_program::pubkey::Pubkey::from(*pk.as_bytes())
    }
}

impl TryFrom<solana_program::pubkey::Pubkey> for PublicKey {
    type Error = SignatureError;

    fn try_from(pubkey: solana_program::pubkey::Pubkey) -> Result<Self, Self::Error> {
        PublicKey::from_bytes(pubkey.to_bytes())
    }
}

impl TryFrom<&solana_program::pubkey::Pubkey> for PublicKey {
    type Error = SignatureError;

    fn try_from(pubkey: &solana_program::pubkey::Pubkey) -> Result<Self, Self::Error> {
        PublicKey::from_bytes(pubkey.to_bytes())
    }
}

// Wincode SchemaWrite and SchemaRead for PublicKey

#[cfg(feature = "wincode")]
impl SchemaWrite for PublicKey {
    type Src = Self;

    fn size_of(_src: &Self::Src) -> WriteResult<usize> {
        Ok(32)
    }

    fn write(writer: &mut Writer, src: &Self::Src) -> WriteResult<()> {
        writer.write_exact(src.as_bytes())?;
        Ok(())
    }
}

#[cfg(feature = "wincode")]
impl<'de> SchemaRead<'de> for PublicKey {
    type Dst = Self;

    fn read(reader: &mut Reader<'de>, dst: &mut MaybeUninit<PublicKey>) -> ReadResult<()> {
        let bytes: [u8; 32] = unsafe { reader.get_t()? };
        let pk = PublicKey::from_bytes(bytes).map_err(|_| wincode::io::ReadError::ReadSizeLimit(32))?;
        dst.write(pk);
        Ok(())
    }
}

/// Ed25519 signature wrapper around `ed25519_consensus::Signature`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Signature(ed25519_consensus::Signature);

impl Signature {
    /// The length of a signature in bytes.
    pub const LEN: usize = ED25519_SIG_LEN;

    /// Create a signature from raw bytes.
    pub fn from_bytes(bytes: [u8; 64]) -> Result<Self, SignatureError> {
        ed25519_consensus::Signature::try_from(bytes)
            .map(Self)
            .map_err(|_| SignatureError::InvalidSignature)
    }

    /// Convert this signature to raw bytes.
    pub fn to_bytes(&self) -> [u8; 64] {
        self.0.to_bytes()
    }

    /// Verify this signature on a message with the given public key.
    pub fn verify(&self, msg: &[u8], pk: &PublicKey) -> Result<(), SignatureError> {
        pk.verify(msg, self)
    }
}

// Wincode SchemaWrite and SchemaRead for Signature

#[cfg(feature = "wincode")]
impl SchemaWrite for Signature {
    type Src = Self;

    fn size_of(_src: &Self::Src) -> WriteResult<usize> {
        Ok(64)
    }

    fn write(writer: &mut Writer, src: &Self::Src) -> WriteResult<()> {
        writer.write_exact(&src.to_bytes())?;
        Ok(())
    }
}

#[cfg(feature = "wincode")]
impl<'de> SchemaRead<'de> for Signature {
    type Dst = Self;

    fn read(reader: &mut Reader<'de>, dst: &mut MaybeUninit<Signature>) -> ReadResult<()> {
        let bytes: [u8; 64] = unsafe { reader.get_t()? };
        let sig = Signature::from_bytes(bytes).map_err(|_| wincode::io::ReadError::ReadSizeLimit(64))?;
        dst.write(sig);
        Ok(())
    }
}

/// Ed25519 keypair combining a secret key and its derived public key.
pub struct Keypair {
    secret: SecretKey,
    public: PublicKey,
}

impl Keypair {
    /// Generate a new random keypair.
    pub fn new<R: CryptoRng + rand::RngCore>(rng: &mut R) -> Self {
        let secret = SecretKey::new(rng);
        let public = secret.public_key();
        Self { secret, public }
    }

    /// Create a keypair from a secret key.
    pub fn from_secret(secret: SecretKey) -> Self {
        let public = secret.public_key();
        Self { secret, public }
    }

    /// Get a reference to the public key.
    pub fn public_key(&self) -> &PublicKey {
        &self.public
    }

    /// Get a reference to the secret key.
    pub fn secret_key(&self) -> &SecretKey {
        &self.secret
    }

    /// Sign a message with this keypair.
    pub fn sign(&self, msg: &[u8]) -> Signature {
        self.secret.sign(msg)
    }

    /// Get the Solana pubkey for this keypair.
    pub fn solana_pubkey(&self) -> solana_program::pubkey::Pubkey {
        (&self.public).into()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sign_verify() {
        let mut rng = rand::thread_rng();
        let keypair = Keypair::new(&mut rng);
        let message = b"hello world";

        let signature = keypair.sign(message);

        // Verify with public key
        assert!(keypair.public_key().verify(message, &signature).is_ok());

        // Verify with signature method
        assert!(signature.verify(message, keypair.public_key()).is_ok());

        // Wrong message should fail
        assert!(keypair
            .public_key()
            .verify(b"wrong message", &signature)
            .is_err());
    }

    #[test]
    fn test_solana_pubkey_conversion() {
        let mut rng = rand::thread_rng();
        let keypair = Keypair::new(&mut rng);
        let public_key = keypair.public_key();

        // Convert to Solana Pubkey
        let solana_pubkey: solana_program::pubkey::Pubkey = public_key.into();

        // Convert back
        let recovered = PublicKey::try_from(solana_pubkey).expect("should convert back");

        assert_eq!(public_key, &recovered);
    }

    #[test]
    fn test_secret_key_from_bytes() {
        let mut rng = rand::thread_rng();
        let keypair = Keypair::new(&mut rng);
        let secret_bytes = *keypair.secret_key().as_bytes();

        // Recreate from bytes
        let recovered_secret = SecretKey::from_bytes(secret_bytes);
        let recovered_public = recovered_secret.public_key();

        assert_eq!(keypair.public_key(), &recovered_public);
    }

    #[test]
    fn test_public_key_bytes_roundtrip() {
        let mut rng = rand::thread_rng();
        let keypair = Keypair::new(&mut rng);
        let public_key = keypair.public_key();

        let bytes = public_key.to_bytes();
        let recovered = PublicKey::from_bytes(bytes).expect("should recover");

        assert_eq!(public_key, &recovered);
    }

    #[test]
    fn test_signature_bytes_roundtrip() {
        let mut rng = rand::thread_rng();
        let keypair = Keypair::new(&mut rng);
        let message = b"test message";

        let signature = keypair.sign(message);
        let bytes = signature.to_bytes();
        let recovered = Signature::from_bytes(bytes).expect("should recover");

        assert_eq!(signature, recovered);
        assert!(keypair.public_key().verify(message, &recovered).is_ok());
    }

    #[cfg(feature = "wincode")]
    mod wincode_tests {
        use super::*;

        #[test]
        fn test_wincode_roundtrip_public_key() {
            let mut rng = rand::thread_rng();
            let keypair = Keypair::new(&mut rng);
            let public_key = *keypair.public_key();

            // Serialize
            let pk_bytes = wincode::serialize(&public_key).expect("serialize should succeed");

            // Deserialize
            let recovered: PublicKey = wincode::deserialize(&pk_bytes).expect("deserialize should succeed");

            assert_eq!(public_key, recovered);
        }

        #[test]
        fn test_wincode_roundtrip_signature() {
            let mut rng = rand::thread_rng();
            let keypair = Keypair::new(&mut rng);
            let signature = keypair.sign(b"test");

            // Serialize
            let sig_bytes = wincode::serialize(&signature).expect("serialize should succeed");

            // Deserialize
            let recovered: Signature = wincode::deserialize(&sig_bytes).expect("deserialize should succeed");

            assert_eq!(signature, recovered);
        }
    }
}
