//! Owner-signed token that lets one reader fetch hidden tracks on a tape

use core::mem::size_of;

use bytemuck::{Pod, Zeroable};
use tape_crypto::address::Address;
use tape_crypto::ed25519::consts::ED25519_SIG_LEN;
use tape_crypto::ed25519::sig_verify;
#[cfg(not(target_os = "solana"))]
use tape_crypto::ed25519::Keypair;
use tape_crypto::hash::{Hash, HASH_BYTES};

use crate::types::tls::NetworkTlsPubkey;

/// The signature covers this domain tag, ahead of the token fields
pub const SHARE_V1: &[u8; 8] = b"SHARE_V1";

/// Track key that opens every track on the tape
pub const ALL_TRACKS: Hash = Hash([0u8; HASH_BYTES]);

/// The signature covers the domain tag and every field ahead of it
pub const SHARE_MESSAGE_BYTES: usize = SHARE_V1.len() + size_of::<ShareToken>() - ED25519_SIG_LEN;

/// Share token bound to one reader identity, one track key, and one tape share nonce
#[repr(C)]
#[derive(Clone, Copy, Debug, Eq, PartialEq, Pod, Zeroable)]
pub struct ShareToken {
    pub tape: Address,
    pub track_key: Hash,
    pub reader: NetworkTlsPubkey,
    pub signer: Address,
    pub nonce: Hash,
    pub expiry: i64,
    pub signature: [u8; ED25519_SIG_LEN],
}

/// A node checks a token against this tape state and connection
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ShareContext {
    pub tape: Address,
    pub tape_authority: Address,
    pub tape_delegate: Address,
    pub nonce: Hash,
    pub track_key: Hash,
    pub reader: NetworkTlsPubkey,
    pub now: i64,
}

/// The reasons for refusing a share token
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ShareError {
    WrongTape,
    SignerNotOperator,
    Revoked,
    Expired,
    WrongReader,
    WrongKey,
    BadSignature,
    Malformed,
}

impl ShareError {
    /// Short reason, safe to return to the caller
    pub fn reason(&self) -> &'static str {
        match self {
            ShareError::WrongTape => "share token is for another tape",
            ShareError::SignerNotOperator => "share token signer does not operate this tape",
            ShareError::Revoked => "share token was revoked",
            ShareError::Expired => "share token has expired",
            ShareError::WrongReader => "share token is for another reader",
            ShareError::WrongKey => "share token is for another track",
            ShareError::BadSignature => "share token signature is invalid",
            ShareError::Malformed => "share token is malformed",
        }
    }
}

impl ShareToken {
    /// Sign a token with the tape's authority or delegate key
    #[cfg(not(target_os = "solana"))]
    pub fn issue(
        signer: &Keypair,
        tape: Address,
        track_key: Hash,
        reader: NetworkTlsPubkey,
        nonce: Hash,
        expiry: i64,
    ) -> Self {
        let mut token = Self {
            tape,
            track_key,
            reader,
            signer: signer.pubkey().address(),
            nonce,
            expiry,
            signature: [0u8; ED25519_SIG_LEN],
        };

        token.signature = signer.sign(&token.signing_message()).to_bytes();
        token
    }

    /// The signer signs these bytes and the verifier rebuilds them
    pub fn signing_message(&self) -> [u8; SHARE_MESSAGE_BYTES] {
        let fields = &bytemuck::bytes_of(self)[..size_of::<ShareToken>() - ED25519_SIG_LEN];

        let mut out = [0u8; SHARE_MESSAGE_BYTES];
        out[..SHARE_V1.len()].copy_from_slice(SHARE_V1);
        out[SHARE_V1.len()..].copy_from_slice(fields);
        out
    }

    /// Verify the token against the tape and connection before a node serves a hidden track
    pub fn verify(&self, context: &ShareContext) -> Result<(), ShareError> {
        if self.tape != context.tape {
            return Err(ShareError::WrongTape);
        }

        let is_operator = self.signer == context.tape_authority
            || (context.tape_delegate != Address::default()
                && self.signer == context.tape_delegate);
        if !is_operator {
            return Err(ShareError::SignerNotOperator);
        }

        if self.nonce != context.nonce {
            return Err(ShareError::Revoked);
        }

        if self.expiry <= context.now {
            return Err(ShareError::Expired);
        }

        if self.reader != context.reader {
            return Err(ShareError::WrongReader);
        }

        if self.track_key != context.track_key && self.track_key != ALL_TRACKS {
            return Err(ShareError::WrongKey);
        }

        sig_verify(&self.signer.to_bytes(), &self.signature, &self.signing_message())
            .map_err(|_| ShareError::BadSignature)
    }

    /// Serialize for the wire
    pub fn pack(&self) -> [u8; size_of::<ShareToken>()] {
        let mut out = [0u8; size_of::<ShareToken>()];
        out.copy_from_slice(bytemuck::bytes_of(self));
        out
    }

    /// Parse from the wire
    pub fn unpack(bytes: &[u8]) -> Result<Self, ShareError> {
        bytemuck::try_pod_read_unaligned(bytes).map_err(|_| ShareError::Malformed)
    }
}

/// Current unix time in seconds; token expiry is judged by the local clock
#[cfg(not(target_os = "solana"))]
pub fn unix_now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| elapsed.as_secs() as i64)
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    use rand::rngs::OsRng;

    const EXPIRY: i64 = 1_800_000_000;

    fn context(token: &ShareToken) -> ShareContext {
        ShareContext {
            tape: token.tape,
            tape_authority: token.signer,
            tape_delegate: Address::default(),
            nonce: token.nonce,
            track_key: token.track_key,
            reader: token.reader,
            now: token.expiry - 1,
        }
    }

    fn sample(track_key: Hash) -> ShareToken {
        ShareToken::issue(
            &Keypair::new(&mut OsRng),
            Address::new_unique(),
            track_key,
            NetworkTlsPubkey::new([0x13; 32]),
            Hash([0x07; 32]),
            EXPIRY,
        )
    }

    // a freshly issued token passes every check
    #[test]
    fn accepts_valid() {
        let token = sample(Hash([0x42; 32]));

        assert_eq!(token.verify(&context(&token)), Ok(()));
    }

    // a token round-trips through its packed form
    #[test]
    fn pack_roundtrip() {
        let token = sample(Hash([0x42; 32]));

        assert_eq!(ShareToken::unpack(&token.pack()), Ok(token));
        assert_eq!(token.pack().len(), 232);
    }

    // the delegate can sign tokens too
    #[test]
    fn accepts_delegate() {
        let token = sample(Hash([0x42; 32]));
        let mut context = context(&token);
        context.tape_authority = Address::new_unique();
        context.tape_delegate = token.signer;

        assert_eq!(token.verify(&context), Ok(()));
    }

    // advancing the share nonce revokes outstanding tokens
    #[test]
    fn revoked() {
        let token = sample(Hash([0x42; 32]));
        let mut context = context(&token);
        context.nonce = Hash([0x08; 32]);

        assert_eq!(token.verify(&context), Err(ShareError::Revoked));
    }

    // an expired token is refused
    #[test]
    fn expired() {
        let token = sample(Hash([0x42; 32]));
        let mut context = context(&token);
        context.now = token.expiry;

        assert_eq!(token.verify(&context), Err(ShareError::Expired));
    }

    // a token is refused for any other reader
    #[test]
    fn wrong_reader() {
        let token = sample(Hash([0x42; 32]));
        let mut context = context(&token);
        context.reader = NetworkTlsPubkey::new([0x99; 32]);

        assert_eq!(token.verify(&context), Err(ShareError::WrongReader));
    }

    // a token does not open a different track on the same tape
    #[test]
    fn wrong_key() {
        let token = sample(Hash([0x42; 32]));
        let mut context = context(&token);
        context.track_key = Hash([0x01; 32]);

        assert_eq!(token.verify(&context), Err(ShareError::WrongKey));
    }

    // a signer who does not operate the tape is refused
    #[test]
    fn stranger_signature() {
        let token = sample(Hash([0x42; 32]));
        let mut context = context(&token);
        context.tape_authority = Address::new_unique();

        assert_eq!(token.verify(&context), Err(ShareError::SignerNotOperator));
    }

    // tampering with a signed field breaks the signature
    #[test]
    fn tampered_field() {
        let mut token = sample(Hash([0x42; 32]));
        let context = context(&token);
        token.expiry += 1;

        assert_eq!(token.verify(&context), Err(ShareError::BadSignature));
    }

    // an all-tracks token opens every track on its tape
    #[test]
    fn all_tracks() {
        let token = sample(ALL_TRACKS);
        let mut context = context(&token);
        context.track_key = Hash([0x66; 32]);

        assert_eq!(token.verify(&context), Ok(()));
    }
}
