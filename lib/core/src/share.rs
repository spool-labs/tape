//! Owner-signed token that lets one reader fetch hidden tracks on a tape

use core::mem::size_of;

use bytemuck::{Pod, Zeroable};
use tape_crypto::address::Address;
use tape_crypto::ed25519::consts::ED25519_SIG_LEN;
use tape_crypto::ed25519::sig_verify;
#[cfg(not(target_os = "solana"))]
use tape_crypto::ed25519::{Keypair, Signature};
use tape_crypto::hash::{Hash, HASH_BYTES};

use crate::types::tls::NetworkTlsPubkey;

/// The signature covers this domain tag, ahead of the token fields
pub const SHARE_V1: &[u8; 8] = b"SHARE_V1";

/// The reader's per-request signature covers this domain tag
pub const READ_V1: &[u8; 7] = b"READ_V1";

/// A read request may expire at most this many seconds after now
pub const MAX_READ_REQUEST_TTL: i64 = 300;

/// Separates the read request's fields; no field may contain it
#[cfg(not(target_os = "solana"))]
const READ_FIELD_SEPARATOR: u8 = 0x00;

/// Track key that opens every track on the tape
pub const ALL_TRACKS: Hash = Hash([0u8; HASH_BYTES]);

/// The signature covers the domain tag and every field ahead of it
pub const SHARE_MESSAGE_BYTES: usize = SHARE_V1.len() + size_of::<ShareToken>() - ED25519_SIG_LEN;

/// Share token bound to one reader identity, one track key, and one tape nonce
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
    RequestExpired,
    RequestTooFarAhead,
    RequestSignature,
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
            ShareError::RequestExpired => "read request has expired",
            ShareError::RequestTooFarAhead => "read request expiry is too far ahead",
            ShareError::RequestSignature => "read request signature is invalid",
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

/// A request signed by the token's reader, binding the reader key to one call
#[cfg(not(target_os = "solana"))]
pub struct ReadRequest {
    pub method: String,
    pub path: String,
    pub expiry: i64,
}

#[cfg(not(target_os = "solana"))]
impl ReadRequest {
    /// Bind one method, path, and expiry into a request for the reader to sign
    pub fn new(method: &str, path: &str, expiry: i64) -> Self {
        Self {
            method: method.to_ascii_uppercase(),
            path: path.to_string(),
            expiry,
        }
    }

    /// The reader signs these bytes and the verifier rebuilds them
    pub fn signing_message(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(
            READ_V1.len() + self.method.len() + self.path.len() + 2 + size_of::<i64>(),
        );

        out.extend_from_slice(READ_V1);
        out.extend_from_slice(self.method.as_bytes());
        out.push(READ_FIELD_SEPARATOR);
        out.extend_from_slice(self.path.as_bytes());
        out.push(READ_FIELD_SEPARATOR);
        out.extend_from_slice(&self.expiry.to_le_bytes());

        out
    }

    /// Sign one call with the token's reader key
    pub fn sign(keypair: &Keypair, method: &str, path: &str, expiry: i64) -> Signature {
        keypair.sign(&Self::new(method, path, expiry).signing_message())
    }

    /// Check a presented request against the token's reader and the local clock
    pub fn verify(
        reader: &NetworkTlsPubkey,
        method: &str,
        path: &str,
        expiry: i64,
        signature: &Signature,
        now: i64,
    ) -> Result<(), ShareError> {
        if expiry <= now {
            return Err(ShareError::RequestExpired);
        }

        if expiry > now.saturating_add(MAX_READ_REQUEST_TTL) {
            return Err(ShareError::RequestTooFarAhead);
        }

        let message = Self::new(method, path, expiry).signing_message();
        sig_verify(reader.as_bytes(), &signature.to_bytes(), &message)
            .map_err(|_| ShareError::RequestSignature)
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
    const REQUEST_PATH: &str = "/object/abc";

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

    fn request() -> (Keypair, NetworkTlsPubkey, Signature) {
        let reader = Keypair::new(&mut OsRng);
        let pubkey = NetworkTlsPubkey::new(reader.pubkey().to_bytes());
        let signature = ReadRequest::sign(&reader, "GET", REQUEST_PATH, EXPIRY);

        (reader, pubkey, signature)
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

    // advancing the tape's nonce revokes outstanding tokens
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

    // a signed request passes at a clock inside its window
    #[test]
    fn signed_request() {
        let (_reader, pubkey, signature) = request();

        let checked = ReadRequest::verify(&pubkey, "GET", REQUEST_PATH, EXPIRY, &signature, EXPIRY - 10);

        assert_eq!(checked, Ok(()));
    }

    // the method is signed, so the same signature does not open another verb
    #[test]
    fn other_method() {
        let (_reader, pubkey, signature) = request();

        let checked = ReadRequest::verify(&pubkey, "HEAD", REQUEST_PATH, EXPIRY, &signature, EXPIRY - 10);

        assert_eq!(checked, Err(ShareError::RequestSignature));
    }

    // the path is signed, so the same signature does not open another object
    #[test]
    fn other_path() {
        let (_reader, pubkey, signature) = request();

        let checked = ReadRequest::verify(&pubkey, "GET", "/object/def", EXPIRY, &signature, EXPIRY - 10);

        assert_eq!(checked, Err(ShareError::RequestSignature));
    }

    // a request is refused from its expiry second on, matching the token's boundary
    #[test]
    fn stale_request() {
        let (_reader, pubkey, signature) = request();

        let checked = ReadRequest::verify(&pubkey, "GET", REQUEST_PATH, EXPIRY, &signature, EXPIRY);

        assert_eq!(checked, Err(ShareError::RequestExpired));
    }

    // an expiry further ahead than the window allows is refused
    #[test]
    fn distant_request() {
        let reader = Keypair::new(&mut OsRng);
        let pubkey = NetworkTlsPubkey::new(reader.pubkey().to_bytes());
        let expiry = EXPIRY + MAX_READ_REQUEST_TTL + 1;
        let signature = ReadRequest::sign(&reader, "GET", REQUEST_PATH, expiry);

        let checked = ReadRequest::verify(&pubkey, "GET", REQUEST_PATH, expiry, &signature, EXPIRY);

        assert_eq!(checked, Err(ShareError::RequestTooFarAhead));
    }

    // a request signed by any other key is refused
    #[test]
    fn other_reader() {
        let (_reader, _pubkey, signature) = request();
        let stranger = Keypair::new(&mut OsRng);
        let pubkey = NetworkTlsPubkey::new(stranger.pubkey().to_bytes());

        let checked = ReadRequest::verify(&pubkey, "GET", REQUEST_PATH, EXPIRY, &signature, EXPIRY - 10);

        assert_eq!(checked, Err(ShareError::RequestSignature));
    }

    // a lowercase method signs the same message as its uppercase form
    #[test]
    fn method_case() {
        let (reader, pubkey, signature) = request();
        let lowercase = ReadRequest::sign(&reader, "get", REQUEST_PATH, EXPIRY);

        assert_eq!(lowercase.to_bytes(), signature.to_bytes());

        let checked = ReadRequest::verify(&pubkey, "get", REQUEST_PATH, EXPIRY, &signature, EXPIRY - 10);

        assert_eq!(checked, Ok(()));
    }
}
