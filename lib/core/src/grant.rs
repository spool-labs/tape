//! Owner-signed token that lets one reader fetch one restricted track.

use core::mem::size_of;

use bytemuck::{Pod, Zeroable};
use tape_crypto::address::Address;
#[cfg(not(target_os = "solana"))]
use tape_crypto::ed25519::{Keypair, Signature};
use tape_crypto::ed25519::{sig_verify, SignatureError};
use tape_crypto::hash::{Hash, HASH_BYTES};

use crate::types::tls::NetworkTlsPubkey;

/// Domain tag the signature covers, ahead of the grant fields.
pub const GRANT_V1: &[u8; 8] = b"GRANT_V1";

/// Domain tag the reader's per-request signature covers.
pub const READ_V1: &[u8; 7] = b"READ_V1";

/// Key naming every track on the tape rather than one of them.
pub const WILDCARD_TRACK_KEY: Hash = Hash([0u8; HASH_BYTES]);

/// Seconds ahead of now a read request may claim to expire.
pub const MAX_READ_REQUEST_TTL: i64 = 300;

/// Byte separating the read request's fields, none of which may contain it.
const READ_FIELD_SEPARATOR: u8 = 0x00;

/// Bytes an ed25519 signature occupies.
pub const GRANT_SIGNATURE_BYTES: usize = 64;

/// Bytes the signed fields occupy, ahead of the signature.
pub const GRANT_FIELD_BYTES: usize = 32 * 4 + 8 + 8;

/// Bytes the signature actually covers.
pub const GRANT_MESSAGE_BYTES: usize = GRANT_V1.len() + GRANT_FIELD_BYTES;

/// Read grant bound to one reader identity, one track key, and one tape generation.
#[repr(C)]
#[derive(Clone, Copy, Debug, Eq, PartialEq, Pod, Zeroable)]
pub struct ReadGrant {
    pub tape: Address,
    pub key: Hash,
    pub reader: NetworkTlsPubkey,
    pub signer: Address,
    pub grant_generation: u64,
    pub expiry: i64,
    pub signature: [u8; GRANT_SIGNATURE_BYTES],
}

/// Locally known facts a grant is checked against, all of them from replay.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct GrantContext {
    pub tape: Address,
    pub tape_authority: Address,
    pub tape_delegate: Address,
    pub grant_generation: u64,
    pub track_key: Hash,
    pub reader: NetworkTlsPubkey,
    pub now: i64,
}

/// Why a presented grant was refused.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GrantError {
    WrongTape,
    SignerNotOperator,
    StaleGeneration,
    Expired,
    WrongReader,
    WrongKey,
    BadSignature,
    Malformed,
    RequestExpired,
    RequestWindow,
    RequestSignature,
}

pub type PackedReadGrant = [u8; size_of::<ReadGrant>()];

impl GrantError {
    /// Short operator-facing reason, safe to return to the caller
    pub fn reason(&self) -> &'static str {
        match self {
            GrantError::WrongTape => "grant names another tape",
            GrantError::SignerNotOperator => "grant signer does not operate this tape",
            GrantError::StaleGeneration => "grant generation is stale",
            GrantError::Expired => "grant has expired",
            GrantError::WrongReader => "grant names another reader",
            GrantError::WrongKey => "grant names another track",
            GrantError::BadSignature => "grant signature is invalid",
            GrantError::Malformed => "grant is malformed",
            GrantError::RequestExpired => "read request has expired",
            GrantError::RequestWindow => "read request expiry is too far ahead",
            GrantError::RequestSignature => "read request signature is invalid",
        }
    }
}

impl ReadGrant {
    /// Bytes the signer signs, and the verifier reconstructs
    pub fn signing_message(&self) -> [u8; GRANT_MESSAGE_BYTES] {
        let mut out = [0u8; GRANT_MESSAGE_BYTES];
        let mut at = 0;

        out[at..at + GRANT_V1.len()].copy_from_slice(GRANT_V1);
        at += GRANT_V1.len();

        out[at..at + 32].copy_from_slice(&self.tape.to_bytes());
        at += 32;

        out[at..at + 32].copy_from_slice(&self.key.0);
        at += 32;

        out[at..at + 32].copy_from_slice(self.reader.as_bytes());
        at += 32;

        out[at..at + 32].copy_from_slice(&self.signer.to_bytes());
        at += 32;

        out[at..at + 8].copy_from_slice(&self.grant_generation.to_le_bytes());
        at += 8;

        out[at..at + 8].copy_from_slice(&self.expiry.to_le_bytes());

        out
    }

    /// Run every check a node makes before serving a restricted slice
    pub fn verify(&self, context: &GrantContext) -> Result<(), GrantError> {
        if self.tape != context.tape {
            return Err(GrantError::WrongTape);
        }

        let is_operator = self.signer == context.tape_authority
            || (context.tape_delegate != Address::default()
                && self.signer == context.tape_delegate);
        if !is_operator {
            return Err(GrantError::SignerNotOperator);
        }

        if self.grant_generation != context.grant_generation {
            return Err(GrantError::StaleGeneration);
        }

        if self.expiry <= context.now {
            return Err(GrantError::Expired);
        }

        if self.reader != context.reader {
            return Err(GrantError::WrongReader);
        }

        if self.key != context.track_key && self.key != WILDCARD_TRACK_KEY {
            return Err(GrantError::WrongKey);
        }

        sig_verify(
            &self.signer.to_bytes(),
            &self.signature,
            &self.signing_message(),
        )
        .map_err(signature_error)?;

        Ok(())
    }

    /// Serialize for the wire
    pub fn pack(&self) -> PackedReadGrant {
        let mut out = [0u8; size_of::<Self>()];
        out.copy_from_slice(bytemuck::bytes_of(self));
        out
    }

    /// Parse from the wire
    pub fn unpack(bytes: &[u8]) -> Result<Self, GrantError> {
        let bytes: PackedReadGrant = bytes.try_into().map_err(|_| GrantError::Malformed)?;

        let mut value = Self::zeroed();
        bytemuck::bytes_of_mut(&mut value).copy_from_slice(&bytes);

        Ok(value)
    }
}

/// One reader-signed request, binding the holder of a grant's reader key to the
/// call it presents that grant on.
#[cfg(not(target_os = "solana"))]
pub struct ReadRequest {
    pub method: String,
    pub path: String,
    pub expiry: i64,
}

#[cfg(not(target_os = "solana"))]
impl ReadRequest {
    /// Bind one method, path, and expiry into the request a reader signs
    pub fn new(method: &str, path: &str, expiry: i64) -> Self {
        Self {
            method: method.to_ascii_uppercase(),
            path: path.to_string(),
            expiry,
        }
    }

    /// Bytes the reader signs, and the verifier reconstructs
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

    /// Sign one call with the reader key a grant names
    pub fn sign(keypair: &Keypair, method: &str, path: &str, expiry: i64) -> Signature {
        keypair.sign(&Self::new(method, path, expiry).signing_message())
    }

    /// Check a presented request against the grant's reader and the local clock
    pub fn verify(
        reader: &NetworkTlsPubkey,
        method: &str,
        path: &str,
        expiry: i64,
        signature: &Signature,
        now: i64,
    ) -> Result<(), GrantError> {
        if expiry < now {
            return Err(GrantError::RequestExpired);
        }

        if expiry > now.saturating_add(MAX_READ_REQUEST_TTL) {
            return Err(GrantError::RequestWindow);
        }

        let message = Self::new(method, path, expiry).signing_message();
        sig_verify(reader.as_bytes(), &signature.to_bytes(), &message)
            .map_err(|_| GrantError::RequestSignature)
    }
}

fn signature_error(_error: SignatureError) -> GrantError {
    GrantError::BadSignature
}

#[cfg(not(target_os = "solana"))]
mod issue {
    use tape_crypto::address::Address;
    use tape_crypto::ed25519::Keypair;
    use tape_crypto::hash::Hash;

    use crate::types::tls::NetworkTlsPubkey;

    use super::{ReadGrant, GRANT_SIGNATURE_BYTES};

    impl ReadGrant {
        /// Sign a grant with the tape's authority or delegate key
        pub fn issue(
            signer: &Keypair,
            tape: Address,
            key: Hash,
            reader: NetworkTlsPubkey,
            grant_generation: u64,
            expiry: i64,
        ) -> Self {
            let mut grant = Self {
                tape,
                key,
                reader,
                signer: signer.pubkey().address(),
                grant_generation,
                expiry,
                signature: [0u8; GRANT_SIGNATURE_BYTES],
            };

            grant.signature = signer.sign(&grant.signing_message()).to_bytes();
            grant
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use rand::rngs::OsRng;
    use tape_crypto::ed25519::Keypair;

    const REQUEST_PATH: &str = "/object/abc";
    const REQUEST_EXPIRY: i64 = 1_800_000_000;

    fn context(grant: &ReadGrant) -> GrantContext {
        GrantContext {
            tape: grant.tape,
            tape_authority: grant.signer,
            tape_delegate: Address::default(),
            grant_generation: grant.grant_generation,
            track_key: grant.key,
            reader: grant.reader,
            now: grant.expiry - 1,
        }
    }

    fn request() -> (Keypair, NetworkTlsPubkey, Signature) {
        let reader = Keypair::new(&mut OsRng);
        let pubkey = NetworkTlsPubkey::new(reader.pubkey().to_bytes());
        let signature = ReadRequest::sign(&reader, "GET", REQUEST_PATH, REQUEST_EXPIRY);

        (reader, pubkey, signature)
    }

    fn sample() -> (Keypair, ReadGrant) {
        let signer = Keypair::new(&mut OsRng);
        let grant = ReadGrant::issue(
            &signer,
            Address::new_unique(),
            Hash([0x42; 32]),
            NetworkTlsPubkey::new([0x13; 32]),
            7,
            1_800_000_000,
        );

        (signer, grant)
    }

    // a freshly issued grant passes every check
    #[test]
    fn accepts_valid() {
        let (_signer, grant) = sample();

        assert_eq!(grant.verify(&context(&grant)), Ok(()));
    }

    // a grant round-trips through its packed form
    #[test]
    fn pack_roundtrip() {
        let (_signer, grant) = sample();

        assert_eq!(ReadGrant::unpack(&grant.pack()), Ok(grant));
        assert_eq!(grant.pack().len(), 208);
    }

    // the delegate can sign grants too
    #[test]
    fn accepts_delegate() {
        let (_signer, grant) = sample();
        let mut context = context(&grant);
        context.tape_authority = Address::new_unique();
        context.tape_delegate = grant.signer;

        assert_eq!(grant.verify(&context), Ok(()));
    }

    // a generation bump invalidates outstanding grants
    #[test]
    fn stale_generation() {
        let (_signer, grant) = sample();
        let mut context = context(&grant);
        context.grant_generation += 1;

        assert_eq!(grant.verify(&context), Err(GrantError::StaleGeneration));
    }

    // an expired grant is refused
    #[test]
    fn expired() {
        let (_signer, grant) = sample();
        let mut context = context(&grant);
        context.now = grant.expiry;

        assert_eq!(grant.verify(&context), Err(GrantError::Expired));
    }

    // a grant is useless to any other reader identity
    #[test]
    fn wrong_reader() {
        let (_signer, grant) = sample();
        let mut context = context(&grant);
        context.reader = NetworkTlsPubkey::new([0x99; 32]);

        assert_eq!(grant.verify(&context), Err(GrantError::WrongReader));
    }

    // a grant does not open a different track on the same tape
    #[test]
    fn wrong_key() {
        let (_signer, grant) = sample();
        let mut context = context(&grant);
        context.track_key = Hash([0x01; 32]);

        assert_eq!(grant.verify(&context), Err(GrantError::WrongKey));
    }

    // a stranger's signature is refused even when they name themselves signer
    #[test]
    fn stranger_signature() {
        let (_signer, grant) = sample();
        let mut context = context(&grant);
        context.tape_authority = Address::new_unique();

        assert_eq!(grant.verify(&context), Err(GrantError::SignerNotOperator));
    }

    // tampering with a signed field breaks the signature
    #[test]
    fn tampered_field() {
        let (_signer, mut grant) = sample();
        let context = context(&grant);
        grant.expiry += 1;

        assert_eq!(grant.verify(&context), Err(GrantError::BadSignature));
    }

    // a wildcard grant opens every track on the tape it names
    #[test]
    fn wildcard_key() {
        let signer = Keypair::new(&mut OsRng);
        let grant = ReadGrant::issue(
            &signer,
            Address::new_unique(),
            WILDCARD_TRACK_KEY,
            NetworkTlsPubkey::new([0x13; 32]),
            7,
            1_800_000_000,
        );
        let mut context = context(&grant);
        context.track_key = Hash([0x66; 32]);

        assert_eq!(grant.verify(&context), Ok(()));
    }

    // a request the reader signed passes at a clock inside its window
    #[test]
    fn signed_request() {
        let (_reader, pubkey, signature) = request();

        let checked = ReadRequest::verify(
            &pubkey,
            "GET",
            REQUEST_PATH,
            REQUEST_EXPIRY,
            &signature,
            REQUEST_EXPIRY - 10,
        );

        assert_eq!(checked, Ok(()));
    }

    // the method is signed, so the same signature does not open another verb
    #[test]
    fn other_method() {
        let (_reader, pubkey, signature) = request();

        let checked = ReadRequest::verify(
            &pubkey,
            "HEAD",
            REQUEST_PATH,
            REQUEST_EXPIRY,
            &signature,
            REQUEST_EXPIRY - 10,
        );

        assert_eq!(checked, Err(GrantError::RequestSignature));
    }

    // the path is signed, so the same signature does not open another object
    #[test]
    fn other_path() {
        let (_reader, pubkey, signature) = request();

        let checked = ReadRequest::verify(
            &pubkey,
            "GET",
            "/object/def",
            REQUEST_EXPIRY,
            &signature,
            REQUEST_EXPIRY - 10,
        );

        assert_eq!(checked, Err(GrantError::RequestSignature));
    }

    // a request past its expiry is refused
    #[test]
    fn stale_request() {
        let (_reader, pubkey, signature) = request();

        let checked = ReadRequest::verify(
            &pubkey,
            "GET",
            REQUEST_PATH,
            REQUEST_EXPIRY,
            &signature,
            REQUEST_EXPIRY + 1,
        );

        assert_eq!(checked, Err(GrantError::RequestExpired));
    }

    // an expiry further ahead than the window allows is refused
    #[test]
    fn distant_request() {
        let reader = Keypair::new(&mut OsRng);
        let pubkey = NetworkTlsPubkey::new(reader.pubkey().to_bytes());
        let expiry = REQUEST_EXPIRY + MAX_READ_REQUEST_TTL + 1;
        let signature = ReadRequest::sign(&reader, "GET", REQUEST_PATH, expiry);

        let checked = ReadRequest::verify(
            &pubkey,
            "GET",
            REQUEST_PATH,
            expiry,
            &signature,
            REQUEST_EXPIRY,
        );

        assert_eq!(checked, Err(GrantError::RequestWindow));
    }

    // a request signed by any other key is refused
    #[test]
    fn other_reader() {
        let (_reader, _pubkey, signature) = request();
        let stranger = Keypair::new(&mut OsRng);
        let pubkey = NetworkTlsPubkey::new(stranger.pubkey().to_bytes());

        let checked = ReadRequest::verify(
            &pubkey,
            "GET",
            REQUEST_PATH,
            REQUEST_EXPIRY,
            &signature,
            REQUEST_EXPIRY - 10,
        );

        assert_eq!(checked, Err(GrantError::RequestSignature));
    }

    // a lowercase method signs the same message as its uppercase form
    #[test]
    fn method_case() {
        let (reader, pubkey, signature) = request();
        let lowercase = ReadRequest::sign(&reader, "get", REQUEST_PATH, REQUEST_EXPIRY);

        assert_eq!(lowercase.to_bytes(), signature.to_bytes());

        let checked = ReadRequest::verify(
            &pubkey,
            "get",
            REQUEST_PATH,
            REQUEST_EXPIRY,
            &signature,
            REQUEST_EXPIRY - 10,
        );

        assert_eq!(checked, Ok(()));
    }
}
