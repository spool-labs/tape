//! Generic signed message wrapper.

use wincode_derive::{SchemaRead, SchemaWrite};
use tape_crypto::ed25519::{PublicKey, Signature};

/// A message with an Ed25519 signature.
///
/// The `message` field contains wincode-serialized payload bytes.
/// The `signature` is an Ed25519 signature over those bytes.
#[derive(Debug, Clone, PartialEq, Eq, SchemaRead, SchemaWrite)]
pub struct SignedMessage {
    pub message: Vec<u8>,
    pub pubkey: PublicKey,
    pub signature: Signature,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{SlicePayload, MERKLE_HEIGHT};
    use tape_crypto::ed25519::SecretKey;
    use tape_crypto::Hash;

    #[test]
    fn roundtrip() {
        let inner = SlicePayload::new(
            vec![0xCD; 512],
            Hash::from([0x11; 32]),
            vec![Hash::from([0x22; 32]); MERKLE_HEIGHT],
        );
        let sk = SecretKey::from_bytes([0x11; 32]);
        let pk = sk.public_key();
        let msg_bytes = wincode::serialize(&inner).unwrap();
        let sig = sk.sign(&msg_bytes);

        let msg = SignedMessage {
            message: msg_bytes,
            pubkey: pk,
            signature: sig,
        };
        let bytes = wincode::serialize(&msg).unwrap();
        let recovered: SignedMessage = wincode::deserialize(&bytes).unwrap();
        assert_eq!(msg, recovered);

        let inner_recovered: SlicePayload = wincode::deserialize(&recovered.message).unwrap();
        assert_eq!(inner, inner_recovered);
    }
}
