//! Generic signed message wrapper.

use wincode_derive::{SchemaRead, SchemaWrite};

/// A message with an Ed25519 signature.
///
/// The `message` field contains wincode-serialized payload bytes.
/// The `signature` is an Ed25519 signature over those bytes.
#[derive(Debug, Clone, PartialEq, Eq, SchemaRead, SchemaWrite)]
pub struct SignedMessage {
    pub message: Vec<u8>,
    pub pubkey: [u8; 32],
    pub signature: [u8; 64],
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{SlicePayload, MERKLE_HEIGHT};
    use tape_crypto::Hash;

    #[test]
    fn roundtrip() {
        let inner = SlicePayload::new(
            vec![0xCD; 512],
            Hash::from([0x11; 32]),
            vec![Hash::from([0x22; 32]); MERKLE_HEIGHT],
        );
        let msg = SignedMessage {
            message: wincode::serialize(&inner).unwrap(),
            pubkey: [0xAA; 32],
            signature: [0xBB; 64],
        };
        let bytes = wincode::serialize(&msg).unwrap();
        let recovered: SignedMessage = wincode::deserialize(&bytes).unwrap();
        assert_eq!(msg, recovered);

        let inner_recovered: SlicePayload = wincode::deserialize(&recovered.message).unwrap();
        assert_eq!(inner, inner_recovered);
    }
}
