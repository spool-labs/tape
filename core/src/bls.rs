#![allow(unexpected_cfgs)]

use tape_crypto::bls12254::errors::BLSError;
use tape_crypto::bls12254::min_sig::*;
use bytemuck::{Pod, Zeroable};

// Note we're using G2 for public keys and G1 for signatures, which is the
// opposite of the more common choice (min_pk). This is because Solana does 
// not have syscalls needed to do verification on G2 signatures.

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Pod, Zeroable)]
pub struct BlsPrivateKey(pub PrivKey);

#[cfg(not(target_os = "solana"))]
impl BlsPrivateKey {
    /// Generate a new random private key.
    pub fn from_random() -> BlsPrivateKey {
        BlsPrivateKey(PrivKey::from_random())
    }

    /// Derive the public key in G2 from this private key.
    pub fn public_key(&self) -> Result<BlsPubkey, BLSError> {
        let pk = G2Point::try_from(&self.0)?;
        Ok(BlsPubkey(pk))
    }

    /// Sign a PoP message derived from the canonical (compressed) public key.
    pub fn proof_of_possession(&self) -> Result<BlsSignature, BLSError> {
        let pop = self.0.proof_of_possession()?;
        let pop_compressed = G1CompressedPoint::try_from(pop)?;
        Ok(BlsSignature(pop_compressed))
    }
}


#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Pod, Zeroable)]
pub struct BlsSignature(pub G1CompressedPoint);

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Pod, Zeroable)]
pub struct BlsPubkey(pub G2Point); // using the uncompressed form to reduce CU

impl BlsPubkey {

    /// Verify a proof of possession (PoP) against this public key.
    pub fn is_valid(&self, pop: BlsSignature) -> bool {
        let pubkey = G2Point::try_from(self.0);
        let signature = G1Point::try_from(&pop.0);

        if pubkey.is_err() || signature.is_err() {
            return false;
        }

        let pubkey = pubkey.unwrap();
        let signature = signature.unwrap();

        verify_proof_of_possession(&pubkey, &signature).is_ok()
    }
}



#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pop_roundtrip_valid() {
        let sk = BlsPrivateKey::from_random();
        let pk = sk.public_key().expect("derive pubkey");
        let pop = sk.proof_of_possession().expect("make pop");

        assert!(pk.is_valid(pop), "PoP should verify against its public key");
    }

    #[test]
    fn pop_invalid_with_different_pubkey() {
        let sk1 = BlsPrivateKey::from_random();
        let sk2 = BlsPrivateKey::from_random();

        let pk1 = sk1.public_key().expect("pk1");
        let pk2 = sk2.public_key().expect("pk2");

        let pop1 = sk1.proof_of_possession().expect("pop1");

        assert!(pk1.is_valid(pop1), "PoP should verify on matching pk");
        assert!(!pk2.is_valid(pop1), "PoP must not verify on a different pk");
    }
}
