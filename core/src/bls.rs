#![allow(unexpected_cfgs)]

use core::{fmt, str::from_utf8_unchecked};

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

    /// Sign an arbitrary message, returning the signature.
    pub fn sign<T: AsRef<[u8]>>(&self, message: T) -> Result<BlsSignature, BLSError> {
        let sig = self.0.sign(message)?;
        let sig_compressed = G1CompressedPoint::try_from(sig)?; 
        Ok(BlsSignature(sig_compressed))
    }
}

#[repr(C)]
#[derive(Clone, Copy, PartialEq, Pod, Zeroable)]
pub struct BlsSignature(pub G1CompressedPoint);

impl BlsSignature {

    /// Aggregate many compressed BLS signatures into one compressed signature
    pub fn aggregate(partials: &[BlsSignature]) -> Result<BlsSignature, BLSError> {
        if partials.is_empty() {
            return Err(BLSError::SerializationError);
        }

        // Decompress → aggregate → recompress
        let decompressed: Result<Vec<G1Point>, _> = partials
            .iter()
            .map(|s| G1Point::try_from(&s.0))
            .collect();

        let agg = aggregate_partials(&decompressed?)?;
        let compressed = G1CompressedPoint::try_from(agg)?;

        Ok(BlsSignature(compressed))
    }

    /// Verify an aggregated signature against exact list of signers
    pub fn verify_aggregate<M: AsRef<[u8]>>(
        &self,
        message: M,
        signer_pubkeys: &[BlsPubkey],
    ) -> Result<(), BLSError> {
        if signer_pubkeys.is_empty() {
            return Err(BLSError::SerializationError);
        }

        let decompressed_sig = G1Point::try_from(&self.0)?;
        let g2_points: Vec<G2Point> = signer_pubkeys
            .iter()
            .map(|pk| Ok(pk.0))
            .collect::<Result<Vec<_>, _>>()?;

        verify_aggregate(message, &g2_points, &decompressed_sig)
    }

    /// Size of the compressed signature in bytes
    pub const fn size() -> usize {
        32 // G1 compressed
    }
}


#[repr(C)]
#[derive(Clone, Copy, PartialEq, Pod, Zeroable)]
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

fn write_sig_base58(f: &mut fmt::Formatter<'_>, sig: &BlsSignature) -> fmt::Result {
    const SIG_BYTES: usize = 32; // G1 compressed
    const SIG_MAX_BASE58: usize = 44;

    let sig_bytes = sig.0.0;
    if sig_bytes.len() != 32 {
        return f.write_str("<invalid bls signature>");
    }
    let mut in32 = [0u8; SIG_BYTES];
    in32.copy_from_slice(&sig_bytes);

    let mut out = [0u8; SIG_MAX_BASE58];
    let len = five8::encode_32(&in32, &mut out) as usize;
    let s = unsafe { from_utf8_unchecked(&out[..len]) };
    f.write_str(s)
}

fn write_pubkey_base58(f: &mut fmt::Formatter<'_>, pk: &BlsPubkey) -> fmt::Result {
    const PK_BYTES: usize = 64;  // G2 compressed
    const PK_MAX_BASE58: usize = 88;

    match G2CompressedPoint::try_from(&pk.0) {
        Ok(comp) => {
            let pk_bytes = comp.0;
            if pk_bytes.len() != PK_BYTES {
                return f.write_str("<invalid bls pubkey>");
            }
            let mut in64 = [0u8; PK_BYTES];
            in64.copy_from_slice(&pk_bytes);

            let mut out = [0u8; PK_MAX_BASE58];
            let len = five8::encode_64(&in64, &mut out) as usize;
            let s = unsafe { from_utf8_unchecked(&out[..len]) };
            f.write_str(s)
        }
        Err(_) => f.write_str("<invalid bls pubkey>"),
    }
}

impl fmt::Debug for BlsPubkey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write_pubkey_base58(f, self)
    }
}

impl fmt::Display for BlsPubkey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write_pubkey_base58(f, self)
    }
}

impl fmt::Debug for BlsSignature {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write_sig_base58(f, self)
    }
}

impl fmt::Display for BlsSignature {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write_sig_base58(f, self)
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

    #[test]
    fn sign_and_verify_message() {
        let sk = BlsPrivateKey::from_random();
        let pk = sk.public_key().expect("derive pubkey");
        let message = b"test message";

        let sig = sk.sign(message).expect("sign message");
        let sig_point = G1Point::try_from(&sig.0).expect("decompress sig");

        assert!(pk.0.verify(&sig_point, message).is_ok(), "signature should verify");
    }

    #[test]
    fn aggregate_full_roundtrip_2_of_3_committee() {
        let message = b"consensus finality goes brrr";

        // Committee setup: 3 members
        let sk1 = BlsPrivateKey::from_random();
        let sk2 = BlsPrivateKey::from_random();
        let sk3 = BlsPrivateKey::from_random();

        let pk1 = sk1.public_key().unwrap();
        let pk2 = sk2.public_key().unwrap();
        let pk3 = sk3.public_key().unwrap();

        // Only 2 sign (say 1 and 3)
        let sig1 = sk1.sign(message).unwrap();
        let sig3 = sk3.sign(message).unwrap();

        // Off-chain aggregator combines them
        let aggregated = BlsSignature::aggregate(&[sig1, sig3]).unwrap();

        // On-chain: verify using exact list of who signed
        let signers = [pk1, pk3]; // order doesn't matter
        aggregated.verify_aggregate(message, &signers).unwrap();

        // Negative: wrong message fails
        let wrong_msg = b"hacked!";
        assert_eq!(
            aggregated.verify_aggregate(wrong_msg, &signers).unwrap_err(),
            BLSError::BLSVerificationError
        );

        // Negative: missing signer fails
        let only_one = [pk1];
        assert_eq!(
            aggregated.verify_aggregate(message, &only_one).unwrap_err(),
            BLSError::BLSVerificationError
        );

        // Negative: extra signer fails
        let all_three = [pk1, pk2, pk3];
        assert_eq!(
            aggregated.verify_aggregate(message, &all_three).unwrap_err(),
            BLSError::BLSVerificationError
        );

        // Negative: duplicate pubkey rejected
        let dup = [pk1, pk1];
        assert_eq!(
            aggregated.verify_aggregate(message, &dup).unwrap_err(),
            BLSError::SerializationError
        );
    }

    #[test]
    fn aggregate_10_signers_stress_test() {
        let message = b"10x speed, 1x size";

        let keys: Vec<_> = (0..10)
            .map(|_| BlsPrivateKey::from_random())
            .collect();

        let pubkeys: Vec<_> = keys.iter()
            .map(|sk| sk.public_key().unwrap())
            .collect();

        // All 10 sign
        let partials: Vec<_> = keys.iter()
            .map(|sk| sk.sign(message).unwrap())
            .collect();

        let aggregated = BlsSignature::aggregate(&partials).unwrap();

        // Verify all 10
        aggregated.verify_aggregate(message, &pubkeys).unwrap();

        // Tamper one bit → fails
        let mut tampered = aggregated;
        tampered.0 .0[0] ^= 0x01;
        assert!(
            tampered.verify_aggregate(message, &pubkeys).is_err(),
        );
    }
}
