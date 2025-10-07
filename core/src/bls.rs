use tape_crypto::bls12254::min_sig::*;
use bytemuck::{Pod, Zeroable};

// Note we're using G2 for public keys and G1 for signatures, which is the
// opposite of the more common choice (min_pk). This is because Solana does 
// not have syscalls needed to do verification on G2 signatures.

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Pod, Zeroable)]
pub struct BlsSignature(pub G1CompressedPoint);

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Pod, Zeroable)]
pub struct BlsPublicKey(pub G2CompressedPoint);

