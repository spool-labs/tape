use bytemuck::{Pod, Zeroable};

// Note we're using G2 for public keys and G1 for signatures, which is the
// opposite of the more common choice (min_pk). This is because Solana does 
// not have syscalls needed to do verification on G2 signatures.

pub const G1_COMPRESSED_SIZE: usize = 32;
pub const G2_COMPRESSED_SIZE: usize = 64;

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Pod, Zeroable)]
pub struct Bn128Signature(pub [u8; G1_COMPRESSED_SIZE]);

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Pod, Zeroable)]
pub struct Bn128PublicKey(pub [u8; G2_COMPRESSED_SIZE]);


