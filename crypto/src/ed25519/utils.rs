use core::mem::MaybeUninit;
use core::ptr;

use solana_curve25519::{
    edwards::{multiply_edwards, PodEdwardsPoint},
    scalar::PodScalar,
};

/// Split the signature into two 32-byte arrays.
#[inline(always)]
pub fn split_signature(sig: &[u8; 64]) -> ([u8; 32], [u8; 32]) {
    let mut sig_lower: MaybeUninit<[u8; 32]> = MaybeUninit::uninit();
    let mut sig_upper: MaybeUninit<[u8; 32]> = MaybeUninit::uninit();

    // SAFETY: The length of `sig` is 64 bytes; we copy 32 bytes into each half.
    unsafe {
        ptr::copy_nonoverlapping(sig.as_ptr(), sig_lower.as_mut_ptr() as *mut u8, 32);
        ptr::copy_nonoverlapping(sig.as_ptr().add(32), sig_upper.as_mut_ptr() as *mut u8, 32);
        (sig_lower.assume_init(), sig_upper.assume_init())
    }
}

/// Determine if this point is of small order.
///
/// Return:
/// - true if in the torsion subgroup E[8]
/// - false otherwise
pub fn is_small_order(point: &PodEdwardsPoint) -> bool {
    // Create a PodScalar representing the scalar value 8
    let scalar_8 = scalar_from_u64(8);

    // Multiply the point by the scalar 8
    if let Some(result_point) = multiply_edwards(&scalar_8, point) {
        // Compare the result to the identity point
        result_point == identity()
    } else {
        // If multiplication failed, return false
        false
    }
}

/// Create the identity point (neutral element) in compressed form.
pub fn identity() -> PodEdwardsPoint {
    let mut bytes = [0u8; 32];
    bytes[0] = 1; // The compressed identity point has first byte as 1
    PodEdwardsPoint(bytes)
}

/// Create a PodScalar from a u64 integer.
pub fn scalar_from_u64(n: u64) -> PodScalar {
    let mut bytes = [0u8; 32];
    bytes[..8].copy_from_slice(&n.to_le_bytes());
    PodScalar(bytes)
}
