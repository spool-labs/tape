use core::ops::Add;
use num::CheckedAdd;
use solana_bn254::{
    compression::prelude::{alt_bn128_g1_compress, alt_bn128_g1_decompress},
    prelude::{alt_bn128_addition, alt_bn128_multiplication},
};

use crate::bls12254::errors::BLSError;
use super::privkey::PrivKey;
use bytemuck::{Pod, Zeroable};
#[cfg(feature = "wincode")]
use wincode_derive::{SchemaRead, SchemaWrite};

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Pod, Zeroable)]
#[cfg_attr(feature = "wincode", derive(SchemaRead, SchemaWrite))]
pub struct G1Point(pub [u8; 64]);

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Pod, Zeroable)]
#[cfg_attr(feature = "wincode", derive(SchemaRead, SchemaWrite))]
pub struct G1CompressedPoint(pub [u8; 32]);

impl Add for G1Point {
    type Output = G1Point;

    fn add(self, rhs: Self) -> G1Point {
        self.checked_add(&rhs).expect("G1Point addition failed")
    }
}

impl CheckedAdd for G1Point {
    fn checked_add(&self, rhs: &Self) -> Option<Self> {
        let mut combined_input = [0u8; 128];

        unsafe {
            *(combined_input.as_mut_ptr() as *mut [u8; 64]) = self.0;
            *(combined_input.as_mut_ptr().add(64) as *mut [u8; 64]) = rhs.0;
        }

        let result = (|| -> Result<Self, BLSError> {
            let result =
                alt_bn128_addition(&combined_input).map_err(|_| BLSError::AltBN128AddError)?;
            Ok(G1Point(
                result.try_into().map_err(|_| BLSError::AltBN128AddError)?,
            ))
        })();

        result.ok()
    }
}

impl TryFrom<PrivKey> for G1CompressedPoint {
    type Error = BLSError;

    fn try_from(value: PrivKey) -> Result<Self, Self::Error> {
        let input = [
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02,
            value.0[0],  value.0[1],  value.0[2],  value.0[3],  value.0[4],  value.0[5],  value.0[6],
            value.0[7],  value.0[8],  value.0[9],  value.0[10], value.0[11], value.0[12], value.0[13],
            value.0[14], value.0[15], value.0[16], value.0[17], value.0[18], value.0[19],
            value.0[20], value.0[21], value.0[22], value.0[23], value.0[24], value.0[25],
            value.0[26], value.0[27], value.0[28], value.0[29], value.0[30], value.0[31],
        ];

        let mut g1_sol_uncompressed = [0; 64];
        g1_sol_uncompressed.clone_from_slice(
            &alt_bn128_multiplication(&input).map_err(|_| BLSError::AltBN128MulError)?,
        );
        let compressed =
            alt_bn128_g1_compress(&g1_sol_uncompressed).map_err(|_| BLSError::SecretKeyError)?;
        Ok(G1CompressedPoint(compressed))
    }
}

impl TryFrom<PrivKey> for G1Point {
    type Error = BLSError;

    fn try_from(value: PrivKey) -> Result<Self, Self::Error> {
        let input = [
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02,
            value.0[0],  value.0[1],  value.0[2],  value.0[3],  value.0[4],  value.0[5],  value.0[6],
            value.0[7],  value.0[8],  value.0[9],  value.0[10], value.0[11], value.0[12], value.0[13],
            value.0[14], value.0[15], value.0[16], value.0[17], value.0[18], value.0[19],
            value.0[20], value.0[21], value.0[22], value.0[23], value.0[24], value.0[25],
            value.0[26], value.0[27], value.0[28], value.0[29], value.0[30], value.0[31],
        ];

        let mut g1_sol_uncompressed = [0; 64];
        g1_sol_uncompressed.clone_from_slice(
            &alt_bn128_multiplication(&input).map_err(|_| BLSError::SecretKeyError)?,
        );
        Ok(G1Point(g1_sol_uncompressed))
    }
}

impl TryFrom<G1Point> for G1CompressedPoint {
    type Error = BLSError;

    fn try_from(value: G1Point) -> Result<Self, Self::Error> {
        Ok(G1CompressedPoint(
            alt_bn128_g1_compress(&value.0).map_err(|_| BLSError::G1PointCompressionError)?,
        ))
    }
}

impl TryFrom<&G1CompressedPoint> for G1Point {
    type Error = BLSError;

    fn try_from(value: &G1CompressedPoint) -> Result<Self, Self::Error> {
        Ok(G1Point(
            alt_bn128_g1_decompress(&value.0).map_err(|_| BLSError::G1PointDecompressionError)?,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::{G1CompressedPoint, G1Point};
    use crate::bls12254::min_sig::PrivKey;

    #[test]
    fn g1_keygen_roundtrip_random() {
        let sk = PrivKey::from_random();
        // Build both compressed and uncompressed pubkeys from the same SK
        let sk_bytes = sk.0;
        let pk_uncompressed = G1Point::try_from(PrivKey(sk_bytes)).expect("g1 from sk");
        let pk_compressed = G1CompressedPoint::try_from(PrivKey(sk_bytes)).expect("g1c from sk");

        // Decompress compressed and compare with uncompressed
        let decomp = G1Point::try_from(&pk_compressed).expect("decompress");
        assert_eq!(decomp.0, pk_uncompressed.0, "G1 compress/decompress mismatch");
    }

    #[test]
    fn g1_compress_decompress_idempotent() {
        let sk = PrivKey::from_random();
        let pk_uncompressed = G1Point::try_from(sk).expect("g1 from sk");
        let pk_compressed = G1CompressedPoint::try_from(pk_uncompressed.clone()).expect("compress");
        let roundtrip = G1CompressedPoint::try_from(
            G1Point::try_from(&pk_compressed).expect("decompress again")
        ).expect("recompress");
        assert_eq!(roundtrip.0, pk_compressed.0, "G1 compress->decompress->compress changed bytes");
    }

    #[test]
    fn g1_addition_is_commutative_and_associative() {
        // Build three random G1 points from secret keys
        let a = G1Point::try_from(PrivKey::from_random()).expect("a");
        let b = G1Point::try_from(PrivKey::from_random()).expect("b");
        let c = G1Point::try_from(PrivKey::from_random()).expect("c");

        // Commutative: a + b = b + a
        let ab = a.clone() + b.clone();
        let ba = b.clone() + a.clone();
        assert_eq!(ab.0, ba.0);

        // Associative: (a + b) + c = a + (b + c)
        let lhs = (a.clone() + b.clone()) + c.clone();
        let rhs = a + (b + c);
        assert_eq!(lhs.0, rhs.0);
    }
}
