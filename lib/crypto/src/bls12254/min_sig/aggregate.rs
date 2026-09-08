// BLS multi-signature helpers
//
// These functions provide a way to prove that “at least t specific people signed this message”
// without uploading a pile of signatures. Each chosen signer makes a small partial signature.
// Someone off-chain combines those partials into one short, aggregated signature and sends that
// plus the list of who signed (using a bitmap). On-chain, the program already knows everyone’s
// public keys (with PoP), it checks that this one signature really corresponds to the message and
// exactly the listed people. If it passes, you’ve shown both that enough people signed (threshold)
// and exactly which people did (attribution).
//
// For example, a 2-of-3 committee with public keys PK1, PK2, PK3 
// (Solana can do ~100 sigs in one tx)
//
// Committee:
//   PK1, PK2, PK3 in G2
// Threshold:
//   t = 2
//
// Signing:
//   Signers 1 and 3 each create a partial in G1 for message m:
//     s1, s3
//   Aggregator sums the partials off-chain:
//     S_sum = s1 + s3
//   Aggregator submits:
//     - indices: [1, 3]
//     - aggregated signature: S_sum
//
// On-chain verification:
//   1) Compute H(m) in G1
//   2) Look up PK1 and PK3 from the committee registry
//   3) Build pairing input with three pairs:
//        (H(m), PK1)
//        (H(m), PK3)
//        (S_sum, -G2_one)
//   4) Run the pairing check; if it returns 1, the signature is valid
//
// Result:
//   Valid and attributable to indices {1, 3} because only PK1 and PK3 were used

use crate::bls12254::G2_MINUS_ONE;
use crate::bls12254::BLSError;
use super::g1::G1Point;
use super::g2::G2Point;
use super::hash::hash_to_curve;

use solana_bn254::prelude::{
    alt_bn128_addition, alt_bn128_pairing,
};

pub fn aggregate_partials(partials: &[G1Point]) -> Result<G1Point, BLSError> {
    if partials.is_empty() {
        return Err(BLSError::SerializationError);
    }
    let mut acc = partials[0].0;

    for s in &partials[1..] {
        let mut inbuf = [0u8; 128];
        inbuf[..64].copy_from_slice(&acc);
        inbuf[64..].copy_from_slice(&s.0);
        let out = alt_bn128_addition(&inbuf).map_err(|_| BLSError::AltBN128AddError)?;
        acc.copy_from_slice(&out[..64]);
    }
    Ok(G1Point(acc))
}

pub fn verify_aggregate<M: AsRef<[u8]>>(
    message: M,
    signer_pubkeys: &[G2Point],
    s_sum: &G1Point,
) -> Result<(), BLSError> {
    let k = signer_pubkeys.len();
    if k == 0 || s_sum.0 == [0u8; 64] {
        return Err(BLSError::SerializationError);
    }
    if !check_pubkeys(signer_pubkeys) {
        return Err(BLSError::SerializationError);
    }

    // Hash message to G1 once
    let h_g1 = hash_to_curve(message.as_ref())?.0;

    // Build input for pairing:
    // For each signer: pair (H(m), PK_i)
    // Final pair: (S_sum, -G2).
    let mut input = vec![0u8; 192 * (k + 1)];

    for (i, pubkey) in signer_pubkeys.iter().enumerate() {
        let off = 192 * i;
        input[off..off + 64].copy_from_slice(&h_g1);
        input[off + 64..off + 192].copy_from_slice(&pubkey.0);
    }

    let off = 192 * k;
    input[off..off + 64].copy_from_slice(&s_sum.0);
    input[off + 64..off + 192].copy_from_slice(&G2_MINUS_ONE);

    let r = alt_bn128_pairing(&input).map_err(|_| BLSError::AltBN128PairingError)?;
    let ok = r.iter().take(31).all(|&b| b == 0) && r[31] == 1;
    if ok {
        Ok(())
    } else {
        Err(BLSError::BLSVerificationError)
    }
}

fn check_pubkeys(pubkeys: &[G2Point]) -> bool {
    for i in 0..pubkeys.len() {
        if pubkeys[i].0 == [0u8; 128] {
            return false;
        }

        for j in (i + 1)..pubkeys.len() {
            if pubkeys[i].0 == pubkeys[j].0 {
                return false;
            }
        }
    }
    true
}


#[cfg(test)]
mod tests {
    use super::*;
    use crate::bls12254::min_sig::g1::G1Point;
    use crate::bls12254::min_sig::g2::{G2CompressedPoint, G2Point};
    use crate::bls12254::min_sig::privkey::PrivKey;
    use crate::bls12254::errors::BLSError;
    use rand::RngCore;

    fn bitmap_indices(bitmap: &[u8], n: usize) -> Vec<usize> {
        assert!(n <= bitmap.len() * 8, "bitmap too small for n");
        let mut out = Vec::with_capacity(bitmap.len() * 4);

        for i in 0..n {
            let byte_idx = i / 8;
            let bit_idx = i % 8;
            let b = bitmap[byte_idx];
            if ((b >> bit_idx) & 1) == 1 {
                out.push(i);
            }
        }
        out
    }

    fn indices_to_bitmap(indices: &[usize], n: usize) -> Vec<u8> {
        let byte_len = (n + 7) / 8;
        let mut bitmap = vec![0u8; byte_len];

        for &i in indices {
            assert!(i < n, "index {} out of range for n={}", i, n);
            let byte_idx = i / 8;
            let bit_idx = i % 8;
            bitmap[byte_idx] |= 1u8 << bit_idx;
        }
        bitmap
    }

    struct CommitteeMember {
        index: usize,
        pubkey: G2CompressedPoint,
    }

    struct OnChainCommittee {
        members: Vec<CommitteeMember>,
    }

    fn member() -> (PrivKey, G2Point) {
        let secret = PrivKey::from_random();
        let pubkey = G2Point::try_from(&secret).unwrap();
        (secret, pubkey)
    }

    #[test]
    fn test_agg_random() {
        let n = 10;
        let msg = b"fast-agg";

        let secrets: Vec<PrivKey> = (0..n).map(|_| PrivKey::from_random()).collect();
        let pubkeys: Vec<G2Point> = secrets.iter().map(|k| G2Point::try_from(k).unwrap()).collect();

        let partials: Vec<G1Point> = secrets.iter()
            .map(|k| k.sign(msg).unwrap())
            .collect();

        let agg = aggregate_partials(&partials).unwrap();

        verify_aggregate(msg, &pubkeys, &agg).unwrap();
    }

    #[test]
    fn test_agg_wrong_msg() {
        let n = 10;
        let m1 = b"m1";
        let m2 = b"m2";

        let secrets: Vec<PrivKey> = (0..n).map(|_| PrivKey::from_random()).collect();
        let pubkeys: Vec<G2Point> = secrets.iter().map(|k| G2Point::try_from(k).unwrap()).collect();

        let partials: Vec<G1Point> = secrets.iter()
            .map(|k| k.sign(m1).unwrap())
            .collect();

        let agg = aggregate_partials(&partials).unwrap();
        let err = verify_aggregate(m2, &pubkeys, &agg).unwrap_err();
        assert_eq!(err, BLSError::BLSVerificationError);
    }

    #[test]
    fn test_agg_dup_pks() {
        let msg = b"dup-pubkey";

        let secret = PrivKey::from_random();
        let pubkey = G2Point::try_from(&secret).unwrap();

        let s = secret.sign(msg).unwrap();
        let agg = aggregate_partials(&[s.clone(), s]).unwrap();

        let err = verify_aggregate(msg, &[pubkey, pubkey], &agg).unwrap_err();
        assert_eq!(err, BLSError::SerializationError);
    }

    #[test]
    fn test_zero_aggregate_signature_rejected() {
        let msg = b"zero-sig";
        let (_secret, pubkey) = member();
        let zero_sig = G1Point([0u8; 64]);

        let err = verify_aggregate(msg, &[pubkey], &zero_sig).unwrap_err();
        assert_eq!(err, BLSError::SerializationError);
    }

    #[test]
    fn test_zero_pubkey_rejected() {
        let msg = b"zero-pubkey";
        let (secret, _pubkey) = member();
        let sig = secret.sign(msg).expect("sign");
        let zero_pubkey = G2Point([0u8; 128]);

        let err = verify_aggregate(msg, &[zero_pubkey], &sig).unwrap_err();
        assert_eq!(err, BLSError::SerializationError);
    }

    #[test]
    fn test_bitmap_agg() {
        let msg = b"hello, world";
        let size = 10;

        let committee: Vec<(PrivKey, G2Point)> = (0..size)
            .map(|_| member())
            .collect();

        let onchain_committee = OnChainCommittee {
            members: committee.iter().enumerate()
                .map(|(i, (_, pk))| CommitteeMember {
                    index: i,
                    pubkey: G2CompressedPoint::try_from(pk)
                        .unwrap(),
                })
                .collect(),
        };

        let bmp_size = (size + 7) / 8;
        let mut rng = rand::thread_rng();
        let mut bmp = vec![0u8; bmp_size];
        rng.fill_bytes(&mut bmp);

        let indices = bitmap_indices(&bmp, size);

        let sigs = indices.iter()
            .map(|i| committee[*i].0.sign(msg).unwrap())
            .collect::<Vec<_>>();

        let agg = aggregate_partials(&sigs)
            .unwrap();

        let pubkeys = indices.iter()
            .map(|i| {
                onchain_committee.members.iter()
                    .find(|m| m.index == *i)
                    .unwrap()
            })
            .map(|m| G2Point::try_from(m.pubkey).unwrap())
            .collect::<Vec<_>>();

        verify_aggregate(msg, &pubkeys, &agg).unwrap();
    }

    #[test]
    fn test_bitmap_roundtrip() {
        let cases = vec![
            (vec![], 0, vec![]),
            (vec![], 1, vec![0u8]),
            (vec![0], 1, vec![1u8]),
            (vec![0, 2, 7], 8, vec![0b10000101]),
            (vec![0, 2, 7, 8], 9, vec![0b10000101, 0b00000001]),
            (vec![1, 3, 5, 9, 15], 16, vec![0b00101010, 0b10000010]),
        ];

        for (indices, n, exp_bmp) in cases {
            let bmp = indices_to_bitmap(&indices, n);
            assert_eq!(bmp, exp_bmp);

            let back_indices = bitmap_indices(&bmp, n);
            assert_eq!(back_indices, indices);
        }
    }

    #[test]
    #[should_panic(expected = "out of range")]
    fn test_invalid_index() {
        indices_to_bitmap(&[5], 5);
        indices_to_bitmap(&[5], 4);
    }

    #[test]
    #[should_panic(expected = "bitmap too small for n")]
    fn test_small_bitmap() {
        bitmap_indices(&[0u8], 9);
    }
}
