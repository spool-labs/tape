//! Client-side SigV4 primitives on tape-sha256 for the S3 tests

use tape_crypto::hash::{hash, hashv, HASH_BYTES};

/// HMAC-SHA256 pads its key to one SHA-256 block
const HMAC_BLOCK_BYTES: usize = 64;
/// RFC 2104 inner pad byte
const HMAC_INNER_PAD: u8 = 0x36;
/// RFC 2104 outer pad byte
const HMAC_OUTER_PAD: u8 = 0x5c;
/// Last component of every SigV4 credential scope
const AWS4_REQUEST: &[u8] = b"aws4_request";

/// Lowercase hex SHA-256 of the data
pub fn sha256_hex(data: &[u8]) -> String {
    hex::encode(hash(data).to_bytes())
}

/// HMAC-SHA256 of the data under the key, per RFC 2104
pub fn hmac_sha256(key: &[u8], data: &[u8]) -> [u8; HASH_BYTES] {
    let mut block = [0u8; HMAC_BLOCK_BYTES];
    if key.len() > HMAC_BLOCK_BYTES {
        block[..HASH_BYTES].copy_from_slice(hash(key).as_ref());
    } else {
        block[..key.len()].copy_from_slice(key);
    }

    let mut inner_pad = [HMAC_INNER_PAD; HMAC_BLOCK_BYTES];
    let mut outer_pad = [HMAC_OUTER_PAD; HMAC_BLOCK_BYTES];
    for (index, byte) in block.iter().enumerate() {
        inner_pad[index] ^= byte;
        outer_pad[index] ^= byte;
    }

    let inner = hashv(&[&inner_pad, data]);
    hashv(&[&outer_pad, inner.as_ref()]).to_bytes()
}

/// The SigV4 signing key for one date, region and service
pub fn derive_signing_key(secret: &str, date_stamp: &str, region: &str, service: &str) -> [u8; HASH_BYTES] {
    let key_secret = format!("AWS4{secret}");
    let key_date = hmac_sha256(key_secret.as_bytes(), date_stamp.as_bytes());
    let key_region = hmac_sha256(&key_date, region.as_bytes());
    let key_service = hmac_sha256(&key_region, service.as_bytes());
    hmac_sha256(&key_service, AWS4_REQUEST)
}
