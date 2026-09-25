//! Wire form of a slice window: the sidecar node count, the nodes, then the bytes

use bytes::Bytes;
use tape_crypto::Hash;

use crate::api::ApiError;

const COUNT_BYTES: usize = 4;
const HASH_BYTES: usize = 32;

/// Lay out a sidecar and a run of slice bytes for the wire
pub fn encode_slice_window(sidecar: &[Hash], data: &[u8]) -> Vec<u8> {
    let mut body = Vec::with_capacity(COUNT_BYTES + sidecar.len() * HASH_BYTES + data.len());
    body.extend_from_slice(&(sidecar.len() as u32).to_le_bytes());
    for node in sidecar {
        body.extend_from_slice(node.as_ref());
    }
    body.extend_from_slice(data);
    body
}

/// Split a wire body back into the sidecar and the slice bytes, sharing the body's buffer
pub fn decode_slice_window(body: Bytes) -> Result<(Vec<Hash>, Bytes), ApiError> {
    let malformed = || ApiError::Serialization("slice window body is short".into());
    let count_bytes: [u8; COUNT_BYTES] = body
        .get(..COUNT_BYTES)
        .and_then(|head| head.try_into().ok())
        .ok_or_else(malformed)?;
    let count = u32::from_le_bytes(count_bytes) as usize;
    let data_start = count
        .checked_mul(HASH_BYTES)
        .and_then(|nodes| nodes.checked_add(COUNT_BYTES))
        .filter(|start| *start <= body.len())
        .ok_or_else(malformed)?;

    let mut sidecar = Vec::with_capacity(count);
    for node in body[COUNT_BYTES..data_start].chunks_exact(HASH_BYTES) {
        let node: [u8; HASH_BYTES] = node.try_into().map_err(|_| malformed())?;
        sidecar.push(Hash::from(node));
    }
    Ok((sidecar, body.slice(data_start..)))
}

#[cfg(test)]
mod tests {
    use super::*;

    // a window survives the wire, and a body cut inside its sidecar is refused
    #[test]
    fn round_trip() {
        let sidecar = vec![Hash::from([1u8; 32]), Hash::from([2u8; 32])];
        let body = Bytes::from(encode_slice_window(&sidecar, b"slice bytes"));

        let (nodes, data) = decode_slice_window(body.clone()).expect("decode");
        let short = decode_slice_window(body.slice(..20));

        assert_eq!(nodes, sidecar);
        assert_eq!(&data[..], b"slice bytes");
        assert!(short.is_err());
    }
}
