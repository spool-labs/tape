//! The bytes a slice is stored as: its sidecar and its payload under one key
//!
//! Four bytes of little-endian sidecar length, the sidecar, then the payload. Self
//! describing, because the sidecar is variable width: one node per sample window
//! of the slice, and none at all above the size the sub-leaf tree covers, which
//! the envelope writes as a length of zero.
//!
//! Every reader of a slice value goes through here. The payload is what the caller
//! stored and the only thing that leaves the node, so a read that skips the strip
//! ships sidecar bytes to a peer and fails its root check.

use tape_core::erasure::{SAMPLE_WINDOW_HEIGHT, SUB_TREE_HEIGHT};
use tape_crypto::hash::HASH_BYTES;
use tape_crypto::Hash;

/// Bytes the envelope spends saying how long the sidecar is
pub const ENVELOPE_LEN: usize = 4;

/// The widest sidecar a slice can carry, one node per window of the largest slice
pub const MAX_SIDECAR_BYTES: usize = (1 << (SUB_TREE_HEIGHT - SAMPLE_WINDOW_HEIGHT)) * HASH_BYTES;

/// Bytes the head of a stored slice occupies at its widest
///
/// What a reader asks for when it wants the sidecar and none of the payload. A
/// shorter value answers short rather than refusing, the way a `pread` does.
pub const MAX_HEAD_BYTES: usize = ENVELOPE_LEN + MAX_SIDECAR_BYTES;

/// The stored form of a slice and the sidecar it was written with
pub fn fuse(payload: &[u8], sidecar: Option<&[Hash]>) -> Vec<u8> {
    let nodes = sidecar.unwrap_or_default();
    let mut stored = Vec::with_capacity(ENVELOPE_LEN + nodes.len() * HASH_BYTES + payload.len());
    stored.extend_from_slice(&((nodes.len() * HASH_BYTES) as u32).to_le_bytes());
    for node in nodes {
        stored.extend_from_slice(&node.0);
    }
    stored.extend_from_slice(payload);
    stored
}

/// Where the payload begins, from the head of a stored slice
///
/// Clamped to what is there, so a short read of the head answers where the payload
/// would start rather than past the end of the buffer.
pub fn payload_start(stored: &[u8]) -> usize {
    ENVELOPE_LEN.saturating_add(sidecar_len(stored))
}

/// The payload of a stored slice, without the sidecar it rides behind
pub fn payload(stored: &[u8]) -> &[u8] {
    stored.get(payload_start(stored)..).unwrap_or_default()
}

/// The sidecar nodes of a stored slice, empty for one written without any
pub fn sidecar(stored: &[u8]) -> Vec<Hash> {
    let len = sidecar_len(stored);
    let Some(nodes) = stored.get(ENVELOPE_LEN..ENVELOPE_LEN + len) else {
        return Vec::new();
    };
    nodes
        .chunks_exact(HASH_BYTES)
        .map(|node| Hash(node.try_into().expect("a chunk of hash bytes is a hash")))
        .collect()
}

/// Sidecar bytes the envelope declares, capped at what a sidecar can be
fn sidecar_len(stored: &[u8]) -> usize {
    let Some(head) = stored.get(..ENVELOPE_LEN) else {
        return 0;
    };
    let declared = u32::from_le_bytes(head.try_into().expect("four bytes are a u32")) as usize;
    declared.min(MAX_SIDECAR_BYTES)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tape_core::erasure::slice_sidecar;

    // what goes in comes back out, in both halves
    #[test]
    fn roundtrip() {
        let bytes: Vec<u8> = (0..300_000).map(|byte| (byte * 13 % 249) as u8).collect();
        let nodes = slice_sidecar(&bytes).expect("a slice this size has a sidecar");

        let stored = fuse(&bytes, Some(&nodes));
        assert_eq!(stored.len(), ENVELOPE_LEN + nodes.len() * HASH_BYTES + bytes.len());
        assert_eq!(payload(&stored), bytes.as_slice());
        assert_eq!(sidecar(&stored), nodes);
    }

    // a slice too large for a sidecar carries a length of zero and nothing else
    #[test]
    fn without_a_sidecar() {
        let stored = fuse(&[7u8; 64], None);

        assert_eq!(stored.len(), ENVELOPE_LEN + 64);
        assert_eq!(payload(&stored), [7u8; 64]);
        assert!(sidecar(&stored).is_empty());
    }

    // the head alone says where the payload starts and what the sidecar is
    #[test]
    fn head_is_enough() {
        let nodes = vec![Hash([3u8; HASH_BYTES]), Hash([4u8; HASH_BYTES])];
        let stored = fuse(&[9u8; 4096], Some(&nodes));

        let head = &stored[..ENVELOPE_LEN + nodes.len() * HASH_BYTES];
        assert_eq!(payload_start(head), payload_start(&stored));
        assert_eq!(sidecar(head), nodes);
    }

    // a value shorter than its own envelope reads as empty rather than panicking
    #[test]
    fn truncated() {
        assert!(payload(&[]).is_empty());
        assert!(sidecar(&[0u8; 2]).is_empty());
        assert!(payload(&[0xFF; ENVELOPE_LEN]).is_empty());
    }
}
