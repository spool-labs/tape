//! Inline fetch against in-memory peers
//!
//! Lives outside the crate because `peer-memory` depends on `tape-protocol`, so
//! unit tests would see two copies of the `Api` trait.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use bytemuck::Zeroable;
use peer_memory::MemoryApi;
use tape_core::track::blob::BlobEncoding;
use tape_core::track::data::BlobData;
use tape_core::types::SpoolIndex;
use tape_crypto::address::Address;
use tape_crypto::hash::hash;
use tape_protocol::api::{ApiError, GetTrackDataRes, PeerReq, PeerRes};
use tape_protocol::inline::{fetch_inline_from_peers, InlineFetchError};

const PAYLOAD: &[u8] = b"inline payload";

/// An in-memory cluster where nodes missing from `answers` fail, recording who was asked
fn cluster(answers: HashMap<Address, BlobData>) -> (MemoryApi, Arc<Mutex<Vec<Address>>>) {
    let asked = Arc::new(Mutex::new(Vec::new()));
    let recorder = asked.clone();
    let api = MemoryApi::new(move |node, req| {
        recorder.lock().expect("recorder lock").push(node);
        let PeerReq::GetTrackData(_) = req else {
            return PeerRes::GetHealth(Err(ApiError::Other("unexpected request".into())));
        };
        match answers.get(&node) {
            Some(data) => PeerRes::GetTrackData(Ok(GetTrackDataRes { data: data.clone() })),
            None => PeerRes::GetTrackData(Err(ApiError::Other("unavailable".into()))),
        }
    });
    (api, asked)
}

fn peers(nodes: &[Address]) -> Vec<(SpoolIndex, Address)> {
    let mut peers = Vec::with_capacity(nodes.len());
    for (index, node) in nodes.iter().enumerate() {
        peers.push((SpoolIndex(index as u64), *node));
    }
    peers
}

fn inline(bytes: &[u8]) -> BlobData {
    BlobData::Inline(bytes.to_vec())
}

fn coded() -> BlobData {
    BlobData::Coded(BlobEncoding::zeroed())
}

// the first peer with verified bytes answers and later peers are never asked
#[tokio::test]
async fn first_verified() {
    let (failing, first, second) = (Address::new_unique(), Address::new_unique(), Address::new_unique());
    let (api, asked) = cluster(HashMap::from([(first, inline(PAYLOAD)), (second, inline(PAYLOAD))]));

    let payload = fetch_inline_from_peers(&api, &peers(&[failing, first, second]), Address::new_unique(), hash(PAYLOAD))
        .await
        .expect("verified payload");

    assert_eq!(payload.node, first);
    assert_eq!(payload.bytes, PAYLOAD);
    assert_eq!(*asked.lock().expect("recorder lock"), vec![failing, first]);
}

// a payload that does not match the value hash is skipped
#[tokio::test]
async fn mismatched_hash() {
    let (tampered, honest) = (Address::new_unique(), Address::new_unique());
    let (api, _) = cluster(HashMap::from([(tampered, inline(b"tampered")), (honest, inline(PAYLOAD))]));

    let payload = fetch_inline_from_peers(&api, &peers(&[tampered, honest]), Address::new_unique(), hash(PAYLOAD))
        .await
        .expect("verified payload");

    assert_eq!(payload.node, honest);
}

// a coded answer for an inline track is skipped
#[tokio::test]
async fn coded_answer() {
    let (coded_node, honest) = (Address::new_unique(), Address::new_unique());
    let (api, _) = cluster(HashMap::from([(coded_node, coded()), (honest, inline(PAYLOAD))]));

    let payload = fetch_inline_from_peers(&api, &peers(&[coded_node, honest]), Address::new_unique(), hash(PAYLOAD))
        .await
        .expect("verified payload");

    assert_eq!(payload.node, honest);
}

// a node holding several spools in the group is asked once
#[tokio::test]
async fn repeated_node() {
    let node = Address::new_unique();
    let (api, asked) = cluster(HashMap::new());

    let result = fetch_inline_from_peers(&api, &peers(&[node, node, node]), Address::new_unique(), hash(PAYLOAD)).await;

    assert!(matches!(result, Err(InlineFetchError::Unavailable { .. })));
    assert_eq!(asked.lock().expect("recorder lock").len(), 1);
}

// failing, tampered and coded peers leave the track unavailable
#[tokio::test]
async fn no_usable_peer() {
    let (failing, tampered, coded_node) = (Address::new_unique(), Address::new_unique(), Address::new_unique());
    let (api, _) = cluster(HashMap::from([(tampered, inline(b"tampered")), (coded_node, coded())]));
    let track = Address::new_unique();

    let result = fetch_inline_from_peers(&api, &peers(&[failing, tampered, coded_node]), track, hash(PAYLOAD)).await;

    assert!(matches!(result, Err(InlineFetchError::Unavailable { track: missing }) if missing == track));
}

// an empty group has no one to ask
#[tokio::test]
async fn no_peers() {
    let (api, asked) = cluster(HashMap::new());

    let result = fetch_inline_from_peers(&api, &[], Address::new_unique(), hash(PAYLOAD)).await;

    assert!(matches!(result, Err(InlineFetchError::NoPeers { .. })));
    assert!(asked.lock().expect("recorder lock").is_empty());
}
