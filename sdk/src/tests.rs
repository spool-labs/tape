use std::sync::Arc;

use arc_swap::ArcSwap;
use bytemuck::Zeroable;
use peer_manager::PeerManager;
use peer_memory::MemoryApi;
use rpc::RpcError;
use rpc_client::RpcClient;
use rpc_litesvm::LiteSvmRpc;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::{Keypair, Signer};
use tape_api::program::tapedrive::{self, archive_pda, tape_pda, track_pda};
use tape_api::state::{Archive, Tape, Track};
use tape_core::encoding::EncodingProfile;
use tape_core::spooler::SpoolGroup;
use tape_core::tape::TrackData;
use tape_core::types::coin::{Coin, TAPE};
use tape_core::types::{EpochNumber, StorageUnits};
use tape_crypto::{Hash, hash};
use tape_protocol::ProtocolState;

use crate::codec::encoder::BlobEncoder;
use crate::error::TapedriveError;
use crate::track::write::should_retry_certification;
use crate::Tapedrive;

fn setup() -> (LiteSvmRpc, Tapedrive<LiteSvmRpc, MemoryApi>) {
    let rpc = LiteSvmRpc::new();
    let payer = Keypair::new();
    let rpc_client = Arc::new(RpcClient::from_rpc(rpc.clone()));
    let peer_manager = Arc::new(PeerManager::new());
    let api = Arc::new(MemoryApi::noop());
    let state = ArcSwap::from_pointee(ProtocolState::default());
    let tapedrive = Tapedrive::from_parts(state, peer_manager, api, rpc_client, Some(&payer));
    (rpc, tapedrive)
}

fn pipe(rpc: &LiteSvmRpc, address: Pubkey, packed: &[u8]) {
    rpc.set_account_data(address, tapedrive::ID, packed)
        .unwrap();
}

fn make_tape(authority: Pubkey) -> Tape {
    let mut tape: Tape = Zeroable::zeroed();
    tape.authority = authority;
    tape.capacity = StorageUnits::mb(100);
    tape.used = StorageUnits::mb(10);
    tape.active_epoch = EpochNumber(1);
    tape.expiry_epoch = EpochNumber(10);
    tape
}

fn make_track(tape_address: Pubkey, key: Hash) -> Track {
    let mut track: Track = Zeroable::zeroed();
    track.tape = tape_address;
    track.key = key;
    track.size = StorageUnits::mb(5);
    track.data = TrackData::new(EpochNumber(1), Hash::default(), SpoolGroup(0));
    track.data.set_profile(EncodingProfile::clay_default());
    track
}

fn make_archive(price: u64) -> Archive {
    let mut archive: Archive = Zeroable::zeroed();
    archive.storage_price = Coin::<TAPE>::new(price);
    archive
}

#[tokio::test]
async fn get_tape() {
    let (rpc, tapedrive) = setup();
    let authority = Keypair::new();
    let (tape_address, _) = tape_pda(authority.pubkey());

    let tape = make_tape(authority.pubkey());
    pipe(&rpc, tape_address, &tape.pack());

    let result = tapedrive.get_tape(&tape_address).await.unwrap();
    assert_eq!(result.capacity, StorageUnits::mb(100));
    assert_eq!(result.used, StorageUnits::mb(10));
    assert_eq!(result.active_epoch, EpochNumber(1));
    assert_eq!(result.expiry_epoch, EpochNumber(10));
}

#[tokio::test]
async fn get_track() {
    let (rpc, tapedrive) = setup();
    let authority = Keypair::new();
    let key = Hash::default();
    let (tape_address, _) = tape_pda(authority.pubkey());
    let (track_address, _) = track_pda(authority.pubkey(), key);

    let track = make_track(tape_address, key);
    pipe(&rpc, track_address, &track.pack());

    let result = tapedrive.get_track(&track_address).await.unwrap();
    assert_eq!(result.tape, tape_address);
    assert_eq!(result.key, key);
    assert_eq!(result.size, StorageUnits::mb(5));
}

#[tokio::test]
async fn list_tracks() {
    let (rpc, tapedrive) = setup();

    let authority_a = Keypair::new();
    let authority_b = Keypair::new();
    let (tape_a, _) = tape_pda(authority_a.pubkey());
    let (tape_b, _) = tape_pda(authority_b.pubkey());

    let key1 = hash::hash(b"track1");
    let key2 = hash::hash(b"track2");
    let key3 = hash::hash(b"track3");

    let (addr1, _) = track_pda(authority_a.pubkey(), key1);
    let (addr2, _) = track_pda(authority_a.pubkey(), key2);
    let (addr3, _) = track_pda(authority_b.pubkey(), key3);

    pipe(&rpc, addr1, &make_track(tape_a, key1).pack());
    pipe(&rpc, addr2, &make_track(tape_a, key2).pack());
    pipe(&rpc, addr3, &make_track(tape_b, key3).pack());

    let tracks_a = tapedrive.list_tracks(&tape_a).await.unwrap();
    assert_eq!(tracks_a.len(), 2);
    for (_, track) in &tracks_a {
        assert_eq!(track.tape, tape_a);
    }

    let tracks_b = tapedrive.list_tracks(&tape_b).await.unwrap();
    assert_eq!(tracks_b.len(), 1);
    assert_eq!(tracks_b[0].1.tape, tape_b);
}

#[tokio::test]
async fn estimate_cost() {
    let (rpc, tapedrive) = setup();
    let (archive_address, _) = archive_pda();

    pipe(&rpc, archive_address, &make_archive(100).pack());

    let cost = tapedrive.estimate_cost(StorageUnits::mb(50), 4).await.unwrap();
    assert_eq!(cost.as_u64(), 20_000);
}

#[tokio::test]
async fn verify_match() {
    let (rpc, tapedrive) = setup();
    let authority = Keypair::new();
    let key = hash::hash(b"test-track");
    let (tape_address, _) = tape_pda(authority.pubkey());
    let (track_address, _) = track_pda(authority.pubkey(), key);

    let data = b"hello world, this is test data for verify".to_vec();
    let profile = EncodingProfile::clay_default();

    let mut encoder = BlobEncoder::with_profile(profile);
    let (_, root) = encoder.encode_with_root(data.clone()).unwrap();

    let mut track = make_track(tape_address, key);
    track.data.commitment_hash = root;
    track.data.set_profile(profile);
    pipe(&rpc, track_address, &track.pack());

    assert!(tapedrive.verify(&track_address, &data).await.unwrap());
}

#[tokio::test]
async fn verify_mismatch() {
    let (rpc, tapedrive) = setup();
    let authority = Keypair::new();
    let key = hash::hash(b"test-track");
    let (tape_address, _) = tape_pda(authority.pubkey());
    let (track_address, _) = track_pda(authority.pubkey(), key);

    let data = b"hello world, this is test data for verify".to_vec();
    let profile = EncodingProfile::clay_default();

    let mut encoder = BlobEncoder::with_profile(profile);
    let (_, root) = encoder.encode_with_root(data.clone()).unwrap();

    let mut track = make_track(tape_address, key);
    track.data.commitment_hash = root;
    track.data.set_profile(profile);
    pipe(&rpc, track_address, &track.pack());

    let wrong_data = b"this is different data entirely".to_vec();
    assert!(!tapedrive.verify(&track_address, &wrong_data).await.unwrap());
}

#[tokio::test]
async fn tape_not_found() {
    let (_rpc, tapedrive) = setup();
    let address = Pubkey::new_unique();
    assert!(tapedrive.get_tape(&address).await.is_err());
}

#[tokio::test]
async fn track_not_found() {
    let (_rpc, tapedrive) = setup();
    let address = Pubkey::new_unique();
    assert!(tapedrive.get_track(&address).await.is_err());
}

#[test]
fn retries_certification_on_bad_signature() {
    let err = TapedriveError::Rpc(RpcError::Transaction(
        "custom program error: 0x12".to_string(),
    ));
    assert!(should_retry_certification(&err));
}

#[test]
fn retries_certification_on_retryable_rpc() {
    let err = TapedriveError::Rpc(RpcError::Request(
        "connection reset".to_string(),
    ));
    assert!(should_retry_certification(&err));
}

#[test]
fn does_not_retry_non_retryable_rpc() {
    let err = TapedriveError::Rpc(RpcError::AccountNotFound(Pubkey::new_unique()));
    assert!(!should_retry_certification(&err));
}
