use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use arc_swap::ArcSwap;
use bytemuck::Zeroable;
use peer_manager::{PeerManager, PeerNode};
use peer_memory::MemoryApi;
use rpc_client::RpcClient;
use rpc_litesvm::LiteSvmRpc;
use tape_api::program::tapedrive::{self, tape_pda, track_pda};
use tape_api::state::Tape;
use tape_core::bls::BlsPubkey;
use tape_core::spooler::SpoolGroup;
use tape_core::track::data::TrackData;
use tape_core::track::store::TrackStore;
use tape_core::track::types::{
    CompressedTrack, CompressedTrackProof, TrackKind, TrackState,
};
use tape_core::system::CommitteeMember;
use tape_core::types::coin::{Coin, TAPE};
use tape_core::types::network::NetworkAddress;
use tape_core::types::{EpochNumber, NodeId, StorageUnits, TrackNumber};
use tape_crypto::{hash, Hash};
use tape_crypto::address::Address;
use tape_crypto::ed25519::Keypair;
use tape_protocol::api::{
    ApiError, FindTrackVersion, FindTrackRes, GetTrackByNumberRes, GetTrackDataRes,
    GetTrackProofRes, GetTrackRes, ListTracksByTapeRes, PeerReq, PeerRes,
};
use tape_protocol::ProtocolState;

use tape_sdk::tapedrive::Tapedrive;

struct Fixture {
    rpc: LiteSvmRpc,
    client: Tapedrive<LiteSvmRpc, MemoryApi>,
    tracks: Arc<Mutex<HashMap<Address, CompressedTrack>>>,
    data: Arc<Mutex<HashMap<Address, TrackData>>>,
}

fn unexpected_error() -> ApiError {
    ApiError::Other("unexpected".into())
}

fn unexpected_peer_response(request: &PeerReq) -> PeerRes {
    match request {
        PeerReq::GetTrack(_) => PeerRes::GetTrack(Err(unexpected_error())),
        PeerReq::GetTrackByNumber(_) => PeerRes::GetTrackByNumber(Err(unexpected_error())),
        PeerReq::FindTrack(_) => PeerRes::FindTrack(Err(unexpected_error())),
        PeerReq::ListTracksByTape(_) => PeerRes::ListTracksByTape(Err(unexpected_error())),
        PeerReq::GetTrackData(_) => PeerRes::GetTrackData(Err(unexpected_error())),
        PeerReq::GetTrackProof(_) => PeerRes::GetTrackProof(Err(unexpected_error())),
        PeerReq::SyncSlices(_) => PeerRes::SyncSlices(Err(unexpected_error())),
        PeerReq::SyncTracks(_) => PeerRes::SyncTracks(Err(unexpected_error())),
        PeerReq::Repair(_) => PeerRes::Repair(Err(unexpected_error())),
        PeerReq::Certify(_) => PeerRes::Certify(Err(unexpected_error())),
        PeerReq::SignSnapshot(_) => PeerRes::SignSnapshot(Err(unexpected_error())),
        PeerReq::Invalidate(_) => PeerRes::Invalidate(Err(unexpected_error())),
        PeerReq::GetHealth(_) => PeerRes::GetHealth(Err(unexpected_error())),
        PeerReq::GetStats(_) => PeerRes::GetStats(Err(unexpected_error())),
        PeerReq::PutSlice(_) => PeerRes::PutSlice(Err(unexpected_error())),
        PeerReq::GetSlice(_) => PeerRes::GetSlice(Err(unexpected_error())),
    }
}

impl Fixture {
    fn insert_track(&self, track: CompressedTrack, data: TrackData) -> Address {
        let address = track_pda(track.tape, track.track_number).0;
        self.tracks.lock().unwrap().insert(address, track);
        self.data.lock().unwrap().insert(address, data);
        address
    }
}

fn setup() -> Fixture {
    let rpc = LiteSvmRpc::new();
    let mut rng = rand::thread_rng();
    let payer = Keypair::new(&mut rng);
    let rpc_client = Arc::new(RpcClient::from_rpc(rpc.clone()));

    let tracks = Arc::new(Mutex::new(HashMap::<Address, CompressedTrack>::new()));
    let data = Arc::new(Mutex::new(HashMap::<Address, TrackData>::new()));
    let proofs = Arc::new(Mutex::new(HashMap::<Address, CompressedTrackProof>::new()));

    let api = Arc::new(MemoryApi::new({
        let tracks = tracks.clone();
        let data = data.clone();
        let proofs = proofs.clone();
        move |_node, req| match req {
            PeerReq::GetTrack(req) => match tracks.lock().unwrap().get(&req.track).copied() {
                Some(track) => PeerRes::GetTrack(Ok(GetTrackRes { track })),
                None => PeerRes::GetTrack(Err(ApiError::NotFound)),
            },
            PeerReq::GetTrackByNumber(req) => {
                let address = track_pda(req.tape, req.track_number).0;
                match tracks.lock().unwrap().get(&address).copied() {
                    Some(track) => PeerRes::GetTrackByNumber(Ok(GetTrackByNumberRes { track })),
                    None => PeerRes::GetTrackByNumber(Err(ApiError::NotFound)),
                }
            }
            PeerReq::FindTrack(req) => {
                let mut matches = tracks
                    .lock()
                    .unwrap()
                    .values()
                    .copied()
                    .filter(|track| track.tape == req.tape && track.key == req.key)
                    .collect::<Vec<_>>();
                matches.sort_by_key(|track| track.track_number.0);

                let track = match req.version {
                    FindTrackVersion::Latest => matches.pop(),
                    FindTrackVersion::Number(track_number) => matches
                        .into_iter()
                        .find(|track| track.track_number == track_number),
                };

                match track {
                    Some(track) => PeerRes::FindTrack(Ok(FindTrackRes { track })),
                    None => PeerRes::FindTrack(Err(ApiError::NotFound)),
                }
            }
            PeerReq::ListTracksByTape(req) => {
                let mut tracks = tracks
                    .lock()
                    .unwrap()
                    .values()
                    .copied()
                    .filter(|track| track.tape == req.tape)
                    .collect::<Vec<_>>();
                tracks.sort_by_key(|track| track.track_number.0);

                let start = req
                    .cursor
                    .and_then(|cursor| {
                        tracks
                            .iter()
                            .position(|track| track.track_number == cursor)
                            .map(|idx| idx + 1)
                    })
                    .unwrap_or(0);
                let limit = req.limit as usize;
                let page = tracks
                    .iter()
                    .skip(start)
                    .take(limit)
                    .copied()
                    .collect::<Vec<_>>();
                let next_cursor = if start + page.len() < tracks.len() {
                    page.last().map(|track| track.track_number)
                } else {
                    None
                };

                PeerRes::ListTracksByTape(Ok(ListTracksByTapeRes {
                    tracks: page,
                    next_cursor,
                }))
            }
            PeerReq::GetTrackData(req) => match data.lock().unwrap().get(&req.track).cloned() {
                Some(data) => PeerRes::GetTrackData(Ok(GetTrackDataRes { data })),
                None => PeerRes::GetTrackData(Err(ApiError::NotFound)),
            },
            PeerReq::GetTrackProof(req) => match proofs.lock().unwrap().get(&req.track).copied() {
                Some(proof) => PeerRes::GetTrackProof(Ok(GetTrackProofRes { proof })),
                None => PeerRes::GetTrackProof(Err(ApiError::NotFound)),
            },
            PeerReq::SyncSlices(_) => unexpected_peer_response(&req),
            PeerReq::SyncTracks(_) => unexpected_peer_response(&req),
            PeerReq::Repair(_) => unexpected_peer_response(&req),
            PeerReq::Certify(_) => unexpected_peer_response(&req),
            PeerReq::SignSnapshot(_) => unexpected_peer_response(&req),
            PeerReq::Invalidate(_) => unexpected_peer_response(&req),
            PeerReq::GetHealth(_) => unexpected_peer_response(&req),
            PeerReq::GetStats(_) => unexpected_peer_response(&req),
            PeerReq::PutSlice(_) => unexpected_peer_response(&req),
            PeerReq::GetSlice(_) => unexpected_peer_response(&req),
        }
    }));

    let peer_manager = Arc::new(PeerManager::new());
    peer_manager.add_peer(make_peer(NodeId(1), 3001));

    let mut state = ProtocolState::default();
    state
        .committee
        .push(CommitteeMember::new(NodeId(1), Coin::<TAPE>::new(1000)));

    let client = Tapedrive::from_parts(
        ArcSwap::from_pointee(state),
        peer_manager,
        api,
        rpc_client,
        Some(payer),
    );

    Fixture {
        rpc,
        client,
        tracks,
        data,
    }
}

fn make_peer(node_id: NodeId, port: u16) -> PeerNode {
    PeerNode {
        node_id,
        authority: Address::new_unique(),
        state_address: Address::new_unique(),
        bls_pubkey: BlsPubkey::zeroed(),
        tls_pubkey: Address::new_unique(),
        network_address: NetworkAddress::new_ipv4([127, 0, 0, 1], port),
    }
}

fn pipe(rpc: &LiteSvmRpc, address: Address, packed: &[u8]) {
    rpc.set_account_data(address, tapedrive::ID, packed).unwrap();
}

fn make_tape(authority: Address) -> Tape {
    let mut tape: Tape = Zeroable::zeroed();
    tape.authority = authority;
    tape.capacity = StorageUnits::mb(100);
    tape.used = StorageUnits::mb(10);
    tape.active_epoch = EpochNumber(1);
    tape.expiry_epoch = EpochNumber(10);
    tape.tracks = TrackStore::zeroed();
    tape
}

fn make_raw_track(
    tape: Address,
    key: Hash,
    track_number: u64,
    raw: &[u8],
) -> (CompressedTrack, TrackData) {
    let bytes = raw.to_vec();
    let track = CompressedTrack {
        tape: tape.into(),
        key,
        track_number: TrackNumber(track_number),
        kind: TrackKind::Raw as u64,
        state: TrackState::Certified as u64,
        size: StorageUnits::from_bytes(bytes.len() as u64),
        spool_group: SpoolGroup(0),
        value_hash: hash::hash(&bytes),
    };
    (track, TrackData::Raw(bytes))
}

#[tokio::test]
async fn get_tape_uses_rpc_account() {
    let fixture = setup();
    let mut rng = rand::thread_rng();
    let authority = Keypair::new(&mut rng);
    let tape_address = tape_pda(authority.pubkey().into()).0;

    let tape = make_tape(authority.pubkey().into());
    pipe(&fixture.rpc, tape_address, &tape.pack());

    let result = fixture.client.get_tape(&tape_address).await.unwrap();
    assert_eq!(result.capacity, StorageUnits::mb(100));
    assert_eq!(result.used, StorageUnits::mb(10));
    assert_eq!(result.active_epoch, EpochNumber(1));
    assert_eq!(result.expiry_epoch, EpochNumber(10));
}

#[tokio::test]
async fn track_queries_use_memory_peer_catalog() {
    let fixture = setup();
    let mut rng = rand::thread_rng();
    let tape_authority = Keypair::new(&mut rng);
    let tape_address: Address = tape_pda(tape_authority.pubkey().into()).0;

    let key_a = hash::hash(b"track-a");
    let key_b = hash::hash(b"track-b");

    let (track0, data0) = make_raw_track(tape_address, key_a, 0, b"v0");
    let (track1, data1) = make_raw_track(tape_address, key_b, 1, b"v1");
    let (track2, data2) = make_raw_track(tape_address, key_a, 2, b"v2");

    let address0 = fixture.insert_track(track0, data0);
    fixture.insert_track(track1, data1);
    fixture.insert_track(track2, data2);

    let fetched = fixture.client.get_track(&address0).await.unwrap();
    assert_eq!(fetched.track_number, TrackNumber(0));

    let by_number = fixture
        .client
        .get_track_by_number(&tape_address, TrackNumber(1))
        .await
        .unwrap();
    assert_eq!(by_number.key, key_b);

    let latest = fixture
        .client
        .find_track(&tape_address, key_a, FindTrackVersion::Latest)
        .await
        .unwrap();
    assert_eq!(latest.track_number, TrackNumber(2));

    let exact = fixture
        .client
        .find_track(&tape_address, key_a, FindTrackVersion::Number(TrackNumber(0)))
        .await
        .unwrap();
    assert_eq!(exact.track_number, TrackNumber(0));

    let (tracks, next_cursor) = fixture
        .client
        .list_tracks_by_tape(&tape_address, None, 16)
        .await
        .unwrap();
    assert_eq!(tracks.len(), 3);
    assert_eq!(tracks[0].track_number, TrackNumber(0));
    assert_eq!(tracks[1].track_number, TrackNumber(1));
    assert_eq!(tracks[2].track_number, TrackNumber(2));
    assert_eq!(next_cursor, None);
}

#[tokio::test]
async fn read_raw_track_uses_memory_peer_data() {
    let fixture = setup();
    let mut rng = rand::thread_rng();
    let tape_authority = Keypair::new(&mut rng);
    let tape_address: Address = tape_pda(tape_authority.pubkey().into()).0;
    let key = hash::hash(b"raw-track");
    let raw = b"hello raw track";

    let (track, data) = make_raw_track(tape_address, key, 0, raw);
    let address = fixture.insert_track(track, data);

    let read = fixture.client.read(&address).await.unwrap();
    assert_eq!(read, raw);
}

#[tokio::test]
async fn verify_raw_track_uses_value_hash() {
    let fixture = setup();
    let mut rng = rand::thread_rng();
    let tape_authority = Keypair::new(&mut rng);
    let tape_address: Address = tape_pda(tape_authority.pubkey().into()).0;
    let key = hash::hash(b"verify-raw-track");
    let raw = b"verified data";

    let (track, data) = make_raw_track(tape_address, key, 0, raw);
    let address = fixture.insert_track(track, data);

    assert!(fixture.client.verify(&address, raw).await.unwrap());
    assert!(!fixture.client.verify(&address, b"wrong").await.unwrap());
}
