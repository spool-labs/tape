use std::collections::{BTreeSet, HashMap};
use std::sync::{Arc, Mutex};

use arc_swap::ArcSwap;
use bytemuck::Zeroable;
use peer_manager::{PeerManager, PeerNode};
use peer_memory::MemoryApi;
use rpc_client::RpcClient;
use rpc_litesvm::LiteSvmRpc;
use tape_api::program::tapedrive::{self, tape_pda, track_pda};
use tape_api::state::{Group, Tape};
use tape_core::bls::BlsPubkey;
use tape_core::erasure::GROUP_SIZE;
use tape_core::spooler::GroupIndex;
use tape_core::system::{Member, NodePreferences, Spool};
use tape_core::track::data::BlobData;
use tape_core::track::archive::TrackArchive;
use tape_core::track::types::{
    CompressedTrack, CompressedTrackProof, TrackKind, TrackState,
};
use tape_core::types::coin::TAPE;
use tape_core::types::network::NetworkAddress;
use tape_core::types::tls::NetworkTlsPubkey;
use tape_core::types::{ContentType, EpochNumber, SlotNumber, StorageUnits, TrackNumber};
use tape_crypto::{hash, Hash};
use tape_crypto::address::Address;
use tape_crypto::ed25519::Keypair;
use tape_protocol::api::{
    ApiError, FindTrackVersion, FindTrackRes, GetTrackByNumberRes, GetTrackDataRes,
    GetTrackProofRes, GetTrackRes, ListObjectsRes, ListTracksByTapeRes, ObjectListItem, PeerReq,
    PeerRes,
};
use tape_protocol::ProtocolState;

use tape_sdk::object::ListObjectsQuery;
use tape_sdk::tapedrive::Tapedrive;

struct Fixture {
    rpc: LiteSvmRpc,
    client: Tapedrive<LiteSvmRpc, MemoryApi>,
    tracks: Arc<Mutex<HashMap<Address, CompressedTrack>>>,
    data: Arc<Mutex<HashMap<Address, BlobData>>>,
    objects: Arc<Mutex<HashMap<Address, Vec<ObjectListItem>>>>,
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
        PeerReq::ListObjects(_) => PeerRes::ListObjects(Err(unexpected_error())),
        PeerReq::GetTrackData(_) => PeerRes::GetTrackData(Err(unexpected_error())),
        PeerReq::GetTrackProof(_) => PeerRes::GetTrackProof(Err(unexpected_error())),
        PeerReq::SyncSlices(_) => PeerRes::SyncSlices(Err(unexpected_error())),
        PeerReq::SyncTracks(_) => PeerRes::SyncTracks(Err(unexpected_error())),
        PeerReq::Repair(_) => PeerRes::Repair(Err(unexpected_error())),
        PeerReq::Certify(_) => PeerRes::Certify(Err(unexpected_error())),
        PeerReq::Vote(_) => PeerRes::Vote(Err(unexpected_error())),
        PeerReq::Invalidate(_) => PeerRes::Invalidate(Err(unexpected_error())),
        PeerReq::GetHealth(_) => PeerRes::GetHealth(Err(unexpected_error())),
        PeerReq::GetStats(_) => PeerRes::GetStats(Err(unexpected_error())),
        PeerReq::PutSlice(_) => PeerRes::PutSlice(Err(unexpected_error())),
        PeerReq::GetSlice(_) => PeerRes::GetSlice(Err(unexpected_error())),
    }
}

impl Fixture {
    fn insert_track(&self, track: CompressedTrack, data: BlobData) -> Address {
        let address = track_pda(track.tape, track.track_number).0;
        self.tracks.lock().unwrap().insert(address, track);
        self.data.lock().unwrap().insert(address, data);
        address
    }

    fn insert_object(&self, bucket: Address, object: ObjectListItem) {
        self.objects
            .lock()
            .unwrap()
            .entry(bucket)
            .or_default()
            .push(object);
    }
}

fn setup() -> Fixture {
    let rpc = LiteSvmRpc::new();
    let mut rng = rand::thread_rng();
    let payer = Keypair::new(&mut rng);
    let rpc_client = Arc::new(RpcClient::from_rpc(rpc.clone()));

    let tracks = Arc::new(Mutex::new(HashMap::<Address, CompressedTrack>::new()));
    let data = Arc::new(Mutex::new(HashMap::<Address, BlobData>::new()));
    let objects = Arc::new(Mutex::new(HashMap::<Address, Vec<ObjectListItem>>::new()));
    let proofs = Arc::new(Mutex::new(HashMap::<Address, CompressedTrackProof>::new()));

    let api = Arc::new(MemoryApi::new({
        let tracks = tracks.clone();
        let data = data.clone();
        let objects = objects.clone();
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
            PeerReq::ListObjects(req) => {
                let mut matches = objects
                    .lock()
                    .unwrap()
                    .get(&req.bucket)
                    .cloned()
                    .unwrap_or_default()
                    .into_iter()
                    .filter(|object| object.name.starts_with(&req.prefix))
                    .collect::<Vec<_>>();
                matches.sort_by(|left, right| left.name.cmp(&right.name));

                let mut objects = Vec::new();
                let mut common_prefixes = BTreeSet::new();
                for object in matches {
                    if let Some(delimiter) = req.delimiter.as_ref() {
                        let suffix = &object.name[req.prefix.len()..];
                        if let Some(pos) = find_subslice(suffix, delimiter) {
                            common_prefixes.insert(
                                object.name[..req.prefix.len() + pos + delimiter.len()].to_vec(),
                            );
                            continue;
                        }
                    }
                    objects.push(object);
                }

                let start = req.cursor.as_ref().and_then(|cursor| {
                    objects
                        .iter()
                        .position(|object| object.name.as_slice() >= cursor.as_slice())
                }).unwrap_or(0);
                let limit = req.limit as usize;
                let page = objects
                    .iter()
                    .skip(start)
                    .take(limit)
                    .cloned()
                    .collect::<Vec<_>>();
                let next_cursor = objects
                    .get(start + page.len())
                    .map(|object| object.name.clone());
                let is_truncated = next_cursor.is_some();

                PeerRes::ListObjects(Ok(ListObjectsRes {
                    objects: page,
                    common_prefixes: common_prefixes.into_iter().collect(),
                    next_cursor,
                    is_truncated,
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
            PeerReq::Vote(_) => unexpected_peer_response(&req),
            PeerReq::Invalidate(_) => unexpected_peer_response(&req),
            PeerReq::GetHealth(_) => unexpected_peer_response(&req),
            PeerReq::GetStats(_) => unexpected_peer_response(&req),
            PeerReq::PutSlice(_) => unexpected_peer_response(&req),
            PeerReq::GetSlice(_) => unexpected_peer_response(&req),
        }
    }));

    let peer_manager = Arc::new(PeerManager::new());
    let node = address(1);
    peer_manager.add_peer(make_peer(node, 3001));

    let mut state = ProtocolState::default();
    state.current.epoch.id = EpochNumber(1);
    state
        .current
        .committee
        .push(Member::new(node, TAPE(1000)));
    let mut group = Group {
        id: GroupIndex(0),
        epoch: EpochNumber(1),
        size: StorageUnits::mb(1),
        ..Group::zeroed()
    };
    for position in 0..GROUP_SIZE {
        group.spools[position] = Spool::new(node, BlsPubkey::zeroed());
    }
    state.current.groups.push(group);

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
        objects,
    }
}

fn find_subslice(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    if needle.is_empty() {
        return None;
    }
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}

fn make_peer(node: Address, port: u16) -> PeerNode {
    PeerNode {
        node,
        bls_pubkey: BlsPubkey::zeroed(),
        tls_pubkey: NetworkTlsPubkey::new_unique(),
        network_address: NetworkAddress::new_ipv4([127, 0, 0, 1], port),
        preferences: NodePreferences::zeroed(),
        stake: TAPE(0),
        name: [0u8; 32],
    }
}

fn address(byte: u8) -> Address {
    let mut bytes = [0u8; 32];
    bytes[0] = byte;
    Address::new(bytes)
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
    tape.tracks = TrackArchive::zeroed();
    tape
}

fn make_raw_track(
    tape: Address,
    key: Hash,
    track_number: u64,
    raw: &[u8],
) -> (CompressedTrack, BlobData) {
    let bytes = raw.to_vec();
    let track = CompressedTrack {
        tape: tape.into(),
        key,
        track_number: TrackNumber(track_number),
        kind: TrackKind::Inline as u64,
        state: TrackState::Certified as u64,
        size: StorageUnits::from_bytes(bytes.len() as u64),
        group: GroupIndex(0),
        value_hash: hash::hash(&bytes),
    };
    (track, BlobData::Inline(bytes))
}

fn object_item(name: &[u8], track_number: TrackNumber) -> ObjectListItem {
    ObjectListItem {
        name: name.to_vec(),
        size: StorageUnits::from_bytes(10),
        etag: hash::hash(name),
        block_time: Some(1_700_000_000),
        slot: SlotNumber(track_number.0),
        data_tape: Address::new([0x44; 32]),
        track_number,
        kind: TrackKind::Inline as u64,
        content_type: ContentType::TextPlain,
    }
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
async fn object_list_uses_peer_index() {
    let fixture = setup();
    let bucket = Address::new_unique();

    fixture.insert_object(bucket, object_item(b"photos/a.txt", TrackNumber(0)));
    fixture.insert_object(bucket, object_item(b"photos/b.txt", TrackNumber(1)));
    fixture.insert_object(bucket, object_item(b"photos/nested/c.txt", TrackNumber(2)));
    fixture.insert_object(bucket, object_item(b"docs/c.txt", TrackNumber(2)));

    let page = fixture
        .client
        .list_objects(
            &bucket,
            ListObjectsQuery::new("photos/").with_delimiter("/"),
        )
        .await
        .unwrap();

    assert_eq!(
        page.objects
            .iter()
            .map(|object| object.name.as_slice())
            .collect::<Vec<_>>(),
        vec![b"photos/a.txt".as_slice(), b"photos/b.txt".as_slice()]
    );
    assert_eq!(page.common_prefixes, vec![b"photos/nested/".to_vec()]);
    assert_eq!(page.objects[0].size, StorageUnits::from_bytes(10));
    assert_eq!(page.objects[0].content_type, ContentType::TextPlain);
    assert_eq!(page.objects[0].block_time, Some(1_700_000_000));

    let head = fixture
        .client
        .head_object(&bucket, "photos/a.txt")
        .await
        .unwrap();
    assert_eq!(head.size, 10);
    assert_eq!(head.content_type, ContentType::TextPlain);
    assert_eq!(head.block_time, Some(1_700_000_000));
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
