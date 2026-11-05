//! These are the test nodes and fake peers that the spool tests use, so that
//! each test can set up a node that is missing a slice and choose how its
//! peers behave.
//!
//! ```text
//! a node at the tip that is missing its slice of one coded track
//! fake peers that serve the slice, don't have it, or are down
//! ```

use std::sync::Arc;

use peer_memory::MemoryApi;
use tape_api::program::tapedrive::TrackAddress;
use tape_core::encoding::TAPENET_PROFILE;
use tape_core::erasure::leaf_position;
use tape_core::spooler::{GroupFold, GroupIndex, VirtualGroup};
use tape_core::types::{EpochNumber, SpoolIndex, TrackNumber};
use tape_crypto::address::Address;
use tape_protocol::api::{
    ApiError, PeerReq, PeerRes, RepairReq, RepairRes, SyncSliceEntry,
    SyncSlicesReq, SyncSlicesRes,
};
use tape_store::ops::{SliceOps, SpoolOps};

use crate::core::ingest::IngestState;
use crate::features::challenge::fixture::{
    coded, payload, put_tape, write_coded, Coded,
};
use crate::features::http::handlers::track::repair::extract_repair_data;
use crate::harness::{NodeHarness, TestContext};

/// The tape the fixture's tracks go on
const SEED: u8 = 7;

/// Bytes in a fixture track
const TRACK_BYTES: usize = 40_000;

/// What the fake peers do with what they are asked
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Peers {
    Serve,
    Lack,
    Down,
}

/// A harness node with one live spool, and the track it lacks a slice of
pub struct Lacking {
    pub harness: NodeHarness,
    pub ctx: TestContext,
    pub spool: SpoolIndex,
    pub fold: GroupFold,
    pub track: Coded,
    pub address: Address,
}

impl Lacking {
    /// The stored spool our slice of the track goes under
    pub fn stored(&self) -> SpoolIndex {
        let position = leaf_position(self.spool).as_usize();
        self.track.track.group.spool_at(position)
    }

    /// Whether the node holds its slice of the track now
    pub fn has_slice(&self) -> bool {
        let held = self.ctx.store.has_slice(self.stored(), self.address);
        held.expect("has slice")
    }
}

/// A harness of 20 nodes whose peers act as `peers` on `track`'s slices
pub async fn lacking(peers: Peers) -> Lacking {
    let data = payload(SEED, 0, TRACK_BYTES);
    let track = coded(TAPENET_PROFILE, SEED, 0, VirtualGroup(0), &data);
    let shared = Arc::new(track.clone());
    let api = fake_peers(shared, peers);
    let width = usize::from(TAPENET_PROFILE.n());
    let harness = NodeHarness::builder()
        .nodes(width)
        .api(api)
        .build()
        .await
        .expect("build harness");
    let ctx = harness.ctx_for(0);
    let state = ctx.state();
    let spool = state.member_live_spools(ctx.node_address())[0];
    let fold = state.fold().expect("a live group");

    put_tape(&ctx.store, SEED, EpochNumber(1_000), TrackNumber(0));
    write_coded(&ctx.store, &track, EpochNumber(1));
    let address = track.track.address();
    let lacking = Lacking { harness, ctx, spool, fold, track, address };
    let stored = lacking.stored();
    lacking.ctx.store.delete_slice(stored, address).expect("drop our slice");
    lacking
}

/// Put the node at the tip, where its turns run
pub fn at_tip(ctx: &TestContext) {
    ctx.ingest.publish(IngestState::AtTip);
}

/// Record a holder of our position for the epoch before now, with nobody
/// at the group's other positions
pub fn hold_before(lacking: &Lacking, holder: Address) {
    let group = GroupIndex::containing(lacking.spool);
    let ours = leaf_position(lacking.spool).as_usize();
    let mut owners = Vec::new();
    for position in 0..usize::from(TAPENET_PROFILE.n()) {
        let owner = if position == ours { holder } else { Address::default() };
        owners.push((group.spool_at(position), owner));
    }
    let before = lacking.ctx.state().epoch().saturating_sub(EpochNumber(1));
    lacking.ctx.store.put_owners(before, &owners).expect("record owners");
}

fn fake_peers(track: Arc<Coded>, peers: Peers) -> MemoryApi {
    let handler = move |_: Address, req: PeerReq| answer(&track, peers, req);
    MemoryApi::new(handler)
}

fn answer(track: &Coded, peers: Peers, req: PeerReq) -> PeerRes {
    match req {
        PeerReq::SyncSlices(req) => {
            PeerRes::SyncSlices(slices(track, peers, &req))
        }
        PeerReq::Repair(req) => PeerRes::Repair(repair(track, peers, &req)),
        PeerReq::PutSlice(_)
        | PeerReq::GetSlice(_)
        | PeerReq::GetTrack(_)
        | PeerReq::GetTrackByNumber(_)
        | PeerReq::FindTrack(_)
        | PeerReq::ListTracksByTape(_)
        | PeerReq::ListObjects(_)
        | PeerReq::GetTrackData(_)
        | PeerReq::GetTrackProof(_)
        | PeerReq::SyncTrackData(_)
        | PeerReq::Certify(_)
        | PeerReq::Invalidate(_)
        | PeerReq::Vote(_)
        | PeerReq::Challenge(_)
        | PeerReq::GetHealth(_) => PeerRes::GetHealth(Err(not_served())),
    }
}

fn slices(
    track: &Coded,
    peers: Peers,
    req: &SyncSlicesReq,
) -> Result<SyncSlicesRes, ApiError> {
    let address = track.track.address();
    let position = leaf_position(req.spool_index).as_usize();
    let (entries, missing) = (Vec::new(), Vec::new());
    let mut response = SyncSlicesRes { entries, missing };
    match peers {
        Peers::Down => return Err(ApiError::ConnectionFailed("down".into())),
        Peers::Lack => response.missing.push(address),
        Peers::Serve => {
            let slice_data = track.slices[position].clone();
            let track_address = address.to_bytes();
            response.entries.push(SyncSliceEntry { track_address, slice_data });
        }
    }
    Ok(response)
}

fn repair(
    track: &Coded,
    peers: Peers,
    req: &RepairReq,
) -> Result<RepairRes, ApiError> {
    match peers {
        Peers::Down => return Err(ApiError::ConnectionFailed("down".into())),
        Peers::Lack => return Err(ApiError::NotFound),
        Peers::Serve => {}
    }
    let position = leaf_position(req.helper_spool).as_usize();
    let helper = &track.slices[position];
    let cut = extract_repair_data(TAPENET_PROFILE, &req.stripes, helper);
    let data = cut.map_err(ApiError::Other)?;
    Ok(RepairRes { data })
}

fn not_served() -> ApiError {
    ApiError::Other("not served".into())
}
