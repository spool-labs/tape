//! Request/response types for peer operations.

use tape_core::bls::BlsSignature;
use tape_core::prelude::{CompressedTrack, EpochNumber, NodeId, SpoolGroup, SpoolIndex, TrackData, TrackNumber};
use tape_core::track::types::CompressedTrackProof;
use tape_crypto::prelude::{Address, Hash};
use crate::api::types::{
    InconsistencyProof, NodeStats, SlicePayload, StripeSubChunkRequest, SyncSliceEntry,
    SyncTrackEntry,
};
use wincode_derive::{SchemaRead, SchemaWrite};

use crate::api::ApiError;

#[derive(Clone, Debug)]
pub struct PutSliceReq {
    pub track: Address,
    pub spool: SpoolIndex,
    pub payload: SlicePayload,
}

#[derive(Clone, Debug)]
pub struct PutSliceRes;

#[derive(Clone, Debug)]
pub struct GetSliceReq {
    pub track: Address,
    pub spool: SpoolIndex,
}

#[derive(Clone, Debug)]
pub struct GetSliceRes {
    pub data: Vec<u8>,
}

#[derive(Clone, Debug)]
pub struct GetTrackReq {
    pub track: Address,
}

#[derive(Clone, Debug)]
pub struct GetTrackRes {
    pub track: CompressedTrack,
}

#[derive(Clone, Debug)]
pub struct GetTrackByNumberReq {
    pub tape: Address,
    pub track_number: TrackNumber,
}

#[derive(Clone, Debug)]
pub struct GetTrackByNumberRes {
    pub track: CompressedTrack,
}

#[derive(Clone, Debug, PartialEq, Eq, SchemaRead, SchemaWrite)]
pub enum FindTrackVersion {
    Latest,
    Number(TrackNumber),
}

#[derive(Clone, Debug)]
pub struct FindTrackReq {
    pub tape: Address,
    pub key: Hash,
    pub version: FindTrackVersion,
}

#[derive(Clone, Debug)]
pub struct FindTrackRes {
    pub track: CompressedTrack,
}

#[derive(Clone, Debug)]
pub struct ListTracksByTapeReq {
    pub tape: Address,
    pub cursor: Option<TrackNumber>,
    pub limit: u32,
}

#[derive(Clone, Debug)]
pub struct ListTracksByTapeRes {
    pub tracks: Vec<CompressedTrack>,
    pub next_cursor: Option<TrackNumber>,
}

#[derive(Clone, Debug)]
pub struct GetTrackDataReq {
    pub track: Address,
}

#[derive(Clone, Debug)]
pub struct GetTrackDataRes {
    pub data: TrackData,
}

#[derive(Clone, Debug)]
pub struct GetTrackProofReq {
    pub track: Address,
}

#[derive(Clone, Debug)]
pub struct GetTrackProofRes {
    pub proof: CompressedTrackProof,
}

#[derive(Clone, Debug)]
pub struct SyncSlicesReq {
    pub spool_index: SpoolIndex,
    pub cursor: Option<[u8; 32]>,
    pub limit: u32,
}

#[derive(Clone, Debug)]
pub struct SyncSlicesRes {
    pub entries: Vec<SyncSliceEntry>,
    pub next_cursor: Option<[u8; 32]>,
}

#[derive(Clone, Debug)]
pub struct SyncTracksReq {
    pub spool_index: SpoolIndex,
    pub cursor: Option<[u8; 32]>,
    pub limit: u32,
}

#[derive(Clone, Debug)]
pub struct SyncTracksRes {
    pub entries: Vec<SyncTrackEntry>,
    pub next_cursor: Option<[u8; 32]>,
}

#[derive(Clone, Debug)]
pub struct RepairReq {
    pub track: Address,
    pub helper_spool: SpoolIndex,
    pub stripes: Vec<StripeSubChunkRequest>,
}

#[derive(Clone, Debug)]
pub struct RepairRes {
    pub data: Vec<u8>,
}

#[derive(Clone, Debug)]
pub struct CertifyReq {
    pub track: Address,
}

#[derive(Clone, Debug)]
pub struct CertifyRes {
    pub signature: BlsSignature,
    pub node_id: NodeId,
    pub epoch: EpochNumber,
}

#[derive(Clone, Debug)]
pub struct SignSnapshotReq {
    pub epoch: EpochNumber,
    pub group: SpoolGroup,
    pub commitment: Hash,
}

#[derive(Clone, Debug)]
pub struct SignSnapshotRes {
    pub signature: BlsSignature,
    pub node_id: NodeId,
    pub epoch: EpochNumber,
}

#[derive(Clone, Debug)]
pub struct InvalidateReq {
    pub track: Address,
    pub proof: InconsistencyProof,
}

#[derive(Clone, Debug)]
pub struct InvalidateRes {
    pub signature: BlsSignature,
    pub node_id: NodeId,
    pub epoch: EpochNumber,
}

#[derive(Clone, Debug)]
pub struct GetHealthReq;

#[derive(Clone, Debug)]
pub struct GetHealthRes {
    pub ok: bool,
}

#[derive(Clone, Debug)]
pub struct GetStatsReq;

#[derive(Clone, Debug)]
pub struct GetStatsRes {
    pub stats: NodeStats,
}


pub enum PeerReq {
    PutSlice(PutSliceReq),
    GetSlice(GetSliceReq),
    GetTrack(GetTrackReq),
    GetTrackByNumber(GetTrackByNumberReq),
    FindTrack(FindTrackReq),
    ListTracksByTape(ListTracksByTapeReq),
    GetTrackData(GetTrackDataReq),
    GetTrackProof(GetTrackProofReq),
    SyncSlices(SyncSlicesReq),
    SyncTracks(SyncTracksReq),
    Repair(RepairReq),
    Certify(CertifyReq),
    SignSnapshot(SignSnapshotReq),
    Invalidate(InvalidateReq),
    GetHealth(GetHealthReq),
    GetStats(GetStatsReq),
}

pub enum PeerRes {
    PutSlice(Result<PutSliceRes, ApiError>),
    GetSlice(Result<GetSliceRes, ApiError>),
    GetTrack(Result<GetTrackRes, ApiError>),
    GetTrackByNumber(Result<GetTrackByNumberRes, ApiError>),
    FindTrack(Result<FindTrackRes, ApiError>),
    ListTracksByTape(Result<ListTracksByTapeRes, ApiError>),
    GetTrackData(Result<GetTrackDataRes, ApiError>),
    GetTrackProof(Result<GetTrackProofRes, ApiError>),
    SyncSlices(Result<SyncSlicesRes, ApiError>),
    SyncTracks(Result<SyncTracksRes, ApiError>),
    Repair(Result<RepairRes, ApiError>),
    Certify(Result<CertifyRes, ApiError>),
    SignSnapshot(Result<SignSnapshotRes, ApiError>),
    Invalidate(Result<InvalidateRes, ApiError>),
    GetHealth(Result<GetHealthRes, ApiError>),
    GetStats(Result<GetStatsRes, ApiError>),
}
