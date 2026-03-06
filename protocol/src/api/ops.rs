//! Request/response types for peer operations.

use tape_core::bls::BlsSignature;
use tape_core::spooler::SpoolIndex;
use tape_core::types::{EpochNumber, NodeId};
use tape_crypto::Hash;
use crate::api::types::{ InconsistencyProof, NodeStats, SlicePayload, SnapshotSignatureSubmission, SyncSpoolEntry, };
use tape_crypto::Pubkey;

use crate::api::ApiError;

#[derive(Clone, Debug)]
pub struct PutSliceReq {
    pub track: Pubkey,
    pub spool: SpoolIndex,
    pub payload: SlicePayload,
}

#[derive(Clone, Debug)]
pub struct PutSliceRes;

#[derive(Clone, Debug)]
pub struct GetSliceReq {
    pub track: Pubkey,
    pub spool: SpoolIndex,
}

#[derive(Clone, Debug)]
pub struct GetSliceRes {
    pub data: Vec<u8>,
}

#[derive(Clone, Debug)]
pub struct GetMetadataReq {
    pub track: Pubkey,
}

#[derive(Clone, Debug)]
pub struct GetMetadataRes {
    pub data: Vec<u8>,
}

#[derive(Clone, Debug)]
pub struct SyncReq {
    pub spool_index: u16,
    pub cursor: Option<[u8; 32]>,
    pub limit: u32,
}

#[derive(Clone, Debug)]
pub struct SyncRes {
    pub entries: Vec<SyncSpoolEntry>,
    pub next_cursor: Option<[u8; 32]>,
}

#[derive(Clone, Debug)]
pub struct RepairReq {
    pub track: Pubkey,
    pub helper_spool: SpoolIndex,
    pub stripes: Vec<crate::api::types::StripeSubChunkRequest>,
}

#[derive(Clone, Debug)]
pub struct RepairRes {
    pub data: Vec<u8>,
}

#[derive(Clone, Debug)]
pub struct CertifyReq {
    pub track: Pubkey,
}

#[derive(Clone, Debug)]
pub struct CertifyRes {
    pub signature: BlsSignature,
    pub node_id: NodeId,
    pub epoch: EpochNumber,
}

#[derive(Clone, Debug)]
pub struct InvalidateReq {
    pub track: Pubkey,
    pub proof: InconsistencyProof,
}

#[derive(Clone, Debug)]
pub struct InvalidateRes {
    pub signature: BlsSignature,
    pub node_id: NodeId,
    pub epoch: EpochNumber,
}

#[derive(Clone, Debug)]
pub struct PutSnapshotReq {
    pub epoch: EpochNumber,
    pub chunk_index: u64,
    pub submission: SnapshotSignatureSubmission,
}

#[derive(Clone, Debug)]
pub struct PutSnapshotRes;

#[derive(Clone, Debug)]
pub struct GetSnapshotReq {
    pub epoch: EpochNumber,
}

#[derive(Clone, Debug)]
pub struct GetSnapshotRes {
    pub commitments: Vec<Hash>,
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
    GetMetadata(GetMetadataReq),
    Sync(SyncReq),
    Repair(RepairReq),
    Certify(CertifyReq),
    Invalidate(InvalidateReq),
    PutSnapshot(PutSnapshotReq),
    GetSnapshot(GetSnapshotReq),
    GetHealth(GetHealthReq),
    GetStats(GetStatsReq),
}

pub enum PeerRes {
    PutSlice(Result<PutSliceRes, ApiError>),
    GetSlice(Result<GetSliceRes, ApiError>),
    GetMetadata(Result<GetMetadataRes, ApiError>),
    Sync(Result<SyncRes, ApiError>),
    Repair(Result<RepairRes, ApiError>),
    Certify(Result<CertifyRes, ApiError>),
    Invalidate(Result<InvalidateRes, ApiError>),
    PutSnapshot(Result<PutSnapshotRes, ApiError>),
    GetSnapshot(Result<GetSnapshotRes, ApiError>),
    GetHealth(Result<GetHealthRes, ApiError>),
    GetStats(Result<GetStatsRes, ApiError>),
}
