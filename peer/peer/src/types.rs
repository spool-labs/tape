//! Request/response types for peer operations.

use tape_core::bls::{BlsPubkey, BlsSignature};
use tape_core::spooler::SpoolIndex;
use tape_core::types::network::NetworkAddress;
use tape_core::types::{EpochNumber, NodeId};
use tape_crypto::Hash;
use tape_node_api::{
    InconsistencyProof, NodeStats, SlicePayload, SnapshotSignatureSubmission,
    SyncSpoolEntry,
};
use tape_crypto::Pubkey;

use crate::PeerError;

// ---------------------------------------------------------------------------
// PeerNode — identity record for a known peer
// ---------------------------------------------------------------------------

#[derive(Clone, Debug)]
pub struct PeerNode {
    pub node_id: NodeId,
    pub authority: Pubkey,
    pub state_address: Pubkey,
    pub bls_pubkey: BlsPubkey,
    pub tls_pubkey: Pubkey,
    pub network_address: NetworkAddress,
}

// ---------------------------------------------------------------------------
// Per-method Req/Res types
// ---------------------------------------------------------------------------

pub struct PutSliceReq {
    pub track: Pubkey,
    pub spool: SpoolIndex,
    pub payload: SlicePayload,
}

pub struct PutSliceRes;

pub struct GetSliceReq {
    pub track: Pubkey,
    pub spool: SpoolIndex,
}

pub struct GetSliceRes {
    pub data: Vec<u8>,
}

pub struct GetMetadataReq {
    pub track: Pubkey,
}

pub struct GetMetadataRes {
    pub data: Vec<u8>,
}

pub struct SyncReq {
    pub spool_index: u16,
    pub cursor: Option<[u8; 32]>,
    pub limit: u32,
}

pub struct SyncRes {
    pub entries: Vec<SyncSpoolEntry>,
    pub next_cursor: Option<[u8; 32]>,
}

pub struct RepairReq {
    pub track: Pubkey,
    pub helper_spool: SpoolIndex,
    pub stripes: Vec<tape_node_api::StripeSubChunkRequest>,
}

pub struct RepairRes {
    pub data: Vec<u8>,
}

pub struct CertifyReq {
    pub track: Pubkey,
}

pub struct CertifyRes {
    pub signature: BlsSignature,
    pub node_id: NodeId,
    pub epoch: EpochNumber,
}

pub struct InvalidateReq {
    pub track: Pubkey,
    pub proof: InconsistencyProof,
}

pub struct InvalidateRes {
    pub signature: BlsSignature,
    pub node_id: NodeId,
    pub epoch: EpochNumber,
}

pub struct PutSnapshotReq {
    pub epoch: EpochNumber,
    pub chunk_index: u64,
    pub submission: SnapshotSignatureSubmission,
}

pub struct PutSnapshotRes;

pub struct GetSnapshotReq {
    pub epoch: EpochNumber,
}

pub struct GetSnapshotRes {
    pub commitments: Vec<Hash>,
}

pub struct GetHealthReq;

pub struct GetHealthRes {
    pub ok: bool,
}

pub struct GetStatsReq;

pub struct GetStatsRes {
    pub stats: NodeStats,
}

// ---------------------------------------------------------------------------
// Aggregate enums for MemoryPeerClient callback
// ---------------------------------------------------------------------------

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
    PutSlice(Result<PutSliceRes, PeerError>),
    GetSlice(Result<GetSliceRes, PeerError>),
    GetMetadata(Result<GetMetadataRes, PeerError>),
    Sync(Result<SyncRes, PeerError>),
    Repair(Result<RepairRes, PeerError>),
    Certify(Result<CertifyRes, PeerError>),
    Invalidate(Result<InvalidateRes, PeerError>),
    PutSnapshot(Result<PutSnapshotRes, PeerError>),
    GetSnapshot(Result<GetSnapshotRes, PeerError>),
    GetHealth(Result<GetHealthRes, PeerError>),
    GetStats(Result<GetStatsRes, PeerError>),
}
