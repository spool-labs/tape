//! Protocol API: routes, wire types, ops, and the `Api` trait.

pub mod error;
pub mod ops;
pub mod routes;
pub mod types;

pub use error::ApiError;
pub use ops::*;
pub use routes::*;
pub use types::*;

use async_trait::async_trait;
use tape_crypto::Address;

/// Content type for binary request/response bodies.
pub const BINARY_CONTENT: &str = "application/octet-stream";

/// Content type for JSON responses.
pub const JSON_CONTENT: &str = "application/json";


#[async_trait]
pub trait Api: Send + Sync {
    async fn put_slice(&self, node: Address, req: &PutSliceReq) -> Result<PutSliceRes, ApiError>;
    async fn get_slice(&self, node: Address, req: &GetSliceReq) -> Result<GetSliceRes, ApiError>;
    async fn get_track(&self, node: Address, req: &GetTrackReq) -> Result<GetTrackRes, ApiError>;
    async fn get_track_by_number(&self, node: Address, req: &GetTrackByNumberReq) -> Result<GetTrackByNumberRes, ApiError>;
    async fn find_track(&self, node: Address, req: &FindTrackReq) -> Result<FindTrackRes, ApiError>;
    async fn list_tracks_by_tape(&self, node: Address, req: &ListTracksByTapeReq) -> Result<ListTracksByTapeRes, ApiError>;
    async fn list_objects(&self, node: Address, req: &ListObjectsReq) -> Result<ListObjectsRes, ApiError>;
    async fn get_track_data(&self, node: Address, req: &GetTrackDataReq) -> Result<GetTrackDataRes, ApiError>;
    async fn get_track_proof(&self, node: Address, req: &GetTrackProofReq) -> Result<GetTrackProofRes, ApiError>;
    async fn sync_slices(&self, node: Address, req: &SyncSlicesReq) -> Result<SyncSlicesRes, ApiError>;
    async fn sync_tracks(&self, node: Address, req: &SyncTracksReq) -> Result<SyncTracksRes, ApiError>;
    async fn repair(&self, node: Address, req: &RepairReq) -> Result<RepairRes, ApiError>;
    async fn certify(&self, node: Address, req: &CertifyReq) -> Result<CertifyRes, ApiError>;
    async fn invalidate(&self, node: Address, req: &InvalidateReq) -> Result<InvalidateRes, ApiError>;
    async fn vote(&self, node: Address, req: &VoteReq) -> Result<VoteRes, ApiError>;
    async fn get_health(&self, node: Address, req: &GetHealthReq) -> Result<GetHealthRes, ApiError>;
    async fn get_stats(&self, node: Address, req: &GetStatsReq) -> Result<GetStatsRes, ApiError>;
}
