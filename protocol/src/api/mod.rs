//! Protocol API: routes, wire types, ops, and the `Api` trait.

pub mod error;
pub mod ops;
pub mod routes;
pub mod types;

pub use error::ApiError;
pub use ops::*;
pub use routes::*;
pub use types::*;

/// Merkle tree height for blob encoding.
/// 2^5 = 32 leaves (20 used = SPOOL_GROUP_SIZE).
pub const MERKLE_HEIGHT: usize = tape_core::erasure::COMMITMENT_TREE_HEIGHT;

/// Content type for binary request/response bodies.
pub const BINARY_CONTENT: &str = "application/octet-stream";

/// Content type for JSON responses.
pub const CONTENT_TYPE_JSON: &str = "application/json";

use async_trait::async_trait;
use tape_core::types::NodeId;

#[async_trait]
pub trait Api: Send + Sync {
    async fn put_slice(&self, node: NodeId, req: &PutSliceReq) -> Result<PutSliceRes, ApiError>;
    async fn get_slice(&self, node: NodeId, req: &GetSliceReq) -> Result<GetSliceRes, ApiError>;
    async fn get_metadata(&self, node: NodeId, req: &GetMetadataReq) -> Result<GetMetadataRes, ApiError>;
    async fn sync(&self, node: NodeId, req: &SyncReq) -> Result<SyncRes, ApiError>;
    async fn repair(&self, node: NodeId, req: &RepairReq) -> Result<RepairRes, ApiError>;
    async fn certify(&self, node: NodeId, req: &CertifyReq) -> Result<CertifyRes, ApiError>;
    async fn invalidate(&self, node: NodeId, req: &InvalidateReq) -> Result<InvalidateRes, ApiError>;
    async fn put_snapshot(&self, node: NodeId, req: &PutSnapshotReq) -> Result<PutSnapshotRes, ApiError>;
    async fn get_snapshot(&self, node: NodeId, req: &GetSnapshotReq) -> Result<GetSnapshotRes, ApiError>;
    async fn get_health(&self, node: NodeId, req: &GetHealthReq) -> Result<GetHealthRes, ApiError>;
    async fn get_stats(&self, node: NodeId, req: &GetStatsReq) -> Result<GetStatsRes, ApiError>;
}
