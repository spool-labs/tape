//! Core peer trait and types for node-to-node communication.
//!
//! This crate defines the `Peer` trait which abstracts over different peer implementations:
//! - `peer-http` — Production client (reqwest-based)
//! - `peer-memory` — Test mock (callback-based)
//!
//! All methods use the uniform `(NodeId, &Req) -> Result<Res, PeerError>` convention.

mod error;
mod peers;
mod types;

pub use error::PeerError;
pub use peers::TrustedPeers;
pub use types::*;

pub use async_trait::async_trait;

use tape_core::types::NodeId;

#[async_trait]
pub trait Peer: Send + Sync {
    fn peers(&self) -> &TrustedPeers;

    async fn put_slice(&self, node: NodeId, req: &PutSliceReq) -> Result<PutSliceRes, PeerError>;
    async fn get_slice(&self, node: NodeId, req: &GetSliceReq) -> Result<GetSliceRes, PeerError>;
    async fn get_metadata(&self, node: NodeId, req: &GetMetadataReq) -> Result<GetMetadataRes, PeerError>;
    async fn sync(&self, node: NodeId, req: &SyncReq) -> Result<SyncRes, PeerError>;
    async fn repair(&self, node: NodeId, req: &RepairReq) -> Result<RepairRes, PeerError>;
    async fn certify(&self, node: NodeId, req: &CertifyReq) -> Result<CertifyRes, PeerError>;
    async fn invalidate(&self, node: NodeId, req: &InvalidateReq) -> Result<InvalidateRes, PeerError>;
    async fn put_snapshot(&self, node: NodeId, req: &PutSnapshotReq) -> Result<PutSnapshotRes, PeerError>;
    async fn get_snapshot(&self, node: NodeId, req: &GetSnapshotReq) -> Result<GetSnapshotRes, PeerError>;
    async fn get_health(&self, node: NodeId, req: &GetHealthReq) -> Result<GetHealthRes, PeerError>;
    async fn get_stats(&self, node: NodeId, req: &GetStatsReq) -> Result<GetStatsRes, PeerError>;
}
