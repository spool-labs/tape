//! In-memory mock implementation of the `Peer` trait for testing.

use std::sync::Arc;

use async_trait::async_trait;
use tape_peer::{
    CertifyReq, CertifyRes, GetHealthReq, GetHealthRes, GetMetadataReq, GetMetadataRes,
    GetSliceReq, GetSliceRes, GetSnapshotReq, GetSnapshotRes, GetStatsReq, GetStatsRes,
    InvalidateReq, InvalidateRes, Peer, PeerError, PeerReq, PeerRes, PutSliceReq,
    PutSliceRes, PutSnapshotReq, PutSnapshotRes, RepairReq, RepairRes, SyncReq, SyncRes,
    TrustedPeers,
};
use tape_core::types::NodeId;

pub struct MemoryPeerClient {
    peers: TrustedPeers,
    handler: Arc<dyn Fn(NodeId, PeerReq) -> PeerRes + Send + Sync>,
}

impl MemoryPeerClient {
    pub fn new(handler: impl Fn(NodeId, PeerReq) -> PeerRes + Send + Sync + 'static) -> Self {
        Self {
            peers: TrustedPeers::new(),
            handler: Arc::new(handler),
        }
    }

    /// Creates a client where every call returns `PeerError::Other("not implemented")`.
    pub fn noop() -> Self {
        Self::new(|_, req| match req {
            PeerReq::PutSlice(_) => PeerRes::PutSlice(Err(not_impl())),
            PeerReq::GetSlice(_) => PeerRes::GetSlice(Err(not_impl())),
            PeerReq::GetMetadata(_) => PeerRes::GetMetadata(Err(not_impl())),
            PeerReq::Sync(_) => PeerRes::Sync(Err(not_impl())),
            PeerReq::Repair(_) => PeerRes::Repair(Err(not_impl())),
            PeerReq::Certify(_) => PeerRes::Certify(Err(not_impl())),
            PeerReq::Invalidate(_) => PeerRes::Invalidate(Err(not_impl())),
            PeerReq::PutSnapshot(_) => PeerRes::PutSnapshot(Err(not_impl())),
            PeerReq::GetSnapshot(_) => PeerRes::GetSnapshot(Err(not_impl())),
            PeerReq::GetHealth(_) => PeerRes::GetHealth(Err(not_impl())),
            PeerReq::GetStats(_) => PeerRes::GetStats(Err(not_impl())),
        })
    }
}

fn not_impl() -> PeerError {
    PeerError::Other("not implemented".into())
}

macro_rules! dispatch {
    ($self:ident, $node:ident, $req:expr, $variant:ident) => {{
        let res = ($self.handler)($node, PeerReq::$variant($req));
        match res {
            PeerRes::$variant(r) => r,
            _ => Err(PeerError::Other("handler returned wrong variant".into())),
        }
    }};
}

#[async_trait]
impl Peer for MemoryPeerClient {
    fn peers(&self) -> &TrustedPeers {
        &self.peers
    }

    async fn put_slice(&self, node: NodeId, req: &PutSliceReq) -> Result<PutSliceRes, PeerError> {
        dispatch!(self, node, PutSliceReq { track: req.track, spool: req.spool, payload: req.payload.clone() }, PutSlice)
    }

    async fn get_slice(&self, node: NodeId, req: &GetSliceReq) -> Result<GetSliceRes, PeerError> {
        dispatch!(self, node, GetSliceReq { track: req.track, spool: req.spool }, GetSlice)
    }

    async fn get_metadata(&self, node: NodeId, req: &GetMetadataReq) -> Result<GetMetadataRes, PeerError> {
        dispatch!(self, node, GetMetadataReq { track: req.track }, GetMetadata)
    }

    async fn sync(&self, node: NodeId, req: &SyncReq) -> Result<SyncRes, PeerError> {
        dispatch!(self, node, SyncReq { spool_index: req.spool_index, cursor: req.cursor, limit: req.limit }, Sync)
    }

    async fn repair(&self, node: NodeId, req: &RepairReq) -> Result<RepairRes, PeerError> {
        dispatch!(self, node, RepairReq { track: req.track, helper_spool: req.helper_spool, stripes: req.stripes.clone() }, Repair)
    }

    async fn certify(&self, node: NodeId, req: &CertifyReq) -> Result<CertifyRes, PeerError> {
        dispatch!(self, node, CertifyReq { track: req.track }, Certify)
    }

    async fn invalidate(&self, node: NodeId, req: &InvalidateReq) -> Result<InvalidateRes, PeerError> {
        dispatch!(self, node, InvalidateReq { track: req.track, proof: req.proof.clone() }, Invalidate)
    }

    async fn put_snapshot(&self, node: NodeId, req: &PutSnapshotReq) -> Result<PutSnapshotRes, PeerError> {
        dispatch!(self, node, PutSnapshotReq { epoch: req.epoch, chunk_index: req.chunk_index, submission: req.submission.clone() }, PutSnapshot)
    }

    async fn get_snapshot(&self, node: NodeId, req: &GetSnapshotReq) -> Result<GetSnapshotRes, PeerError> {
        dispatch!(self, node, GetSnapshotReq { epoch: req.epoch }, GetSnapshot)
    }

    async fn get_health(&self, node: NodeId, _req: &GetHealthReq) -> Result<GetHealthRes, PeerError> {
        dispatch!(self, node, GetHealthReq, GetHealth)
    }

    async fn get_stats(&self, node: NodeId, _req: &GetStatsReq) -> Result<GetStatsRes, PeerError> {
        dispatch!(self, node, GetStatsReq, GetStats)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn noop_returns_error() {
        let client = MemoryPeerClient::noop();
        let res = client.get_health(NodeId(1), &GetHealthReq).await;
        assert!(res.is_err());
    }

    #[tokio::test]
    async fn custom_handler() {
        let client = MemoryPeerClient::new(|node, req| match req {
            PeerReq::GetHealth(_) => PeerRes::GetHealth(Ok(GetHealthRes { ok: node.0 == 1 })),
            _ => PeerRes::GetHealth(Err(PeerError::Other("unexpected".into()))),
        });

        let res = client.get_health(NodeId(1), &GetHealthReq).await.unwrap();
        assert!(res.ok);

        let res = client.get_health(NodeId(2), &GetHealthReq).await.unwrap();
        assert!(!res.ok);
    }
}
