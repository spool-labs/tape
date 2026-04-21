use std::sync::Arc;

use async_trait::async_trait;
use tape_protocol::api::{
    Api, ApiError, CertifyReq, CertifyRes, FindTrackReq, FindTrackRes, GetHealthReq,
    GetHealthRes, GetSliceReq, GetSliceRes, GetStatsReq, GetStatsRes, GetTrackByNumberReq,
    GetTrackByNumberRes, GetTrackDataReq, GetTrackDataRes, GetTrackProofReq, GetTrackProofRes,
    GetTrackReq, GetTrackRes, InvalidateReq, InvalidateRes, ListTracksByTapeReq,
    ListTracksByTapeRes, PeerReq, PeerRes, PutSliceReq, PutSliceRes, RepairReq, RepairRes,
    SnapshotVoteReq, SnapshotVoteRes,
    SyncSlicesReq, SyncSlicesRes, SyncTracksReq, SyncTracksRes,
};
use tape_core::types::NodeId;

pub struct MemoryApi {
    handler: Arc<dyn Fn(NodeId, PeerReq) -> PeerRes + Send + Sync>,
}

impl MemoryApi {
    pub fn new(handler: impl Fn(NodeId, PeerReq) -> PeerRes + Send + Sync + 'static) -> Self {
        Self {
            handler: Arc::new(handler),
        }
    }

    /// Creates a client where every call returns `ApiError::Other("not implemented")`.
    pub fn noop() -> Self {
        Self::new(|_, req| match req {
            PeerReq::PutSlice(_) => PeerRes::PutSlice(Err(not_impl())),
            PeerReq::GetSlice(_) => PeerRes::GetSlice(Err(not_impl())),
            PeerReq::GetTrack(_) => PeerRes::GetTrack(Err(not_impl())),
            PeerReq::GetTrackByNumber(_) => PeerRes::GetTrackByNumber(Err(not_impl())),
            PeerReq::FindTrack(_) => PeerRes::FindTrack(Err(not_impl())),
            PeerReq::ListTracksByTape(_) => PeerRes::ListTracksByTape(Err(not_impl())),
            PeerReq::GetTrackData(_) => PeerRes::GetTrackData(Err(not_impl())),
            PeerReq::GetTrackProof(_) => PeerRes::GetTrackProof(Err(not_impl())),
            PeerReq::SyncSlices(_) => PeerRes::SyncSlices(Err(not_impl())),
            PeerReq::SyncTracks(_) => PeerRes::SyncTracks(Err(not_impl())),
            PeerReq::Repair(_) => PeerRes::Repair(Err(not_impl())),
            PeerReq::Certify(_) => PeerRes::Certify(Err(not_impl())),
            PeerReq::SnapshotVote(_) => PeerRes::SnapshotVote(Err(not_impl())),
            PeerReq::Invalidate(_) => PeerRes::Invalidate(Err(not_impl())),
            PeerReq::GetHealth(_) => PeerRes::GetHealth(Err(not_impl())),
            PeerReq::GetStats(_) => PeerRes::GetStats(Err(not_impl())),
        })
    }
}

fn not_impl() -> ApiError {
    ApiError::Other("not implemented".into())
}

macro_rules! dispatch {
    ($self:ident, $node:ident, $req:expr, $variant:ident) => {{
        let res = ($self.handler)($node, PeerReq::$variant($req));
        match res {
            PeerRes::$variant(r) => r,
            _ => Err(ApiError::Other("handler returned wrong variant".into())),
        }
    }};
}

#[async_trait]
impl Api for MemoryApi {
    async fn put_slice(&self, node: NodeId, req: &PutSliceReq) -> Result<PutSliceRes, ApiError> {
        dispatch!(self, node, PutSliceReq { track: req.track, spool: req.spool, payload: req.payload.clone() }, PutSlice)
    }

    async fn get_slice(&self, node: NodeId, req: &GetSliceReq) -> Result<GetSliceRes, ApiError> {
        dispatch!(self, node, GetSliceReq { track: req.track, spool: req.spool }, GetSlice)
    }

    async fn get_track(&self, node: NodeId, req: &GetTrackReq) -> Result<GetTrackRes, ApiError> {
        dispatch!(self, node, GetTrackReq { track: req.track }, GetTrack)
    }

    async fn get_track_by_number(&self, node: NodeId, req: &GetTrackByNumberReq) -> Result<GetTrackByNumberRes, ApiError> {
        dispatch!(self, node, GetTrackByNumberReq { tape: req.tape, track_number: req.track_number }, GetTrackByNumber)
    }

    async fn find_track(&self, node: NodeId, req: &FindTrackReq) -> Result<FindTrackRes, ApiError> {
        dispatch!(self, node, FindTrackReq { tape: req.tape, key: req.key, version: req.version.clone() }, FindTrack)
    }

    async fn list_tracks_by_tape(&self, node: NodeId, req: &ListTracksByTapeReq) -> Result<ListTracksByTapeRes, ApiError> {
        dispatch!(self, node, ListTracksByTapeReq { tape: req.tape, cursor: req.cursor, limit: req.limit }, ListTracksByTape)
    }

    async fn get_track_data(&self, node: NodeId, req: &GetTrackDataReq) -> Result<GetTrackDataRes, ApiError> {
        dispatch!(self, node, GetTrackDataReq { track: req.track }, GetTrackData)
    }

    async fn get_track_proof(&self, node: NodeId, req: &GetTrackProofReq) -> Result<GetTrackProofRes, ApiError> {
        dispatch!(self, node, GetTrackProofReq { track: req.track }, GetTrackProof)
    }

    async fn sync_slices(&self, node: NodeId, req: &SyncSlicesReq) -> Result<SyncSlicesRes, ApiError> {
        dispatch!(self, node, SyncSlicesReq { spool_index: req.spool_index, cursor: req.cursor, limit: req.limit }, SyncSlices)
    }

    async fn sync_tracks(&self, node: NodeId, req: &SyncTracksReq) -> Result<SyncTracksRes, ApiError> {
        dispatch!(self, node, SyncTracksReq { spool_index: req.spool_index, cursor: req.cursor, limit: req.limit }, SyncTracks)
    }

    async fn repair(&self, node: NodeId, req: &RepairReq) -> Result<RepairRes, ApiError> {
        dispatch!(self, node, RepairReq { track: req.track, helper_spool: req.helper_spool, stripes: req.stripes.clone() }, Repair)
    }

    async fn certify(&self, node: NodeId, req: &CertifyReq) -> Result<CertifyRes, ApiError> {
        dispatch!(self, node, CertifyReq { track: req.track }, Certify)
    }

    async fn snapshot_vote(
        &self,
        node: NodeId,
        req: &SnapshotVoteReq,
    ) -> Result<SnapshotVoteRes, ApiError> {
        dispatch!(
            self,
            node,
            SnapshotVoteReq {
                node_id: req.node_id,
                kind: req.kind,
                message: req.message.clone(),
                signature: req.signature,
            },
            SnapshotVote
        )
    }

    async fn invalidate(&self, node: NodeId, req: &InvalidateReq) -> Result<InvalidateRes, ApiError> {
        dispatch!(self, node, InvalidateReq { track: req.track, proof: req.proof.clone() }, Invalidate)
    }

    async fn get_health(&self, node: NodeId, _req: &GetHealthReq) -> Result<GetHealthRes, ApiError> {
        dispatch!(self, node, GetHealthReq, GetHealth)
    }

    async fn get_stats(&self, node: NodeId, _req: &GetStatsReq) -> Result<GetStatsRes, ApiError> {
        dispatch!(self, node, GetStatsReq, GetStats)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tape_core::bls::BlsPrivateKey;
    use tape_core::bls::BlsSignature;
    use tape_core::cert::{SNAPSHOT_SIGN_MESSAGE_SIZE, SNAPSHOT_WRITE_MESSAGE_SIZE};
    use tape_protocol::api::SnapshotVoteKind;

    fn snapshot_vote_signature(message: &[u8]) -> BlsSignature {
        BlsPrivateKey::from_random().sign(message).unwrap()
    }

    #[tokio::test]
    async fn noop_returns_error() {
        let client = MemoryApi::noop();
        let res = client.get_health(NodeId(1), &GetHealthReq).await;
        assert!(res.is_err());
    }

    #[tokio::test]
    async fn custom_handler() {
        let client = MemoryApi::new(move |node, req| match req {
            PeerReq::GetHealth(_) => PeerRes::GetHealth(Ok(GetHealthRes { ok: node.0 == 1 })),
            PeerReq::SnapshotVote(req) => {
                assert_eq!(req.node_id, NodeId(9));
                PeerRes::SnapshotVote(Ok(SnapshotVoteRes))
            }
            _ => PeerRes::GetHealth(Err(ApiError::Other("unexpected".into()))),
        });

        let res = client.get_health(NodeId(1), &GetHealthReq).await.unwrap();
        assert!(res.ok);

        let res = client.get_health(NodeId(2), &GetHealthReq).await.unwrap();
        assert!(!res.ok);
    }

    #[tokio::test]
    async fn snapshot_vote_write_dispatch() {
        let message = vec![0xAB; SNAPSHOT_WRITE_MESSAGE_SIZE];
        let signature = snapshot_vote_signature(b"snapshot-write");
        let expected_message = message.clone();
        let client = MemoryApi::new(move |node, req| match req {
            PeerReq::SnapshotVote(req) => {
                assert_eq!(node, NodeId(7));
                assert_eq!(req.node_id, NodeId(9));
                assert_eq!(req.kind, SnapshotVoteKind::WriteChunk);
                assert_eq!(req.message, expected_message);
                assert_eq!(req.signature, signature);
                PeerRes::SnapshotVote(Ok(SnapshotVoteRes))
            }
            _ => PeerRes::SnapshotVote(Err(ApiError::Other("unexpected".into()))),
        });

        client
            .snapshot_vote(
                NodeId(7),
                &SnapshotVoteReq {
                    node_id: NodeId(9),
                    kind: SnapshotVoteKind::WriteChunk,
                    message,
                    signature,
                },
            )
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn snapshot_vote_complete_group_dispatch() {
        let message = vec![0xCD; SNAPSHOT_SIGN_MESSAGE_SIZE];
        let signature = snapshot_vote_signature(b"snapshot-complete");
        let expected_message = message.clone();
        let client = MemoryApi::new(move |node, req| match req {
            PeerReq::SnapshotVote(req) => {
                assert_eq!(node, NodeId(7));
                assert_eq!(req.node_id, NodeId(9));
                assert_eq!(req.kind, SnapshotVoteKind::CompleteGroup);
                assert_eq!(req.message, expected_message);
                assert_eq!(req.signature, signature);
                PeerRes::SnapshotVote(Ok(SnapshotVoteRes))
            }
            _ => PeerRes::SnapshotVote(Err(ApiError::Other("unexpected".into()))),
        });

        client
            .snapshot_vote(
                NodeId(7),
                &SnapshotVoteReq {
                    node_id: NodeId(9),
                    kind: SnapshotVoteKind::CompleteGroup,
                    message,
                    signature,
                },
            )
            .await
            .unwrap();
    }
}
