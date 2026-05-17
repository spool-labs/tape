use std::sync::Arc;

use async_trait::async_trait;
use tape_protocol::api::{
    Api, ApiError, CertifyReq, CertifyRes, FindTrackReq, FindTrackRes, GetHealthReq,
    GetHealthRes, GetSliceReq, GetSliceRes, GetStatsReq, GetStatsRes, GetTrackByNumberReq,
    GetTrackByNumberRes, GetTrackDataReq, GetTrackDataRes, GetTrackProofReq, GetTrackProofRes,
    GetTrackReq, GetTrackRes, InvalidateReq, InvalidateRes, ListTracksByTapeReq,
    ListTracksByTapeRes, PeerReq, PeerRes, PutSliceReq, PutSliceRes, RepairReq, RepairRes,
    SyncSlicesReq, SyncSlicesRes, SyncTracksReq, SyncTracksRes, VoteReq, VoteRes,
};
use tape_crypto::Address;

pub struct MemoryApi {
    handler: Arc<dyn Fn(Address, PeerReq) -> PeerRes + Send + Sync>,
}

impl MemoryApi {
    pub fn new(handler: impl Fn(Address, PeerReq) -> PeerRes + Send + Sync + 'static) -> Self {
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
            PeerReq::Invalidate(_) => PeerRes::Invalidate(Err(not_impl())),
            PeerReq::Vote(_) => PeerRes::Vote(Err(not_impl())),
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
    async fn put_slice(&self, node: Address, req: &PutSliceReq) -> Result<PutSliceRes, ApiError> {
        dispatch!(self, node, PutSliceReq { track: req.track, spool: req.spool, payload: req.payload.clone() }, PutSlice)
    }

    async fn get_slice(&self, node: Address, req: &GetSliceReq) -> Result<GetSliceRes, ApiError> {
        dispatch!(self, node, GetSliceReq { track: req.track, spool: req.spool }, GetSlice)
    }

    async fn get_track(&self, node: Address, req: &GetTrackReq) -> Result<GetTrackRes, ApiError> {
        dispatch!(self, node, GetTrackReq { track: req.track }, GetTrack)
    }

    async fn get_track_by_number(&self, node: Address, req: &GetTrackByNumberReq) -> Result<GetTrackByNumberRes, ApiError> {
        dispatch!(self, node, GetTrackByNumberReq { tape: req.tape, track_number: req.track_number }, GetTrackByNumber)
    }

    async fn find_track(&self, node: Address, req: &FindTrackReq) -> Result<FindTrackRes, ApiError> {
        dispatch!(self, node, FindTrackReq { tape: req.tape, key: req.key, version: req.version.clone() }, FindTrack)
    }

    async fn list_tracks_by_tape(&self, node: Address, req: &ListTracksByTapeReq) -> Result<ListTracksByTapeRes, ApiError> {
        dispatch!(self, node, ListTracksByTapeReq { tape: req.tape, cursor: req.cursor, limit: req.limit }, ListTracksByTape)
    }

    async fn get_track_data(&self, node: Address, req: &GetTrackDataReq) -> Result<GetTrackDataRes, ApiError> {
        dispatch!(self, node, GetTrackDataReq { track: req.track }, GetTrackData)
    }

    async fn get_track_proof(&self, node: Address, req: &GetTrackProofReq) -> Result<GetTrackProofRes, ApiError> {
        dispatch!(self, node, GetTrackProofReq { track: req.track }, GetTrackProof)
    }

    async fn sync_slices(&self, node: Address, req: &SyncSlicesReq) -> Result<SyncSlicesRes, ApiError> {
        dispatch!(self, node, SyncSlicesReq { spool_index: req.spool_index, cursor: req.cursor, limit: req.limit }, SyncSlices)
    }

    async fn sync_tracks(&self, node: Address, req: &SyncTracksReq) -> Result<SyncTracksRes, ApiError> {
        dispatch!(self, node, SyncTracksReq { spool_index: req.spool_index, cursor: req.cursor, limit: req.limit }, SyncTracks)
    }

    async fn repair(&self, node: Address, req: &RepairReq) -> Result<RepairRes, ApiError> {
        dispatch!(self, node, RepairReq { track: req.track, helper_spool: req.helper_spool, stripes: req.stripes.clone() }, Repair)
    }

    async fn certify(&self, node: Address, req: &CertifyReq) -> Result<CertifyRes, ApiError> {
        dispatch!(self, node, CertifyReq { track: req.track }, Certify)
    }

    async fn vote(&self, node: Address, req: &VoteReq) -> Result<VoteRes, ApiError> {
        dispatch!(
            self,
            node,
            VoteReq {
                signer: req.signer,
                candidate: req.candidate,
                group: req.group,
                signature: req.signature,
            },
            Vote
        )
    }

    async fn invalidate(&self, node: Address, req: &InvalidateReq) -> Result<InvalidateRes, ApiError> {
        dispatch!(self, node, InvalidateReq { track: req.track, proof: req.proof.clone() }, Invalidate)
    }

    async fn get_health(&self, node: Address, _req: &GetHealthReq) -> Result<GetHealthRes, ApiError> {
        dispatch!(self, node, GetHealthReq, GetHealth)
    }

    async fn get_stats(&self, node: Address, _req: &GetStatsReq) -> Result<GetStatsRes, ApiError> {
        dispatch!(self, node, GetStatsReq, GetStats)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tape_core::bls::BlsPrivateKey;
    use tape_core::bls::BlsSignature;
    use tape_core::spooler::SpoolGroup;
    use tape_core::system::VoteKind;
    use tape_core::types::EpochNumber;
    use tape_crypto::Hash;
    use tape_protocol::api::VoteCandidate;

    fn address(byte: u8) -> Address {
        let mut bytes = [0u8; 32];
        bytes[0] = byte;
        Address::new(bytes)
    }

    fn vote_signature(message: &[u8]) -> BlsSignature {
        BlsPrivateKey::from_random().sign(message).unwrap()
    }

    fn candidate(kind: VoteKind) -> VoteCandidate {
        VoteCandidate {
            kind,
            voting_epoch: EpochNumber(11),
            target_epoch: EpochNumber(10),
            hash: Hash::from([0xAB; 32]),
        }
    }

    #[tokio::test]
    async fn noop_returns_error() {
        let client = MemoryApi::noop();
        let res = client.get_health(address(1), &GetHealthReq).await;
        assert!(res.is_err());
    }

    #[tokio::test]
    async fn custom_handler() {
        let client = MemoryApi::new(move |node, req| match req {
            PeerReq::GetHealth(_) => PeerRes::GetHealth(Ok(GetHealthRes {
                ok: node == address(1),
            })),
            PeerReq::Vote(req) => {
                assert_eq!(req.signer, address(9));
                PeerRes::Vote(Ok(VoteRes))
            }
            _ => PeerRes::GetHealth(Err(ApiError::Other("unexpected".into()))),
        });

        let res = client.get_health(address(1), &GetHealthReq).await.unwrap();
        assert!(res.ok);

        let res = client.get_health(address(2), &GetHealthReq).await.unwrap();
        assert!(!res.ok);
    }

    #[tokio::test]
    async fn vote_dispatches_snapshot_candidate() {
        let signature = vote_signature(b"snapshot");
        let expected_candidate = candidate(VoteKind::Snapshot);
        let client = MemoryApi::new(move |node, req| match req {
            PeerReq::Vote(req) => {
                assert_eq!(node, address(7));
                assert_eq!(req.signer, address(9));
                assert_eq!(req.candidate, expected_candidate);
                assert_eq!(req.group, SpoolGroup(4));
                assert_eq!(req.signature, signature);
                PeerRes::Vote(Ok(VoteRes))
            }
            _ => PeerRes::Vote(Err(ApiError::Other("unexpected".into()))),
        });

        client
            .vote(
                address(7),
                &VoteReq {
                    signer: address(9),
                    candidate: expected_candidate,
                    group: SpoolGroup(4),
                    signature,
                },
            )
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn vote_dispatches_assignment_candidate() {
        let signature = vote_signature(b"assignment");
        let expected_candidate = candidate(VoteKind::Assignment);
        let client = MemoryApi::new(move |node, req| match req {
            PeerReq::Vote(req) => {
                assert_eq!(node, address(7));
                assert_eq!(req.signer, address(9));
                assert_eq!(req.candidate, expected_candidate);
                assert_eq!(req.group, SpoolGroup(8));
                assert_eq!(req.signature, signature);
                PeerRes::Vote(Ok(VoteRes))
            }
            _ => PeerRes::Vote(Err(ApiError::Other("unexpected".into()))),
        });

        client
            .vote(
                address(7),
                &VoteReq {
                    signer: address(9),
                    candidate: expected_candidate,
                    group: SpoolGroup(8),
                    signature,
                },
            )
            .await
            .unwrap();
    }
}
