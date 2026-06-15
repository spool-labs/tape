use std::fmt::Display;

use axum::body::Bytes;
use axum::extract::{Path, State};
use axum::http::{header, StatusCode};
use axum::response::IntoResponse;

use rpc::Rpc;
use store::Store;
use tape_core::bft::is_supermajority;
use tape_core::cert::track::TrackInvalidateMessage;
use tape_core::erasure::GROUP_SIZE;
use tape_core::track::data::BlobData;
use tape_core::track::types::CompressedTrack;
use tape_core::types::{BitmapRead, EpochNumber};
use tape_crypto::address::Address;
use tape_protocol::Api;
use tape_protocol::api::{
    BINARY_CONTENT, BlsInconsistencyResponse, InconsistencyProof, InconsistencyRequest,
};
use tape_store::ops::{TrackDataOps, TrackOps};

use crate::features::http::auth::PeerAuth;
use crate::features::http::error::RouteError;
use crate::features::http::state::{AppState, current_epoch};

pub async fn invalidate<Db: Store, Cluster: Api, Blockchain: Rpc>(
    State(state): State<AppState<Db, Cluster, Blockchain>>,
    _peer: PeerAuth,
    Path(track_id): Path<String>,
    body: Bytes,
) -> Result<impl IntoResponse, RouteError> {

    let request: InconsistencyRequest = wincode::deserialize(&body)
        .map_err(|error| RouteError::BadRequest(format!("inconsistency request: {error}")))?;

    let epoch = current_epoch(&state)?;
    let track: Address = track_id
        .parse()
        .map_err(|error| RouteError::BadRequest(format!("invalid track id: {error}")))?;

    let track_key = track;

    let track_info = state
        .context
        .store
        .get_track(track_key)
        .map_err(store_error)?
        .ok_or(RouteError::NotFound)?;
    if !track_info.is_coded() {
        return Err(RouteError::BadRequest("raw tracks cannot be invalidated".into()));
    }

    let track_data = state
        .context
        .store
        .get_track_data(track_key)
        .map_err(store_error)?
        .ok_or(RouteError::NotFound)?;
    let BlobData::Coded(blob) = track_data else {
        return Err(RouteError::BadRequest("track data is not blob metadata".into()));
    };

    if blob.commitment_root() == request.proof.observed_root {
        return Err(RouteError::BadRequest("roots match, no inconsistency".into()));
    }

    verify_inconsistency_proof(&state, &track_info, epoch, &request.proof)?;

    let message = TrackInvalidateMessage::new(
        epoch,
        track_info.get_hash(),
        request.proof.observed_root,
    );
    let signature = state
        .context
        .bls_sign(&message.to_bytes())
        .map_err(|error| RouteError::Internal(format!("bls sign: {error:?}")))?;

    let response = BlsInconsistencyResponse {
        signature,
        node: state.context.node_address(),
        epoch,
    };

    let bytes = wincode::serialize(&response)
        .map_err(|error| RouteError::Internal(format!("serialize invalidate response: {error}")))?;

    Ok((
        StatusCode::OK,
        [(header::CONTENT_TYPE, BINARY_CONTENT)],
        bytes,
    ))
}

fn verify_inconsistency_proof<Db: Store, Cluster: Api, Blockchain: Rpc>(
    state: &AppState<Db, Cluster, Blockchain>,
    track_info: &CompressedTrack,
    epoch: EpochNumber,
    proof: &InconsistencyProof,
) -> Result<(), RouteError> {

    let protocol = state.context.state();
    if protocol.epoch() != epoch || protocol.current.committee.is_empty() {
        return Err(RouteError::BadRequest("committee missing".into()));
    }

    let signer_indices = proof.spool_bitmap.indices();
    if signer_indices.is_empty() {
        return Err(RouteError::BadRequest(
            "inconsistency proof has no signers".into(),
        ));
    }

    let mut signer_pubkeys = Vec::with_capacity(signer_indices.len());
    let group = protocol
        .current
        .groups
        .iter()
        .find(|group| group.id == track_info.group)
        .ok_or_else(|| RouteError::BadRequest("track group missing".into()))?;

    for signer_index in signer_indices {
        let spool = group
            .spools
            .get(signer_index)
            .ok_or_else(|| RouteError::BadRequest("unknown signer in bitmap".into()))?;

        signer_pubkeys.push(spool.bls_pubkey);
    }

    if !is_supermajority(signer_pubkeys.len() as u64, GROUP_SIZE as u64) {
        return Err(RouteError::BadRequest(
            "inconsistency proof lacks quorum for spool group".into(),
        ));
    }

    let message = TrackInvalidateMessage::new(
        epoch,
        track_info.get_hash(),
        proof.observed_root,
    );
    proof
        .signature
        .verify_aggregate(message.to_bytes(), &signer_pubkeys)
        .map_err(|_| RouteError::InvalidSignature)?;

    Ok(())
}

fn store_error(error: impl Display) -> RouteError {
    RouteError::Internal(error.to_string())
}

#[cfg(test)]
mod tests {
    use axum::body::Bytes;
    use axum::extract::{Path, State};
    use bytemuck::Zeroable;

    use tape_api::program::tapedrive::{snapshot_tape_pda, track_pda};
    use tape_core::bls::BlsSignature;
    use tape_core::encoding::EncodingProfile;
    use tape_core::erasure::{SLICE_TREE_HEIGHT, GROUP_SIZE};
    use tape_snapshot::snapshot_chunk_key;
    use tape_core::spooler::GroupIndex;
    use tape_core::tape::{snapshot_tape_number, TapeFlags};
    use tape_core::track::blob::BlobEncoding;
    use tape_core::track::types::{CompressedTrack, TrackKind, TrackState};
    use tape_core::types::{
        ChunkNumber, EpochNumber, SlotNumber, StorageUnits, StripeCount, TrackNumber,
    };
    use tape_crypto::Hash;
    use tape_crypto::merkle::root_from_leaf_hashes;
    use tape_protocol::api::{InconsistencyProof, InconsistencyRequest};
    use tape_store::ops::{ObjectInfoOps, TapeOps};
    use tape_store::types::{ObjectInfo, SystemObjectKind, TapeInfo};

    use super::*;
    use crate::features::http::state::AppState;
    use crate::harness::{NodeHarness, TestContext};

    async fn test_context() -> TestContext {
        NodeHarness::builder()
            .nodes(25)
            .no_prev_snapshot_tape()
            .build()
            .await
            .expect("build harness")
            .ctx_for(0)
    }

    #[tokio::test]
    async fn matching_root_rejected() {
        let ctx = test_context().await;

        let leaves = [Hash::from([0x44; 32]); GROUP_SIZE];
        let commitment = root_from_leaf_hashes::<SLICE_TREE_HEIGHT>(&leaves);
        let blob = BlobEncoding {
            size: StorageUnits::from_bytes(1_537),
            commitment,
            profile: EncodingProfile::basic_default(),
            stripe_size: StorageUnits::from_bytes(512),
            stripe_count: StripeCount(4),
            leaves,
        };

        let epoch = EpochNumber(5);

        let group = GroupIndex(2);
        let track_number = TrackNumber(9);
        let (snapshot_tape, _) = snapshot_tape_pda(epoch);
        let track_address = track_pda(snapshot_tape, track_number).0;

        ctx.store
            .put_tape(
                snapshot_tape,
                TapeInfo {
                    id: snapshot_tape_number(epoch),
                    flags: TapeFlags::SYSTEM,
                    end_epoch: EpochNumber(u64::MAX),
                    next_track_number: TrackNumber(track_number.0 + 1),
                },
            )
            .expect("seed snapshot tape");
        let track = CompressedTrack {
            tape: snapshot_tape,
            key: snapshot_chunk_key(epoch, group, ChunkNumber(0)),
            track_number,
            kind: TrackKind::Coded as u64,
            state: TrackState::Certified as u64,
            size: blob.size,
            group: group,
            value_hash: blob.get_hash(),
        };

        ctx.store
            .put_track(track_address, track)
            .expect("seed track");

        ctx.store
            .put_track_data(track_address, BlobData::Coded(blob))
            .expect("seed track data");

        ctx.store
            .put_object_info(
                track_address,
                ObjectInfo::System {
                    kind: SystemObjectKind::Snapshot {
                        epoch,
                    },
                    track_address,
                    registered_epoch: epoch,
                    certified_epoch: Some(epoch),
                    slot: SlotNumber(epoch.0),
                },
            )
            .expect("seed object info");

        let request = InconsistencyRequest {
            proof: InconsistencyProof {
                observed_root: blob.commitment_root(),
                spool_bitmap: tape_core::types::SpoolBitmap::zeroed(),
                signature: BlsSignature::zeroed(),
            },
        };

        let body = wincode::serialize(&request).expect("serialize request");

        let peer = PeerAuth {
            node: ctx.node_address(),
            tls_pubkey: tape_core::types::tls::NetworkTlsPubkey::new_unique(),
        };
        let err = invalidate(
            State(AppState {
                context: ctx.clone(),
            }),
            peer,
            Path(track_address.to_string()),
            Bytes::from(body),
        )
        .await
        .err()
        .expect("matching-root inconsistency should be rejected");

        match err {
            RouteError::BadRequest(message) => assert!(message.contains("roots match")),
            RouteError::NotFound
            | RouteError::NotResponsible
            | RouteError::BlacklistedObject
            | RouteError::NotInCommittee
            | RouteError::InvalidSignature
            | RouteError::Forbidden(_)
            | RouteError::Internal(_) => panic!("unexpected RouteError variant"),
        }
    }
}
