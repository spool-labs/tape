use std::fmt::Display;

use axum::body::Bytes;
use axum::extract::{Path, State};
use axum::http::{header, StatusCode};
use axum::response::IntoResponse;
use bytemuck::cast;

use rpc::Rpc;
use store::Store;
use tape_core::bft::is_supermajority;
use tape_core::cert::InvalidateMessage;
use tape_core::erasure::{SPOOL_GROUP_SIZE, group_for_spool};
use tape_core::track::data::TrackData;
use tape_core::track::types::CompressedTrack;
use tape_core::types::{CommitteeBitmap, EpochNumber};
use tape_crypto::address::Address;
use tape_protocol::Api;
use tape_protocol::api::{
    BINARY_CONTENT, BlsInconsistencyResponse, InconsistencyProof, InconsistencyRequest,
};
use tape_store::ops::{TrackDataOps, TrackOps};

use crate::features::http::error::RouteError;
use crate::features::http::state::{AppState, current_epoch};

pub async fn invalidate<Db: Store, Cluster: Api, Blockchain: Rpc>(
    State(state): State<AppState<Db, Cluster, Blockchain>>,
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
    if !track_info.is_blob() {
        return Err(RouteError::BadRequest("raw tracks cannot be invalidated".into()));
    }

    let track_data = state
        .context
        .store
        .get_track_data(track_key)
        .map_err(store_error)?
        .ok_or(RouteError::NotFound)?;
    let TrackData::Blob(blob) = track_data else {
        return Err(RouteError::BadRequest("track data is not blob metadata".into()));
    };

    if blob.commitment_root() == request.proof.observed_root {
        return Err(RouteError::BadRequest("roots match, no inconsistency".into()));
    }

    verify_inconsistency_proof(&state, &track_info, epoch, &request.proof)?;

    let message = InvalidateMessage::new(
        epoch,
        track_info.get_hash().into(),
        request.proof.observed_root.into(),
    );
    let signature = state
        .context
        .bls_sign(&message.to_bytes())
        .map_err(|error| RouteError::Internal(format!("bls sign: {error:?}")))?;

    let response = BlsInconsistencyResponse {
        signature,
        node_id: state.context.node_id(),
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
    if protocol.epoch != epoch || protocol.committee.is_empty() {
        return Err(RouteError::BadRequest("committee missing".into()));
    }

    let bitmap: CommitteeBitmap = cast(proof.committee_bitmap);
    let signer_indices = bitmap.indices(protocol.committee.len());
    if signer_indices.is_empty() {
        return Err(RouteError::BadRequest(
            "inconsistency proof has no signers".into(),
        ));
    }

    let mut signer_weight = 0u64;
    let mut signer_pubkeys = Vec::with_capacity(signer_indices.len());

    for signer_index in signer_indices {
        let member = protocol
            .committee
            .get(signer_index)
            .ok_or_else(|| RouteError::BadRequest("unknown signer in bitmap".into()))?;

        signer_weight += protocol
            .member_spools(signer_index)
            .iter()
            .filter(|&&spool| group_for_spool(spool) == track_info.spool_group)
            .count() as u64;
        signer_pubkeys.push(member.key);
    }

    if !is_supermajority(signer_weight, SPOOL_GROUP_SIZE as u64) {
        return Err(RouteError::BadRequest(
            "inconsistency proof lacks quorum for spool group".into(),
        ));
    }

    let message = InvalidateMessage::new(
        epoch,
        track_info.get_hash().into(),
        proof.observed_root.into(),
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
    //! Black-box test that hits the inconsistency handler against a
    //! *projected* snapshot track. We exercise the matching-root rejection
    //! path: same blob, same commitment_root, so the handler must
    //! short-circuit before any committee/BLS work. The point of the test
    //! is to confirm that nothing in the snapshot projection breaks the
    //! lookups (`get_track`, `get_track_data`) or the `commitment_root()`
    //! comparison the handler relies on.

    use super::*;
    use axum::body::Bytes;
    use axum::extract::{Path, State};
    use bytemuck::{cast, Zeroable};
    use tape_api::program::tapedrive::{snapshot_tape_pda, track_pda};
    use tape_core::bls::BlsSignature;
    use tape_core::encoding::EncodingProfile;
    use tape_core::erasure::{COMMITMENT_TREE_HEIGHT, SPOOL_GROUP_SIZE};
    use tape_core::snapshot::chunk::snapshot_chunk_key;
    use tape_core::spooler::SpoolGroup;
    use tape_core::track::blob::BlobInfo;
    use tape_core::track::types::{CompressedTrack, TrackKind, TrackState};
    use tape_core::types::{
        EpochNumber, SlotNumber, StorageUnits, StripeCount, TrackNumber,
    };
    use tape_crypto::merkle::root_from_leaf_hashes;
    use tape_protocol::ProtocolState;
    use tape_protocol::api::{InconsistencyProof, InconsistencyRequest};
    use tape_store::ops::{ObjectInfoOps, TapeOps};
    use tape_store::types::{ObjectInfo, TapeInfo};

    use crate::context::test_utils::test_context;
    use crate::features::http::state::AppState;

    #[tokio::test]
    async fn rejects_matching_root_for_projected_snapshot_track() {
        let ctx = test_context();
        ctx.set_state(ProtocolState {
            epoch: EpochNumber(6),
            ..ProtocolState::default()
        })
        .unwrap();

        // Build a synthetic snapshot blob whose commitment_root() matches its
        // commitment field — this is the structural invariant the projection
        // path preserves and the handler relies on.
        let leaves = [tape_crypto::Hash::from([0x44; 32]); SPOOL_GROUP_SIZE];
        let commitment = root_from_leaf_hashes::<COMMITMENT_TREE_HEIGHT>(&leaves);
        let blob = BlobInfo {
            size: StorageUnits::from_bytes(1_537),
            root: tape_crypto::Hash::from([0x55; 32]),
            commitment,
            profile: EncodingProfile::basic_default(),
            stripe_size: StorageUnits::from_bytes(512),
            stripe_count: StripeCount(4),
            leaves,
        };

        let epoch = EpochNumber(5);
        let parent_epoch = EpochNumber(4);
        let group = SpoolGroup(2);
        let track_number = TrackNumber(9);
        let (snapshot_tape, _) = snapshot_tape_pda(epoch);
        let track_address = track_pda(snapshot_tape, track_number).0;

        ctx.store
            .put_tape(
                snapshot_tape,
                TapeInfo {
                    end_epoch: EpochNumber(u64::MAX),
                    next_track_number: TrackNumber(track_number.0 + 1),
                },
            )
            .unwrap();
        let track = CompressedTrack {
            tape: snapshot_tape,
            key: snapshot_chunk_key(epoch, group, parent_epoch),
            track_number,
            kind: TrackKind::Blob as u64,
            state: TrackState::Certified as u64,
            size: blob.size,
            spool_group: group,
            value_hash: blob.get_hash(),
        };
        ctx.store.put_track(track_address, track).unwrap();
        ctx.store
            .put_track_data(track_address, TrackData::Blob(blob))
            .unwrap();
        ctx.store
            .put_object_info(
                track_address,
                ObjectInfo::Valid {
                    track_address,
                    registered_epoch: epoch,
                    certified_epoch: Some(epoch),
                    slot: SlotNumber(epoch.0 * 10 + 1),
                },
            )
            .unwrap();

        // Build a proof whose observed_root *matches* commitment_root() — the
        // handler must reject before any quorum or signature work.
        let request = InconsistencyRequest {
            proof: InconsistencyProof {
                observed_root: blob.commitment_root(),
                committee_bitmap: cast([0u64; 2]),
                signature: BlsSignature::zeroed(),
            },
        };
        let body = wincode::serialize(&request).unwrap();

        let err = invalidate(
            State(AppState {
                context: ctx.clone(),
            }),
            Path(track_address.to_string()),
            Bytes::from(body),
        )
        .await
        .err()
        .expect("matching-root inconsistency should be rejected");

        match err {
            RouteError::BadRequest(msg) => assert!(
                msg.contains("roots match"),
                "expected 'roots match' message, got {msg}"
            ),
            _ => panic!("unexpected RouteError variant"),
        }
    }
}
