use std::collections::HashSet;
use std::time::Duration;
use thiserror::Error;
use tracing::{debug, info, warn};

use rpc::{CommitmentLevel, EncodedConfirmedTransactionWithStatusMeta, Rpc};
use rpc_client::parse_tape_error;
use tape_api::compute::{CERTIFY_TRACK_CU, TRACK_WRITE_CU};
use tape_api::errors::TapeError;
use tape_api::event::TrackWritten;
use tape_api::instruction::{build_certify_track_ix, build_track_write_ix, track_write_ix_len};
use solana_instruction::Instruction;
use tape_blocks::{parse_event_data, TapedriveEvent};
use tape_core::bft::min_correct;
use tape_core::erasure::GROUP_SIZE;
use tape_core::prelude::{
    BlobEncoding, CompressedTrack, EncodingProfile, EpochNumber, GroupIndex, StorageUnits,
    StripeCount, TrackNumber, TrackState,
};
use tape_core::track::data::{track_key, BlobData, BlobDataSlice, BlobInfo, TrackObjectInfo};
use tape_core::track::types::CompressedTrackProof;
use tape_core::types::ContentType;
use tape_crypto::prelude::{Address, Hash};
use tape_crypto::tx::Txid;
use tape_protocol::Api;
use tape_protocol::api::GetTrackDataReq;
use tape_protocol::api::GetTrackByNumberReq;
use futures::stream::StreamExt;
use tape_retry::{retry, retry_if, RetryConfig, Retryable};
use tape_slicer::{num_stripes, pick_stripe_size};
use tokio::time::sleep;

use crate::codec::encoder::BlobEncoder;
use crate::error::UploadError;
use crate::error::TapedriveError;
use crate::keys::operator::TapeOperator;
use crate::keys::tape_key::TapeKey;
use crate::metrics::{Operation, Phase};
use crate::tapedrive::Tapedrive;
use crate::track::{bootstrap_network_state, query};
use crate::transfer::certify::{CertificationCollector, CollectedSignatures};
use crate::transfer::uploader::{DistributedUploader, SliceWithProof};

// The program accepts up to 10 KiB for raw TrackWrite payloads.
pub const SDK_INLINE_RAW_MAX_BYTES: usize = 825;

/// Poll cadence for visibility and certification waits.
const POLL_INTERVAL_MS: u64 = 400;

/// Visibility poll attempts before giving up.
const VISIBILITY_POLL_LIMIT: usize = 30;

pub const UNNAMED_TRACK: &[u8] = b"";
pub const UNTYPED_TRACK: ContentType = ContentType::Unknown;

#[derive(Clone)]
pub struct UploadPlan {
    pub slices: Vec<SliceWithProof>,
    pub commitment_hash: Hash,
    pub storage_units: StorageUnits,
    pub profile: EncodingProfile,
    pub stripe_size: usize,
    pub stripe_count: usize,
    pub leaves: [Hash; GROUP_SIZE],
}

#[derive(Clone)]
pub struct WrittenTrack {
    pub address: Address,
    pub track: CompressedTrack,
}

#[derive(Debug, Error)]
enum TrackCompletionError {
    #[error(transparent)]
    Client(#[from] TapedriveError),

    #[error("track not certified yet")]
    NotCertifiedYet,
}

impl<Blockchain: Rpc, Cluster: Api> Tapedrive<Blockchain, Cluster> {

    /// Write an unnamed content-addressed track to an existing tape.
    ///
    /// Unnamed tracks are excluded from object listings.
    pub async fn write_track(
        &self,
        tape_key: &TapeKey,
        data: &[u8],
    ) -> Result<CompressedTrack, TapedriveError> {
        self.write_named_track(
            tape_key,
            UNNAMED_TRACK,
            UNTYPED_TRACK,
            data,
        )
        .await
    }

    /// Write a named track to an existing tape.
    ///
    /// Named tracks on non-system tapes are materialized into object listings.
    pub async fn write_named_track(
        &self,
        tape_key: &TapeKey,
        name: impl AsRef<[u8]>,
        content_type: ContentType,
        data: &[u8],
    ) -> Result<CompressedTrack, TapedriveError> {
        self.write_named_track_as(tape_key, name, content_type, data)
            .await
    }

    /// Write a named track to an existing tape,.
    pub async fn write_named_track_as(
        &self,
        operator: &impl TapeOperator,
        name: impl AsRef<[u8]>,
        content_type: ContentType,
        data: &[u8],
    ) -> Result<CompressedTrack, TapedriveError> {
        write_track(
            self,
            operator,
            name.as_ref(),
            content_type,
            data,
        )
        .await
    }

    /// Write unnamed raw bytes to an existing tape.
    ///
    /// Unnamed raw tracks are content-addressed and excluded from object listings.
    pub async fn write_raw(
        &self,
        tape_key: &TapeKey,
        raw: &[u8],
    ) -> Result<CompressedTrack, TapedriveError> {
        self.write_named_raw(
            tape_key,
            UNNAMED_TRACK,
            UNTYPED_TRACK,
            raw,
        )
        .await
    }

    /// Write named raw bytes to an existing tape.
    pub async fn write_named_raw(
        &self,
        tape_key: &TapeKey,
        name: impl AsRef<[u8]>,
        content_type: ContentType,
        raw: &[u8],
    ) -> Result<CompressedTrack, TapedriveError> {
        let name = name.as_ref();
        if !inline_write_fits(name, raw.len()) {
            return Err(TapedriveError::InvalidArgument(format!(
                "raw inline write exceeds SDK transaction limit; use write_track() or write_blob()"
            )));
        }

        let timer = self
            .timer(Operation::WriteRaw, Phase::Total)
            .bytes(raw.len() as u64);

        let result = submit_raw(
            self,
            tape_key,
            name,
            content_type,
            raw,
            Operation::WriteRaw
        ).await;

        timer.finish_result(&result);

        let written = result?;
        Ok(written.track)
    }

    /// Register an unnamed blob track and return the upload plan needed to land its slices.
    ///
    /// Unnamed blob tracks are content-addressed and excluded from object listings.
    pub async fn write_blob(
        &self,
        tape_key: &TapeKey,
        data: &[u8],
    ) -> Result<(WrittenTrack, UploadPlan), TapedriveError> {
        self.write_named_blob(
            tape_key,
            UNNAMED_TRACK,
            UNTYPED_TRACK,
            data,
        )
        .await
    }

    /// Register a named blob track and return the upload plan needed to land its slices.
    pub async fn write_named_blob(
        &self,
        tape_key: &TapeKey,
        name: impl AsRef<[u8]>,
        content_type: ContentType,
        data: &[u8],
    ) -> Result<(WrittenTrack, UploadPlan), TapedriveError> {
        let timer = self
            .timer(Operation::WriteBlob, Phase::Total)
            .bytes(data.len() as u64);

        let result = submit_blob(
            self,
            tape_key,
            name.as_ref(),
            content_type,
            data,
            Operation::WriteBlob
        ).await;

        timer.finish_result(&result);
        result
    }

    /// Upload blob slices for a previously written blob track.
    pub async fn upload(
        &self,
        written: &WrittenTrack,
        plan: &UploadPlan,
    ) -> Result<(), TapedriveError> {
        let bytes = plan.slices.iter().map(|slice| slice.data.len() as u64).sum();
        let timer = self
            .timer(Operation::Upload, Phase::Total)
            .bytes(bytes)
            .chunks(plan.slices.len() as u64);

        let result = upload(
            self,
            written,
            plan,
            Operation::Upload
        ).await;

        timer.finish_result(&result);
        result
    }

    /// Collect signatures and submit the certify instruction for a written track.
    pub async fn certify(
        &self,
        tape_key: &TapeKey,
        written: &WrittenTrack,
    ) -> Result<(), TapedriveError> {
        let timer = self.timer(Operation::Certify, Phase::Total).chunks(1);

        let result = certify_once(
            self,
            tape_key,
            written,
            Operation::Certify
        ).await;

        timer.finish_result(&result);
        result
    }
}

fn prepare_plan(data: Vec<u8>) -> Result<UploadPlan, TapedriveError> {
    let data_len = data.len();
    let profile = EncodingProfile::clay_default();
    let mut encoder = BlobEncoder::with_profile(profile);
    let (slices, merkle_root, leaves) = encoder
        .encode_with_leaves(data)
        .map_err(|e| TapedriveError::Encoding(e.to_string()))?;

    Ok(UploadPlan {
        slices,
        commitment_hash: merkle_root.into(),
        storage_units: StorageUnits::from_bytes(data_len as u64),
        profile,
        stripe_size: pick_stripe_size(data_len),
        stripe_count: num_stripes(data_len, pick_stripe_size(data_len)),
        leaves,
    })
}

/// Encode a blob into its upload plan on a blocking thread; encoding a 64 MiB
/// chunk is seconds of CPU work that would otherwise stall the runtime.
pub(crate) async fn encode_blob<Blockchain: Rpc, Cluster: Api>(
    client: &Tapedrive<Blockchain, Cluster>,
    data: Vec<u8>,
    operation: Operation,
) -> Result<UploadPlan, TapedriveError> {
    let encode_timer = client
        .timer(operation, Phase::Encode)
        .bytes(data.len() as u64);
    let result = match tokio::task::spawn_blocking(move || prepare_plan(data)).await {
        Ok(plan) => plan,
        Err(join) => Err(TapedriveError::Encoding(format!("encode task failed: {join}"))),
    };
    encode_timer.finish_result(&result);
    result
}

/// Register an already-encoded blob on-chain.
pub(crate) async fn register_blob<Blockchain: Rpc, Cluster: Api>(
    client: &Tapedrive<Blockchain, Cluster>,
    tape_key: &impl TapeOperator,
    name: &[u8],
    content_type: ContentType,
    logical_size: StorageUnits,
    plan: UploadPlan,
    operation: Operation,
) -> Result<(WrittenTrack, UploadPlan), TapedriveError> {
    let register_timer = client
        .timer(operation, Phase::Register)
        .bytes(plan.storage_units.to_bytes())
        .chunks(1);
    let result = send_blob(client, tape_key, name, content_type, logical_size, plan).await;
    register_timer.finish_result(&result);
    result
}

fn track_object(
    name: &[u8],
    content_type: ContentType,
    logical_size: StorageUnits,
) -> Option<TrackObjectInfo> {
    if name.is_empty() {
        None
    } else {
        Some(TrackObjectInfo {
            name: name.to_vec(),
            content_type,
            logical_size,
        })
    }
}

fn inline_write_data_limit() -> usize {
    track_write_ix_len(SDK_INLINE_RAW_MAX_BYTES, None)
        .expect("unnamed inline track write size should fit usize")
}

pub(crate) fn inline_write_fits(name: &[u8], payload_len: usize) -> bool {
    let object_name_len = (!name.is_empty()).then_some(name.len());

    track_write_ix_len(payload_len, object_name_len)
        .is_some_and(|len| len <= inline_write_data_limit())
}

async fn submit_raw<Blockchain: Rpc, Cluster: Api>(
    client: &Tapedrive<Blockchain, Cluster>,
    tape_key: &impl TapeOperator,
    name: &[u8],
    content_type: ContentType,
    raw: &[u8],
    operation: Operation,
) -> Result<WrittenTrack, TapedriveError> {
    submit_raw_with_logical_size(
        client,
        tape_key,
        name,
        content_type,
        StorageUnits::from_bytes(raw.len() as u64),
        raw,
        operation,
    )
    .await
}

pub(crate) async fn submit_raw_with_logical_size<Blockchain: Rpc, Cluster: Api>(
    client: &Tapedrive<Blockchain, Cluster>,
    tape_key: &impl TapeOperator,
    name: &[u8],
    content_type: ContentType,
    logical_size: StorageUnits,
    raw: &[u8],
    operation: Operation,
) -> Result<WrittenTrack, TapedriveError> {
    let timer = client
        .timer(operation, Phase::Register)
        .bytes(raw.len() as u64)
        .chunks(1);

    let result = send_raw(client, tape_key, name, content_type, logical_size, raw).await;

    timer.finish_result(&result);
    result
}

async fn send_raw<Blockchain: Rpc, Cluster: Api>(
    client: &Tapedrive<Blockchain, Cluster>,
    tape_key: &impl TapeOperator,
    name: &[u8],
    content_type: ContentType,
    logical_size: StorageUnits,
    raw: &[u8],
) -> Result<WrittenTrack, TapedriveError> {
    let payer = client.payer()?;
    let tape_signer = tape_key.keypair();
    let data = BlobDataSlice::Inline(raw);
    let key = track_key(name, &data);
    let object = track_object(name, content_type, logical_size);

    let write_ix = build_track_write_ix(
        payer.pubkey().into(),
        tape_key.pubkey().into(),
        tape_key.address(),
        BlobInfo {
            object,
            data: BlobData::Inline(raw.to_vec()),
        },
    )
    .map_err(|error| TapedriveError::InvalidArgument(error.to_string()))?;

    let signature = client
        .rpc()
        .send_instructions_with_signers_and_compute_unit_limit(
            payer,
            TRACK_WRITE_CU,
            vec![write_ix],
            &[tape_signer],
            client.rpc().rpc().commitment(),
            true,
        )
        .await?;

    let written = fetch_track_written_event(client, &signature).await?;
    let track_address: Address = written.track.into();
    let meta = data.meta().unwrap();
    let track = CompressedTrack {
        tape: written.tape,
        track_number: written.track_number,
        key,
        kind: meta.kind as u64,
        state: meta.state as u64,
        size: meta.size,
        group: written.group,
        value_hash: meta.value_hash,
    };

    debug_assert_eq!(track.get_hash(), written.track_hash);

    Ok(WrittenTrack {
        address: track_address,
        track,
    })
}

pub(crate) async fn submit_blob<Blockchain: Rpc, Cluster: Api>(
    client: &Tapedrive<Blockchain, Cluster>,
    tape_key: &impl TapeOperator,
    name: &[u8],
    content_type: ContentType,
    data: &[u8],
    operation: Operation,
) -> Result<(WrittenTrack, UploadPlan), TapedriveError> {
    submit_blob_with_logical_size(
        client,
        tape_key,
        name,
        content_type,
        StorageUnits::from_bytes(data.len() as u64),
        data,
        operation,
    )
    .await
}

pub(crate) async fn submit_blob_with_logical_size<Blockchain: Rpc, Cluster: Api>(
    client: &Tapedrive<Blockchain, Cluster>,
    tape_key: &impl TapeOperator,
    name: &[u8],
    content_type: ContentType,
    logical_size: StorageUnits,
    data: &[u8],
    operation: Operation,
) -> Result<(WrittenTrack, UploadPlan), TapedriveError> {
    let plan = encode_blob(client, data.to_vec(), operation).await?;
    register_blob(client, tape_key, name, content_type, logical_size, plan, operation).await
}

/// A registered blob whose transaction has been sent but whose TrackWritten
/// event has not been resolved yet.
pub(crate) struct SentBlob {
    signature: Txid,
    blob: BlobEncoding,
    key: Hash,
    plan: UploadPlan,
}

fn build_blob_write(
    payer: Address,
    tape_key: &impl TapeOperator,
    name: &[u8],
    content_type: ContentType,
    logical_size: StorageUnits,
    plan: &UploadPlan,
) -> Result<(Instruction, BlobEncoding, Hash), TapedriveError> {
    let blob = BlobEncoding {
        size: plan.storage_units,
        commitment: plan.commitment_hash,
        profile: plan.profile,
        stripe_size: StorageUnits::from_bytes(plan.stripe_size as u64),
        stripe_count: StripeCount(plan.stripe_count as u64),
        leaves: plan.leaves,
    };

    let key = track_key(name, &BlobDataSlice::Coded(blob));
    let object = track_object(name, content_type, logical_size);
    let write_ix = build_track_write_ix(
        payer,
        tape_key.pubkey().into(),
        tape_key.address(),
        BlobInfo {
            object,
            data: BlobData::Coded(blob),
        },
    )
    .map_err(|error| TapedriveError::InvalidArgument(error.to_string()))?;

    Ok((write_ix, blob, key))
}

/// Resolve a sent register transaction into its written track. Waits until
/// the transaction is queryable, so this carries the confirmed-level wait for
/// registers sent at processed level.
pub(crate) async fn resolve_sent_blob<Blockchain: Rpc, Cluster: Api>(
    client: &Tapedrive<Blockchain, Cluster>,
    sent: SentBlob,
) -> Result<(WrittenTrack, UploadPlan), TapedriveError> {
    let written = fetch_track_written_event(client, &sent.signature).await?;
    let track_address: Address = written.track.into();
    let meta = BlobDataSlice::Coded(sent.blob).meta()
        .ok_or(TapedriveError::InvalidArgument("invalid blob commitment".into()))?;

    let track = CompressedTrack {
        tape: written.tape,
        track_number: written.track_number,
        key: sent.key,
        kind: meta.kind as u64,
        state: meta.state as u64,
        size: meta.size,
        group: written.group,
        value_hash: meta.value_hash,
    };

    debug_assert_eq!(track.get_hash(), written.track_hash);

    Ok((
        WrittenTrack {
            address: track_address,
            track,
        },
        sent.plan,
    ))
}

/// Send an encoded blob's register transaction, returning once it is
/// processed on the current fork. The next register can be sent immediately;
/// resolve_sent_blob carries the confirmed-level wait.
pub(crate) async fn register_blob_processed<Blockchain: Rpc, Cluster: Api>(
    client: &Tapedrive<Blockchain, Cluster>,
    tape_key: &impl TapeOperator,
    name: &[u8],
    content_type: ContentType,
    logical_size: StorageUnits,
    plan: UploadPlan,
    operation: Operation,
) -> Result<SentBlob, TapedriveError> {
    let payer = client.payer()?;
    let tape_signer = tape_key.keypair();
    let (write_ix, blob, key) =
        build_blob_write(payer.pubkey().into(), tape_key, name, content_type, logical_size, &plan)?;

    let register_timer = client
        .timer(operation, Phase::Register)
        .bytes(plan.storage_units.to_bytes())
        .chunks(1);
    let result = client
        .rpc()
        .send_instructions_with_signers_and_compute_unit_limit(
            payer,
            TRACK_WRITE_CU,
            vec![write_ix],
            &[tape_signer],
            CommitmentLevel::Processed,
            true,
        )
        .await;
    register_timer.finish_result(&result);
    let signature = result?;

    Ok(SentBlob { signature, blob, key, plan })
}

async fn send_blob<Blockchain: Rpc, Cluster: Api>(
    client: &Tapedrive<Blockchain, Cluster>,
    tape_key: &impl TapeOperator,
    name: &[u8],
    content_type: ContentType,
    logical_size: StorageUnits,
    plan: UploadPlan,
) -> Result<(WrittenTrack, UploadPlan), TapedriveError> {
    let payer = client.payer()?;
    let tape_signer = tape_key.keypair();
    let (write_ix, blob, key) =
        build_blob_write(payer.pubkey().into(), tape_key, name, content_type, logical_size, &plan)?;

    let signature = client
        .rpc()
        .send_instructions_with_signers_and_compute_unit_limit(
            payer,
            TRACK_WRITE_CU,
            vec![write_ix],
            &[tape_signer],
            client.rpc().rpc().commitment(),
            true,
        )
        .await?;

    resolve_sent_blob(client, SentBlob { signature, blob, key, plan }).await
}

async fn upload_once<Blockchain: Rpc, Cluster: Api>(
    client: &Tapedrive<Blockchain, Cluster>,
    track_address: Address,
    group: GroupIndex,
    slices: Vec<SliceWithProof>,
    operation: Operation,
) -> Result<(), TapedriveError> {
    let bytes = slices.iter().map(|slice| slice.data.len() as u64).sum();
    let chunks = slices.len() as u64;

    let locate = client.timer(operation, Phase::Locate);
    let state = bootstrap_network_state(client, Some(operation)).await;
    locate.finish_result(&state);

    let state = state?;

    let uploader = DistributedUploader::new(track_address, group, slices, &state)
        .map_err(TapedriveError::Upload)?;

    let store = client
        .timer(operation, Phase::Store)
        .bytes(bytes)
        .chunks(chunks);

    let result = uploader
        .upload_all(client.api.clone())
        .await
        .map_err(TapedriveError::Upload);

    store.finish_result(&result);
    result
}

async fn upload<Blockchain: Rpc, Cluster: Api>(
    client: &Tapedrive<Blockchain, Cluster>,
    written: &WrittenTrack,
    plan: &UploadPlan,
    operation: Operation,
) -> Result<(), TapedriveError> {
    let visibility = client.timer(operation, Phase::Visibility);

    let result = wait_for_visibility(
        client,
        written.address,
        written.track.group,
        operation,
    )
    .await;

    visibility.finish_result(&result);
    result?;

    upload_once(
        client,
        written.address,
        written.track.group,
        plan.slices.clone(),
        operation,
    )
    .await
}

async fn wait_for_visibility<Blockchain: Rpc, Cluster: Api>(
    client: &Tapedrive<Blockchain, Cluster>,
    track_address: Address,
    group: GroupIndex,
    operation: Operation,
) -> Result<(), TapedriveError> {
    let state = bootstrap_network_state(client, Some(operation)).await?;

    let group_peers = state.group_peers(group);
    let required = min_correct(state.group_member_count(group) as u64) as usize;

    let mut seen = HashSet::new();
    let peers: Vec<_> = group_peers
        .iter()
        .filter(|(_, node_id)| seen.insert(*node_id))
        .map(|(_, node_id)| *node_id)
        .collect();
    let target = peers.len();

    let mut attempt = 0usize;

    loop {
        // Probe every peer concurrently: a round costs one round-trip
        // instead of one per peer.
        let probes = peers.iter().map(|node_id| async move {
            let req = GetTrackDataReq { track: track_address };
            match client.api.get_track_data(*node_id, &req).await {
                Ok(_) => true,
                Err(error) => {
                    debug!(
                        node = %node_id,
                        error = %error,
                        "track metadata not yet visible on node"
                    );
                    false
                }
            }
        });
        let visible = futures::future::join_all(probes)
            .await
            .into_iter()
            .filter(|visible| *visible)
            .count();

        if visible >= required {
            if visible < target {
                info!(
                    visible,
                    target,
                    required,
                    "track metadata reached quorum"
                );
            }
            return Ok(());
        }

        attempt += 1;
        if attempt > VISIBILITY_POLL_LIMIT {
            return Err(TapedriveError::Upload(UploadError::Network(format!(
                "track metadata visible on {visible}/{target} nodes, need {required}"
            ))));
        }

        if attempt % 5 == 0 {
            warn!(
                attempt,
                visible,
                target,
                required,
                "track metadata not yet visible on required nodes"
            );
        }

        sleep(Duration::from_millis(POLL_INTERVAL_MS)).await;
    }
}

fn should_retry_upload(err: &TapedriveError) -> bool {
    match err {
        TapedriveError::Upload(UploadError::EpochChanged { .. })
        | TapedriveError::Upload(UploadError::InsufficientQuorum { .. })
        | TapedriveError::Upload(UploadError::InsufficientSlices { .. })
        | TapedriveError::Upload(UploadError::NoNodesAvailable)
        | TapedriveError::Upload(UploadError::Semaphore)
        | TapedriveError::Upload(UploadError::Network(_))
        | TapedriveError::Network(_) => true,
        TapedriveError::Upload(UploadError::Peer(err)) => err.is_retryable(),
        _ => false,
    }
}

pub(crate) fn should_retry_certification(err: &TapedriveError) -> bool {
    match err {
        TapedriveError::NotFound => true,
        TapedriveError::Certification(_) => true,
        TapedriveError::Peer(err) => err.is_retryable(),
        TapedriveError::Rpc(rpc) => {
            matches!(
                parse_tape_error(rpc),
                Some(TapeError::BadSignature | TapeError::BadProof | TapeError::EpochChanged)
            )
                || rpc.is_retriable()
        }
        _ => false,
    }
}

fn should_retry_track_completion(err: &TrackCompletionError) -> bool {
    match err {
        TrackCompletionError::NotCertifiedYet => true,
        TrackCompletionError::Client(TapedriveError::NotFound) => true,
        TrackCompletionError::Client(TapedriveError::Peer(err)) => err.is_retryable(),
        _ => false,
    }
}

/// Collect the certification signatures for a stored track. Signatures sign
/// the track's leaf hash, so collection needs the slices on nodes but has no
/// dependency on other tracks' certifies.
pub(crate) async fn collect_certification<Blockchain: Rpc, Cluster: Api>(
    client: &Tapedrive<Blockchain, Cluster>,
    written: &WrittenTrack,
    operation: Operation,
) -> Result<CollectedSignatures, TapedriveError> {
    let collect = client.timer(operation, Phase::CertifyCollect);
    let result = async {
        let state = bootstrap_network_state(client, Some(operation)).await?;
        let collector = CertificationCollector::with_defaults();
        collector
            .collect_signatures(client.api.as_ref(), &written.address, written.track.group, &state)
            .await
            .map_err(TapedriveError::Certification)
    }
    .await;
    collect.finish_result(&result);
    result
}

/// The track leaf the on-chain certify handler writes: certification flips
/// the state to certified and changes nothing else.
pub(crate) fn certified_track(track: &CompressedTrack) -> CompressedTrack {
    let mut updated = *track;
    updated.state = TrackState::Certified.into();
    updated
}

/// Fetch the proof and submit the certify transaction using signatures that
/// were already collected. The proof is only valid against the tape root left
/// by the previous certify, so calls on one tape must stay ordered.
pub(crate) async fn submit_certification<Blockchain: Rpc, Cluster: Api>(
    client: &Tapedrive<Blockchain, Cluster>,
    tape_key: &impl TapeOperator,
    written: &WrittenTrack,
    collected: &CollectedSignatures,
    operation: Operation,
) -> Result<(), TapedriveError> {
    let proof_timer = client.timer(operation, Phase::CertifyProof);
    let proof = query::query_track_proof(client, &written.address).await;
    proof_timer.finish_result(&proof);
    let proof = proof?;

    submit_certification_with_proof(
        client,
        tape_key,
        proof,
        collected,
        client.rpc().rpc().commitment(),
        operation,
    )
    .await
}

/// Submit the certify transaction for a prebuilt proof, waiting for the given
/// commitment. The proof is only valid against the tape root left by the
/// previous certify, so calls on one tape must stay ordered regardless of
/// commitment. Certifies always skip preflight: they sit on the
/// latency-sensitive write path and rejections are not expected in steady
/// state.
pub(crate) async fn submit_certification_with_proof<Blockchain: Rpc, Cluster: Api>(
    client: &Tapedrive<Blockchain, Cluster>,
    tape_key: &impl TapeOperator,
    proof: CompressedTrackProof,
    collected: &CollectedSignatures,
    commitment: CommitmentLevel,
    operation: Operation,
) -> Result<(), TapedriveError> {
    let payer = client.payer()?;
    let tape_signer = tape_key.keypair();

    let certify_ix = build_certify_track_ix(
        payer.pubkey().into(),
        tape_key.pubkey().into(),
        proof,
        EpochNumber(collected.epoch),
        collected.bitmap,
        collected.aggregated_signature,
    );

    let submit = client.timer(operation, Phase::CertifySubmit);
    let sent = client
        .rpc()
        .send_instructions_with_signers_and_compute_unit_limit(
            payer,
            CERTIFY_TRACK_CU,
            vec![certify_ix],
            &[tape_signer],
            commitment,
            true,
        )
        .await;
    let result = match sent {
        Ok(_) => Ok(()),
        Err(err) => match parse_tape_error(&err) {
            Some(TapeError::AlreadyCertified) => Ok(()),
            _ => Err(TapedriveError::Rpc(err)),
        },
    };
    submit.finish_result(&result);
    result
}

async fn certify_once<Blockchain: Rpc, Cluster: Api>(
    client: &Tapedrive<Blockchain, Cluster>,
    tape_key: &impl TapeOperator,
    written: &WrittenTrack,
    operation: Operation,
) -> Result<(), TapedriveError> {
    let collected = collect_certification(client, written, operation).await?;
    submit_certification(client, tape_key, written, &collected, operation).await
}

pub(crate) async fn wait_for_certified_track<Blockchain: Rpc, Cluster: Api>(
    client: &Tapedrive<Blockchain, Cluster>,
    tape: &Address,
    track_number: TrackNumber,
) -> Result<CompressedTrack, TapedriveError> {
    let result = retry_if(
        completion_poll_config(),
        None,
        || async {
            // Race every peer and accept the first response that is already
            // certified: the fastest responder may lag the certify tx, so a
            // fresh answer from any node wins over a quick stale one.
            let peers = query::queryable_peers(client)
                .await
                .map_err(TrackCompletionError::from)?;
            let mut requests = query::race_peers(peers, |node| {
                let req = GetTrackByNumberReq { tape: *tape, track_number };
                async move { client.api.get_track_by_number(node, &req).await }
            });

            let mut uncertified = None;
            while let Some(result) = requests.next().await {
                if let Ok(res) = result {
                    if res.track.is_certified() {
                        return Ok(res.track);
                    }
                    uncertified = Some(res.track);
                }
            }

            match uncertified {
                Some(_) => Err(TrackCompletionError::NotCertifiedYet),
                None => Err(TrackCompletionError::Client(TapedriveError::NotFound)),
            }
        },
        should_retry_track_completion,
    )
    .await;

    match result {
        Ok(track) => Ok(track),
        Err(TrackCompletionError::Client(err)) => Err(err),
        Err(TrackCompletionError::NotCertifiedYet) => Err(TapedriveError::Upload(
            UploadError::Network("track never became visible as certified".into()),
        )),
    }
}

pub async fn write_track<Blockchain: Rpc, Cluster: Api>(
    client: &Tapedrive<Blockchain, Cluster>,
    tape_key: &impl TapeOperator,
    name: &[u8],
    content_type: ContentType,
    data: &[u8],
) -> Result<CompressedTrack, TapedriveError> {
    let timer = client
        .timer(Operation::WriteTrack, Phase::Total)
        .bytes(data.len() as u64);
    let result = async {
        if data.len() <= SDK_INLINE_RAW_MAX_BYTES {
            let written = submit_raw(
                client,
                tape_key,
                name,
                content_type,
                data,
                Operation::WriteTrack,
            )
            .await?;
            return Ok(written.track);
        }

        let (written, plan) = submit_blob(
            client,
            tape_key,
            name,
            content_type,
            data,
            Operation::WriteTrack,
        )
        .await?;
        upload_with_retry(client, &written, &plan, Operation::WriteTrack).await?;
        certify_with_retry(client, tape_key, &written, Operation::WriteTrack).await
    }
    .await;
    timer.finish_result(&result);
    result
}

pub(crate) async fn upload_with_retry<Blockchain: Rpc, Cluster: Api>(
    client: &Tapedrive<Blockchain, Cluster>,
    written: &WrittenTrack,
    plan: &UploadPlan,
    operation: Operation,
) -> Result<(), TapedriveError> {
    retry_if(
        write_retry_config(),
        None,
        || upload(client, written, plan, operation),
        should_retry_upload,
    ).await
}

/// Submit certification with retry, without waiting for peer visibility.
pub(crate) async fn certify_submit_with_retry<Blockchain: Rpc, Cluster: Api>(
    client: &Tapedrive<Blockchain, Cluster>,
    tape_key: &impl TapeOperator,
    written: &WrittenTrack,
    operation: Operation,
) -> Result<(), TapedriveError> {
    retry_if(
        write_retry_config(),
        None,
        || certify_once(client, tape_key, written, operation),
        should_retry_certification,
    )
    .await
}

pub(crate) async fn certify_with_retry<Blockchain: Rpc, Cluster: Api>(
    client: &Tapedrive<Blockchain, Cluster>,
    tape_key: &impl TapeOperator,
    written: &WrittenTrack,
    operation: Operation,
) -> Result<CompressedTrack, TapedriveError> {
    certify_submit_with_retry(client, tape_key, written, operation).await?;

    let visible = client.timer(operation, Phase::CertifyVisible).chunks(1);
    let result = wait_for_certified_track(client, &tape_key.address(), written.track.track_number).await;
    visible.finish_result(&result);
    result
}

async fn fetch_track_written_event<Blockchain: Rpc, Cluster: Api>(
    client: &Tapedrive<Blockchain, Cluster>,
    signature: &Txid,
) -> Result<TrackWritten, TapedriveError> {
    let transaction = retry(
        write_retry_config(),
        None,
        || async { client.rpc().get_transaction(signature).await },
    )
    .await
    .map_err(TapedriveError::Rpc)?;

    extract_track_written_event(&transaction)
}

fn write_retry_config() -> RetryConfig {
    RetryConfig {
        base_delay: Duration::from_secs(1),
        max_delay: Duration::from_secs(60),
        max_retries: None,
    }
}

/// Flat short poll for states that land within seconds; a backoff would
/// oversleep the arrival.
fn completion_poll_config() -> RetryConfig {
    RetryConfig {
        base_delay: Duration::from_millis(POLL_INTERVAL_MS),
        max_delay: Duration::from_millis(POLL_INTERVAL_MS),
        max_retries: Some(40),
    }
}

fn extract_track_written_event(
    transaction: &EncodedConfirmedTransactionWithStatusMeta,
) -> Result<TrackWritten, TapedriveError> {
    let logs = transaction
        .transaction
        .meta
        .as_ref()
        .and_then(|meta| meta.log_messages.as_ref().map(|logs| logs))
        .ok_or_else(|| TapedriveError::InvalidArgument("transaction missing log messages".into()))?;

    for log in logs {
        if let Some(TapedriveEvent::TrackWritten(event)) = parse_event_data(log)
            .map_err(|error| TapedriveError::InvalidArgument(format!("parse event: {error}")))?
        {
            return Ok(event);
        }
    }

    Err(TapedriveError::NotFound)
}

#[cfg(test)]
mod tests {
    use tape_protocol::api::ApiError;

    use crate::error::TapedriveError;

    use super::should_retry_certification;
    use super::{inline_write_fits, SDK_INLINE_RAW_MAX_BYTES};
    use tape_api::instruction::TRACK_WRITE_MAX_BYTES;

    // The SDK inline write limit must always remain below the program limit.
    #[test]
    fn sdk_inline_raw_limit_is_below_program_limit() {
        assert_eq!(SDK_INLINE_RAW_MAX_BYTES, 825);
        assert!(SDK_INLINE_RAW_MAX_BYTES < TRACK_WRITE_MAX_BYTES);
    }

    #[test]
    fn inline_write_budget_accounts_for_object_trailer() {
        let name = b"object/name";
        let max_named_payload = (0..=SDK_INLINE_RAW_MAX_BYTES)
            .rev()
            .find(|payload_len| inline_write_fits(name, *payload_len))
            .expect("named inline payload should have some capacity");

        assert!(inline_write_fits(b"", SDK_INLINE_RAW_MAX_BYTES));
        assert!(!inline_write_fits(name, SDK_INLINE_RAW_MAX_BYTES));
        assert!(max_named_payload < SDK_INLINE_RAW_MAX_BYTES);
        assert!(inline_write_fits(name, max_named_payload));
        assert!(!inline_write_fits(name, max_named_payload + 1));
    }

    // Certification should retry when proof visibility lags behind peer state.
    #[test]
    fn certification_retries_stale_track_proof() {
        assert!(should_retry_certification(&TapedriveError::Peer(
            ApiError::StaleTrackProof,
        )));
    }

    #[test]
    fn certification_retries_missing_track_proof() {
        assert!(should_retry_certification(&TapedriveError::NotFound));
    }

    // EpochChanged means signatures were collected against a now-stale epoch;
    // certify_with_retry must recollect from peers, not just resubmit.
    #[test]
    fn certification_retries_epoch_changed() {
        let err = TapedriveError::Rpc(rpc::RpcError::Transaction(
            "custom program error: 0x34".to_string(),
        ));
        assert!(should_retry_certification(&err));
    }
}
