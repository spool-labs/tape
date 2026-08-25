use std::collections::{HashMap, HashSet};
use std::time::Duration;
use thiserror::Error;
use tracing::{debug, info, warn};

use rpc::{CommitmentLevel, EncodedConfirmedTransactionWithStatusMeta, Rpc, RpcError};
use rpc_client::parse_tape_error;
use tape_api::compute::{CERTIFY_TRACK_CU, TRACK_WRITE_CU};
use tape_api::errors::TapeError;
use tape_api::event::TrackWritten;
use tape_api::instruction::{build_certify_track_ix, build_track_write_ix, track_write_ix_len};
use tape_api::program::tapedrive::track_pda;
use solana_instruction::Instruction;
use tape_blocks::{parse_event_data, TapedriveEvent};
use tape_core::bft::min_correct;
use tape_core::erasure::GROUP_SIZE;
use tape_core::prelude::{
    BlobEncoding, CompressedTrack, EncodingProfile, EpochNumber, GroupIndex, StorageUnits,
    StripeCount, TrackNumber, TrackState,
};
use tape_core::track::data::{track_key, BlobData, BlobDataSlice, BlobInfo, TrackObjectInfo};
use tape_core::track::mirror::ArchiveMirror;
use tape_core::track::types::CompressedTrackProof;
use tape_core::types::ContentType;
use tape_crypto::hash::hash;
use tape_crypto::prelude::{Address, Hash};
use tape_crypto::tx::Txid;
use tape_protocol::api::CertifyRes;
use tape_protocol::Api;
use tape_protocol::api::GetTrackDataReq;
use tape_protocol::api::GetTrackByNumberReq;
use futures::stream::StreamExt;
use tape_retry::{retry, retry_if, Backoff, RetryConfig, Retryable};
use tape_slicer::num_stripes;
use tokio::time::sleep;

use crate::codec::encoder::BlobEncoder;
use crate::error::UploadError;
use crate::error::TapedriveError;
use crate::keys::operator::TapeOperator;
use crate::keys::tape_key::TapeKey;
use crate::metrics::{Operation, Phase};
use crate::stream::read::read_manifest;
use crate::tapedrive::Tapedrive;
use crate::track::{bootstrap_network_state, query};
use crate::transfer::certify::{CertificationCollector, CollectedSignatures};
use crate::transfer::uploader::{DistributedUploader, SliceWithProof};

// The program accepts up to 10 KiB for raw TrackWrite payloads.
pub const SDK_INLINE_RAW_MAX_BYTES: usize = 825;

/// Poll cadence for visibility and certification waits.
const POLL_INTERVAL_MS: u64 = 400;

/// How long to wait for a track to become visible before giving up.
///
/// A duration rather than an attempt count, because what is being waited on is
/// chain-side: nodes serve a track once their protocol state advances, and how
/// long that takes has nothing to do with how often the client asks. Counting
/// attempts couples the budget to `POLL_INTERVAL_MS`, so tightening the poll
/// for responsiveness silently divides the timeout by the same factor.
///
/// Sized for the worst case rather than the common one. A single self-voting
/// validator roots 31 slots behind head, so a cold localnet can hold a track
/// invisible for ~15 s; a live cluster clears it in a slot or two. This bounds
/// a pathology, it does not pace a write.
const VISIBILITY_TIMEOUT: Duration = Duration::from_secs(45);

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
    /// Unnamed tracks are excluded from object listings. Returns once
    /// certification is confirmed on-chain; peers may lag briefly.
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
    /// Named tracks on non-system tapes are materialized into object
    /// listings. Returns once certification is confirmed on-chain; peers may
    /// lag briefly.
    pub async fn write_named_track(
        &self,
        tape_key: &TapeKey,
        name: impl AsRef<[u8]>,
        content_type: ContentType,
        data: &[u8],
    ) -> Result<CompressedTrack, TapedriveError> {
        self.write_named_track_as(tape_key, name, content_type, data, None)
            .await
    }

    /// Write a named track to an existing tape.
    ///
    /// `plan` lets a caller that already encoded these bytes, deciding whether
    /// to write at all, hand that encode over instead of paying for it twice.
    pub async fn write_named_track_as(
        &self,
        operator: &impl TapeOperator,
        name: impl AsRef<[u8]>,
        content_type: ContentType,
        data: &[u8],
        plan: Option<UploadPlan>,
    ) -> Result<CompressedTrack, TapedriveError> {
        write_track_only(self, operator, name.as_ref(), content_type, data, plan).await
    }

    /// Write a named object track, resuming or overwriting an existing one.
    ///
    /// `existing` is the object's current track address, if the caller has it
    /// (e.g. the gateway's resolved object). A matching incomplete track is
    /// finished, a matching complete one is skipped, a different one is
    /// overwritten and reclaimed, and an absent one is written fresh. Callers
    /// that hold the address avoid a find_track scan on the hot path.
    pub async fn write_or_resume_track_as(
        &self,
        operator: &impl TapeOperator,
        name: impl AsRef<[u8]>,
        content_type: ContentType,
        data: &[u8],
        existing: Option<Address>,
    ) -> Result<ObjectWrite, TapedriveError> {
        resume_or_write_track(
            self,
            operator,
            name.as_ref(),
            content_type,
            data,
            existing,
            OnConflict::Overwrite,
        )
        .await
    }

    /// Reclaim the object a track backs, freeing its capacity.
    ///
    /// A stream (manifest) track is reclaimed whole: the manifest lists every
    /// chunk's track number, so each chunk is deleted and then the manifest. A
    /// single-track object is just deleted. Per-track best-effort: a failure
    /// leaves that track for a later overwrite or sweep, never erroring. Used to
    /// reclaim the prior object an overwrite orphaned.
    pub async fn reclaim_object_as(
        &self,
        operator: &impl TapeOperator,
        track: Address,
    ) -> Result<(), TapedriveError> {
        match self.get_track(&track).await {
            Ok(_) => {}
            Err(TapedriveError::NotFound) => return Ok(()),
            Err(error) => return Err(error),
        };

        // Only a stream's track parses as a manifest, so the parse is the test,
        // not the track size; each chunk it lists is reclaimed before the track.
        if let Ok((manifest, _)) = read_manifest(self, &track).await {
            let tape = operator.address();
            for entry in &manifest.chunks {
                let chunk = track_pda(tape, entry.track_number).0;
                if let Err(error) = self.delete_as(operator, chunk).await {
                    debug!(%error, chunk = %entry.track_number, "stream chunk reclaim failed");
                }
            }
        }

        if let Err(error) = self.delete_as(operator, track).await {
            debug!(%error, %track, "object track reclaim failed");
        }
        Ok(())
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
            return Err(TapedriveError::InvalidArgument("raw inline write exceeds SDK transaction limit; use write_track() or write_blob()".to_string()));
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
    ) -> Result<Vec<CertifyRes>, TapedriveError> {
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

        let result =
            certify_once(self, tape_key, written, Operation::Certify, &mut None, &[]).await;

        timer.finish_result(&result);
        result
    }
}

/// An etag and, for coded sizes, the encode that produced it.
///
/// Carrying the plan lets a write of the same bytes skip a second encode.
/// Inline-sized payloads hash directly and have no plan.
pub struct ContentEtag {
    pub etag: Hash,
    pub plan: Option<UploadPlan>,
}

/// The etag content ends up with when written as a named object: the value
/// hash for inline-sized payloads, the coded commitment otherwise.
///
/// Encoding runs in full for coded sizes, so this trades CPU for detecting
/// unchanged content before paying for a write. A caller that then writes can
/// hand the returned plan back rather than encoding twice.
pub async fn content_etag(data: &[u8]) -> Result<ContentEtag, TapedriveError> {
    if data.len() <= SDK_INLINE_RAW_MAX_BYTES {
        return Ok(ContentEtag {
            etag: hash(data),
            plan: None,
        });
    }
    let owned = data.to_vec();
    match tokio::task::spawn_blocking(move || prepare_plan(owned)).await {
        Ok(plan) => {
            let plan = plan?;
            Ok(ContentEtag {
                etag: plan.commitment_hash,
                plan: Some(plan),
            })
        }
        Err(join) => Err(TapedriveError::Encoding(format!("etag encode task failed: {join}"))),
    }
}

fn prepare_plan(data: Vec<u8>) -> Result<UploadPlan, TapedriveError> {
    let data_len = data.len();
    let profile = EncodingProfile::clay_default();
    let mut encoder = BlobEncoder::with_profile(profile);
    let (slices, merkle_root, leaves) = encoder
        .encode_with_leaves(data)
        .map_err(|e| TapedriveError::Encoding(e.to_string()))?;

    // Stripe geometry must come from the encoder so the plan always matches
    // the slice metadata it just produced.
    let stripe_size = encoder.stripe_size();

    Ok(UploadPlan {
        slices,
        commitment_hash: merkle_root,
        storage_units: StorageUnits::from_bytes(data_len as u64),
        profile,
        stripe_size,
        stripe_count: num_stripes(data_len, stripe_size),
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
    let track_address: Address = written.track;
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

/// The on-chain blob encoding a plan registers as; also the input to a coded
/// track's logical key.
pub(crate) fn plan_blob(plan: &UploadPlan) -> BlobEncoding {
    BlobEncoding {
        size: plan.storage_units,
        commitment: plan.commitment_hash,
        profile: plan.profile,
        stripe_size: StorageUnits::from_bytes(plan.stripe_size as u64),
        stripe_count: StripeCount(plan.stripe_count as u64),
        leaves: plan.leaves,
    }
}

fn build_blob_write(
    payer: Address,
    tape_key: &impl TapeOperator,
    name: &[u8],
    content_type: ContentType,
    logical_size: StorageUnits,
    plan: &UploadPlan,
) -> Result<(Instruction, BlobEncoding, Hash), TapedriveError> {
    let blob = plan_blob(plan);

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
    let track_address: Address = written.track;
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
) -> Result<Vec<CertifyRes>, TapedriveError> {
    let bytes = slices.iter().map(|slice| slice.data.len() as u64).sum();
    let chunks = slices.len() as u64;

    let locate = client.timer(operation, Phase::Locate);
    let state = bootstrap_network_state(client, Some(operation)).await;
    locate.finish_result(&state);

    let state = state?;

    let uploader = DistributedUploader::new(
        track_address,
        group,
        slices,
        &state,
        client.write_options.slice_concurrency,
    )
    .map_err(TapedriveError::Upload)?
    .with_reputation(client.reputation.clone());

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

/// Upload a track's slices, attempting immediately before any visibility
/// wait. Nodes ingest a register within milliseconds and slice-level
/// failures are retryable or left for recovery, so the eager attempt is
/// cheap and usually saves the whole visibility poll; only a retryable
/// failure falls back to waiting for quorum before trying again.
async fn upload<Blockchain: Rpc, Cluster: Api>(
    client: &Tapedrive<Blockchain, Cluster>,
    written: &WrittenTrack,
    plan: &UploadPlan,
    operation: Operation,
) -> Result<Vec<CertifyRes>, TapedriveError> {
    let eager = upload_once(
        client,
        written.address,
        written.track.group,
        plan.slices.clone(),
        operation,
    )
    .await;
    match eager {
        Ok(receipts) => return Ok(receipts),
        Err(err) if should_retry_upload(&err) => {
            debug!(error = %err, "eager upload failed; waiting for track visibility");
        }
        Err(err) => return Err(err),
    }

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
    let started = std::time::Instant::now();
    let mut first_seen: HashMap<Address, u64> = HashMap::new();

    loop {
        // Probe every peer concurrently: a round costs one round-trip
        // instead of one per peer.
        let probes = peers.iter().map(|node_id| async move {
            let req = GetTrackDataReq { track: track_address };
            match client.api.get_track_data(*node_id, &req).await {
                Ok(_) => (*node_id, true),
                Err(error) => {
                    debug!(
                        node = %node_id,
                        error = %error,
                        "track metadata not yet visible on node"
                    );
                    (*node_id, false)
                }
            }
        });
        let answers = futures::future::join_all(probes).await;
        let elapsed_ms = started.elapsed().as_millis() as u64;

        // Whether owners flip together or on their own schedules is the
        // difference between one gate opening and each node catching up, and
        // only the per-node arrival time distinguishes them.
        for (node, is_visible) in &answers {
            if *is_visible && !first_seen.contains_key(node) {
                first_seen.insert(*node, elapsed_ms);
                debug!(
                    node = %node,
                    at_ms = elapsed_ms,
                    attempt,
                    "track became visible on node"
                );
            }
        }

        let visible = answers.iter().filter(|(_, seen)| *seen).count();
        debug!(
            attempt,
            visible,
            required,
            target,
            elapsed_ms,
            "visibility round"
        );

        if visible >= required {
            info!(
                visible,
                target,
                required,
                attempts = attempt + 1,
                elapsed_ms,
                spread_ms = first_seen.values().max().copied().unwrap_or(0)
                    - first_seen.values().min().copied().unwrap_or(0),
                "track metadata visible"
            );
            return Ok(());
        }

        attempt += 1;
        if started.elapsed() >= VISIBILITY_TIMEOUT {
            return Err(TapedriveError::Upload(UploadError::Network(format!(
                "track metadata visible on {visible}/{target} nodes, need {required} \
                 after {:.1}s",
                started.elapsed().as_secs_f64()
            ))));
        }

        if attempt.is_multiple_of(5) {
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
        TapedriveError::RateLimited { .. } => true,
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
    banked: &[CertifyRes],
) -> Result<CollectedSignatures, TapedriveError> {
    let collect = client.timer(operation, Phase::CertifyCollect);
    let result = async {
        let state = bootstrap_network_state(client, Some(operation)).await?;
        let collector = CertificationCollector::with_defaults();
        collector
            .collect_signatures(
                client.api.as_ref(),
                &written.address,
                written.track.group,
                &state,
                banked,
            )
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
    if result.is_ok() {
        // The program accepted signatures for the collected epoch, which
        // proves the cached committee is current; batches skip the next
        // trust-window system read.
        client.state().touch();
    }
    submit.finish_result(&result);
    result
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

/// A completed object write: the track, and the ETag the object index will
/// record for it.
///
/// A coded track's canonical ETag is its blob commitment, which cannot be
/// derived from the track row alone. Reporting it here is what lets a caller
/// answer with the same ETag the index will serve later, instead of a
/// placeholder that changes once the index catches up.
pub struct ObjectWrite {
    pub track: CompressedTrack,
    pub etag: Hash,
}

impl ObjectWrite {
    /// An inline track is its own content, so the track row carries the ETag.
    fn inline(track: CompressedTrack) -> Self {
        let etag = track.value_hash;
        Self { track, etag }
    }

    fn coded(track: CompressedTrack, commitment: Hash) -> Self {
        Self {
            track,
            etag: commitment,
        }
    }
}

/// Write a track and report only the track, for callers with no object ETag to
/// answer with.
pub async fn write_track_only<Blockchain: Rpc, Cluster: Api>(
    client: &Tapedrive<Blockchain, Cluster>,
    tape_key: &impl TapeOperator,
    name: &[u8],
    content_type: ContentType,
    data: &[u8],
    plan: Option<UploadPlan>,
) -> Result<CompressedTrack, TapedriveError> {
    write_track(client, tape_key, name, content_type, data, plan)
        .await
        .map(|written| written.track)
}

/// Write a single track, registering, uploading, and certifying it
///
/// Returns once certification is confirmed on-chain. Peers may lag briefly
/// before reporting the track certified.
///
/// `plan` must be the encode of `data`; it only skips work, it does not change
/// what is written. `prepare_plan` is a pure function of the bytes, so a plan
/// from `content_etag` is identical to the one this would otherwise build.
pub async fn write_track<Blockchain: Rpc, Cluster: Api>(
    client: &Tapedrive<Blockchain, Cluster>,
    tape_key: &impl TapeOperator,
    name: &[u8],
    content_type: ContentType,
    data: &[u8],
    plan: Option<UploadPlan>,
) -> Result<ObjectWrite, TapedriveError> {
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
            return Ok(ObjectWrite::inline(written.track));
        }

        // The tape fetched before the register carries the track tree the
        // certify proof is built against; the processed-level register lets
        // the confirmed wait ride the event fetch.
        let tape = client.get_tape(&tape_key.address()).await?;
        let mut mirror = ArchiveMirror::new(&tape.tracks);

        let plan = match plan {
            Some(plan) => plan,
            None => encode_blob(client, data.to_vec(), Operation::WriteTrack).await?,
        };
        let sent = register_blob_processed(
            client,
            tape_key,
            name,
            content_type,
            StorageUnits::from_bytes(data.len() as u64),
            plan,
            Operation::WriteTrack,
        )
        .await?;
        let (written, plan) = resolve_sent_blob(client, sent).await?;
        let commitment = plan.commitment_hash;

        // A register landing between the tape fetch and ours breaks the
        // mirror's sequence; those writes certify through the peer path.
        let mirrored = mirror.append(&written.track).is_ok();

        let receipts = upload_with_retry(client, &written, &plan, Operation::WriteTrack).await?;

        if !mirrored {
            return certify_with_retry(
                client,
                tape_key,
                &written,
                Operation::WriteTrack,
                &receipts,
            )
            .await
            .map(|track| ObjectWrite::coded(track, commitment));
        }

        certify_with_mirror(
            client,
            tape_key,
            &mirror,
            &written,
            Operation::WriteTrack,
            &receipts,
        )
        .await?;

        // The certify transaction is confirmed, so the on-chain leaf is
        // final; readers poll peers, so their visibility is not waited on.
        Ok(ObjectWrite::coded(certified_track(&written.track), commitment))
    }
    .await;
    timer.finish_result(&result);
    result
}

/// One-call write that reserves a fresh tape or resumes an interrupted one.
///
/// A missing tape is reserved and written normally. An existing tape means a
/// prior call was interrupted after reserving, so its single track is resumed.
/// Backs the one-call write and write_named entry points.
pub(crate) async fn write_or_resume<Blockchain: Rpc, Cluster: Api>(
    client: &Tapedrive<Blockchain, Cluster>,
    tape_key: &TapeKey,
    name: &[u8],
    content_type: ContentType,
    data: &[u8],
    epochs: u64,
) -> Result<CompressedTrack, TapedriveError> {
    match client.get_tape(&tape_key.address()).await {
        Err(TapedriveError::Rpc(RpcError::AccountNotFound(_))) => {
            let capacity = StorageUnits::from_bytes(data.len() as u64);
            let reserve_capacity = capacity + StorageUnits::mb(1);

            let reserve = client.timer(Operation::Write, Phase::Reserve);
            let reserved = client.reserve(tape_key, reserve_capacity, epochs).await;
            reserve.finish_result(&reserved);
            reserved?;

            write_track_only(client, tape_key, name, content_type, data, None).await
        }
        // An existing tape means a prior interrupted call. The one-call write
        // dedicates a fresh tape to one blob at track 0, and reusing its key for
        // different data is a conflict, not an overwrite.
        Ok(_) => {
            let first = track_pda(tape_key.address(), TrackNumber(0)).0;
            resume_or_write_track(
                client,
                tape_key,
                name,
                content_type,
                data,
                Some(first),
                OnConflict::Reject,
            )
            .await
            .map(|written| written.track)
        }
        Err(other) => Err(other),
    }
}

/// How to handle a resume where the existing track holds different content.
pub(crate) enum OnConflict {
    /// The identity was reused for different data; refuse (one-call write).
    Reject,
    /// Last-write-wins: write the new content and reclaim the stale track.
    Overwrite,
}

/// Resume, skip, overwrite, or write a single named track at a known position.
///
/// `existing` is where the caller located this object's current track, if any:
/// track 0 for the one-call write, the resolved object for the gateway,
/// find_track for an SDK append. A matching certified track is returned as is;
/// a matching registered track is finished (upload the missing slices, then
/// certify); a missing track is written fresh. A track whose content differs is
/// rejected as a conflict, or overwritten and the stale track reclaimed, per
/// `on_conflict`.
pub(crate) async fn resume_or_write_track<Blockchain: Rpc, Cluster: Api>(
    client: &Tapedrive<Blockchain, Cluster>,
    operator: &impl TapeOperator,
    name: &[u8],
    content_type: ContentType,
    data: &[u8],
    existing: Option<Address>,
    on_conflict: OnConflict,
) -> Result<ObjectWrite, TapedriveError> {
    let Some(track_address) = existing else {
        return write_track(client, operator, name, content_type, data, None).await;
    };

    let existing_track = match client.get_track(&track_address).await {
        Ok(track) => track,
        // The located track is gone or never landed: write fresh.
        Err(TapedriveError::NotFound) => {
            return write_track(client, operator, name, content_type, data, None).await;
        }
        Err(other) => return Err(other),
    };

    // Recover the expected identity the register path produces (via
    // BlobDataSlice::meta), plus the coded upload plan needed to finish. Inline
    // tracks certify at register, so a matching inline track is already complete.
    let (expected_key, expected_value_hash, coded_plan) = if data.len() <= SDK_INLINE_RAW_MAX_BYTES {
        let slice = BlobDataSlice::Inline(data);
        let meta = slice
            .meta()
            .ok_or_else(|| TapedriveError::Encoding("inline blob has no commitment".into()))?;
        (track_key(name, &slice), meta.value_hash, None)
    } else {
        let (plan, key, value_hash) = coded_identity(client, name, data, Operation::Write).await?;
        (key, value_hash, Some(plan))
    };

    if existing_track.key == expected_key && existing_track.value_hash == expected_value_hash {
        // Same content: an inline track is certified at register, so a match is
        // already complete; a coded track is finished in place (skip if certified,
        // else upload + certify).
        return match coded_plan {
            Some(plan) => {
                let commitment = plan.commitment_hash;
                finish_coded_track(client, operator, existing_track, &plan, Operation::Write)
                    .await
                    .map(|track| ObjectWrite::coded(track, commitment))
            }
            None => Ok(ObjectWrite::inline(existing_track)),
        };
    }

    // Different content at this position.
    match on_conflict {
        OnConflict::Reject => Err(TapedriveError::WriteConflict {
            track_number: existing_track.track_number,
        }),
        OnConflict::Overwrite => {
            let written = write_track(client, operator, name, content_type, data, None).await?;
            // Reclaim the stale track; best-effort, never fails the write that
            // already landed. A failure leaves it for a later overwrite or sweep.
            if let Err(error) = client.delete_as(operator, track_address).await {
                debug!(%error, %track_address, "overwrite reclaim failed; stale track left for later reclaim");
            }
            Ok(written)
        }
    }
}

/// Confirm an existing track is the one this write would produce. A key or
/// value-hash mismatch means the tape holds a different track at this position;
/// used by the stream resume path, where a chunk mismatch is always a conflict.
pub(crate) fn ensure_track_matches(
    track: &CompressedTrack,
    expected_key: Hash,
    expected_value_hash: Hash,
) -> Result<(), TapedriveError> {
    if track.key == expected_key && track.value_hash == expected_value_hash {
        Ok(())
    } else {
        Err(TapedriveError::WriteConflict {
            track_number: track.track_number,
        })
    }
}

/// Encode `data` as a coded blob and return its upload plan plus the identity
/// (logical key, value hash) a registered coded track for `name` would carry.
/// Shared by the single-track and stream resume paths, which all compare an
/// existing track against a freshly encoded blob.
pub(crate) async fn coded_identity<Blockchain: Rpc, Cluster: Api>(
    client: &Tapedrive<Blockchain, Cluster>,
    name: &[u8],
    data: &[u8],
    operation: Operation,
) -> Result<(UploadPlan, Hash, Hash), TapedriveError> {
    let plan = encode_blob(client, data.to_vec(), operation).await?;
    let slice = BlobDataSlice::Coded(plan_blob(&plan));
    let meta = slice
        .meta()
        .ok_or_else(|| TapedriveError::Encoding("coded blob has no commitment".into()))?;
    let key = track_key(name, &slice);
    Ok((plan, key, meta.value_hash))
}

/// Complete a matching but incomplete coded track in place: return it if already
/// certified, else upload the missing slices and certify. Certification uses the
/// order-independent peer-proof path, not write_track's mirror shortcut, whose
/// proof is only valid for tracks certified in tape order.
pub(crate) async fn finish_coded_track<Blockchain: Rpc, Cluster: Api>(
    client: &Tapedrive<Blockchain, Cluster>,
    operator: &impl TapeOperator,
    existing: CompressedTrack,
    plan: &UploadPlan,
    operation: Operation,
) -> Result<CompressedTrack, TapedriveError> {
    if existing.is_certified() {
        return Ok(existing);
    }
    let written = WrittenTrack {
        address: track_pda(existing.tape, existing.track_number).0,
        track: existing,
    };
    let receipts = upload_with_retry(client, &written, plan, operation).await?;
    certify_with_retry(client, operator, &written, operation, &receipts).await
}

/// Certify a written track with a proof from a mirror seeded before its
/// register, skipping the peer proof query. The submit waits for the
/// client's commitment, so success means the certify is durable. Retryable
/// failures fall back to the peer-proof path, which keeps the collected
/// signatures and refetches the proof.
async fn certify_with_mirror<Blockchain: Rpc, Cluster: Api>(
    client: &Tapedrive<Blockchain, Cluster>,
    tape_key: &impl TapeOperator,
    mirror: &ArchiveMirror,
    written: &WrittenTrack,
    operation: Operation,
    banked: &[CertifyRes],
) -> Result<(), TapedriveError> {
    let collected = match collect_certification(client, written, operation, banked).await {
        Ok(collected) => collected,
        Err(err) if should_retry_certification(&err) => {
            debug!(error = %err, "signature collection failed; falling back to peer proofs");
            return certify_submit_with_retry(client, tape_key, written, operation, None, banked)
                .await;
        }
        Err(err) => return Err(err),
    };

    let Ok(proof) = mirror.proof_for(written.track.track_number) else {
        return certify_submit_with_retry(
            client,
            tape_key,
            written,
            operation,
            Some(collected),
            banked,
        )
        .await;
    };

    let submitted = submit_certification_with_proof(
        client,
        tape_key,
        proof,
        &collected,
        client.rpc().rpc().commitment(),
        operation,
    )
    .await;
    match submitted {
        Ok(()) => Ok(()),
        Err(err) if should_retry_certification(&err) => {
            warn!(error = %err, track = %written.address, "mirror-proof certify failed; falling back to peer proofs");
            let collected = (!needs_fresh_signatures(&err)).then_some(collected);
            certify_submit_with_retry(client, tape_key, written, operation, collected, banked).await
        }
        Err(err) => Err(err),
    }
}

pub(crate) async fn upload_with_retry<Blockchain: Rpc, Cluster: Api>(
    client: &Tapedrive<Blockchain, Cluster>,
    written: &WrittenTrack,
    plan: &UploadPlan,
    operation: Operation,
) -> Result<Vec<CertifyRes>, TapedriveError> {
    retry_if(
        write_retry_config(),
        None,
        || upload(client, written, plan, operation),
        should_retry_upload,
    ).await
}

/// One certification attempt: collect signatures unless the cache already
/// holds them, then submit. The cache survives across retries so a dropped
/// transaction or stale proof does not pay for a full re-collect.
async fn certify_once<Blockchain: Rpc, Cluster: Api>(
    client: &Tapedrive<Blockchain, Cluster>,
    tape_key: &impl TapeOperator,
    written: &WrittenTrack,
    operation: Operation,
    collected: &mut Option<CollectedSignatures>,
    banked: &[CertifyRes],
) -> Result<(), TapedriveError> {
    let signatures = match collected.take() {
        Some(signatures) => signatures,
        None => collect_certification(client, written, operation, banked).await?,
    };
    let result = submit_certification(client, tape_key, written, &signatures, operation).await;
    *collected = Some(signatures);
    result
}

/// Submit certification with retry, without waiting for peer visibility.
///
/// Signatures are collected once and reused across submit retries; the
/// proof refetches on every attempt since it must match the current tape
/// root. Only signature-scoped rejections force a fresh collection.
pub(crate) async fn certify_submit_with_retry<Blockchain: Rpc, Cluster: Api>(
    client: &Tapedrive<Blockchain, Cluster>,
    tape_key: &impl TapeOperator,
    written: &WrittenTrack,
    operation: Operation,
    mut collected: Option<CollectedSignatures>,
    banked: &[CertifyRes],
) -> Result<(), TapedriveError> {
    let mut backoff = Backoff::new(write_retry_config());

    loop {
        match certify_once(client, tape_key, written, operation, &mut collected, banked).await {
            Ok(()) => return Ok(()),
            Err(err) if should_retry_certification(&err) => {
                if needs_fresh_signatures(&err) {
                    // An epoch roll invalidates the cached committee along
                    // with the signatures; force the next collect to verify.
                    collected = None;
                    client.state().invalidate();
                }
                warn!(error = %err, track = %written.address, "certify attempt failed; retrying");
                match backoff.next_delay() {
                    Some(delay) => sleep(delay).await,
                    None => return Err(err),
                }
            }
            Err(err) => return Err(err),
        }
    }
}

/// True when a rejection invalidates the collected signatures themselves,
/// rather than the proof or the transaction attempt.
fn needs_fresh_signatures(err: &TapedriveError) -> bool {
    match err {
        TapedriveError::Rpc(rpc) => matches!(
            parse_tape_error(rpc),
            Some(TapeError::BadSignature | TapeError::EpochChanged)
        ),
        _ => false,
    }
}

pub(crate) async fn certify_with_retry<Blockchain: Rpc, Cluster: Api>(
    client: &Tapedrive<Blockchain, Cluster>,
    tape_key: &impl TapeOperator,
    written: &WrittenTrack,
    operation: Operation,
    banked: &[CertifyRes],
) -> Result<CompressedTrack, TapedriveError> {
    certify_submit_with_retry(client, tape_key, written, operation, None, banked).await?;

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

    use super::{
        content_etag, hash, inline_write_fits, prepare_plan, should_retry_certification,
        SDK_INLINE_RAW_MAX_BYTES,
    };
    use tape_api::instruction::TRACK_WRITE_MAX_BYTES;

    fn blob(len: usize) -> Vec<u8> {
        (0..len).map(|i| (i * 31 + 7) as u8).collect()
    }

    // a reused plan writes exactly what a fresh encode would have
    #[tokio::test]
    async fn plan_reuse() {
        let data = blob(SDK_INLINE_RAW_MAX_BYTES * 4);
        let computed = content_etag(&data).await.expect("etag");
        let plan = computed.plan.expect("coded payload carries its plan");
        let fresh = prepare_plan(data).expect("fresh plan");

        assert_eq!(plan.commitment_hash, fresh.commitment_hash);
        assert_eq!(computed.etag, fresh.commitment_hash);
        assert_eq!(plan.storage_units, fresh.storage_units);
        assert_eq!(plan.stripe_size, fresh.stripe_size);
        assert_eq!(plan.stripe_count, fresh.stripe_count);
        assert_eq!(plan.leaves, fresh.leaves);
        assert_eq!(plan.slices.len(), fresh.slices.len());
    }

    // inline payloads never encode, so they carry no plan
    #[tokio::test]
    async fn inline_no_plan() {
        let data = blob(SDK_INLINE_RAW_MAX_BYTES);
        let computed = content_etag(&data).await.expect("etag");

        assert!(computed.plan.is_none());
        assert_eq!(computed.etag, hash(&data));
    }

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
        let err = TapedriveError::Rpc(rpc::RpcError::Transaction {
            err: None,
            message: "custom program error: 0x34".to_string(),
            simulated: false,
        });
        assert!(should_retry_certification(&err));
    }
}
