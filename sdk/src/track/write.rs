use std::collections::{HashMap, HashSet};
use std::time::Duration;
use thiserror::Error;
use tracing::{debug, info, warn};

use std::future::Future;

use tape_rpc::{
    CommitmentLevel, EncodedConfirmedTransactionWithStatusMeta, Rpc, RpcError, TransactionError,
};
use tape_rpc_client::{parse_tape_error, sign_transaction};
use tape_api::compute::{CERTIFY_TRACK_CU, MAX_COMPUTE_UNIT_LIMIT, TRACK_WRITE_CU};
use tape_api::errors::TapeError;
use tape_api::event::TrackWritten;
use tape_api::instruction::{
    build_certify_track_ix, build_track_write_ix, track_write_ix_len, MAX_INLINE_WRITE,
};
use tape_api::program::tapedrive::TrackAddress;
use solana_hash::Hash as Blockhash;
use solana_instruction::Instruction;
use tape_blocks::{parse_event_data, TapedriveEvent};
use tape_core::erasure::MAX_GROUP_SIZE;
use tape_core::prelude::{
    BlobEncoding, CompressedTrack, EpochNumber, ErasureCode, StorageUnits, StripeCount,
    TrackNumber, TrackState,
};
use tape_core::track::data::{
    track_key, BlobData, BlobDataSlice, BlobInfo, TrackMeta, TrackObjectInfo,
};
use tape_core::spooler::VirtualGroup;
use tape_core::tape::TapeFlags;
use tape_core::track::archive::{TrackArchive, TRACK_TREE_HEIGHT};
use tape_core::track::mirror::ArchiveMirror;
use tape_core::track::types::CompressedTrackProof;
use tape_core::types::{ContentType, TapeNumber};
use tape_crypto::hash::hash;
use tape_crypto::merkle::{hash_leaf, MerkleLeafTree};
use tape_crypto::prelude::{Address, Hash};
use tape_crypto::tx::Txid;
use tape_protocol::api::CertifyRes;
use tape_protocol::Api;
use tape_protocol::api::GetTrackDataReq;
use tape_protocol::api::GetTrackReq;
use futures::stream::StreamExt;
use tape_retry::{retry_if, Backoff, RetryConfig, Retryable};
use tape_slicer::num_stripes;
use tokio::time::sleep;

use crate::codec::encoder::BlobEncoder;
use crate::error::UploadError;
use crate::error::TapedriveError;
use crate::keys::operator::TapeOperator;
use crate::keys::tape_key::TapeKey;
use crate::metrics::{Operation, Phase};
use crate::progress::{Step, UnitKind, LONE_UNIT};
use crate::stream::read::read_manifest;
use crate::tapedrive::Tapedrive;
use crate::track::{bootstrap_network_state, network_params, params_of, query};
use crate::transfer::certify::{CertificationCollector, CollectedSignatures};
use crate::transfer::uploader::{DistributedUploader, SliceWithProof, UploadProgress};

/// Poll cadence for visibility and certification waits.
const POLL_INTERVAL_MS: u64 = 400;

/// How long to wait for a track to become visible before giving up.
const VISIBILITY_TIMEOUT: Duration = Duration::from_secs(45);

/// Most registers one transaction holds, by its compute budget
const MAX_REGISTERS_PER_TRANSACTION: usize = (MAX_COMPUTE_UNIT_LIMIT / TRACK_WRITE_CU) as usize;

pub const UNNAMED_TRACK: &[u8] = b"";
pub const UNTYPED_TRACK: ContentType = ContentType::Unknown;

#[derive(Clone)]
pub struct UploadPlan {
    pub profile: ErasureCode,
    pub slices: Vec<SliceWithProof>,
    pub commitment_hash: Hash,
    pub storage_units: StorageUnits,
    pub stripe_size: usize,
    pub stripe_count: usize,
    pub leaves: [Hash; MAX_GROUP_SIZE],
    pub content_hash: Option<Hash>,
}

#[derive(Clone)]
pub struct WrittenTrack {
    pub address: Address,
    pub track: CompressedTrack,
}

/// An etag and, for coded sizes, the encode that produced it.
pub struct ContentEtag {
    pub etag: Hash,
    pub plan: Option<UploadPlan>,
}

/// A completed object write: the track, and the ETag the object index will
/// record for it.
pub struct ObjectWrite {
    pub track: CompressedTrack,
    pub etag: Hash,
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
        write_track_only(
            self,
            operator,
            name.as_ref(),
            content_type,
            data,
            plan,
            self.write_options.flags,
        )
        .await
    }

    /// Write a named object track, resuming or overwriting an existing one.
    ///
    /// `existing` is the object's current track address, if the caller has it
    /// (e.g. the gateway's resolved object). A matching incomplete track is
    /// finished, a matching complete one is skipped, a different one is
    /// overwritten and deleted, and an absent one is written fresh. Callers
    /// that hold the address avoid a find_track scan on the hot path. `plan`
    /// is a plan from `content_etag_named` for these bytes, which spares the encode.
    pub async fn write_or_resume_track_as(
        &self,
        operator: &impl TapeOperator,
        name: impl AsRef<[u8]>,
        content_type: ContentType,
        data: &[u8],
        existing: Option<Address>,
        plan: Option<UploadPlan>,
    ) -> Result<ObjectWrite, TapedriveError> {
        resume_or_write_track(
            self,
            operator,
            name.as_ref(),
            content_type,
            data,
            existing,
            plan,
            self.write_options.flags,
        )
        .await
    }

    /// Delete an object's track and, for a stream, its chunk tracks.
    ///
    /// An immutable object is refused before any of its tracks is touched.
    pub async fn delete_object_as(
        &self,
        operator: &impl TapeOperator,
        track: Address,
    ) -> Result<(), TapedriveError> {
        match self.get_track(&track).await {
            Ok(found) if found.is_immutable() => return Err(TapedriveError::Immutable),
            Ok(_) => {}
            Err(TapedriveError::NotFound) => return Ok(()),
            Err(error) => return Err(error),
        };

        // Only a stream's track parses as a manifest. Its chunks are deleted
        // before the track.
        if let Ok((manifest, _)) = read_manifest(self, &track).await {
            for entry in &manifest.chunks {
                if let Err(error) = self.delete_as(operator, entry.track).await {
                    debug!(%error, chunk = %entry.track, "stream chunk delete failed");
                }
            }
        }

        if let Err(error) = self.delete_as(operator, track).await {
            debug!(%error, %track, "object track delete failed");
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
            Operation::WriteRaw,
            self.write_options.flags,
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
        self.write_owned_blob(tape_key, name.as_ref(), content_type, data.to_vec())
            .await
    }

    /// Register a named blob track, moving its bytes into the encode
    pub(crate) async fn write_owned_blob(
        &self,
        tape_key: &TapeKey,
        name: &[u8],
        content_type: ContentType,
        data: Vec<u8>,
    ) -> Result<(WrittenTrack, UploadPlan), TapedriveError> {
        let timer = self
            .timer(Operation::WriteBlob, Phase::Total)
            .bytes(data.len() as u64);

        let result = submit_blob(
            self,
            tape_key,
            name,
            content_type,
            data,
            Operation::WriteBlob,
            self.write_options.flags,
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
        let bytes = plan.slices.iter().map(|slice| slice.data().len() as u64).sum();
        let timer = self
            .timer(Operation::Upload, Phase::Total)
            .bytes(bytes)
            .chunks(plan.slices.len() as u64);

        let result = upload(
            self,
            written,
            plan,
            Operation::Upload,
            &mut UploadProgress::default(),
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

    /// Certify a written track using receipts returned by [`Self::upload`].
    ///
    /// Tracks already certified during registration return immediately.
    /// Otherwise this waits for certification to confirm and become visible
    /// to storage peers. Supplying upload receipts avoids recollecting them.
    pub async fn certify_with_receipts(
        &self,
        tape_key: &TapeKey,
        written: &WrittenTrack,
        receipts: &[CertifyRes],
    ) -> Result<CompressedTrack, TapedriveError> {
        self.certify_with_receipts_as(tape_key, written, receipts)
            .await
    }

    /// Certify a written track as its owner or an authorized delegate.
    pub async fn certify_with_receipts_as(
        &self,
        operator: &impl TapeOperator,
        written: &WrittenTrack,
        receipts: &[CertifyRes],
    ) -> Result<CompressedTrack, TapedriveError> {
        let timer = self.timer(Operation::Certify, Phase::Total).chunks(1);
        let result = if written.track.is_certified() {
            Ok(written.track)
        } else {
            certify_with_retry(
                self,
                operator,
                written,
                Operation::Certify,
                receipts,
            )
            .await
        };

        timer.finish_result(&result);
        result
    }
}

/// The etag content ends up with when written as a named object: the value
/// hash for inline-sized payloads, the coded commitment otherwise.
///
/// The commitment depends on the network's profile, so the same bytes get a
/// different coded etag on a network with another profile.
///
/// Encoding runs in full for coded sizes, so this trades CPU for detecting
/// unchanged content before paying for a write. A caller that then writes can
/// hand the returned plan back rather than encoding twice.
pub async fn content_etag<Data>(
    profile: ErasureCode,
    data: Data,
) -> Result<ContentEtag, TapedriveError>
where
    Data: AsRef<[u8]> + Send + 'static,
{
    content_etag_named(profile, b"", data).await
}

/// `content_etag` for a named write: the object trailer shares the inline
/// budget with the payload, so a name can push a payload that would be inline
/// unnamed onto the coded path, and the etag must follow that decision.
pub async fn content_etag_named<Data>(
    profile: ErasureCode,
    name: &[u8],
    data: Data,
) -> Result<ContentEtag, TapedriveError>
where
    Data: AsRef<[u8]> + Send + 'static,
{
    if inline_write_fits(name, data.as_ref().len()) {
        return Ok(ContentEtag {
            etag: hash(data.as_ref()),
            plan: None,
        });
    }
    let encode = move || prepare_checked_plan(profile, data.as_ref());
    match tokio::task::spawn_blocking(encode).await {
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

/// Write a track and report only the track, for callers with no object ETag to
/// answer with.
pub async fn write_track_only<Blockchain: Rpc, Cluster: Api>(
    client: &Tapedrive<Blockchain, Cluster>,
    tape_key: &impl TapeOperator,
    name: &[u8],
    content_type: ContentType,
    data: &[u8],
    plan: Option<UploadPlan>,
    flags: u64,
) -> Result<CompressedTrack, TapedriveError> {
    write_track(client, tape_key, name, content_type, data, plan, flags)
        .await
        .map(|written| written.track)
}

/// Write a single track, registering, uploading, and certifying it
///
/// Returns once certification is confirmed on-chain. Peers may lag briefly
/// before reporting the track certified.
///
/// `plan` must be the encode of `data`; it only skips work, it does not change
/// what is written. `prepare_plan` is a pure function of the profile and the
/// bytes, so a plan from `content_etag` with this network's profile is
/// identical to the one this would otherwise build; an unpaired plan is refused.
pub async fn write_track<Blockchain: Rpc, Cluster: Api>(
    client: &Tapedrive<Blockchain, Cluster>,
    tape_key: &impl TapeOperator,
    name: &[u8],
    content_type: ContentType,
    data: &[u8],
    plan: Option<UploadPlan>,
    flags: u64,
) -> Result<ObjectWrite, TapedriveError> {
    if let Some(plan) = &plan {
        check_plan_matches(plan, data)?;
    }
    write_encoded_track(client, tape_key, name, content_type, data, plan, flags).await
}

/// Write a single track with a plan already known to be the encode of `data`
async fn write_encoded_track<Blockchain: Rpc, Cluster: Api>(
    client: &Tapedrive<Blockchain, Cluster>,
    tape_key: &impl TapeOperator,
    name: &[u8],
    content_type: ContentType,
    data: &[u8],
    plan: Option<UploadPlan>,
    flags: u64,
) -> Result<ObjectWrite, TapedriveError> {
    let timer = client
        .timer(Operation::WriteTrack, Phase::Total)
        .bytes(data.len() as u64);
    let result = async {
        if inline_write_fits(name, data.len()) {
            let written = submit_raw(
                client,
                tape_key,
                name,
                content_type,
                data,
                Operation::WriteTrack,
                flags,
            )
            .await?;
            return Ok(ObjectWrite::inline(written.track));
        }

        let size = StorageUnits::from_bytes(data.len() as u64);
        client.report(Step::Planned { kind: UnitKind::Track, sizes: vec![size] });

        // The tape fetched before the register carries the track tree the
        // certify proof is built against; the processed-level register lets
        // the confirmed wait ride the event fetch.
        let tape = client.get_tape(&tape_key.address()).await?;
        let mut mirror = ArchiveMirror::new(&tape.tracks);

        let plan = match plan {
            Some(plan) => plan,
            None => {
                client.report(Step::Encoding { unit: LONE_UNIT });
                encode_blob(client, data.to_vec(), Operation::WriteTrack).await?
            }
        };
        client.report(Step::Registering { unit: LONE_UNIT });
        let sent = register_blob_processed(
            client,
            tape_key,
            name,
            content_type,
            StorageUnits::from_bytes(data.len() as u64),
            plan,
            Operation::WriteTrack,
            flags,
        )
        .await?;
        let (written, plan) = resolve_sent_blob(client, sent).await?;
        let commitment = plan.commitment_hash;
        client.report(Step::Storing { unit: LONE_UNIT, track: written.address });

        // A register landing between the tape fetch and ours breaks the
        // mirror's sequence; those writes certify through the peer path.
        let mirrored = mirror.append(&written.track).is_ok();

        let receipts = upload_with_retry(client, &written, &plan, Operation::WriteTrack).await?;

        if !mirrored {
            let track = certify_with_retry(
                client,
                tape_key,
                &written,
                Operation::WriteTrack,
                &receipts,
            )
            .await?;
            client.report(Step::Done { track: written.address });
            return Ok(ObjectWrite::coded(track, commitment));
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
        client.report(Step::Done { track: written.address });

        // The certify transaction is confirmed, so the on-chain leaf is
        // final; readers poll peers, so their visibility is not waited on.
        Ok(ObjectWrite::coded(certified_track(&written.track), commitment))
    }
    .await;
    timer.finish_result(&result);
    result
}

#[derive(Debug, Error)]
enum TrackCompletionError {
    #[error(transparent)]
    Client(#[from] TapedriveError),

    #[error("track not certified yet")]
    NotCertifiedYet,
}

/// A submitted inline-track registration whose confirmed event has not yet
/// been resolved. Batch writers submit these at processed commitment so later
/// registrations can overlap the confirmation wait without losing track order.
pub(crate) struct SentRaw {
    signature: Txid,
    event: usize,
    key: Hash,
    kind: u64,
    state: u64,
    size: StorageUnits,
    value_hash: Hash,
}

/// A registered blob whose transaction has been sent but whose TrackWritten
/// event has not been resolved yet.
pub(crate) struct SentBlob {
    signature: Txid,
    event: usize,
    blob: BlobEncoding,
    key: Hash,
    plan: UploadPlan,
}

/// A register transaction built against the tape and not yet sent.
pub(crate) struct PreparedRegister {
    instruction: Instruction,
    payload: RegisterPayload,
    bytes: u64,
    unit: Option<usize>,
}

impl PreparedRegister {
    /// Report this register as `unit` of its transfer when it goes out
    pub(crate) fn for_unit(mut self, unit: usize) -> Self {
        self.unit = Some(unit);
        self
    }

    /// The unit of its transfer this register reports as, where it has one
    pub(crate) fn unit(&self) -> Option<usize> {
        self.unit
    }
}

/// What a register's track row is made of once its event comes back. The
/// event carries the flags, since the tape adds its own to the write's.
enum RegisterPayload {
    Raw {
        key: Hash,
        kind: u64,
        state: u64,
        size: StorageUnits,
        value_hash: Hash,
    },
    Blob {
        blob: Box<BlobEncoding>,
        key: Hash,
        plan: Box<UploadPlan>,
    },
}

/// A sent register, whichever shape it registered.
pub(crate) enum SentRegister {
    Raw(SentRaw),
    Blob(Box<SentBlob>),
}

impl SentRegister {
    /// The transaction this register went out in
    pub(crate) fn signature(&self) -> Txid {
        match self {
            SentRegister::Raw(raw) => raw.signature,
            SentRegister::Blob(blob) => blob.signature,
        }
    }
}

/// A register whose event came back, whichever shape it registered.
pub(crate) enum ResolvedRegister {
    Raw(WrittenTrack),
    Blob(WrittenTrack, Box<UploadPlan>),
}

impl RegisterPayload {
    fn into_sent(self, signature: Txid, event: usize) -> SentRegister {
        match self {
            RegisterPayload::Raw { key, kind, state, size, value_hash } => {
                SentRegister::Raw(SentRaw {
                    signature,
                    event,
                    key,
                    kind,
                    state,
                    size,
                    value_hash,
                })
            }
            RegisterPayload::Blob { blob, key, plan } => SentRegister::Blob(Box::new(
                SentBlob { signature, event, blob: *blob, key, plan: *plan },
            )),
        }
    }
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

/// Encode `data` and hash it, so a write can check the plan when it comes back
fn prepare_checked_plan(profile: ErasureCode, data: &[u8]) -> Result<UploadPlan, TapedriveError> {
    let content_hash = hash(data);
    let mut plan = prepare_plan(profile, data)?;
    plan.content_hash = Some(content_hash);
    Ok(plan)
}

fn prepare_plan(profile: ErasureCode, data: &[u8]) -> Result<UploadPlan, TapedriveError> {
    let data_len = data.len();
    let mut encoder =
        BlobEncoder::try_new(profile).map_err(|e| TapedriveError::Encoding(e.to_string()))?;
    let (slices, merkle_root, leaves) = encoder
        .encode_with_leaves(data)
        .map_err(|e| TapedriveError::Encoding(e.to_string()))?;

    // Stripe geometry must come from the encoder so the plan always matches
    // the slice metadata it just produced.
    let stripe_size = encoder.stripe_size();

    Ok(UploadPlan {
        profile,
        slices,
        commitment_hash: merkle_root,
        storage_units: StorageUnits::from_bytes(data_len as u64),
        stripe_size,
        stripe_count: num_stripes(data_len, stripe_size),
        leaves,
        content_hash: None,
    })
}

/// Refuse a plan that is not the encode of `data`, which would otherwise store
/// the wrong content under an etag computed from the right one.
fn check_plan_matches(plan: &UploadPlan, data: &[u8]) -> Result<(), TapedriveError> {
    if plan.storage_units.to_bytes() != data.len() as u64 || plan.content_hash != Some(hash(data)) {
        return Err(TapedriveError::Encoding(
            "upload plan was not encoded from these bytes".into(),
        ));
    }
    Ok(())
}

/// Encode a blob into its upload plan on a blocking thread; encoding a 64 MiB
/// chunk is seconds of CPU work that would otherwise stall the runtime.
///
/// The network's profile picks the code, so this fetches the network state
/// first when the client has none yet.
pub(crate) async fn encode_blob<Blockchain: Rpc, Cluster: Api>(
    client: &Tapedrive<Blockchain, Cluster>,
    data: Vec<u8>,
    operation: Operation,
) -> Result<UploadPlan, TapedriveError> {
    let profile = network_params(client, Some(operation)).await?.profile;

    let encode_timer = client
        .timer(operation, Phase::Encode)
        .bytes(data.len() as u64);
    let encode = move || prepare_plan(profile, &data);
    let result = match tokio::task::spawn_blocking(encode).await {
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
    flags: u64,
) -> Result<(WrittenTrack, UploadPlan), TapedriveError> {
    let register_timer = client
        .timer(operation, Phase::Register)
        .bytes(plan.storage_units.to_bytes())
        .chunks(1);
    let result = send_blob(
        client,
        tape_key,
        name,
        content_type,
        logical_size,
        plan,
        flags,
    )
    .await;
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
    track_write_ix_len(MAX_INLINE_WRITE, None)
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
    flags: u64,
) -> Result<WrittenTrack, TapedriveError> {
    submit_raw_with_logical_size(
        client,
        tape_key,
        name,
        content_type,
        StorageUnits::from_bytes(raw.len() as u64),
        raw,
        operation,
        flags,
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
    flags: u64,
) -> Result<WrittenTrack, TapedriveError> {
    let timer = client
        .timer(operation, Phase::Register)
        .bytes(raw.len() as u64)
        .chunks(1);

    let result = send_raw(
        client,
        tape_key,
        name,
        content_type,
        logical_size,
        raw,
        flags,
    )
    .await;

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
    flags: u64,
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
        flags,
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
    let meta = TrackMeta::inline(raw);
    let track = CompressedTrack {
        tape: written.tape,
        tape_number: written.tape_number,
        track_number: written.track_number,
        key,
        kind: meta.kind as u64,
        state: meta.state as u64,
        flags: written.flags,
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

/// Build the register transaction for an inline track.
pub(crate) fn prepare_raw_register<Blockchain: Rpc, Cluster: Api>(
    client: &Tapedrive<Blockchain, Cluster>,
    tape_key: &impl TapeOperator,
    name: &[u8],
    content_type: ContentType,
    logical_size: StorageUnits,
    raw: &[u8],
    flags: u64,
) -> Result<PreparedRegister, TapedriveError> {
    let payer = client.payer()?;
    let data = BlobDataSlice::Inline(raw);
    let meta = TrackMeta::inline(raw);
    let key = track_key(name, &data);
    let object = track_object(name, content_type, logical_size);
    let instruction = build_track_write_ix(
        payer.pubkey().into(),
        tape_key.pubkey().into(),
        tape_key.address(),
        BlobInfo {
            object,
            data: BlobData::Inline(raw.to_vec()),
        },
        flags,
    )
    .map_err(|error| TapedriveError::InvalidArgument(error.to_string()))?;

    Ok(PreparedRegister {
        instruction,
        payload: RegisterPayload::Raw {
            key,
            kind: meta.kind as u64,
            state: meta.state as u64,
            size: meta.size,
            value_hash: meta.value_hash,
        },
        bytes: raw.len() as u64,
        unit: None,
    })
}

/// Split a run of registers into transactions, in order, each holding as
/// many registers as its size and compute budget allow.
///
/// Every write appends to the tape's tree without a proof, so the registers
/// of one transaction land in its order.
pub(crate) fn pack_registers<Blockchain: Rpc, Cluster: Api>(
    client: &Tapedrive<Blockchain, Cluster>,
    tape_key: &impl TapeOperator,
    registers: Vec<PreparedRegister>,
) -> Result<Vec<Vec<PreparedRegister>>, TapedriveError> {
    let payer = client.payer()?;
    let tape_signer = tape_key.keypair();
    let mut packed: Vec<Vec<PreparedRegister>> = Vec::new();
    let mut instructions = Vec::with_capacity(MAX_REGISTERS_PER_TRANSACTION);
    for register in registers {
        if let Some(open) = packed.last_mut() {
            instructions.push(register.instruction.clone());
            let units = register_units(instructions.len());
            // The size check is all a sign needs, so any blockhash measures it.
            let is_fit = instructions.len() <= MAX_REGISTERS_PER_TRANSACTION
                && sign_transaction(payer, &[tape_signer], units, &instructions, Blockhash::default())
                    .is_ok();
            if is_fit {
                open.push(register);
                continue;
            }
            instructions.clear();
        }
        instructions.push(register.instruction.clone());
        packed.push(vec![register]);
    }
    Ok(packed)
}

/// The compute budget of a transaction that holds `count` registers
fn register_units(count: usize) -> u32 {
    TRACK_WRITE_CU.saturating_mul(u32::try_from(count).unwrap_or(u32::MAX))
}

/// Send a run of registers in the order they were built, packed into as few
/// transactions as hold them.
///
/// A chain that runs a JSON-RPC batch in order takes the whole run in one
/// round trip, signed against one blockhash; anywhere else the transactions
/// go one at a time, exactly as a lone register does.
pub(crate) async fn send_registers<Blockchain: Rpc, Cluster: Api>(
    client: &Tapedrive<Blockchain, Cluster>,
    tape_key: &impl TapeOperator,
    registers: Vec<PreparedRegister>,
    operation: Operation,
) -> Result<Vec<SentRegister>, TapedriveError> {
    let count = registers.len();
    let mut transactions = Vec::new();
    let mut payloads = Vec::with_capacity(count);
    let mut units = Vec::new();
    let mut bytes = 0;
    for packed in pack_registers(client, tape_key, registers)? {
        let mut instructions = Vec::with_capacity(packed.len());
        let mut packed_units = Vec::with_capacity(packed.len());
        let mut packed_payloads = Vec::with_capacity(packed.len());
        for register in packed {
            instructions.push(register.instruction);
            packed_payloads.push(register.payload);
            packed_units.push(register.unit);
            bytes += register.bytes;
        }
        transactions.push(instructions);
        payloads.push(packed_payloads);
        units.push(packed_units);
    }

    let register = client
        .timer(operation, Phase::Register)
        .bytes(bytes)
        .chunks(count as u64);
    let sent = submit_registers(client, tape_key, transactions, &units).await;
    register.finish_result(&sent);

    let mut out = Vec::with_capacity(count);
    for (packed, signature) in payloads.into_iter().zip(sent?) {
        for (event, payload) in packed.into_iter().enumerate() {
            out.push(payload.into_sent(signature, event));
        }
    }

    Ok(out)
}

/// Submit the run and hand back a signature per transaction, in order.
///
/// `units` holds each register's unit of its transfer, where it has one, and
/// the client's progress hears of each as its register goes out.
async fn submit_registers<Blockchain: Rpc, Cluster: Api>(
    client: &Tapedrive<Blockchain, Cluster>,
    tape_key: &impl TapeOperator,
    transactions: Vec<Vec<Instruction>>,
    units: &[Vec<Option<usize>>],
) -> Result<Vec<Txid>, TapedriveError> {
    let payer = client.payer()?;
    let tape_signer = tape_key.keypair();
    let mut signatures = Vec::with_capacity(transactions.len());

    if !client.rpc().rpc().executes_batches_in_order().await {
        for (instructions, units) in transactions.into_iter().zip(units) {
            for unit in units.iter().flatten() {
                client.report(Step::Registering { unit: *unit });
            }
            signatures.push(
                client
                    .rpc()
                    .send_instructions_with_signers_and_compute_unit_limit(
                        payer,
                        register_units(instructions.len()),
                        instructions,
                        &[tape_signer],
                        CommitmentLevel::Processed,
                        true,
                    )
                    .await?,
            );
        }

        return Ok(signatures);
    }

    // The whole run goes out in one round trip.
    for unit in units.iter().flatten().flatten() {
        client.report(Step::Registering { unit: *unit });
    }
    let blockhash = client.rpc().rpc().get_latest_blockhash().await?;
    let mut signed = Vec::with_capacity(transactions.len());
    for instructions in transactions {
        signed.push(sign_transaction(
            payer,
            &[tape_signer],
            register_units(instructions.len()),
            &instructions,
            blockhash,
        )?);
    }
    let transactions = signed;

    for (index, sent) in client
        .rpc()
        .rpc()
        .send_transactions(&transactions)
        .await?
        .into_iter()
        .enumerate()
    {
        match sent {
            Ok(signature) => signatures.push(signature),
            Err(err) => {
                warn!(
                    index,
                    entries = transactions.len(),
                    verdict = ?parse_tape_error(&err),
                    %err,
                    "register run refused",
                );
                return Err(TapedriveError::Rpc(err));
            }
        }
    }

    Ok(signatures)
}

/// Resolve the registers of one transaction from a single read of its events.
pub(crate) async fn resolve_registers<Blockchain: Rpc, Cluster: Api>(
    client: &Tapedrive<Blockchain, Cluster>,
    sent: Vec<SentRegister>,
) -> Result<Vec<ResolvedRegister>, TapedriveError> {
    let Some(signature) = sent.first().map(SentRegister::signature) else {
        return Ok(Vec::new());
    };
    if sent.iter().any(|register| register.signature() != signature) {
        return Err(TapedriveError::InvalidArgument(
            "registers resolved together must share a transaction".into(),
        ));
    }

    let events = fetch_track_written_events(client, &signature).await?;
    let mut resolved = Vec::with_capacity(sent.len());
    for register in sent {
        resolved.push(match register {
            SentRegister::Raw(raw) => ResolvedRegister::Raw(raw_written(raw, &events)?),
            SentRegister::Blob(blob) => {
                let (written, plan) = blob_written(*blob, &events)?;
                ResolvedRegister::Blob(written, Box::new(plan))
            }
        });
    }
    Ok(resolved)
}

/// Build an inline register's track from its transaction's events
fn raw_written(sent: SentRaw, events: &[TrackWritten]) -> Result<WrittenTrack, TapedriveError> {
    let written = events.get(sent.event).ok_or(TapedriveError::NotFound)?;
    let track = CompressedTrack {
        tape: written.tape,
        tape_number: written.tape_number,
        track_number: written.track_number,
        key: sent.key,
        kind: sent.kind,
        state: sent.state,
        flags: written.flags,
        size: sent.size,
        group: written.group,
        value_hash: sent.value_hash,
    };
    check_event(&track, written)?;
    Ok(WrittenTrack {
        address: written.track,
        track,
    })
}

pub(crate) async fn submit_blob<Blockchain: Rpc, Cluster: Api>(
    client: &Tapedrive<Blockchain, Cluster>,
    tape_key: &impl TapeOperator,
    name: &[u8],
    content_type: ContentType,
    data: Vec<u8>,
    operation: Operation,
    flags: u64,
) -> Result<(WrittenTrack, UploadPlan), TapedriveError> {
    let logical_size = StorageUnits::from_bytes(data.len() as u64);
    submit_blob_with_logical_size(
        client,
        tape_key,
        name,
        content_type,
        logical_size,
        data,
        operation,
        flags,
    )
    .await
}

pub(crate) async fn submit_blob_with_logical_size<Blockchain: Rpc, Cluster: Api>(
    client: &Tapedrive<Blockchain, Cluster>,
    tape_key: &impl TapeOperator,
    name: &[u8],
    content_type: ContentType,
    logical_size: StorageUnits,
    data: Vec<u8>,
    operation: Operation,
    flags: u64,
) -> Result<(WrittenTrack, UploadPlan), TapedriveError> {
    let plan = encode_blob(client, data, operation).await?;
    register_blob(
        client,
        tape_key,
        name,
        content_type,
        logical_size,
        plan,
        operation,
        flags,
    )
    .await
}

/// The on-chain blob encoding a plan registers as; also the input to a coded
/// track's logical key.
pub(crate) fn plan_blob(plan: &UploadPlan) -> BlobEncoding {
    BlobEncoding {
        size: plan.storage_units,
        commitment: plan.commitment_hash,
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
    flags: u64,
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
        flags,
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
    let events = fetch_track_written_events(client, &sent.signature).await?;
    blob_written(sent, &events)
}

/// Refuse an event that wrote some other leaf than the register built
fn check_event(track: &CompressedTrack, written: &TrackWritten) -> Result<(), TapedriveError> {
    if track.get_hash() != written.track_hash {
        return Err(TapedriveError::InvalidArgument(
            "track written event does not match its register".into(),
        ));
    }
    Ok(())
}

/// Build a coded register's track from its transaction's events
fn blob_written(
    sent: SentBlob,
    events: &[TrackWritten],
) -> Result<(WrittenTrack, UploadPlan), TapedriveError> {
    let written = events.get(sent.event).ok_or(TapedriveError::NotFound)?;
    let track_address: Address = written.track;
    let meta = BlobDataSlice::Coded(sent.blob).meta(sent.plan.profile)
        .ok_or(TapedriveError::InvalidArgument("invalid blob commitment".into()))?;

    let track = CompressedTrack {
        tape: written.tape,
        tape_number: written.tape_number,
        track_number: written.track_number,
        key: sent.key,
        kind: meta.kind as u64,
        state: meta.state as u64,
        flags: written.flags,
        size: meta.size,
        group: written.group,
        value_hash: meta.value_hash,
    };

    check_event(&track, written)?;

    Ok((
        WrittenTrack {
            address: track_address,
            track,
        },
        sent.plan,
    ))
}

/// Build the register transaction for an encoded blob.
pub(crate) fn prepare_blob_register<Blockchain: Rpc, Cluster: Api>(
    client: &Tapedrive<Blockchain, Cluster>,
    tape_key: &impl TapeOperator,
    name: &[u8],
    content_type: ContentType,
    logical_size: StorageUnits,
    plan: UploadPlan,
    flags: u64,
) -> Result<PreparedRegister, TapedriveError> {
    let payer = client.payer()?;
    let bytes = plan.storage_units.to_bytes();
    let (instruction, blob, key) = build_blob_write(
        payer.pubkey().into(),
        tape_key,
        name,
        content_type,
        logical_size,
        &plan,
        flags,
    )?;

    Ok(PreparedRegister {
        instruction,
        payload: RegisterPayload::Blob {
            blob: Box::new(blob),
            key,
            plan: Box::new(plan),
        },
        bytes,
        unit: None,
    })
}

pub(crate) async fn register_blob_processed<Blockchain: Rpc, Cluster: Api>(
    client: &Tapedrive<Blockchain, Cluster>,
    tape_key: &impl TapeOperator,
    name: &[u8],
    content_type: ContentType,
    logical_size: StorageUnits,
    plan: UploadPlan,
    operation: Operation,
    flags: u64,
) -> Result<SentBlob, TapedriveError> {
    let register = prepare_blob_register(
        client,
        tape_key,
        name,
        content_type,
        logical_size,
        plan,
        flags,
    )?;
    let sent = send_registers(client, tape_key, vec![register], operation).await?;

    match sent.into_iter().next() {
        Some(SentRegister::Blob(sent)) => Ok(*sent),
        Some(SentRegister::Raw(_)) | None => Err(TapedriveError::InvalidArgument(
            "coded register came back as another shape".into(),
        )),
    }
}

async fn send_blob<Blockchain: Rpc, Cluster: Api>(
    client: &Tapedrive<Blockchain, Cluster>,
    tape_key: &impl TapeOperator,
    name: &[u8],
    content_type: ContentType,
    logical_size: StorageUnits,
    plan: UploadPlan,
    flags: u64,
) -> Result<(WrittenTrack, UploadPlan), TapedriveError> {
    let payer = client.payer()?;
    let tape_signer = tape_key.keypair();
    let (write_ix, blob, key) =
        build_blob_write(
        payer.pubkey().into(),
        tape_key,
        name,
        content_type,
        logical_size,
        &plan,
        flags,
    )?;

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

    resolve_sent_blob(client, SentBlob { signature, event: 0, blob, key, plan }).await
}

async fn upload_once<Blockchain: Rpc, Cluster: Api>(
    client: &Tapedrive<Blockchain, Cluster>,
    written: &WrittenTrack,
    slices: Vec<SliceWithProof>,
    operation: Operation,
    progress: &mut UploadProgress,
) -> Result<Vec<CertifyRes>, TapedriveError> {
    let bytes = slices.iter().map(|slice| slice.data().len() as u64).sum();
    let chunks = slices.len() as u64;

    let locate = client.timer(operation, Phase::Locate);
    let state = bootstrap_network_state(client, Some(operation)).await;
    locate.finish_result(&state);

    let state = state?;

    let uploader = DistributedUploader::new(
        written.address,
        written.track.group,
        slices,
        &state,
        client.write_options.slice_concurrency,
    )
    .map_err(TapedriveError::Upload)?
    .with_reputation(client.reputation.clone())
    .with_gate(client.push_gate.clone())
    .with_progress(client.progress.clone());

    let store = client
        .timer(operation, Phase::Store)
        .bytes(bytes)
        .chunks(chunks);

    let result = uploader
        .upload_all(client.api.clone(), progress)
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
    progress: &mut UploadProgress,
) -> Result<Vec<CertifyRes>, TapedriveError> {
    let eager = upload_once(client, written, plan.slices.clone(), operation, progress).await;
    match eager {
        Ok(receipts) => return Ok(receipts),
        Err(err) if should_retry_upload(&err) => {
            debug!(error = %err, "eager upload failed; waiting for track visibility");
        }
        Err(err) => return Err(err),
    }

    let visibility = client.timer(operation, Phase::Visibility);

    let result = wait_for_visibility(client, written, operation).await;

    visibility.finish_result(&result);
    result?;

    upload_once(client, written, plan.slices.clone(), operation, progress).await
}

async fn wait_for_visibility<Blockchain: Rpc, Cluster: Api>(
    client: &Tapedrive<Blockchain, Cluster>,
    written: &WrittenTrack,
    operation: Operation,
) -> Result<(), TapedriveError> {
    let state = bootstrap_network_state(client, Some(operation)).await?;
    let track_address = written.address;

    let group_peers = state.group_peers(written.track.group);
    let required = params_of(&state)?.thresholds.certify as usize;

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
            let req = GetTrackDataReq {
                track: track_address,
            };
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
    .map(|_| ())
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
) -> Result<Option<Txid>, TapedriveError> {
    let payer = client.payer()?;
    let tape_signer = tape_key.keypair();

    let certify_ix = build_certify_track_ix(
        payer.pubkey().into(),
        tape_key.pubkey().into(),
        proof,
        EpochNumber(collected.epoch),
        collected.group,
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
        Ok(signature) => Ok(Some(signature)),
        Err(err) => match parse_tape_error(&err) {
            Some(TapeError::AlreadyCertified) => Ok(None),
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

/// Wait for certifies sent at processed to reach the client's commitment
pub(crate) async fn confirm_certifies<Blockchain: Rpc, Cluster: Api>(
    client: &Tapedrive<Blockchain, Cluster>,
    signatures: &[Txid],
    operation: Operation,
) -> Result<(), TapedriveError> {
    let confirm = client
        .timer(operation, Phase::CertifyVisible)
        .chunks(signatures.len() as u64);
    let waits = signatures
        .iter()
        .map(|signature| client.rpc().rpc().wait_for_signature(signature));
    let mut result = Ok(());
    for waited in futures::future::join_all(waits).await {
        let executed = match waited {
            Ok(executed) => executed,
            Err(error) => {
                result = Err(TapedriveError::Rpc(error));
                break;
            }
        };
        if let Err(error) = executed {
            result = Err(TapedriveError::Rpc(RpcError::Transaction {
                message: error.to_string(),
                err: Some(error),
                simulated: false,
            }));
            break;
        }
    }
    confirm.finish_result(&result);
    result
}

pub(crate) async fn wait_for_certified_track<Blockchain: Rpc, Cluster: Api>(
    client: &Tapedrive<Blockchain, Cluster>,
    track: &Address,
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
                let req = GetTrackReq { track: *track };
                async move { client.api.get_track(node, &req).await }
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
        // Nodes can trail the chain; the track account is the last word.
        Err(TrackCompletionError::NotCertifiedYet) => {
            match client.get_track(track).await {
                Ok(track) if track.is_certified() => Ok(track),
                _ => Err(TapedriveError::Upload(UploadError::Network(
                    "track never became visible as certified".into(),
                ))),
            }
        }
    }
}

/// The content address a track with this key and content gets on `tape`
fn content_address(
    tape: Address,
    tape_number: TapeNumber,
    key: Hash,
    meta: &TrackMeta,
    flags: u64,
) -> Hash {
    CompressedTrack {
        tape,
        tape_number,
        track_number: TrackNumber(0),
        key,
        kind: meta.kind as u64,
        state: meta.state as u64,
        flags,
        size: meta.size,
        group: VirtualGroup(0),
        value_hash: meta.value_hash,
    }
    .content_address()
}

/// The content address of a coded track for `name` registered with this plan
pub(crate) fn coded_content_address(
    tape: Address,
    tape_number: TapeNumber,
    name: &[u8],
    plan: &UploadPlan,
    flags: u64,
) -> Result<Hash, TapedriveError> {
    let slice = BlobDataSlice::Coded(plan_blob(plan));
    let meta = slice
        .meta(plan.profile)
        .ok_or_else(|| TapedriveError::Encoding("coded blob has no commitment".into()))?;
    Ok(content_address(tape, tape_number, track_key(name, &slice), &meta, flags))
}

/// The content address of an inline track for `name` holding these bytes
pub(crate) fn inline_content_address(
    tape: Address,
    tape_number: TapeNumber,
    name: &[u8],
    data: &[u8],
    flags: u64,
) -> Hash {
    let key = track_key(name, &BlobDataSlice::Inline(data));
    content_address(tape, tape_number, key, &TrackMeta::inline(data), flags)
}

/// Tracks one listing page of a resumed write asks for
const RESUME_LIST_PAGE: u32 = 1024;

/// What a tape holds, by content address, for a resumed write to match against
pub(crate) struct TapeContents {
    tape_number: TapeNumber,
    tracks: HashMap<Hash, CompressedTrack>,
    flags: u64,
}

impl TapeContents {
    /// List the tape, asking again while the listing trails the chain.
    ///
    /// A peer that has not seen the newest write would hide it, and the resume
    /// would write it twice. A listing that rebuilds the tape's root on chain
    /// is complete. One that never does is used as it is, since a miss only
    /// costs a second track.
    pub(crate) async fn load<Blockchain: Rpc, Cluster: Api>(
        client: &Tapedrive<Blockchain, Cluster>,
        tape: Address,
    ) -> Result<Self, TapedriveError> {
        let mut backoff = Backoff::new(completion_poll_config());
        loop {
            let account = client.get_tape(&tape).await?;
            let flags = TapeFlags::track_flags(account.flags);
            let archive = account.tracks;
            if archive.tree.next_index == 0 {
                return Ok(Self::from_tracks(Vec::new(), flags, account.id));
            }

            // A peer may still list tracks from before the tape's tree was
            // cleared. They have no leaf, and a write must never match one.
            let mut listed = list_tape(client, tape).await?;
            listed.retain(|track| archive.position(track.track_number).is_some());

            if listed_root(&listed, &archive) == Some(archive.tree.root()) {
                return Ok(Self::from_tracks(listed, flags, account.id));
            }
            match backoff.next_delay() {
                Some(delay) => sleep(delay).await,
                None => return Ok(Self::from_tracks(listed, flags, account.id)),
            }
        }
    }

    /// Of the tracks with one content, the certified one with the highest
    /// number stays, or the highest-numbered when none is certified, so an
    /// unfinished write gets finished.
    pub(crate) fn from_tracks(
        listed: Vec<CompressedTrack>,
        flags: u64,
        tape_number: TapeNumber,
    ) -> Self {
        let mut tracks: HashMap<Hash, CompressedTrack> = HashMap::new();
        for track in listed {
            let held = tracks.entry(track.content_address()).or_insert(track);
            if track.is_certified() || !held.is_certified() {
                *held = track;
            }
        }
        Self { tracks, flags, tape_number }
    }

    /// A write lands with the tape's flags added, and a content-address
    /// lookup has to match that
    pub(crate) fn track_flags(&self, flags: u64) -> u64 {
        flags | self.flags
    }

    pub(crate) fn tape_number(&self) -> TapeNumber {
        self.tape_number
    }

    pub(crate) fn find(&self, content: &Hash) -> Option<CompressedTrack> {
        self.tracks.get(content).copied()
    }
}

/// Every track on a tape, in track-number order
async fn list_tape<Blockchain: Rpc, Cluster: Api>(
    client: &Tapedrive<Blockchain, Cluster>,
    tape: Address,
) -> Result<Vec<CompressedTrack>, TapedriveError> {
    let mut tracks = Vec::new();
    let mut cursor = None;
    loop {
        let (page, next) = client
            .list_tracks_by_tape(&tape, cursor, RESUME_LIST_PAGE)
            .await?;
        tracks.extend(page);
        match next {
            Some(next) => cursor = Some(next),
            None => return Ok(tracks),
        }
    }
}

/// The root the listed tracks give the tape's tree, or nothing when a track
/// sits outside the tree as the chain has it
fn listed_root(listed: &[CompressedTrack], archive: &TrackArchive) -> Option<Hash> {
    let leaf_count = usize::try_from(archive.tree.next_index).ok()?;
    let mut leaves = vec![hash_leaf(&[]); leaf_count];
    for track in listed {
        let position = usize::try_from(archive.position(track.track_number)?).ok()?;
        *leaves.get_mut(position)? = track.get_hash();
    }
    let tree = MerkleLeafTree::new(&leaves, TRACK_TREE_HEIGHT).ok()?;
    Some(tree.root())
}

/// One-call write that reserves a fresh tape, or writes to the one already there.
///
/// A missing tape is reserved and written normally. An existing tape gets the
/// track appended, unless the client asks to resume: then the tape is listed
/// first, and a track that holds this content is finished or returned.
/// Backs the one-call write and write_named entry points.
pub(crate) async fn write_or_resume<Blockchain: Rpc, Cluster: Api>(
    client: &Tapedrive<Blockchain, Cluster>,
    tape_key: &TapeKey,
    name: &[u8],
    content_type: ContentType,
    data: &[u8],
    epochs: u64,
    flags: u64,
) -> Result<CompressedTrack, TapedriveError> {
    match client.get_tape(&tape_key.address()).await {
        Err(TapedriveError::Rpc(RpcError::AccountNotFound(_))) => {
            let capacity = StorageUnits::from_bytes(data.len() as u64);
            let reserve_capacity = capacity + StorageUnits::mb(1);

            let reserve = client.timer(Operation::Write, Phase::Reserve);
            let reserved = client.reserve(tape_key, reserve_capacity, epochs).await;
            reserve.finish_result(&reserved);
            reserved?;

            write_track_only(client, tape_key, name, content_type, data, None, flags).await
        }
        Ok(_) if !client.write_options.resume => {
            write_track_only(client, tape_key, name, content_type, data, None, flags).await
        }
        Ok(_) => resume_track(client, tape_key, name, content_type, data, flags).await,
        Err(other) => Err(other),
    }
}

/// Write a track unless the tape already holds this content, which is then
/// finished or returned as it is.
async fn resume_track<Blockchain: Rpc, Cluster: Api>(
    client: &Tapedrive<Blockchain, Cluster>,
    tape_key: &TapeKey,
    name: &[u8],
    content_type: ContentType,
    data: &[u8],
    flags: u64,
) -> Result<CompressedTrack, TapedriveError> {
    let tape = tape_key.address();
    let contents = TapeContents::load(client, tape).await?;
    let flags = contents.track_flags(flags);

    if inline_write_fits(name, data.len()) {
        // Inline tracks certify at register, so one found is complete.
        let content = inline_content_address(tape, contents.tape_number(), name, data, flags);
        return match contents.find(&content) {
            Some(existing) => Ok(existing),
            None => write_track_only(client, tape_key, name, content_type, data, None, flags).await,
        };
    }

    let plan = encode_blob(client, data.to_vec(), Operation::Write).await?;
    let content = coded_content_address(tape, contents.tape_number(), name, &plan, flags)?;
    match contents.find(&content) {
        Some(existing) => {
            let size = StorageUnits::from_bytes(data.len() as u64);
            client.report(Step::Planned { kind: UnitKind::Track, sizes: vec![size] });
            finish_coded_track(client, tape_key, existing, &plan, Operation::Write, LONE_UNIT).await
        }
        None => write_encoded_track(client, tape_key, name, content_type, data, Some(plan), flags)
            .await
            .map(|written| written.track),
    }
}

/// Resume, skip, overwrite, or write a single named track.
///
/// `existing` is where the caller located this object's current track, if any:
/// the resolved object for the gateway, find_track for an SDK append. A
/// matching certified track is returned as is; a matching registered track is
/// finished (upload the missing slices, then certify); a missing track is
/// written fresh. A track whose content differs is overwritten: the new
/// content is written and the old track deleted.
pub(crate) async fn resume_or_write_track<Blockchain: Rpc, Cluster: Api>(
    client: &Tapedrive<Blockchain, Cluster>,
    operator: &impl TapeOperator,
    name: &[u8],
    content_type: ContentType,
    data: &[u8],
    existing: Option<Address>,
    plan: Option<UploadPlan>,
    flags: u64,
) -> Result<ObjectWrite, TapedriveError> {
    let Some(track_address) = existing else {
        return write_track(client, operator, name, content_type, data, plan, flags).await;
    };

    let existing_track = match client.get_track(&track_address).await {
        Ok(track) => track,
        // The located track is gone or never landed: write fresh.
        Err(TapedriveError::NotFound) => {
            return write_track(client, operator, name, content_type, data, plan, flags).await;
        }
        Err(other) => return Err(other),
    };

    // Recover the expected identity the register path produces (via
    // BlobDataSlice::meta), plus the coded upload plan needed to finish. Inline
    // tracks certify at register, so a matching inline track is already complete.
    let (expected_key, expected_value_hash, coded_plan) = if inline_write_fits(name, data.len()) {
        let slice = BlobDataSlice::Inline(data);
        let meta = TrackMeta::inline(data);
        (track_key(name, &slice), meta.value_hash, None)
    } else {
        let plan = match plan {
            Some(plan) => {
                check_plan_matches(&plan, data)?;
                plan
            }
            None => encode_blob(client, data.to_vec(), Operation::Write).await?,
        };
        let (key, value_hash) = coded_key(name, &plan)?;
        (key, value_hash, Some(plan))
    };

    if existing_track.key == expected_key && existing_track.value_hash == expected_value_hash {
        // Same content: an inline track is certified at register, so a match is
        // already complete; a coded track is finished in place (skip if certified,
        // else upload + certify).
        return match coded_plan {
            Some(plan) => {
                let commitment = plan.commitment_hash;
                let size = StorageUnits::from_bytes(data.len() as u64);
                client.report(Step::Planned { kind: UnitKind::Track, sizes: vec![size] });
                finish_coded_track(client, operator, existing_track, &plan, Operation::Write, LONE_UNIT)
                    .await
                    .map(|track| ObjectWrite::coded(track, commitment))
            }
            None => Ok(ObjectWrite::inline(existing_track)),
        };
    }

    // Different content under this name. The identity encode is the write's
    // encode too.
    let written =
        write_encoded_track(client, operator, name, content_type, data, coded_plan, flags).await?;
    // Delete the old track; best-effort, never fails the write that already
    // landed. A failure leaves it for a later overwrite or sweep. An immutable
    // old track stays, and the name now resolves to the new one.
    if existing_track.is_immutable() {
        return Ok(written);
    }
    if let Err(error) = client.delete_as(operator, track_address).await {
        debug!(%error, %track_address, "overwrite delete failed; old track left for a later delete");
    }
    Ok(written)
}

/// The logical key and value hash of a coded track for `name` registered with this plan
fn coded_key(name: &[u8], plan: &UploadPlan) -> Result<(Hash, Hash), TapedriveError> {
    let slice = BlobDataSlice::Coded(plan_blob(plan));
    let meta = slice
        .meta(plan.profile)
        .ok_or_else(|| TapedriveError::Encoding("coded blob has no commitment".into()))?;
    Ok((track_key(name, &slice), meta.value_hash))
}

/// Complete a matching but incomplete coded track in place: return it if already
/// certified, else upload the missing slices and certify. Certification uses the
/// order-independent peer-proof path, not write_track's mirror shortcut, whose
/// proof is only valid for tracks certified in tape order.
///
/// `unit` is the track's unit in its transfer's plan, which the client's
/// progress hears as skipped or as stored and done.
pub(crate) async fn finish_coded_track<Blockchain: Rpc, Cluster: Api>(
    client: &Tapedrive<Blockchain, Cluster>,
    operator: &impl TapeOperator,
    existing: CompressedTrack,
    plan: &UploadPlan,
    operation: Operation,
    unit: usize,
) -> Result<CompressedTrack, TapedriveError> {
    if existing.is_certified() {
        client.report(Step::Skipped { unit });
        return Ok(existing);
    }
    let written = WrittenTrack {
        address: existing.address(),
        track: existing,
    };
    client.report(Step::Storing { unit, track: written.address });
    let receipts = upload_with_retry(client, &written, plan, operation).await?;
    let certified = certify_with_retry(client, operator, &written, operation, &receipts).await?;
    client.report(Step::Done { track: written.address });
    Ok(certified)
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
        Ok(_) => Ok(()),
        Err(err) if should_retry_certification(&err) => {
            warn!(error = %err, track = %written.address, "mirror-proof certify failed; falling back to peer proofs");
            let collected = (!needs_fresh_signatures(&err)).then_some(collected);
            certify_submit_with_retry(client, tape_key, written, operation, collected, banked).await
        }
        Err(err) => Err(err),
    }
}

/// End a write whose track is no longer on its tape. The tape was closed,
/// rewound or renewed after the track was registered, so no retry can store or
/// certify it. The retry loops ask after a failed attempt.
///
/// Only the chain's answer counts. A failed read of the tape leaves the
/// caller to its retry.
async fn ensure_on_tape<Blockchain: Rpc, Cluster: Api>(
    client: &Tapedrive<Blockchain, Cluster>,
    track: &CompressedTrack,
) -> Result<(), TapedriveError> {
    let has_left = match client.get_tape(&track.tape).await {
        Ok(tape) => {
            tape.id != track.tape_number || track.track_number < tape.tracks.tree_start()
        }
        Err(TapedriveError::Rpc(RpcError::AccountNotFound(_))) => true,
        Err(_) => false,
    };

    match has_left {
        true => Err(TapedriveError::TrackLeftTape),
        false => Ok(()),
    }
}

pub(crate) async fn upload_with_retry<Blockchain: Rpc, Cluster: Api>(
    client: &Tapedrive<Blockchain, Cluster>,
    written: &WrittenTrack,
    plan: &UploadPlan,
    operation: Operation,
) -> Result<Vec<CertifyRes>, TapedriveError> {
    // Retries push only spools that are still missing.
    let mut progress = UploadProgress::default();
    let mut backoff = Backoff::new(write_retry_config());
    loop {
        match upload(client, written, plan, operation, &mut progress).await {
            Ok(receipts) => {
                client.report(Step::Certifying { track: written.address });
                return Ok(receipts);
            }
            Err(err) if should_retry_upload(&err) => {
                ensure_on_tape(client, &written.track).await?;
                warn!(error = %err, track = %written.address, "upload attempt failed; retrying");
                match backoff.next_delay() {
                    Some(delay) => sleep(delay).await,
                    None => return Err(err),
                }
            }
            Err(err) => return Err(err),
        }
    }
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
                ensure_on_tape(client, &written.track).await?;
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

/// True when the program refused because the chain's epoch moved past the one
/// the transaction addresses its group account by
pub(crate) fn is_epoch_changed(err: &TapedriveError) -> bool {
    match err {
        TapedriveError::Rpc(rpc) => {
            matches!(parse_tape_error(rpc), Some(TapeError::EpochChanged))
        }
        _ => false,
    }
}

/// True when a rejection invalidates the collected signatures themselves,
/// rather than the proof or the transaction attempt.
pub(crate) fn needs_fresh_signatures(err: &TapedriveError) -> bool {
    if is_epoch_changed(err) {
        return true;
    }

    match err {
        TapedriveError::Rpc(rpc) => matches!(parse_tape_error(rpc), Some(TapeError::BadSignature)),
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
    let result = wait_for_certified_track(client, &written.address).await;
    visible.finish_result(&result);
    result
}

async fn fetch_track_written_event<Blockchain: Rpc, Cluster: Api>(
    client: &Tapedrive<Blockchain, Cluster>,
    signature: &Txid,
) -> Result<TrackWritten, TapedriveError> {
    fetch_track_written_events(client, signature)
        .await?
        .into_iter()
        .next()
        .ok_or(TapedriveError::NotFound)
}

/// Every track a confirmed transaction wrote, in instruction order
async fn fetch_track_written_events<Blockchain: Rpc, Cluster: Api>(
    client: &Tapedrive<Blockchain, Cluster>,
    signature: &Txid,
) -> Result<Vec<TrackWritten>, TapedriveError> {
    let transaction = read_after_confirmation(
        || client.rpc().rpc().wait_for_signature(signature),
        || client.rpc().get_transaction(signature),
    )
    .await
    .map_err(TapedriveError::Rpc)?;

    extract_track_written_events(&transaction)
}

/// Read a sent transaction once it has confirmed
async fn read_after_confirmation<Output, Wait, WaitFuture, Read, ReadFuture>(
    wait: Wait,
    read: Read,
) -> Result<Output, RpcError>
where
    Wait: Fn() -> WaitFuture,
    WaitFuture: Future<Output = Result<Result<(), TransactionError>, RpcError>>,
    Read: Fn() -> ReadFuture,
    ReadFuture: Future<Output = Result<Output, RpcError>>,
{
    retry_if(
        write_retry_config(),
        None,
        || async {
            wait().await?.map_err(|error| RpcError::Transaction {
                message: error.to_string(),
                err: Some(error),
                simulated: false,
            })?;
            read().await
        },
        // A transaction that failed never gains the event.
        |error: &RpcError| !matches!(error, RpcError::Transaction { .. }),
    )
    .await
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

fn extract_track_written_events(
    transaction: &EncodedConfirmedTransactionWithStatusMeta,
) -> Result<Vec<TrackWritten>, TapedriveError> {
    let logs = transaction
        .transaction
        .meta
        .as_ref()
        .and_then(|meta| meta.log_messages.as_ref().map(|logs| logs))
        .ok_or_else(|| TapedriveError::InvalidArgument("transaction missing log messages".into()))?;

    let mut events = Vec::new();
    for log in logs {
        if let Some(TapedriveEvent::TrackWritten(event)) = parse_event_data(log)
            .map_err(|error| TapedriveError::InvalidArgument(format!("parse event: {error}")))?
        {
            events.push(event);
        }
    }

    if events.is_empty() {
        return Err(TapedriveError::NotFound);
    }
    Ok(events)
}

#[cfg(test)]
mod tests {
    use tape_protocol::api::ApiError;

    use crate::error::TapedriveError;

    use std::time::{Duration, Instant};

    use super::{
        check_plan_matches, coded_key, content_etag, content_etag_named, hash, inline_write_fits,
        is_epoch_changed, listed_root, needs_fresh_signatures, plan_blob, prepare_plan,
        read_after_confirmation, should_retry_certification, CompressedTrack, ErasureCode, Hash,
        RpcError, StorageUnits, TapeError, TapeNumber, TrackArchive, TrackNumber, TrackState,
        VirtualGroup, MAX_INLINE_WRITE, Blockhash, ContentType, PreparedRegister,
        pack_registers, prepare_blob_register, prepare_raw_register, register_units,
        MAX_REGISTERS_PER_TRANSACTION,
    };
    use bytemuck::Zeroable;
    use rpc_litesvm::LiteSvmRpc;
    use tape_api::instruction::build_track_write_ix;
    use tape_core::encoding::{RSParams, TAPENET_PROFILE};
    use tape_crypto::ed25519::Keypair;
    use tape_peer_http::HttpApi;
    use tape_rpc_client::sign_transaction;

    use crate::keys::tape_key::TapeKey;
    use crate::tapedrive::Tapedrive;
    use tape_core::track::data::{BlobData, BlobInfo};
    use tape_core::track::types::{TrackFlags, TrackKind};
    use tape_crypto::prelude::Address;

    fn blob(len: usize) -> Vec<u8> {
        (0..len).map(|i| (i * 31 + 7) as u8).collect()
    }

    fn listed(number: u64) -> CompressedTrack {
        CompressedTrack {
            tape: Address::default(),
            tape_number: TapeNumber(1),
            track_number: TrackNumber(number),
            key: Hash::from([number as u8; 32]),
            kind: TrackKind::Inline as u64,
            state: TrackState::Certified as u64,
            flags: TrackFlags::NONE,
            size: StorageUnits::from_bytes(1),
            group: VirtualGroup(0),
            value_hash: Hash::default(),
        }
    }

    // a listing rebuilds the root of a tree that was cleared, where track
    // numbers run ahead of positions
    #[test]
    fn listed_root_after_clear() {
        let mut archive = TrackArchive::zeroed();
        archive.next_number = TrackNumber(41);
        let tracks = [listed(41), listed(42), listed(43)];
        for track in &tracks {
            archive.append(track).expect("append");
        }
        let root = Some(archive.tree.root());

        assert_eq!(listed_root(&tracks, &archive), root);
        assert_ne!(listed_root(&tracks[..2], &archive), root);
        assert_eq!(listed_root(&[listed(40)], &archive), None);
        assert_eq!(listed_root(&[listed(44)], &archive), None);
    }

    fn rejected(error: TapeError) -> TapedriveError {
        let code = u32::from(error);
        TapedriveError::Rpc(RpcError::Transaction {
            err: None,
            message: format!("custom program error: {code:#x}"),
            simulated: false,
        })
    }

    // a reused plan writes exactly what a fresh encode would have
    #[tokio::test]
    async fn plan_reuse() {
        let data = blob(MAX_INLINE_WRITE * 4);
        let computed = content_etag(TAPENET_PROFILE, data.clone()).await.expect("etag");
        let plan = computed.plan.expect("coded payload carries its plan");
        let fresh = prepare_plan(TAPENET_PROFILE, &data).expect("fresh plan");

        assert_eq!(plan.commitment_hash, fresh.commitment_hash);
        assert_eq!(computed.etag, fresh.commitment_hash);
        assert_eq!(plan.storage_units, fresh.storage_units);
        assert_eq!(plan.stripe_size, fresh.stripe_size);
        assert_eq!(plan.stripe_count, fresh.stripe_count);
        assert_eq!(plan.leaves, fresh.leaves);
        assert_eq!(plan.slices.len(), fresh.slices.len());
    }

    // a small profile plans n slices, zero leaves past n, and a blob whose
    // commitment the chain accepts at that width only
    #[test]
    fn small_profile_plan() {
        let profile = ErasureCode::ReedSolomon(RSParams::new(3, 1));
        let width = usize::from(profile.n());
        let data = blob(MAX_INLINE_WRITE * 4);

        let plan = prepare_plan(profile, &data).expect("plan");

        assert_eq!(plan.slices.len(), width);
        assert!(plan.leaves[width..].iter().all(|leaf| *leaf == Hash::default()));
        assert_eq!(plan_blob(&plan).commitment_root(profile), Some(plan.commitment_hash));
        assert_ne!(plan_blob(&plan).commitment_root(TAPENET_PROFILE), Some(plan.commitment_hash));
        assert!(coded_key(b"name", &plan).is_ok());
    }

    // the same bytes get a different coded etag under another profile
    #[tokio::test]
    async fn etag_follows_profile() {
        let data = blob(MAX_INLINE_WRITE * 4);
        let small = ErasureCode::ReedSolomon(RSParams::new(3, 1));

        let tapenet = content_etag(TAPENET_PROFILE, data.clone()).await.expect("etag");
        let other = content_etag(small, data).await.expect("etag");

        assert_ne!(tapenet.etag, other.etag);
    }

    // a register's event is read as soon as it confirms
    #[tokio::test]
    async fn confirmed_read() {
        let confirmed_at = Instant::now() + Duration::from_millis(300);
        let started = Instant::now();

        let read = read_after_confirmation(
            || async move {
                tokio::time::sleep_until(confirmed_at.into()).await;
                Ok(Ok(()))
            },
            || async move {
                match Instant::now() >= confirmed_at {
                    true => Ok(7u64),
                    false => Err(RpcError::TransactionNotFound(Default::default())),
                }
            },
        )
        .await;

        assert_eq!(read.expect("read"), 7);
        assert!(started.elapsed() < Duration::from_millis(600));
    }

    // a plan from a plain encode skipped the content hash, so it cannot pass as a checked reuse
    #[test]
    fn unhashed_plan() {
        let data = blob(MAX_INLINE_WRITE * 4);
        let plan = prepare_plan(TAPENET_PROFILE, &data).expect("plan");

        assert!(check_plan_matches(&plan, &data).is_err());
    }

    // a plan from other bytes is refused, whatever its length
    #[tokio::test]
    async fn unpaired_plan() {
        let data = blob(MAX_INLINE_WRITE * 4);
        let plan = content_etag(TAPENET_PROFILE, data.clone())
            .await
            .expect("etag")
            .plan
            .expect("coded plan");

        assert!(check_plan_matches(&plan, &data).is_ok());

        // Same length, different content: the length check alone would pass it.
        let mut altered = data.clone();
        altered[0] ^= 0xFF;
        assert!(check_plan_matches(&plan, &altered).is_err());

        assert!(check_plan_matches(&plan, &data[..data.len() - 1]).is_err());
    }

    // inline payloads never encode, so they carry no plan
    #[tokio::test]
    async fn inline_no_plan() {
        let data = blob(MAX_INLINE_WRITE);
        let computed = content_etag(TAPENET_PROFILE, data.clone()).await.expect("etag");

        assert!(computed.plan.is_none());
        assert_eq!(computed.etag, hash(&data));
    }

    // A payload that is inline unnamed but not with its name must take the
    // coded path, and its etag must be the coded commitment.
    #[tokio::test]
    async fn named_etag_follows_the_trailer_budget() {
        let name = b"obj-23-4643.txt";
        let data = blob(MAX_INLINE_WRITE);
        assert!(!inline_write_fits(name, data.len()));

        let unnamed = content_etag(TAPENET_PROFILE, data.clone()).await.expect("etag");
        assert!(unnamed.plan.is_none());

        let named = content_etag_named(TAPENET_PROFILE, name, data.clone())
            .await
            .expect("etag");
        let plan = named.plan.expect("coded plan");
        assert_eq!(named.etag, plan.commitment_hash);
    }

    // A payload over the program limit never becomes an instruction.
    #[test]
    fn oversized_payload_refused() {
        let write = |len: usize| {
            build_track_write_ix(
                Address::from([1; 32]),
                Address::from([2; 32]),
                Address::from([3; 32]),
                BlobInfo {
                    object: None,
                    data: BlobData::Inline(blob(len)),
                },
                TrackFlags::NONE,
            )
        };
        assert!(write(MAX_INLINE_WRITE).is_ok());
        assert!(write(MAX_INLINE_WRITE + 1).is_err());
    }

    #[test]
    fn inline_budget() {
        let name = b"object/name";
        let max_named_payload = (0..=MAX_INLINE_WRITE)
            .rev()
            .find(|payload_len| inline_write_fits(name, *payload_len))
            .expect("named inline payload should have some capacity");

        assert!(inline_write_fits(b"", MAX_INLINE_WRITE));
        assert!(!inline_write_fits(name, MAX_INLINE_WRITE));
        assert!(max_named_payload < MAX_INLINE_WRITE);
        assert!(inline_write_fits(name, max_named_payload));
        assert!(!inline_write_fits(name, max_named_payload + 1));
    }

    // Certification should retry when proof visibility lags behind peer state.
    #[test]
    fn stale_proof() {
        assert!(should_retry_certification(&TapedriveError::Peer(
            ApiError::StaleTrackProof,
        )));
    }

    #[test]
    fn missing_proof() {
        assert!(should_retry_certification(&TapedriveError::NotFound));
    }

    // EpochChanged means signatures were collected against a now-stale epoch;
    // certify_with_retry must recollect from peers, not just resubmit.
    #[test]
    fn epoch_change() {
        assert!(should_retry_certification(&rejected(TapeError::EpochChanged)));
    }

    // Rejections of the signatures themselves are retryable only with a fresh collection.
    #[test]
    fn dead_signatures() {
        for error in [TapeError::BadSignature, TapeError::EpochChanged] {
            assert!(needs_fresh_signatures(&rejected(error)));
            assert!(should_retry_certification(&rejected(error)));
        }
    }

    // A stale proof and transport failures leave the collected signatures usable.
    #[test]
    fn live_signatures() {
        assert!(!needs_fresh_signatures(&rejected(TapeError::BadProof)));
        assert!(!needs_fresh_signatures(&TapedriveError::NotFound));
        assert!(!needs_fresh_signatures(&TapedriveError::Peer(
            ApiError::StaleTrackProof,
        )));
    }

    // the epoch turning under a certify reads apart from every other refusal
    #[test]
    fn epoch_turn_verdict() {
        let turned = TapedriveError::Rpc(RpcError::Request(
            "RPC response error: Error processing Instruction 1: custom program error: 0x34"
                .to_string(),
        ));
        let bad_proof = TapedriveError::Rpc(RpcError::Request(
            "RPC response error: Error processing Instruction 1: custom program error: 0x70"
                .to_string(),
        ));

        assert!(is_epoch_changed(&turned));
        assert!(!is_epoch_changed(&bad_proof));
        assert!(!is_epoch_changed(&TapedriveError::NotFound));

        // both still retry, so the run only changes which way it retries
        assert!(should_retry_certification(&turned));
        assert!(should_retry_certification(&bad_proof));
    }

    fn packing_client() -> Tapedrive<LiteSvmRpc, HttpApi> {
        Tapedrive::new(LiteSvmRpc::new(), Keypair::new(&mut rand::thread_rng()))
    }

    // Each packed run signs into one transaction, in input order.
    fn check_packed(
        client: &Tapedrive<LiteSvmRpc, HttpApi>,
        tape_key: &TapeKey,
        packed: &[Vec<PreparedRegister>],
        count: usize,
    ) {
        let payer = client.payer().expect("payer");
        let mut units = Vec::new();
        for run in packed {
            assert!(run.len() <= MAX_REGISTERS_PER_TRANSACTION, "a run stays in the compute budget");
            let instructions: Vec<_> = run.iter().map(|register| register.instruction.clone()).collect();
            sign_transaction(
                payer,
                &[tape_key.keypair()],
                register_units(run.len()),
                &instructions,
                Blockhash::default(),
            )
            .expect("a packed run fits one transaction");
            units.extend(run.iter().map(|register| register.unit().expect("unit")));
        }
        assert_eq!(units, (0..count).collect::<Vec<_>>(), "packing keeps input order");
    }

    // a full batch of small coded objects shares a few transactions
    #[test]
    fn coded_registers_pack() {
        let client = packing_client();
        let tape_key = TapeKey::generate();
        let registers = (0..32)
            .map(|index| {
                let plan = prepare_plan(TAPENET_PROFILE, &blob(4 * 1024)).expect("plan");
                prepare_blob_register(
                    &client,
                    &tape_key,
                    format!("site/assets/object-{index:02}.bin").as_bytes(),
                    ContentType::Unknown,
                    StorageUnits::from_bytes(4 * 1024),
                    plan,
                    TrackFlags::NONE,
                )
                .expect("register")
                .for_unit(index)
            })
            .collect();

        let packed = pack_registers(&client, &tape_key, registers).expect("pack");

        check_packed(&client, &tape_key, &packed, 32);
        assert!(packed.len() < 32 / 3, "{} transactions for 32 registers", packed.len());
    }

    // the largest inline writes still pack more than one to a transaction
    #[test]
    fn inline_registers_pack() {
        let client = packing_client();
        let tape_key = TapeKey::generate();
        let name = b"a.txt";
        let size = (0..=MAX_INLINE_WRITE)
            .rev()
            .find(|size| inline_write_fits(name, *size))
            .expect("an inline size");
        let registers = (0..8)
            .map(|index| {
                prepare_raw_register(
                    &client,
                    &tape_key,
                    name,
                    ContentType::TextPlain,
                    StorageUnits::from_bytes(size as u64),
                    &blob(size),
                    TrackFlags::NONE,
                )
                .expect("register")
                .for_unit(index)
            })
            .collect();

        let packed = pack_registers(&client, &tape_key, registers).expect("pack");

        check_packed(&client, &tape_key, &packed, 8);
        assert!(packed.len() < 8, "{} transactions for 8 registers", packed.len());
    }
}
