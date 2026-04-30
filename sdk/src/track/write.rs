use std::collections::HashSet;
use std::time::Duration;
use thiserror::Error;
use tracing::{debug, info, warn};

use rpc::{EncodedConfirmedTransactionWithStatusMeta, Rpc};
use rpc_client::parse_tape_error;
use tape_api::compute::{CERTIFY_TRACK_CU, TRACK_WRITE_CU};
use tape_api::errors::TapeError;
use tape_api::event::TrackWritten;
use tape_api::instruction::{ 
    build_certify_track_ix, build_track_write_blob_ix, build_track_write_raw_ix 
};
use tape_blocks::{parse_event_data, TapedriveEvent};
use tape_core::bft::min_correct;
use tape_core::erasure::SPOOL_GROUP_SIZE;
use tape_core::prelude::{
    BlobInfo, CompressedTrack, EncodingProfile, EpochNumber, SpoolGroup, StorageUnits,
    StripeCount, TrackNumber,
};
use tape_core::track::data::TrackDataSlice;
use tape_crypto::prelude::{Address, Hash};
use tape_crypto::tx::Txid;
use tape_protocol::Api;
use tape_protocol::api::GetTrackDataReq;
use tape_retry::{retry, retry_if, Backoff, RetryConfig, Retryable};
use tape_slicer::{num_stripes, pick_stripe_size};
use tokio::time::sleep;

use crate::codec::encoder::BlobEncoder;
use crate::error::UploadError;
use crate::error::TapedriveError;
use crate::keys::tape_key::TapeKey;
use crate::metrics::{Operation, Phase};
use crate::tapedrive::Tapedrive;
use crate::track::{bootstrap_network_state, queries};
use crate::transfer::certify::CertificationCollector;
use crate::transfer::uploader::{DistributedUploader, SliceWithProof};

// The program accepts up to 10 KiB for raw TrackWrite payloads, but an SDK end-user write must fit
// inside a single Solana transaction packet. This can be adjusted in the future if 4k transactions
// become widely supported.
pub const SDK_INLINE_RAW_MAX_BYTES: usize = 825;

#[derive(Clone)]
pub struct UploadPlan {
    pub slices: Vec<SliceWithProof>,
    pub commitment_hash: Hash,
    pub storage_units: StorageUnits,
    pub profile: EncodingProfile,
    pub stripe_size: usize,
    pub stripe_count: usize,
    pub leaves: [Hash; SPOOL_GROUP_SIZE],
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
    /// Write a track to an existing tape.
    pub async fn write_track(
        &self,
        tape_key: &TapeKey,
        key: Hash,
        data: &[u8],
    ) -> Result<CompressedTrack, TapedriveError> {
        write_track(self, tape_key, key, data).await
    }

    /// Write raw bytes to an existing tape.
    pub async fn write_raw(
        &self,
        tape_key: &TapeKey,
        key: Hash,
        raw: &[u8],
    ) -> Result<CompressedTrack, TapedriveError> {
        if raw.len() > SDK_INLINE_RAW_MAX_BYTES {
            return Err(TapedriveError::InvalidArgument(format!(
                "raw inline write exceeds SDK limit of {SDK_INLINE_RAW_MAX_BYTES} bytes; use write_track() or write_blob()"
            )));
        }

        let timer = self
            .timer(Operation::WriteRaw, Phase::Total)
            .bytes(raw.len() as u64);
        let result = submit_raw(self, tape_key, key, raw, Operation::WriteRaw).await;
        timer.finish_result(&result);
        let written = result?;
        Ok(written.track)
    }

    /// Register a blob track and return the upload plan needed to land its slices.
    pub async fn write_blob(
        &self,
        tape_key: &TapeKey,
        key: Hash,
        data: &[u8],
    ) -> Result<(WrittenTrack, UploadPlan), TapedriveError> {
        let timer = self
            .timer(Operation::WriteBlob, Phase::Total)
            .bytes(data.len() as u64);
        let result = submit_blob(self, tape_key, key, data, Operation::WriteBlob).await;
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
        let result = upload(self, written, plan, Operation::Upload).await;
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
        let result = certify_once(self, tape_key, written, Operation::Certify).await;
        timer.finish_result(&result);
        result
    }
}

fn prepare_plan(data: &[u8]) -> Result<UploadPlan, TapedriveError> {
    let profile = EncodingProfile::clay_default();
    let mut encoder = BlobEncoder::with_profile(profile);
    let (slices, merkle_root, leaves) = encoder
        .encode_with_leaves(data.to_vec())
        .map_err(|e| TapedriveError::Encoding(e.to_string()))?;

    Ok(UploadPlan {
        slices,
        commitment_hash: merkle_root.into(),
        storage_units: StorageUnits::from_bytes(data.len() as u64),
        profile,
        stripe_size: pick_stripe_size(data.len()),
        stripe_count: num_stripes(data.len(), pick_stripe_size(data.len())),
        leaves,
    })
}

async fn submit_raw<Blockchain: Rpc, Cluster: Api>(
    client: &Tapedrive<Blockchain, Cluster>,
    tape_key: &TapeKey,
    key: Hash,
    raw: &[u8],
    operation: Operation,
) -> Result<WrittenTrack, TapedriveError> {
    let timer = client
        .timer(operation, Phase::Register)
        .bytes(raw.len() as u64)
        .chunks(1);
    let result = send_raw(client, tape_key, key, raw).await;
    timer.finish_result(&result);
    result
}

async fn send_raw<Blockchain: Rpc, Cluster: Api>(
    client: &Tapedrive<Blockchain, Cluster>,
    tape_key: &TapeKey,
    key: Hash,
    raw: &[u8],
) -> Result<WrittenTrack, TapedriveError> {
    let payer = client.payer()?;
    let tape_signer = tape_key.keypair();
    let write_ix = build_track_write_raw_ix(
        payer.pubkey().into(),
        tape_key.pubkey().into(),
        key,
        raw,
    )
    .map_err(|error| TapedriveError::InvalidArgument(error.to_string()))?;

    let signature = client
        .rpc()
        .send_instructions_with_signers_and_compute_unit_limit(
            payer,
            TRACK_WRITE_CU,
            vec![write_ix],
            &[tape_signer],
        )
        .await?;

    let written = fetch_track_written_event(client, &signature).await?;
    let track_address: Address = written.track.into();
    let meta = TrackDataSlice::Raw(raw).meta().unwrap();
    let track = CompressedTrack {
        tape: written.tape,
        track_number: written.track_number,
        key,
        kind: meta.kind as u64,
        state: meta.state as u64,
        size: meta.size,
        spool_group: written.spool_group,
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
    tape_key: &TapeKey,
    key: Hash,
    data: &[u8],
    operation: Operation,
) -> Result<(WrittenTrack, UploadPlan), TapedriveError> {
    let encode_timer = client
        .timer(operation, Phase::Encode)
        .bytes(data.len() as u64);
    let plan = prepare_plan(data);
    encode_timer.finish_result(&plan);
    let plan = plan?;

    let register_timer = client
        .timer(operation, Phase::Register)
        .bytes(data.len() as u64)
        .chunks(1);
    let result = send_blob(client, tape_key, key, data, plan).await;
    register_timer.finish_result(&result);
    result
}

async fn send_blob<Blockchain: Rpc, Cluster: Api>(
    client: &Tapedrive<Blockchain, Cluster>,
    tape_key: &TapeKey,
    key: Hash,
    _data: &[u8],
    plan: UploadPlan,
) -> Result<(WrittenTrack, UploadPlan), TapedriveError> {
    let payer = client.payer()?;
    let tape_signer = tape_key.keypair();
    let blob = BlobInfo {
        size: plan.storage_units,
        commitment: plan.commitment_hash,
        profile: plan.profile,
        stripe_size: StorageUnits::from_bytes(plan.stripe_size as u64),
        stripe_count: StripeCount(plan.stripe_count as u64),
        leaves: plan.leaves,
    };
    let write_ix = build_track_write_blob_ix(
        payer.pubkey().into(),
        tape_key.pubkey().into(),
        key,
        blob,
    )
    .map_err(|error| TapedriveError::InvalidArgument(error.to_string()))?;

    let signature = client
        .rpc()
        .send_instructions_with_signers_and_compute_unit_limit(
            payer,
            TRACK_WRITE_CU,
            vec![write_ix],
            &[tape_signer],
        )
        .await?;

    let written = fetch_track_written_event(client, &signature).await?;
    let track_address: Address = written.track.into();
    let meta = TrackDataSlice::Blob(blob).meta()
        .ok_or(TapedriveError::InvalidArgument("invalid blob commitment".into()))?;
    let track = CompressedTrack {
        tape: written.tape,
        track_number: written.track_number,
        key,
        kind: meta.kind as u64,
        state: meta.state as u64,
        size: meta.size,
        spool_group: written.spool_group,
        value_hash: meta.value_hash,
    };
    debug_assert_eq!(track.get_hash(), written.track_hash);

    Ok((
        WrittenTrack {
            address: track_address,
            track,
        },
        plan,
    ))
}

async fn upload_once<Blockchain: Rpc, Cluster: Api>(
    client: &Tapedrive<Blockchain, Cluster>,
    track_address: Address,
    spool_group: SpoolGroup,
    slices: Vec<SliceWithProof>,
    operation: Operation,
) -> Result<(), TapedriveError> {
    let bytes = slices.iter().map(|slice| slice.data.len() as u64).sum();
    let chunks = slices.len() as u64;

    let locate = client.timer(operation, Phase::Locate);
    let state = bootstrap_network_state(client).await;
    locate.finish_result(&state);
    let state = state?;

    let uploader = DistributedUploader::new(track_address, spool_group, slices, &state)
        .map_err(TapedriveError::Upload)?;

    let store = client
        .timer(operation, Phase::Store)
        .bytes(bytes)
        .chunks(chunks);
    let result = uploader
        .upload_all(client.api.as_ref())
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
    let result = wait_for_visibility(client, written.address, written.track.spool_group).await;
    visibility.finish_result(&result);
    result?;

    upload_once(
        client,
        written.address,
        written.track.spool_group,
        plan.slices.clone(),
        operation,
    )
    .await
}

async fn wait_for_visibility<Blockchain: Rpc, Cluster: Api>(
    client: &Tapedrive<Blockchain, Cluster>,
    track_address: Address,
    spool_group: SpoolGroup,
) -> Result<(), TapedriveError> {
    let state = bootstrap_network_state(client).await?;

    let group_peers = state.group_peers(spool_group);
    let required = min_correct(state.group_member_count(spool_group) as u64) as usize;

    let mut seen = HashSet::new();
    let target = group_peers
        .iter()
        .filter(|(_, node_id)| seen.insert(*node_id))
        .count();

    let mut backoff = Backoff::new(visibility_retry_config());

    loop {
        let mut visible = 0usize;
        let mut seen = HashSet::new();

        for (_, node_id) in &group_peers {
            if !seen.insert(*node_id) {
                continue;
            }

            let req = GetTrackDataReq { track: track_address };
            match client.api.get_track_data(*node_id, &req).await {
                Ok(_) => visible += 1,
                Err(error) => debug!(
                    node = %node_id,
                    error = %error,
                    "track metadata not yet visible on node"
                ),
            }
        }

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

        let Some(delay) = backoff.next_delay() else {
            return Err(TapedriveError::Upload(UploadError::Network(format!(
                "track metadata visible on {visible}/{target} nodes, need {required}"
            ))));
        };

        warn!(
            attempt = backoff.attempt(),
            delay_ms = delay.as_millis() as u64,
            visible,
            target,
            required,
            "track metadata not yet visible on required nodes"
        );

        sleep(delay).await;
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

fn should_retry_certification(err: &TapedriveError) -> bool {
    match err {
        TapedriveError::Certification(_) => true,
        TapedriveError::Peer(err) => err.is_retryable(),
        TapedriveError::Rpc(rpc) => {
            matches!(
                parse_tape_error(rpc),
                Some(TapeError::BadSignature | TapeError::BadProof)
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

async fn certify_once<Blockchain: Rpc, Cluster: Api>(
    client: &Tapedrive<Blockchain, Cluster>,
    tape_key: &TapeKey,
    written: &WrittenTrack,
    operation: Operation,
) -> Result<(), TapedriveError> {
    let track_address = written.address;
    let spool_group = written.track.spool_group;
    let payer = client.payer()?;
    let tape_signer = tape_key.keypair();

    let collect = client.timer(operation, Phase::CertifyCollect);
    let result = async {
        let system = client.rpc().get_system().await?;
        let collector = CertificationCollector::with_defaults();
        collector
            .collect_signatures(client.api.as_ref(), &track_address, spool_group, &system)
            .await
            .map_err(TapedriveError::Certification)
    }
    .await;
    collect.finish_result(&result);
    let collected = result?;

    let proof_timer = client.timer(operation, Phase::CertifyProof);
    let proof = queries::query_track_proof(client, &track_address).await;
    proof_timer.finish_result(&proof);
    let proof = proof?;

    let certify_ix = build_certify_track_ix(
        payer.pubkey().into(),
        tape_key.pubkey().into(),
        proof,
        EpochNumber(collected.epoch),
        collected.bitmap,
        collected.aggregated_signature,
    );

    let submit = client.timer(operation, Phase::CertifySubmit);
    let result = match client
        .rpc()
        .send_instructions_with_signers_and_compute_unit_limit(
            payer,
            CERTIFY_TRACK_CU,
            vec![certify_ix],
            &[tape_signer],
        )
        .await
    {
        Ok(_) => Ok(()),
        Err(err) => match parse_tape_error(&err) {
            Some(TapeError::AlreadyCertified) => Ok(()),
            _ => Err(TapedriveError::Rpc(err)),
        },
    };
    submit.finish_result(&result);
    result
}

async fn wait_for_certified_track<Blockchain: Rpc, Cluster: Api>(
    client: &Tapedrive<Blockchain, Cluster>,
    tape: &Address,
    track_number: TrackNumber,
) -> Result<CompressedTrack, TapedriveError> {
    let result = retry_if(
        write_retry_config(),
        None,
        || async {
            let track = queries::query_track_by_number(client, tape, track_number)
                .await
                .map_err(TrackCompletionError::from)?;
            if track.is_certified() {
                Ok(track)
            } else {
                Err(TrackCompletionError::NotCertifiedYet)
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
    tape_key: &TapeKey,
    key: Hash,
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
                key,
                data,
                Operation::WriteTrack,
            )
            .await?;
            return Ok(written.track);
        }

        let (written, plan) = submit_blob(
            client,
            tape_key,
            key,
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

pub(crate) async fn certify_with_retry<Blockchain: Rpc, Cluster: Api>(
    client: &Tapedrive<Blockchain, Cluster>,
    tape_key: &TapeKey,
    written: &WrittenTrack,
    operation: Operation,
) -> Result<CompressedTrack, TapedriveError> {
    retry_if(
        write_retry_config(),
        None,
        || certify_once(client, tape_key, written, operation),
        should_retry_certification,
    )
    .await?;

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

fn visibility_retry_config() -> RetryConfig {
    RetryConfig {
        base_delay: Duration::from_millis(500),
        max_delay: Duration::from_secs(5),
        max_retries: Some(6),
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

    use super::SDK_INLINE_RAW_MAX_BYTES;
    use super::should_retry_certification;
    use tape_api::instruction::TRACK_WRITE_MAX_BYTES;

    // The SDK inline write limit must always remain below the program limit.
    #[test]
    fn sdk_inline_raw_limit_is_below_program_limit() {
        assert_eq!(SDK_INLINE_RAW_MAX_BYTES, 825);
        assert!(SDK_INLINE_RAW_MAX_BYTES < TRACK_WRITE_MAX_BYTES);
    }

    // Certification should retry when proof visibility lags behind peer state.
    #[test]
    fn certification_retries_stale_track_proof() {
        assert!(should_retry_certification(&TapedriveError::Peer(
            ApiError::StaleTrackProof,
        )));
    }
}
