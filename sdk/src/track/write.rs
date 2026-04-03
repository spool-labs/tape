use std::collections::HashSet;
use thiserror::Error;

use rpc::{EncodedConfirmedTransactionWithStatusMeta, Rpc};
use rpc_client::parse_tape_error;
use tape_api::compute::CERTIFY_TRACK_CU;
use tape_api::errors::TapeError;
use tape_api::event::TrackWritten;
use tape_api::instruction::{ 
    build_certify_track_ix, build_track_write_blob_ix, build_track_write_raw_ix 
};
use tape_blocks::{parse_event_data, TapedriveEvent};
use tape_core::bft::min_correct;
use tape_core::encoding::EncodingProfile;
use tape_core::erasure::SPOOL_GROUP_SIZE;
use tape_core::spooler::SpoolGroup;
use tape_core::track::blob::BlobInfo;
use tape_core::track::types::CompressedTrack;
use tape_core::types::{EpochNumber, StorageUnits, StripeCount};
use tape_crypto::address::Address;
use tape_crypto::Hash;
use tape_crypto::tx::Txid;
use tape_protocol::Api;
use tape_protocol::api::GetTrackDataReq;
use tape_retry::{RetryConfig, Retryable};
use tape_slicer::{num_stripes, pick_stripe_size};

use crate::codec::encoder::BlobEncoder;
use crate::error::UploadError;
use crate::error::TapedriveError;
use crate::keys::tape_key::TapeKey;
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
    pub root_hash: Hash,
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

        let written = submit_raw(self, tape_key, key, raw).await?;
        wait_for_visibility(self, written.address, written.track.spool_group).await?;
        Ok(written.track)
    }

    /// Register a blob track and return the upload plan needed to land its slices.
    pub async fn write_blob(
        &self,
        tape_key: &TapeKey,
        key: Hash,
        data: &[u8],
    ) -> Result<(WrittenTrack, UploadPlan), TapedriveError> {
        submit_blob(self, tape_key, key, data).await
    }

    /// Upload blob slices for a previously written blob track.
    pub async fn upload(
        &self,
        written: &WrittenTrack,
        plan: &UploadPlan,
    ) -> Result<(), TapedriveError> {
        wait_for_visibility(self, written.address, written.track.spool_group).await?;
        upload_once(
            self,
            written.address,
            written.track.spool_group,
            plan.slices.clone(),
        )
        .await
    }

    /// Collect signatures and submit the certify instruction for a written track.
    pub async fn certify(
        &self,
        tape_key: &TapeKey,
        written: &WrittenTrack,
    ) -> Result<(), TapedriveError> {
        certify_once(self, tape_key, written.address, written.track.spool_group).await
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
        root_hash: merkle_root.into(),
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
) -> Result<WrittenTrack, TapedriveError> {
    let payer = client.payer()?;
    let tape_signer = tape_key.keypair();
    let write_ix = build_track_write_raw_ix(
        payer.pubkey().into(),
        tape_key.address(),
        key,
        raw,
    )
    .map_err(|error| TapedriveError::InvalidArgument(error.to_string()))?;

    let signature = client
        .rpc()
        .send_instructions_with_signers(
            payer,
            vec![write_ix],
            &[tape_signer],
        )
        .await?;

    let written = fetch_track_written_event(client, &signature).await?;
    let track_address: Address = written.track.into();
    let track = queries::retry_fetch_track_by_number(
        client,
        &written.tape,
        written.track_number,
    )
    .await?;

    Ok(WrittenTrack {
        address: track_address,
        track,
    })
}

async fn submit_blob<Blockchain: Rpc, Cluster: Api>(
    client: &Tapedrive<Blockchain, Cluster>,
    tape_key: &TapeKey,
    key: Hash,
    data: &[u8],
) -> Result<(WrittenTrack, UploadPlan), TapedriveError> {
    let plan = prepare_plan(data)?;
    let payer = client.payer()?;
    let tape_signer = tape_key.keypair();
    let write_ix = build_track_write_blob_ix(
        payer.pubkey().into(),
        tape_key.address(),
        key,
        BlobInfo {
            size: plan.storage_units,
            root: plan.root_hash,
            commitment: plan.commitment_hash,
            profile: plan.profile,
            stripe_size: StorageUnits::from_bytes(plan.stripe_size as u64),
            stripe_count: StripeCount(plan.stripe_count as u64),
            leaves: plan.leaves,
        },
    )
    .map_err(|error| TapedriveError::InvalidArgument(error.to_string()))?;

    let signature = client
        .rpc()
        .send_instructions_with_signers(
            payer,
            vec![write_ix],
            &[tape_signer],
        )
        .await?;

    let written = fetch_track_written_event(client, &signature).await?;
    let track_address: Address = written.track.into();
    let track = queries::retry_fetch_track_by_number(
        client,
        &written.tape,
        written.track_number,
    )
    .await?;

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
) -> Result<(), TapedriveError> {
    let state = bootstrap_network_state(client).await?;
    let uploader = DistributedUploader::new(track_address, spool_group, slices, &state)
        .map_err(TapedriveError::Upload)?;

    uploader
        .upload_all(client.api.as_ref())
        .await
        .map_err(TapedriveError::Upload)
}

async fn wait_for_visibility<Blockchain: Rpc, Cluster: Api>(
    client: &Tapedrive<Blockchain, Cluster>,
    track_address: Address,
    spool_group: SpoolGroup,
) -> Result<(), TapedriveError> {
    let state = bootstrap_network_state(client).await?;
    let group_peers = state.group_peers(spool_group);
    let required = min_correct(state.group_member_count(spool_group) as u64) as usize;

    tape_retry::retry_if(
        RetryConfig::ten(),
        None,
        || {
            let group_peers = group_peers.clone();
            let api = client.api.clone();
            let mut seen = HashSet::new();
            async move {
                let mut visible = 0usize;

                for (_, node_id) in &group_peers {
                    if !seen.insert(*node_id) {
                        continue;
                    }

                    let req = GetTrackDataReq { track: track_address };
                    if api.get_track_data(*node_id, &req).await.is_ok() {
                        visible += 1;
                    }
                }

                if visible >= required {
                    Ok(())
                } else {
                    Err(TapedriveError::Upload(UploadError::Network(
                        format!("track metadata visible on {visible}/{required} required nodes"),
                    )))
                }
            }
        },
        |_| true,
    )
    .await?;

    Ok(())
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
    track_address: Address,
    spool_group: SpoolGroup,
) -> Result<(), TapedriveError> {
    let payer = client.payer()?;
    let tape_signer = tape_key.keypair();
    let system = client.rpc().get_system().await?;

    let collector = CertificationCollector::with_defaults();
    let collected = collector
        .collect_signatures(
            client.api.as_ref(),
            &track_address,
            spool_group,
            &system,
        )
        .await
        .map_err(TapedriveError::Certification)?;
    let proof = queries::query_track_proof(client, &track_address).await?;

    let certify_ix = build_certify_track_ix(
        payer.pubkey().into(),
        tape_key.address(),
        proof,
        EpochNumber(collected.epoch),
        collected.bitmap,
        collected.aggregated_signature,
    );

    match client
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
    }
}

async fn wait_for_certified_track<Blockchain: Rpc, Cluster: Api>(
    client: &Tapedrive<Blockchain, Cluster>,
    tape: &Address,
    track_number: tape_core::types::TrackNumber,
) -> Result<CompressedTrack, TapedriveError> {
    let result = tape_retry::retry_if(
        RetryConfig::ten(),
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
    if data.len() <= SDK_INLINE_RAW_MAX_BYTES {
        return client.write_raw(tape_key, key, data).await;
    }

    let (written, plan) = client.write_blob(tape_key, key, data).await?;
    tape_retry::retry_if(
        RetryConfig::ten(),
        None,
        || client.upload(&written, &plan),
        should_retry_upload,
    )
    .await?;

    tape_retry::retry_if(
        RetryConfig::ten(),
        None,
        || client.certify(tape_key, &written),
        should_retry_certification,
    )
    .await?;

    wait_for_certified_track(client, &tape_key.address(), written.track.track_number).await
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

async fn fetch_track_written_event<Blockchain: Rpc, Cluster: Api>(
    client: &Tapedrive<Blockchain, Cluster>,
    signature: &Txid,
) -> Result<TrackWritten, TapedriveError> {
    let transaction = tape_retry::retry(
        RetryConfig::ten(),
        None,
        || async { client.rpc().get_transaction(signature).await },
    )
    .await
    .map_err(TapedriveError::Rpc)?;

    extract_track_written_event(&transaction)
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
