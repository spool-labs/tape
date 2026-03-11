use rpc::{Rpc, RpcError};
use rpc_client::parse_tape_error;
use solana_sdk::compute_budget::ComputeBudgetInstruction;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::Signer;
use tape_api::compute::CERTIFY_TRACK_CU;
use tape_api::errors::TapeError;
use tape_api::instruction::{build_certify_track_ix, build_register_track_ix};
use tape_api::program::tapedrive::track_pda;
use tape_api::state::Track;
use tape_core::encoding::EncodingProfile;
use tape_core::erasure::SPOOL_GROUP_SIZE;
use tape_core::types::{EpochNumber, StorageUnits};
use tape_crypto::Hash;
use tape_protocol::Api;
use tape_slicer::{num_stripes, pick_stripe_size};

use crate::codec::encoder::BlobEncoder;
use crate::error::TapedriveError;
use crate::keys::tape_key::TapeKey;
use crate::tapedrive::Tapedrive;
use crate::track::{bootstrap_network_state, queries};
use crate::transfer::certify::CertificationCollector;
use crate::transfer::uploader::{DistributedUploader, SliceWithProof};

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

pub struct RegisteredTrack {
    pub address: Pubkey,
    pub track: Track,
}

impl<Blockchain: Rpc, Cluster: Api> Tapedrive<Blockchain, Cluster> {
    /// Write a track to an existing tape.
    pub async fn write_track(
        &self,
        tape_key: &TapeKey,
        key: Hash,
        data: &[u8],
    ) -> Result<Track, TapedriveError> {
        write_track(self, tape_key, key, data).await
    }
}

pub fn prepare_upload_plan(data: &[u8]) -> Result<UploadPlan, TapedriveError> {
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

pub async fn register_or_resume_track<Blockchain: Rpc, Cluster: Api>(
    client: &Tapedrive<Blockchain, Cluster>,
    tape_key: &TapeKey,
    key: Hash,
    plan: &UploadPlan,
) -> Result<RegisteredTrack, TapedriveError> {
    let payer = client.payer()?;
    let (track_address, _) = track_pda(tape_key.pubkey(), key);

    let track = match client.rpc().get_track_by_address(&track_address).await {
        Ok(track) => track,
        Err(RpcError::AccountNotFound(_)) => {
            let register_ix = build_register_track_ix(
                payer.pubkey(),
                tape_key.pubkey(),
                plan.storage_units,
                plan.root_hash,
                plan.commitment_hash,
                key,
                plan.profile,
                plan.stripe_size as u64,
                plan.stripe_count as u64,
                plan.leaves,
            );

            client
                .rpc()
                .send_instructions_with_signers(
                    payer,
                    vec![register_ix],
                    &[tape_key.as_keypair()],
                )
                .await?;

            queries::retry_fetch_track(client, &track_address).await?
        }
        Err(error) => return Err(TapedriveError::Rpc(error)),
    };

    Ok(RegisteredTrack {
        address: track_address,
        track,
    })
}

pub async fn upload_registered_track<Blockchain: Rpc, Cluster: Api>(
    client: &Tapedrive<Blockchain, Cluster>,
    track_address: Pubkey,
    spool_group: tape_core::spooler::SpoolGroup,
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

pub fn should_retry_certification(err: &TapedriveError) -> bool {
    match err {
        TapedriveError::Certification(_) => true,
        TapedriveError::Rpc(rpc) => {
            matches!(parse_tape_error(rpc), Some(TapeError::BadSignature))
                || rpc.is_retriable()
        }
        _ => false,
    }
}

pub async fn certify_registered_track<Blockchain: Rpc, Cluster: Api>(
    client: &Tapedrive<Blockchain, Cluster>,
    tape_key: &TapeKey,
    key: Hash,
    track_address: Pubkey,
    spool_group: tape_core::spooler::SpoolGroup,
) -> Result<(), TapedriveError> {
    let payer = client.payer()?;
    tape_retry::retry_if(
        tape_retry::RetryConfig::infinite(),
        None,
        || async {
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

            let compute_ix =
                ComputeBudgetInstruction::set_compute_unit_limit(CERTIFY_TRACK_CU);

            let certify_ix = build_certify_track_ix(
                payer.pubkey(),
                tape_key.pubkey(),
                key,
                EpochNumber(collected.epoch),
                collected.bitmap,
                collected.aggregated_signature,
            );

            match client
                .rpc()
                .send_instructions_with_signers(
                    payer,
                    vec![compute_ix, certify_ix],
                    &[tape_key.as_keypair()],
                )
                .await
            {
                Ok(_) => Ok(()),
                Err(err) => match parse_tape_error(&err) {
                    Some(TapeError::AlreadyCertified) => Ok(()),
                    _ => Err(TapedriveError::Rpc(err)),
                },
            }
        },
        should_retry_certification,
    )
    .await
}

pub async fn write_track<Blockchain: Rpc, Cluster: Api>(
    client: &Tapedrive<Blockchain, Cluster>,
    tape_key: &TapeKey,
    key: Hash,
    data: &[u8],
) -> Result<Track, TapedriveError> {
    let plan = prepare_upload_plan(data)?;
    let registered = register_or_resume_track(client, tape_key, key, &plan).await?;

    if registered.track.data.is_certified() {
        return Ok(registered.track);
    }

    upload_registered_track(
        client,
        registered.address,
        registered.track.data.spool_group(),
        plan.slices,
    )
    .await?;

    certify_registered_track(
        client,
        tape_key,
        key,
        registered.address,
        registered.track.data.spool_group(),
    )
    .await?;

    queries::get_track(client, &registered.address).await
}
