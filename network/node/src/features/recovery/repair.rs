use store::Store;
use tape_core::erasure::{group_for_spool, group_start};
use tape_core::spooler::SpoolIndex;
use tape_slicer::adaptive::pick_stripe_size;
use tape_slicer::clay::ClayCoder;
use tape_slicer::metadata::SliceMetadata;
use tape_slicer::slicer::Slicer;
use tape_slicer::SliceIndex;
use tape_store::types::{Pubkey as StorePubkey, TrackInfo};

use crate::core::context::NodeContext;

use super::error::RecoveryError;
use super::helpers::{fan_out_repair_requests, GroupHelper};

/// Repair a single missing slice using Clay code bandwidth-optimal repair.
///
/// Computes the repair plan, fans out sub-chunk requests to the provided helpers,
/// reconstructs the slice, and verifies against the commitment.
/// Returns the repaired slice bytes — the caller decides whether to store.
pub async fn repair_single_slice<S: Store>(
    _ctx: &NodeContext<S>,
    our_spool: SpoolIndex,
    track_address: StorePubkey,
    track_info: &TrackInfo,
    helpers: &[GroupHelper],
) -> Result<Vec<u8>, RecoveryError> {
    let profile = track_info.profile();

    if !profile.is_clay() {
        return Err(RecoveryError::UnsupportedEncoding);
    }

    let blob_len = track_info.original_size as usize;
    let stripe_size = pick_stripe_size(blob_len);
    let clay_params = profile.clay_params();

    let coder = ClayCoder::from_params(clay_params);
    let slicer = Slicer::with_profile(coder, stripe_size, profile.is_clay(), profile);

    let group = group_for_spool(our_spool);
    let start = group_start(group);
    let our_position = (our_spool - start) as usize;

    let lost = SliceIndex::new(our_position)
        .ok_or_else(|| RecoveryError::RepairFailed("invalid position".into()))?;

    let available: Vec<SliceIndex> = helpers
        .iter()
        .filter_map(|h| SliceIndex::new(h.position))
        .collect();

    let plan = slicer
        .repair_plan_from_params(lost, &available, blob_len, stripe_size)
        .map_err(|e| RecoveryError::Slicer(e.to_string()))?;

    let track_id = track_address.to_string();
    let helper_data = fan_out_repair_requests(&helpers, &plan, &track_id).await?;

    let metadata = SliceMetadata::with_profile(blob_len, stripe_size, profile);
    let metadata_bytes = metadata.to_bytes();

    let repaired_slice = slicer
        .repair(&plan, &helper_data, &metadata_bytes)
        .map_err(|e| RecoveryError::Slicer(e.to_string()))?;

    if !track_info.commitment.is_empty()
        && !track_info.verify_slice(our_position, &repaired_slice)
    {
        return Err(RecoveryError::RepairFailed(
            "repaired slice failed leaf hash verification".into(),
        ));
    }

    Ok(repaired_slice)
}

