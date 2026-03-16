use std::collections::HashMap;

use store::Store;
use tape_core::encoding::EncodingType;
use tape_core::erasure::slice_for_spool;
use tape_core::spooler::{SpoolGroup, SpoolIndex};
use tape_protocol::api::types::{RepairRequest, StripeSubChunkRequest};
use tape_slicer::{RepairPlan, SliceIndex};
use tape_store::ops::SliceOps;
use tape_store::TapeStore;
use tape_store::types::{Pubkey, TrackInfo};

pub fn validate_slice_entry(
    spool: SpoolIndex,
    track_info: &TrackInfo,
    data: &[u8],
) -> Result<(), String> {
    let Some(slice_index) = slice_for_spool(track_info.spool_group, spool) else {
        return Err("track not mapped to this spool group".to_string());
    };

    if track_info.original_size > 0 && data.is_empty() {
        return Err("empty slice for non-empty track".to_string());
    }

    let expected_max = track_info
        .stripe_size
        .checked_mul(track_info.stripe_count)
        .ok_or_else(|| "invalid stripe dimensions".to_string())?;

    if expected_max > 0 && data.len() as u64 > expected_max {
        return Err("slice exceeds expected decoded size".to_string());
    }

    if !track_info.verify_slice(slice_index, data) {
        return Err("slice does not match commitment".to_string());
    }

    Ok(())
}

pub fn persist_validated_slice<Db: Store>(
    store: &TapeStore<Db>,
    spool: SpoolIndex,
    track: Pubkey,
    track_info: &TrackInfo,
    data: Vec<u8>,
) -> Result<(), String> {
    validate_slice_entry(spool, track_info, &data)?;
    store
        .put_slice(spool, track, data)
        .map_err(|error| format!("put_slice: {error}"))
}

pub fn build_per_helper_requests(
    plan: &RepairPlan,
    spool_group: SpoolGroup,
) -> HashMap<SliceIndex, Vec<StripeSubChunkRequest>> {
    let mut map: HashMap<SliceIndex, Vec<StripeSubChunkRequest>> = HashMap::new();

    for stripe_repair in &plan.stripes {
        for helper in &stripe_repair.helpers {
            map.entry(helper.slice)
                .or_default()
                .push(StripeSubChunkRequest {
                    stripe: stripe_repair.stripe,
                    sub_chunks: helper.sub_chunks.clone(),
                });
        }
    }

    map.retain(
        |slice_idx, _| spool_group.slice_of(
            spool_group.spool_at(**slice_idx)
        ).is_some());

    map
}

pub fn extract_repair_data(
    track_info: &TrackInfo,
    helper_spool: SpoolIndex,
    request: &RepairRequest,
    helper_slice: &[u8],
) -> Result<Vec<u8>, String> {
    use tape_slicer::ClayCoder;

    let profile = track_info.profile();
    let encoding = profile
        .encoding_type()
        .ok_or_else(|| "unknown encoding type".to_string())?;

    if encoding != EncodingType::Clay {
        return Err("repair only supported for Clay encoding".to_string());
    }

    let group = track_info.spool_group;
    let Some(expected_helper_index) = group.slice_of(helper_spool) else {
        return Err("helper spool not in track group".to_string());
    };
    let request_helper_index = group
        .slice_of(request.helper_spool)
        .ok_or_else(|| "request helper spool not in track group".to_string())?;

    if expected_helper_index != request_helper_index {
        return Err("helper spool mismatch".to_string());
    }

    let coder = ClayCoder::from_params(profile.clay_params());
    let chunk_size = coder.track_chunk_size(
        track_info.stripe_size as usize,
        track_info.original_size as usize,
    );
    let sub_chunk_size = chunk_size
        .checked_div(coder.alpha())
        .ok_or_else(|| "invalid repair geometry".to_string())?;

    let mut output = Vec::new();
    for stripe in &request.stripes {
        let stripe_offset = stripe.stripe as usize * chunk_size;
        let chunk = helper_slice
            .get(stripe_offset..stripe_offset + chunk_size)
            .ok_or_else(|| "stripe out of bounds".to_string())?;

        for &sub_chunk in &stripe.sub_chunks {
            let start = sub_chunk as usize * sub_chunk_size;
            let end = start + sub_chunk_size;
            let data = chunk
                .get(start..end)
                .ok_or_else(|| "sub-chunk out of bounds".to_string())?;
            output.extend_from_slice(data);
        }
    }

    Ok(output)
}
