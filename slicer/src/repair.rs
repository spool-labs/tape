//! Bandwidth-optimal single-slice repair via Clay codes.
//!
//! A `RepairPlan` describes exactly which sub-chunks to fetch from which
//! helper nodes, enabling repair at ~1/d bandwidth cost vs full decode.

use std::collections::HashMap;

use crate::clay::ClayCoder;
use crate::errors::RepairError;
use crate::metadata::SliceMetadata;
use crate::slicer::{shard_to_slice, slice_to_shard, Slicer};
use crate::ErasureCoder;
use crate::SliceIndex;

/// Full repair plan for a lost slice across all stripes.
pub struct RepairPlan {
    /// The slice being repaired.
    pub lost: SliceIndex,
    /// Number of stripes in the blob.
    pub num_stripes: u32,
    /// Full chunk size per stripe (bytes).
    pub chunk_size: u64,
    /// Sub-chunk size (chunk_size / alpha).
    pub sub_chunk_size: u64,
    /// Per-stripe repair plans.
    pub stripes: Vec<StripeRepair>,
}

/// Per-stripe repair plan.
pub struct StripeRepair {
    /// Stripe index.
    pub stripe: u32,
    /// Shard index of the lost slice in this stripe (after rotation).
    pub lost_shard: SliceIndex,
    /// Helper contributions for this stripe.
    pub helpers: Vec<HelperPlan>,
}

/// One helper's contribution to one stripe.
pub struct HelperPlan {
    /// Network-level slice index (which node to contact).
    pub slice: SliceIndex,
    /// Coder-level shard index (for clay.repair()).
    pub shard: SliceIndex,
    /// Sub-chunk indices within the shard to fetch.
    pub sub_chunks: Vec<u32>,
}

impl ClayCoder {
    /// Compute repair plan for a single lost shard.
    ///
    /// Returns `(helper_shard, sub_chunk_indices)` per helper.
    pub fn plan_repair(
        &self,
        lost: SliceIndex,
        available: &[SliceIndex],
    ) -> Result<Vec<(SliceIndex, Vec<u32>)>, RepairError> {
        let avail: Vec<usize> = available.iter().map(|s| **s).collect();
        let helpers = self
            .clay
            .minimum_to_repair(*lost, &avail)
            .map_err(|e| RepairError::Clay(e.to_string()))?;
        helpers
            .into_iter()
            .map(|(idx, sub_chunks)| {
                let si = SliceIndex::new(idx).ok_or(RepairError::InvalidSlice)?;
                Ok((si, sub_chunks.into_iter().map(|v| v as u32).collect()))
            })
            .collect()
    }

    /// Repair a single lost shard from partial helper data.
    ///
    /// `helpers`: shard_idx → concatenated sub-chunks (order from `plan_repair`).
    pub fn repair(
        &self,
        lost: SliceIndex,
        helpers: HashMap<SliceIndex, Vec<u8>>,
        chunk_size: usize,
    ) -> Result<Vec<u8>, RepairError> {
        let helper_data: HashMap<usize, Vec<u8>> = helpers
            .into_iter()
            .map(|(idx, data)| (*idx, data))
            .collect();
        self.clay
            .repair(*lost, &helper_data, chunk_size)
            .map_err(|e| RepairError::Clay(e.to_string()))
    }
}

/// Extract sub-chunks from a full slice for repair.
///
/// Called by a helper node: reads the full slice from local storage,
/// extracts only the sub-chunks specified by the plan, and returns
/// the concatenated bytes (stripe order, then sub-chunk order within
/// each stripe). The result is what gets sent over the network.
pub fn extract_repair_data(
    slice: &[u8],
    plan: &RepairPlan,
    helper: SliceIndex,
) -> Result<Vec<u8>, RepairError> {
    let chunk_size = plan.chunk_size as usize;
    let sub_chunk_size = plan.sub_chunk_size as usize;

    let mut out = Vec::new();

    for stripe in &plan.stripes {
        let chunk_offset = stripe.stripe as usize * chunk_size;
        let chunk_end = chunk_offset + chunk_size;

        for hp in &stripe.helpers {
            if hp.slice != helper {
                continue;
            }
            let chunk = slice.get(chunk_offset..chunk_end).ok_or_else(|| {
                RepairError::InvalidLayout("slice too short for chunk".into())
            })?;
            for &sc_idx in &hp.sub_chunks {
                let start = sc_idx as usize * sub_chunk_size;
                let end = start + sub_chunk_size;
                let sc = chunk.get(start..end).ok_or_else(|| {
                    RepairError::InvalidLayout("sub-chunk out of bounds".into())
                })?;
                out.extend_from_slice(sc);
            }
        }
    }

    Ok(out)
}

impl Slicer<ClayCoder> {
    /// Compute a repair plan from locally-known parameters (no reference slice needed).
    ///
    /// `blob_len` and `stripe_size` come from TrackInfo in the tape-store.
    /// The encoding profile is already configured on the Slicer.
    pub fn repair_plan_from_params(
        &self,
        lost: SliceIndex,
        available: &[SliceIndex],
        blob_len: usize,
        stripe_size: usize,
    ) -> Result<RepairPlan, RepairError> {
        let num_stripes = if blob_len == 0 {
            1
        } else {
            (blob_len + stripe_size - 1) / stripe_size
        };

        let effective_len = stripe_size.min(blob_len);
        let chunk_size = self.coder.chunk_size_for(effective_len);

        let n = self.n();
        let alpha = self.coder.alpha();
        if chunk_size % alpha != 0 {
            return Err(RepairError::InvalidLayout(
                format!("chunk_size ({chunk_size}) not divisible by alpha ({alpha})"),
            ));
        }
        let sub_chunk_size = (chunk_size / alpha) as u64;

        let mut stripes = Vec::with_capacity(num_stripes);

        for s in 0..num_stripes {
            let lost_shard_raw = slice_to_shard(self.strategy, n, s, *lost);
            let lost_shard =
                SliceIndex::new(lost_shard_raw).ok_or(RepairError::InvalidSlice)?;

            let available_shards: Vec<SliceIndex> = available
                .iter()
                .filter_map(|slice| {
                    let shard = slice_to_shard(self.strategy, n, s, **slice);
                    SliceIndex::new(shard)
                })
                .collect();

            let helper_plan = self.coder.plan_repair(lost_shard, &available_shards)?;

            let helpers: Vec<HelperPlan> = helper_plan
                .into_iter()
                .map(|(helper_shard, sub_chunks)| {
                    let helper_slice_raw =
                        shard_to_slice(self.strategy, n, s, *helper_shard);
                    let helper_slice = SliceIndex::new(helper_slice_raw)
                        .ok_or(RepairError::InvalidSlice)?;
                    Ok(HelperPlan {
                        slice: helper_slice,
                        shard: helper_shard,
                        sub_chunks,
                    })
                })
                .collect::<Result<Vec<_>, RepairError>>()?;

            stripes.push(StripeRepair {
                stripe: s as u32,
                lost_shard,
                helpers,
            });
        }

        Ok(RepairPlan {
            lost,
            num_stripes: num_stripes as u32,
            chunk_size: chunk_size as u64,
            sub_chunk_size,
            stripes,
        })
    }

    /// Compute a repair plan for a single lost slice.
    ///
    /// `reference` is any available helper slice (used to extract metadata and chunk size).
    /// Returns a plan describing which sub-chunks to fetch from which helpers.
    pub fn repair_plan(
        &self,
        lost: SliceIndex,
        available: &[SliceIndex],
        reference: &[u8],
    ) -> Result<RepairPlan, RepairError> {
        let metadata = SliceMetadata::from_slice(reference)
            .map_err(|e| RepairError::InvalidLayout(e.to_string()))?;

        let blob_len = metadata.blob_len();
        let stripe_size = metadata.stripe_size();
        let num_stripes = if blob_len == 0 {
            1
        } else {
            (blob_len + stripe_size - 1) / stripe_size
        };

        let total_data_len = reference.len().saturating_sub(SliceMetadata::SIZE);
        if total_data_len == 0 || total_data_len % num_stripes != 0 {
            return Err(RepairError::InvalidLayout(
                "inconsistent slice layout".into(),
            ));
        }
        let chunk_size = total_data_len / num_stripes;

        let n = self.n();
        let alpha = self.coder.alpha();
        if chunk_size % alpha != 0 {
            return Err(RepairError::InvalidLayout(
                format!("chunk_size ({chunk_size}) not divisible by alpha ({alpha})"),
            ));
        }
        let sub_chunk_size = (chunk_size / alpha) as u64;

        let mut stripes = Vec::with_capacity(num_stripes);

        for s in 0..num_stripes {
            let lost_shard_raw = slice_to_shard(self.strategy, n, s, *lost);
            let lost_shard =
                SliceIndex::new(lost_shard_raw).ok_or(RepairError::InvalidSlice)?;

            let available_shards: Vec<SliceIndex> = available
                .iter()
                .filter_map(|slice| {
                    let shard = slice_to_shard(self.strategy, n, s, **slice);
                    SliceIndex::new(shard)
                })
                .collect();

            let helper_plan = self.coder.plan_repair(lost_shard, &available_shards)?;

            let helpers: Vec<HelperPlan> = helper_plan
                .into_iter()
                .map(|(helper_shard, sub_chunks)| {
                    let helper_slice_raw =
                        shard_to_slice(self.strategy, n, s, *helper_shard);
                    let helper_slice = SliceIndex::new(helper_slice_raw)
                        .ok_or(RepairError::InvalidSlice)?;
                    Ok(HelperPlan {
                        slice: helper_slice,
                        shard: helper_shard,
                        sub_chunks,
                    })
                })
                .collect::<Result<Vec<_>, RepairError>>()?;

            stripes.push(StripeRepair {
                stripe: s as u32,
                lost_shard,
                helpers,
            });
        }

        Ok(RepairPlan {
            lost,
            num_stripes: num_stripes as u32,
            chunk_size: chunk_size as u64,
            sub_chunk_size,
            stripes,
        })
    }

    /// Repair a single lost slice from full helper slices.
    ///
    /// Self-contained convenience: computes repair plan, extracts sub-chunks
    /// via `extract_repair_data`, and feeds them into `repair()`.
    pub fn repair_full(
        &self,
        lost: SliceIndex,
        helpers: &[(SliceIndex, &[u8])],
    ) -> Result<Vec<u8>, RepairError> {
        if helpers.is_empty() {
            return Err(RepairError::NotEnoughHelpers {
                needed: 1,
                available: 0,
            });
        }

        let available: Vec<SliceIndex> = helpers.iter().map(|(idx, _)| *idx).collect();
        let reference = helpers[0].1;
        let plan = self.repair_plan(lost, &available, reference)?;

        let partial: HashMap<SliceIndex, Vec<u8>> = helpers
            .iter()
            .map(|(idx, slice)| {
                extract_repair_data(slice, &plan, *idx).map(|data| (*idx, data))
            })
            .collect::<Result<_, RepairError>>()?;

        let meta_start = reference.len().checked_sub(SliceMetadata::SIZE).ok_or_else(|| {
            RepairError::InvalidLayout("slice too short for metadata".into())
        })?;
        let metadata_bytes: &[u8; SliceMetadata::SIZE] =
            reference[meta_start..].try_into().unwrap();

        self.repair(&plan, &partial, metadata_bytes)
    }

    /// Bandwidth-optimal repair from partial helper data.
    ///
    /// Takes a precomputed `RepairPlan` and partial data collected per the plan.
    /// Each helper's `Vec<u8>` contains the concatenated sub-chunks for all stripes
    /// (in stripe order, then sub-chunk-index order within each stripe).
    pub fn repair(
        &self,
        plan: &RepairPlan,
        helpers: &HashMap<SliceIndex, Vec<u8>>,
        metadata_bytes: &[u8; SliceMetadata::SIZE],
    ) -> Result<Vec<u8>, RepairError> {
        let chunk_size = plan.chunk_size as usize;
        let sub_chunk_size = plan.sub_chunk_size as usize;
        let num_stripes = plan.num_stripes as usize;

        let mut repaired_data = Vec::with_capacity(num_stripes * chunk_size);
        let mut helper_offsets: HashMap<SliceIndex, usize> = HashMap::new();

        for stripe_plan in &plan.stripes {
            let mut stripe_helpers: HashMap<SliceIndex, Vec<u8>> = HashMap::new();

            for hp in &stripe_plan.helpers {
                let buf = helpers
                    .get(&hp.slice)
                    .ok_or(RepairError::MissingHelper(hp.slice))?;

                let offset = helper_offsets.entry(hp.slice).or_insert(0);
                let bytes_this_stripe = hp.sub_chunks.len() * sub_chunk_size;

                let partial = buf
                    .get(*offset..*offset + bytes_this_stripe)
                    .ok_or(RepairError::MissingHelper(hp.slice))?
                    .to_vec();
                *offset += bytes_this_stripe;

                stripe_helpers.insert(hp.shard, partial);
            }

            let recovered = self.coder.repair(
                stripe_plan.lost_shard,
                stripe_helpers,
                chunk_size,
            )?;
            repaired_data.extend_from_slice(&recovered);
        }

        repaired_data.extend_from_slice(metadata_bytes);
        Ok(repaired_data)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::*;
    use crate::ErasureCoder;
    use tape_core::encoding::EncodingProfile;

    const N: usize = 20;

    fn mk(len: usize) -> Vec<u8> {
        (0..len).map(|i| (i % 251) as u8).collect()
    }

    fn si(i: usize) -> SliceIndex {
        SliceIndex::new(i).unwrap()
    }

    fn helper_refs(chunks: &[Vec<u8>], lost: usize) -> Vec<(SliceIndex, &[u8])> {
        chunks
            .iter()
            .enumerate()
            .filter(|(i, _)| *i != lost)
            .map(|(i, c)| (si(i), c.as_slice()))
            .collect()
    }

    #[test]
    fn repair_coder_direct() {
        let mut coder = ClayCoder::new(20, 10, 19);
        let original = mk(10_000);
        let chunks = coder.encode(&original).unwrap();
        let chunk_size = chunks[0].len();
        let alpha = coder.alpha();
        let sub_chunk_size = chunk_size / alpha;

        for lost_idx in [0, 5, 19] {
            let lost = si(lost_idx);
            let available: Vec<SliceIndex> = (0..20)
                .filter(|&i| i != lost_idx)
                .map(|i| si(i))
                .collect();

            let plan = coder.plan_repair(lost, &available).unwrap();
            assert_eq!(plan.len(), coder.d());

            let mut helpers: HashMap<SliceIndex, Vec<u8>> = HashMap::new();
            for (helper_si, sub_indices) in &plan {
                let mut partial = Vec::new();
                for &sc in sub_indices {
                    let start = sc as usize * sub_chunk_size;
                    let end = start + sub_chunk_size;
                    partial.extend_from_slice(&chunks[**helper_si][start..end]);
                }
                helpers.insert(*helper_si, partial);
            }

            let recovered = coder.repair(lost, helpers, chunk_size).unwrap();
            assert_eq!(recovered, chunks[lost_idx], "repair failed for shard {lost_idx}");
        }
    }

    #[test]
    fn repair_full_single() {
        let mut slicer = Slicer::with_stripe_size(ClayCoder::new(20, 10, 19), 100_000);
        let payload = mk(10_000);
        let chunks = slicer.encode(&payload).unwrap();

        for lost in 0..N {
            let helpers = helper_refs(&chunks, lost);
            let repaired = slicer.repair_full(si(lost), &helpers).unwrap();
            assert_eq!(repaired, chunks[lost], "repair failed for slice {lost}");
        }
    }

    #[test]
    fn repair_full_rotated() {
        let mut slicer = Slicer::with_profile(
            ClayCoder::new(20, 10, 19),
            2000,
            true,
            EncodingProfile::clay_default(),
        );
        let payload = mk(10_000);
        let chunks = slicer.encode(&payload).unwrap();

        for lost in 0..N {
            let helpers = helper_refs(&chunks, lost);
            let repaired = slicer.repair_full(si(lost), &helpers).unwrap();
            assert_eq!(repaired, chunks[lost], "repair failed for slice {lost}");
        }
    }

    #[test]
    fn repair_plan_helpers() {
        let mut slicer = Slicer::with_stripe_size(ClayCoder::new(20, 10, 19), 100_000);
        let payload = mk(10_000);
        let chunks = slicer.encode(&payload).unwrap();

        let available: Vec<SliceIndex> = (1..N).map(|i| si(i)).collect();
        let plan = slicer.repair_plan(si(0), &available, &chunks[1]).unwrap();

        for stripe in &plan.stripes {
            assert_eq!(stripe.helpers.len(), 19, "expected d=19 helpers per stripe");
        }
    }

    #[test]
    fn repair_plan_bandwidth() {
        let mut slicer = Slicer::with_stripe_size(ClayCoder::new(20, 10, 19), 100_000);
        let payload = mk(50_000);
        let chunks = slicer.encode(&payload).unwrap();

        let available: Vec<SliceIndex> = (1..N).map(|i| si(i)).collect();
        let plan = slicer.repair_plan(si(0), &available, &chunks[1]).unwrap();

        let repair_bytes: u64 = plan
            .stripes
            .iter()
            .map(|s| {
                s.helpers
                    .iter()
                    .map(|h| h.sub_chunks.len() as u64 * plan.sub_chunk_size)
                    .sum::<u64>()
            })
            .sum();

        let k = slicer.k() as u64;
        let full_decode_bytes = k * chunks[0].len() as u64;

        assert!(
            repair_bytes < full_decode_bytes / 5,
            "repair bytes ({repair_bytes}) should be < 20% of full decode ({full_decode_bytes})"
        );
    }

    #[test]
    fn repair_plan_rotation() {
        let mut slicer = Slicer::with_profile(
            ClayCoder::new(20, 10, 19),
            100_000,
            true,
            EncodingProfile::clay_default(),
        );
        let payload = mk(300_000);
        let chunks = slicer.encode(&payload).unwrap();

        let available: Vec<SliceIndex> = (1..N).map(|i| si(i)).collect();
        let plan = slicer.repair_plan(si(0), &available, &chunks[1]).unwrap();

        assert!(plan.stripes.len() > 1, "need multiple stripes for this test");

        let shards: Vec<usize> = plan.stripes.iter().map(|s| *s.lost_shard).collect();
        let unique: HashSet<usize> = shards.iter().copied().collect();
        assert!(
            unique.len() > 1,
            "lost_shard should differ across stripes with rotation, got {:?}",
            shards,
        );
    }

    #[test]
    fn repair_exactly_d() {
        let mut slicer = Slicer::with_stripe_size(ClayCoder::new(20, 10, 19), 100_000);
        let d = slicer.coder.d();
        let payload = mk(10_000);
        let chunks = slicer.encode(&payload).unwrap();

        let helpers: Vec<(SliceIndex, &[u8])> = chunks
            .iter()
            .enumerate()
            .skip(1)
            .take(d)
            .map(|(i, c)| (si(i), c.as_slice()))
            .collect();
        assert_eq!(helpers.len(), d);

        let repaired = slicer.repair_full(si(0), &helpers).unwrap();
        assert_eq!(repaired, chunks[0]);
    }

    #[test]
    fn repair_plan_from_params_matches() {
        let mut slicer = Slicer::with_stripe_size(ClayCoder::new(20, 10, 19), 100_000);
        let payload = mk(50_000);
        let chunks = slicer.encode(&payload).unwrap();

        let available: Vec<SliceIndex> = (1..N).map(|i| si(i)).collect();

        let ref_plan = slicer.repair_plan(si(0), &available, &chunks[1]).unwrap();
        let param_plan = slicer
            .repair_plan_from_params(si(0), &available, 50_000, 100_000)
            .unwrap();

        assert_eq!(ref_plan.num_stripes, param_plan.num_stripes);
        assert_eq!(ref_plan.chunk_size, param_plan.chunk_size);
        assert_eq!(ref_plan.sub_chunk_size, param_plan.sub_chunk_size);
        assert_eq!(ref_plan.stripes.len(), param_plan.stripes.len());

        for (ref_s, param_s) in ref_plan.stripes.iter().zip(param_plan.stripes.iter()) {
            assert_eq!(ref_s.stripe, param_s.stripe);
            assert_eq!(ref_s.lost_shard, param_s.lost_shard);
            assert_eq!(ref_s.helpers.len(), param_s.helpers.len());
            for (rh, ph) in ref_s.helpers.iter().zip(param_s.helpers.iter()) {
                assert_eq!(rh.slice, ph.slice);
                assert_eq!(rh.shard, ph.shard);
                assert_eq!(rh.sub_chunks, ph.sub_chunks);
            }
        }
    }

    #[test]
    fn repair_plan_from_params_rotated() {
        let mut slicer = Slicer::with_profile(
            ClayCoder::new(20, 10, 19),
            2000,
            true,
            EncodingProfile::clay_default(),
        );
        let payload = mk(10_000);
        let chunks = slicer.encode(&payload).unwrap();

        // encode() adapts stripe_size via pick_stripe_size, so use the actual value
        let actual_stripe_size = slicer.stripe_size();

        let available: Vec<SliceIndex> = (1..N).map(|i| si(i)).collect();

        let ref_plan = slicer.repair_plan(si(0), &available, &chunks[1]).unwrap();
        let param_plan = slicer
            .repair_plan_from_params(si(0), &available, 10_000, actual_stripe_size)
            .unwrap();

        assert_eq!(ref_plan.num_stripes, param_plan.num_stripes);
        assert_eq!(ref_plan.chunk_size, param_plan.chunk_size);
        assert_eq!(ref_plan.sub_chunk_size, param_plan.sub_chunk_size);

        for (ref_s, param_s) in ref_plan.stripes.iter().zip(param_plan.stripes.iter()) {
            assert_eq!(ref_s.lost_shard, param_s.lost_shard);
            for (rh, ph) in ref_s.helpers.iter().zip(param_s.helpers.iter()) {
                assert_eq!(rh.slice, ph.slice);
                assert_eq!(rh.sub_chunks, ph.sub_chunks);
            }
        }
    }

    #[test]
    fn repair_insufficient() {
        let mut slicer = Slicer::with_stripe_size(ClayCoder::new(20, 10, 19), 100_000);
        let d = slicer.coder.d();
        let payload = mk(10_000);
        let chunks = slicer.encode(&payload).unwrap();

        let helpers: Vec<(SliceIndex, &[u8])> = chunks
            .iter()
            .enumerate()
            .skip(1)
            .take(d - 1)
            .map(|(i, c)| (si(i), c.as_slice()))
            .collect();
        assert_eq!(helpers.len(), d - 1);

        let result = slicer.repair_full(si(0), &helpers);
        assert!(result.is_err(), "should fail with fewer than d helpers");
    }
}
