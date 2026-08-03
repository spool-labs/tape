//! Measures the cost of faking one sampled unit by reconstruction.
//!
//! This is the crux of the deadline argument. The draft assumes rebuilding
//! discarded data is expensive; the slicer chose Clay because it is cheap. Here
//! we run the real repair path: a free-rider that stored nothing recovers the
//! challenged owner's chunk from d helpers and can then answer any sub-leaf in
//! that stripe. We report the exact bytes moved and the measured decode time,
//! then model wall-clock against an honest local read across a range of RTTs.

use std::collections::HashMap;
use std::time::Instant;

use anyhow::{anyhow, Result};
use tape_slicer::{shard_to_slice, slice_to_shard, ClayCoder, SliceIndex};
use tape_core::encoding::ClayParams;

use crate::spool::Spool;

const DECODE_SAMPLES: usize = 11;
const OWNER_INDEX: usize = 3;
const TARGET_STRIPE: usize = 0;

/// One row of the wall-clock model at a given network round trip. Honest is a
/// local read, free-rider is one parallel helper round trip plus decode, and the
/// margin is their difference.
pub struct WallRow {
    pub rtt_ms: f64,
    pub honest_ms: f64,
    pub freeloader_ms: f64,
    pub margin_ms: f64,
}

/// Reconstruction cost of one sampled unit and the wall-clock comparison. The
/// fetched bytes and decode time are for rebuilding one chunk from the helpers,
/// and the rebuilt chunk is checked byte for byte against the owner's real chunk.
pub struct ReconstructReport {
    /// Bytes of the sampled unit the round asks the owner for.
    pub sub_leaf_bytes: usize,
    /// Bytes of one stripe's coded chunk, the unit being rebuilt.
    pub chunk_size: usize,
    /// Helpers the repair plan contacts.
    pub helper_count: usize,
    /// Bytes each helper contributes.
    pub bytes_per_helper: usize,
    /// Bytes moved across every helper to rebuild one chunk.
    pub bytes_fetched: usize,
    /// Bytes a full recovery moves instead: k complete chunks of the stripe.
    pub recovery_bytes: usize,
    /// Repair traffic as a multiple of the chunk it restores. Appendix B puts
    /// this at d/(d-k+1) for a minimum-storage regenerating code, which is 1.6
    /// at the Clay defaults, so repair beats recovery but not a plain fetch.
    pub chunk_equivalents: f64,
    /// Median decode time for one rebuild.
    pub decode_micros: u128,
    /// Whether the rebuilt chunk matched the owner's real chunk byte for byte.
    pub reconstruct_is_correct: bool,
    /// Bytes an honest owner reads locally to answer the same challenge.
    pub honest_read_bytes: usize,
    /// Wall-clock comparison at each modeled round trip.
    pub rows: Vec<WallRow>,
}

/// The Appendix B repair ratio for a minimum-storage regenerating code.
pub fn repair_chunk_equivalents(data_shards: usize, helper_count: usize) -> f64 {
    helper_count as f64 / (helper_count - data_shards + 1) as f64
}

impl ReconstructReport {
    pub fn measure(
        spool: &Spool,
        sub_leaf_bytes: usize,
        rtts_ms: &[f64],
        net_mbps: f64,
        disk_mbps: f64,
    ) -> Result<Self> {
        let group_size = spool.group_size;
        let chunk_size = spool.chunk_size;
        let sub_chunk_size = spool.sub_chunk_size();
        let sub_leaf = sub_leaf_bytes.min(chunk_size);

        // Map the owner slice to its shard in the target stripe, and every other
        // slice to its shard, so the Clay planner can pick helpers.
        let lost_shard =
            SliceIndex::new(slice_to_shard(spool.strategy, group_size, TARGET_STRIPE, OWNER_INDEX));
        let available_shards: Vec<SliceIndex> = (0..group_size)
            .filter(|index| *index != OWNER_INDEX)
            .map(|index| {
                SliceIndex::new(slice_to_shard(spool.strategy, group_size, TARGET_STRIPE, index))
            })
            .collect();

        let coder = ClayCoder::from_params(ClayParams::default());
        let plan = coder
            .plan_repair(lost_shard, &available_shards)
            .map_err(|error| anyhow!("plan_repair failed: {error:?}"))?;

        // Build each helper's contribution from its real slice bytes: the sub
        // chunks the plan asks for, sliced out of that stripe's chunk.
        let stripe_offset = TARGET_STRIPE * chunk_size;
        let mut helper_buffers: HashMap<SliceIndex, Vec<u8>> = HashMap::new();
        let mut bytes_fetched = 0;
        for (helper_shard, sub_chunks) in &plan {
            let helper_slice_index =
                shard_to_slice(spool.strategy, group_size, TARGET_STRIPE, **helper_shard);
            let slice = spool
                .slices
                .get(helper_slice_index)
                .ok_or_else(|| anyhow!("helper slice {helper_slice_index} missing"))?;
            let chunk = slice
                .get(stripe_offset..stripe_offset + chunk_size)
                .ok_or_else(|| anyhow!("slice too short for stripe chunk"))?;

            let mut buffer = Vec::with_capacity(sub_chunks.len() * sub_chunk_size);
            for &sub in sub_chunks {
                let start = sub as usize * sub_chunk_size;
                let end = start + sub_chunk_size;
                let bytes = chunk
                    .get(start..end)
                    .ok_or_else(|| anyhow!("sub-chunk {sub} out of bounds"))?;
                buffer.extend_from_slice(bytes);
            }
            bytes_fetched += buffer.len();
            helper_buffers.insert(*helper_shard, buffer);
        }

        let helper_count = plan.len();
        let bytes_per_helper = if helper_count == 0 {
            0
        } else {
            bytes_fetched / helper_count
        };

        // Run the real repair, timed. Repair consumes the map, so clone per run.
        let owner_chunk = spool
            .slices
            .get(OWNER_INDEX)
            .and_then(|slice| slice.get(stripe_offset..stripe_offset + chunk_size))
            .ok_or_else(|| anyhow!("owner chunk missing"))?
            .to_vec();

        let mut timings = Vec::with_capacity(DECODE_SAMPLES);
        let mut reconstruct_is_correct = true;
        for _ in 0..DECODE_SAMPLES {
            let helpers = helper_buffers.clone();
            let started = Instant::now();
            let recovered = coder
                .repair(lost_shard, helpers, chunk_size)
                .map_err(|error| anyhow!("repair failed: {error:?}"))?;
            timings.push(started.elapsed().as_micros());
            if recovered != owner_chunk {
                reconstruct_is_correct = false;
            }
        }
        timings.sort_unstable();
        let decode_micros = timings[timings.len() / 2];

        let rows = rtts_ms
            .iter()
            .map(|&rtt_ms| {
                let honest_ms = transfer_ms(sub_leaf as f64, disk_mbps);
                let transfer = transfer_ms(bytes_per_helper as f64, net_mbps);
                let decode_ms = decode_micros as f64 / 1_000.0;
                let freeloader_ms = rtt_ms + transfer + decode_ms;
                WallRow {
                    rtt_ms,
                    honest_ms,
                    freeloader_ms,
                    margin_ms: freeloader_ms - honest_ms,
                }
            })
            .collect();

        Ok(Self {
            sub_leaf_bytes: sub_leaf,
            chunk_size,
            helper_count,
            bytes_per_helper,
            bytes_fetched,
            recovery_bytes: spool.data_shards * chunk_size,
            chunk_equivalents: bytes_fetched as f64 / chunk_size as f64,
            decode_micros,
            reconstruct_is_correct,
            honest_read_bytes: sub_leaf,
            rows,
        })
    }
}

/// Milliseconds to move bytes at mbps megabits per second.
fn transfer_ms(bytes: f64, mbps: f64) -> f64 {
    if mbps <= 0.0 {
        return 0.0;
    }
    bytes * 8.0 / (mbps * 1_000.0)
}

#[cfg(test)]
mod tests {
    use super::*;

    // a free-rider rebuilds the owner's chunk byte for byte from the helpers
    #[test]
    fn exact_rebuild() {
        // Proves the free-rider genuinely recovers the owner's bytes from d
        // helpers, so it can answer any sub-leaf challenge in that stripe.
        let spool = Spool::build(1_000_000).expect("build spool");
        let report =
            ReconstructReport::measure(&spool, 1024, &[1.0, 10.0, 50.0], 1_000.0, 5_000.0).expect("measure reconstruct");
        assert!(report.reconstruct_is_correct);
        assert_eq!(report.helper_count, spool.helper_count);
    }

    // repair traffic matches the appendix ratio and stays under a full recovery
    #[test]
    fn repair_ratio() {
        // Repair moves d/(d-k+1) chunk-equivalents, 1.6 at the Clay defaults.
        // That is far below a full recovery but above the chunk it restores, so
        // rebuilding is the cheap path only relative to recovery.
        let spool = Spool::build(1_000_000).expect("build spool");
        let report =
            ReconstructReport::measure(&spool, 1024, &[10.0], 1_000.0, 5_000.0).expect("measure reconstruct");
        let expected = repair_chunk_equivalents(spool.data_shards, spool.helper_count);
        assert!((report.chunk_equivalents - expected).abs() < 0.01);
        assert!(report.bytes_fetched < report.recovery_bytes);
        assert!(report.bytes_fetched > report.chunk_size);
    }

    // fetching the chunk from one peer moves fewer bytes than rebuilding it
    #[test]
    fn plain_fetch() {
        // The cheapest fake is not repair at all: one peer holds the chunk, and
        // asking for it moves fewer bytes over fewer links than d helpers do.
        // This is the free-rider the deadline sweep cannot catch.
        let spool = Spool::build(1_000_000).expect("build spool");
        let report =
            ReconstructReport::measure(&spool, 1024, &[10.0], 1_000.0, 5_000.0).expect("measure reconstruct");
        assert!(report.chunk_size < report.bytes_fetched);
    }
}
