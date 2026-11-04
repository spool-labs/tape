//! Bandwidth-optimal single-slice repair via Clay codes.
//!
//! A slice is a position in the spool group: a node's piece of the track, 
//! and where its bytes live on the network. A shard is the coder's index 
//! for one of a stripe's n chunks. The rotation shifts the mapping between 
//! the two every stripe, so a slice holds a different shard in each
//! stripe. Planning works in shards, and fetching works in slices.
//!
//! A `RepairPlan` gives, for each stripe, the sub-chunks to fetch from each
//! helper position. `HelperData` holds what an attempt has fetched, and
//! `Slicer::repair` rebuilds the slice once every stripe holds its sub-chunks.

use std::collections::{BTreeMap, HashMap};

use clay_codes::ClayError;

use crate::clay::ClayCoder;
use crate::errors::RepairError;
use crate::metadata::SliceMetadata;
use crate::slicer::{shard_to_slice, slice_to_shard, Slicer};
use crate::stripe::num_stripes;
use crate::ErasureCoder;
use crate::SliceIndex;

/// Each chosen helper's shard and the sub-chunk indices it sends
pub type ShardPlan = Vec<(SliceIndex, Vec<u32>)>;

/// Repair plan for a lost slice, one entry per stripe
pub struct RepairPlan {
    /// The slice being repaired.
    pub lost: SliceIndex,
    /// Full chunk size per stripe (bytes).
    pub chunk_size: u64,
    /// Sub-chunk size (chunk_size / alpha).
    pub sub_chunk_size: u64,
    /// Per-stripe repair plans, in stripe order.
    pub stripes: Vec<StripeRepair>,
}

/// Per-stripe repair plan
pub struct StripeRepair {
    /// Stripe index.
    pub stripe: u32,
    /// Shard index of the lost slice in this stripe (after rotation).
    pub lost_shard: SliceIndex,
    /// Helper contributions for this stripe, or `None` when the available
    /// positions give this stripe no plan.
    pub helpers: Option<Vec<HelperPlan>>,
}

/// One helper's contribution to one stripe
pub struct HelperPlan {
    /// Network-level slice index (which node to contact).
    pub slice: SliceIndex,
    /// Coder-level shard index in this stripe.
    pub shard: SliceIndex,
    /// Sub-chunk indices within the shard to fetch, in the order the coder reads them.
    pub sub_chunks: Vec<u32>,
}

/// Sub-chunks to ask one position for, stripe by stripe
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HelperRequest {
    /// Position to ask.
    pub slice: SliceIndex,
    /// Sub-chunks per stripe, in the order the helper sends them.
    pub stripes: Vec<StripeSubChunks>,
}

/// Sub-chunk indices within one stripe's chunk
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StripeSubChunks {
    /// Stripe index.
    pub stripe: u32,
    /// Sub-chunk indices within the stripe's chunk.
    pub sub_chunks: Vec<u32>,
}

/// Sub-chunks one repair attempt holds, by position and stripe
pub struct HelperData {
    sub_chunk_size: usize,
    responses: Vec<Vec<u8>>,
    pieces: HashMap<(SliceIndex, u32), Vec<Piece>>,
}

/// One response's sub-chunks for one stripe, and where their bytes start
struct Piece {
    sub_chunks: Vec<u32>,
    response: usize,
    offset: usize,
}

impl RepairPlan {
    /// The sub-chunks each position still has to send for this plan
    ///
    /// Leaves out what `held` already holds. Positions come in ascending
    /// order, stripes in plan order, and sub-chunks in the plan's order.
    pub fn requests(&self, held: &HelperData) -> Vec<HelperRequest> {
        let mut by_slice: BTreeMap<SliceIndex, Vec<StripeSubChunks>> = BTreeMap::new();
        for stripe in &self.stripes {
            let Some(helpers) = &stripe.helpers else {
                continue;
            };
            for helper in helpers {
                let mut sub_chunks = Vec::new();
                for &sub_chunk in &helper.sub_chunks {
                    if !held.has(helper.slice, stripe.stripe, sub_chunk) {
                        sub_chunks.push(sub_chunk);
                    }
                }
                if !sub_chunks.is_empty() {
                    let entry = by_slice.entry(helper.slice).or_default();
                    entry.push(StripeSubChunks { stripe: stripe.stripe, sub_chunks });
                }
            }
        }

        let mut requests = Vec::with_capacity(by_slice.len());
        for (slice, stripes) in by_slice {
            requests.push(HelperRequest { slice, stripes });
        }
        requests
    }

    /// Stripes with no plan, or whose planned sub-chunks `held` lacks
    pub fn short_stripes(&self, held: &HelperData) -> Vec<u32> {
        let mut short = Vec::new();
        for stripe in &self.stripes {
            let is_held = match &stripe.helpers {
                Some(helpers) => held.holds(stripe.stripe, helpers),
                None => false,
            };
            if !is_held {
                short.push(stripe.stripe);
            }
        }
        short
    }
}

impl HelperData {
    /// Create an empty store for a plan's sub-chunk size
    pub fn new(plan: &RepairPlan) -> Self {
        Self {
            sub_chunk_size: plan.sub_chunk_size as usize,
            responses: Vec::new(),
            pieces: HashMap::new(),
        }
    }

    /// Add one position's response to the request it answers
    ///
    /// Refuses a response whose length doesn't match the request.
    pub fn insert(&mut self, request: &HelperRequest, data: Vec<u8>) -> Result<(), RepairError> {
        let mut count = 0;
        for stripe in &request.stripes {
            count += stripe.sub_chunks.len();
        }
        let expected = count * self.sub_chunk_size;
        if data.len() != expected {
            return Err(RepairError::HelperLength {
                slice: request.slice,
                expected,
                actual: data.len(),
            });
        }

        let response = self.responses.len();
        let mut offset = 0;
        for stripe in &request.stripes {
            let piece = Piece { sub_chunks: stripe.sub_chunks.clone(), response, offset };
            let pieces = self.pieces.entry((request.slice, stripe.stripe)).or_default();
            pieces.push(piece);
            offset += stripe.sub_chunks.len() * self.sub_chunk_size;
        }
        self.responses.push(data);
        Ok(())
    }

    /// Whether the store holds one sub-chunk of a position's stripe
    pub fn has(&self, slice: SliceIndex, stripe: u32, sub_chunk: u32) -> bool {
        self.find(slice, stripe, sub_chunk).is_some()
    }

    /// Whether the store holds every planned sub-chunk of a stripe
    fn holds(&self, stripe: u32, helpers: &[HelperPlan]) -> bool {
        for helper in helpers {
            for &sub_chunk in &helper.sub_chunks {
                if !self.has(helper.slice, stripe, sub_chunk) {
                    return false;
                }
            }
        }
        true
    }

    /// The bytes of one held sub-chunk
    fn find(&self, slice: SliceIndex, stripe: u32, sub_chunk: u32) -> Option<&[u8]> {
        let pieces = self.pieces.get(&(slice, stripe))?;
        for piece in pieces {
            if let Some(index) = piece.sub_chunks.iter().position(|&held| held == sub_chunk) {
                let start = piece.offset + index * self.sub_chunk_size;
                let response = self.responses.get(piece.response)?;
                return response.get(start..start + self.sub_chunk_size);
            }
        }
        None
    }

    /// A position's sub-chunks for one stripe as one borrowed run, when one
    /// response sent exactly these in this order
    fn run(&self, slice: SliceIndex, stripe: u32, sub_chunks: &[u32]) -> Option<&[u8]> {
        let pieces = self.pieces.get(&(slice, stripe))?;
        for piece in pieces {
            if piece.sub_chunks == sub_chunks {
                let end = piece.offset + sub_chunks.len() * self.sub_chunk_size;
                let response = self.responses.get(piece.response)?;
                return response.get(piece.offset..end);
            }
        }
        None
    }

    /// A position's sub-chunks for one stripe, copied together in the given order
    fn gather(&self, slice: SliceIndex, stripe: u32, sub_chunks: &[u32]) -> Option<Vec<u8>> {
        let mut bytes = Vec::with_capacity(sub_chunks.len() * self.sub_chunk_size);
        for &sub_chunk in sub_chunks {
            bytes.extend_from_slice(self.find(slice, stripe, sub_chunk)?);
        }
        Some(bytes)
    }
}

impl ClayCoder {
    /// Plan the repair of a lost shard from the available shards
    ///
    /// Returns each chosen helper's shard and sub-chunk indices in the
    /// caller's order, or `None` when the available shards give no plan.
    pub fn plan_repair(
        &self,
        lost: SliceIndex,
        available: &[SliceIndex],
    ) -> Result<Option<ShardPlan>, RepairError> {
        let mut nodes = Vec::with_capacity(available.len());
        for shard in available {
            nodes.push(**shard);
        }

        let plan = match self.clay.plan_repair(*lost, &nodes) {
            Ok(plan) => plan,
            Err(ClayError::InsufficientHelpers { .. } | ClayError::MissingYSectionHelper { .. }) => {
                return Ok(None)
            }
            Err(error) => return Err(RepairError::Clay(error.to_string())),
        };

        let mut helpers = Vec::with_capacity(plan.len());
        for (shard, sub_chunks) in plan {
            let mut indices = Vec::with_capacity(sub_chunks.len());
            for sub_chunk in sub_chunks {
                indices.push(sub_chunk as u32);
            }
            helpers.push((SliceIndex::new(shard), indices));
        }
        Ok(Some(helpers))
    }

    /// Repair a lost shard from the sub-chunks its plan asked for
    ///
    /// `helpers` holds one slot per shard. Each planned helper's slot holds
    /// its sub-chunks in the plan's order.
    pub fn repair(
        &self,
        lost: SliceIndex,
        plan: &[HelperPlan],
        helpers: &[Option<&[u8]>],
        chunk_size: usize,
    ) -> Result<Vec<u8>, RepairError> {
        let mut nodes = Vec::with_capacity(plan.len());
        for helper in plan {
            let mut sub_chunks = Vec::with_capacity(helper.sub_chunks.len());
            for &sub_chunk in &helper.sub_chunks {
                sub_chunks.push(sub_chunk as usize);
            }
            nodes.push((*helper.shard, sub_chunks));
        }

        self.clay
            .repair(*lost, &nodes, helpers, chunk_size)
            .map_err(|e| RepairError::Clay(e.to_string()))
    }
}

/// Extract the sub-chunks a request asks for from a full slice
///
/// A helper reads its slice and sends these bytes: stripe by stripe, and each
/// stripe's sub-chunks in the request's order.
pub fn extract_repair_data(
    slice: &[u8],
    plan: &RepairPlan,
    request: &HelperRequest,
) -> Result<Vec<u8>, RepairError> {
    let chunk_size = plan.chunk_size as usize;
    let sub_chunk_size = plan.sub_chunk_size as usize;

    let mut out = Vec::new();
    for stripe in &request.stripes {
        let chunk_offset = stripe.stripe as usize * chunk_size;
        let chunk = slice.get(chunk_offset..chunk_offset + chunk_size).ok_or_else(|| {
            RepairError::InvalidLayout("slice too short for chunk".into())
        })?;
        for &sub_chunk in &stripe.sub_chunks {
            let start = sub_chunk as usize * sub_chunk_size;
            let bytes = chunk.get(start..start + sub_chunk_size).ok_or_else(|| {
                RepairError::InvalidLayout("sub-chunk out of bounds".into())
            })?;
            out.extend_from_slice(bytes);
        }
    }

    Ok(out)
}

impl Slicer<ClayCoder> {
    /// Plan the repair of a lost slice in every stripe of a track
    ///
    /// `available` lists the positions to plan from, in the caller's order of
    /// preference. `blob_len` and `stripe_size` come from the track's encoding.
    pub fn repair_plan(
        &self,
        lost: SliceIndex,
        available: &[SliceIndex],
        blob_len: usize,
        stripe_size: usize,
    ) -> Result<RepairPlan, RepairError> {
        if stripe_size == 0 {
            return Err(RepairError::InvalidLayout("stripe size is zero".into()));
        }

        let chunk_size = self.coder.track_chunk_size(stripe_size, blob_len);
        let alpha = self.coder.alpha();
        if !chunk_size.is_multiple_of(alpha) {
            return Err(RepairError::InvalidLayout(
                format!("chunk_size ({chunk_size}) not divisible by alpha ({alpha})"),
            ));
        }

        let count = num_stripes(blob_len, stripe_size);
        let mut stripes = Vec::with_capacity(count);
        for stripe in 0..count {
            stripes.push(self.plan_stripe(lost, available, stripe as u32)?);
        }

        Ok(RepairPlan {
            lost,
            chunk_size: chunk_size as u64,
            sub_chunk_size: (chunk_size / alpha) as u64,
            stripes,
        })
    }

    /// Plan some stripes of a plan again from a new order of available positions
    pub fn replan(
        &self,
        plan: &mut RepairPlan,
        available: &[SliceIndex],
        stripes: &[u32],
    ) -> Result<(), RepairError> {
        for &stripe in stripes {
            let replanned = self.plan_stripe(plan.lost, available, stripe)?;
            let entry = plan.stripes.get_mut(stripe as usize).ok_or_else(|| {
                RepairError::InvalidLayout(format!("stripe {stripe} is past the plan"))
            })?;
            *entry = replanned;
        }
        Ok(())
    }

    /// Plan one stripe, mapping positions to shards with its rotation
    fn plan_stripe(
        &self,
        lost: SliceIndex,
        available: &[SliceIndex],
        stripe: u32,
    ) -> Result<StripeRepair, RepairError> {
        let n = self.n();
        let index = stripe as usize;

        // a position past the group's width would alias another under rotation
        if *lost >= n {
            return Err(RepairError::InvalidSlice);
        }
        let mut shards = Vec::with_capacity(available.len());
        for slice in available {
            if **slice >= n {
                return Err(RepairError::InvalidSlice);
            }
            shards.push(SliceIndex::new(slice_to_shard(self.strategy, n, index, **slice)));
        }

        let lost_shard = SliceIndex::new(slice_to_shard(self.strategy, n, index, *lost));
        let Some(plan) = self.coder.plan_repair(lost_shard, &shards)? else {
            return Ok(StripeRepair { stripe, lost_shard, helpers: None });
        };

        let mut helpers = Vec::with_capacity(plan.len());
        for (shard, sub_chunks) in plan {
            let slice = SliceIndex::new(shard_to_slice(self.strategy, n, index, *shard));
            helpers.push(HelperPlan { slice, shard, sub_chunks });
        }
        Ok(StripeRepair { stripe, lost_shard, helpers: Some(helpers) })
    }

    /// Repair a lost slice from full helper slices
    ///
    /// Plans with every given helper, extracts each one's sub-chunks with
    /// `extract_repair_data` and feeds them to `repair`.
    pub fn repair_full(
        &self,
        lost: SliceIndex,
        helpers: &[(SliceIndex, &[u8])],
    ) -> Result<Vec<u8>, RepairError> {
        let Some(&(_, reference)) = helpers.first() else {
            return Err(RepairError::NotEnoughHelpers { needed: 1, available: 0 });
        };
        let metadata = self
            .read_metadata(reference)
            .map_err(|e| RepairError::InvalidLayout(e.to_string()))?;

        let mut available = Vec::with_capacity(helpers.len());
        for (slice, _) in helpers {
            available.push(*slice);
        }
        let plan = self.repair_plan(lost, &available, metadata.blob_len(), metadata.stripe_size())?;

        let expected = plan.stripes.len() * plan.chunk_size as usize + SliceMetadata::SIZE;
        if reference.len() != expected {
            return Err(RepairError::InvalidLayout(format!(
                "slice is {} bytes, the plan expects {expected}",
                reference.len(),
            )));
        }

        let mut held = HelperData::new(&plan);
        for request in plan.requests(&held) {
            let mut source = None;
            for &(slice, data) in helpers {
                if slice == request.slice {
                    source = Some(data);
                    break;
                }
            }
            let data = source.ok_or(RepairError::InvalidSlice)?;
            held.insert(&request, extract_repair_data(data, &plan, &request)?)?;
        }

        let metadata_bytes: &[u8; SliceMetadata::SIZE] = reference[expected - SliceMetadata::SIZE..]
            .try_into()
            .map_err(|_| RepairError::InvalidLayout("slice too short for metadata".into()))?;
        self.repair(&plan, &held, metadata_bytes)
    }

    /// Rebuild the lost slice from the sub-chunks an attempt holds
    ///
    /// When a stripe has no plan or lacks sub-chunks, this decodes nothing and
    /// returns `ShortStripes` with every such stripe, so the caller can fetch
    /// for them and call again.
    pub fn repair(
        &self,
        plan: &RepairPlan,
        held: &HelperData,
        metadata_bytes: &[u8; SliceMetadata::SIZE],
    ) -> Result<Vec<u8>, RepairError> {
        let short = plan.short_stripes(held);
        if !short.is_empty() {
            return Err(RepairError::ShortStripes(short));
        }

        let chunk_size = plan.chunk_size as usize;
        let mut repaired = Vec::with_capacity(plan.stripes.len() * chunk_size + SliceMetadata::SIZE);
        for stripe in &plan.stripes {
            let chunk = self.repair_stripe(stripe, held, chunk_size)?;
            repaired.extend_from_slice(&chunk);
        }

        repaired.extend_from_slice(metadata_bytes);
        Ok(repaired)
    }

    /// Decode one stripe's lost chunk, borrowing each helper's bytes where
    /// one response holds them in plan order
    fn repair_stripe(
        &self,
        stripe: &StripeRepair,
        held: &HelperData,
        chunk_size: usize,
    ) -> Result<Vec<u8>, RepairError> {
        let short = || RepairError::ShortStripes(vec![stripe.stripe]);
        let helpers = stripe.helpers.as_ref().ok_or_else(short)?;

        let mut borrowed = Vec::with_capacity(helpers.len());
        let mut gathered = Vec::new();
        for helper in helpers {
            match held.run(helper.slice, stripe.stripe, &helper.sub_chunks) {
                Some(bytes) => borrowed.push((*helper.shard, bytes)),
                None => {
                    let bytes = held.gather(helper.slice, stripe.stripe, &helper.sub_chunks);
                    gathered.push((*helper.shard, bytes.ok_or_else(short)?));
                }
            }
        }

        let mut slots: Vec<Option<&[u8]>> = vec![None; self.n()];
        for (shard, bytes) in borrowed {
            if let Some(slot) = slots.get_mut(shard) {
                *slot = Some(bytes);
            }
        }
        for (shard, bytes) in &gathered {
            if let Some(slot) = slots.get_mut(*shard) {
                *slot = Some(bytes.as_slice());
            }
        }

        self.coder.repair(stripe.lost_shard, helpers, &slots, chunk_size)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::*;
    use tape_core::encoding::TAPENET_CLAY;

    const N: usize = 20;

    /// Stripes in a 64 MiB tapenet track
    const TRACK_STRIPES: usize = 68;

    fn test_coder() -> ClayCoder {
        ClayCoder::from_params(TAPENET_CLAY)
    }

    fn mk(len: usize) -> Vec<u8> {
        (0..len).map(|i| (i % 251) as u8).collect()
    }

    fn si(i: usize) -> SliceIndex {
        SliceIndex::new(i)
    }

    fn helper_refs(chunks: &[Vec<u8>], lost: usize) -> Vec<(SliceIndex, &[u8])> {
        chunks
            .iter()
            .enumerate()
            .filter(|(i, _)| *i != lost)
            .map(|(i, c)| (si(i), c.as_slice()))
            .collect()
    }

    /// A rotated slicer whose stripes are one encode unit each
    fn unit_slicer(coder: ClayCoder) -> Slicer<ClayCoder> {
        let unit = coder.stripe_alignment();
        let mut slicer = Slicer::with_rotation(coder);
        slicer.set_stripe_cap(unit);
        slicer
    }

    /// A rotated tapenet track with as many stripes as a 64 MiB one
    fn track() -> (Slicer<ClayCoder>, Vec<Vec<u8>>) {
        let slicer = unit_slicer(test_coder());
        let unit = slicer.coder.stripe_alignment();
        let slices = slicer.encode(&mk(TRACK_STRIPES * unit)).expect("encode");
        (slicer, slices)
    }

    /// Repair `lost` from every position outside `down`
    fn repair_without(
        slicer: &Slicer<ClayCoder>,
        slices: &[Vec<u8>],
        lost: usize,
        down: &[usize],
    ) -> Result<Vec<u8>, RepairError> {
        let helpers: Vec<(SliceIndex, &[u8])> = slices
            .iter()
            .enumerate()
            .filter(|(i, _)| *i != lost && !down.contains(i))
            .map(|(i, s)| (si(i), s.as_slice()))
            .collect();
        slicer.repair_full(si(lost), &helpers)
    }

    /// Check each stripe's plan costs d·β plus (k − 1)·β per down position in
    /// the lost shard's y-section
    fn assert_cost(slicer: &Slicer<ClayCoder>, lost: usize, down: &[usize]) {
        let k = slicer.k();
        let beta = slicer.coder.beta();
        let q = slicer.coder.d() - k + 1;
        let available: Vec<SliceIndex> = (0..N)
            .filter(|i| *i != lost && !down.contains(i))
            .map(si)
            .collect();
        let blob_len = TRACK_STRIPES * slicer.coder.stripe_alignment();
        let plan = slicer.repair_plan(si(lost), &available, blob_len, slicer.stripe_size()).expect("plan");

        for stripe in &plan.stripes {
            let helpers = stripe.helpers.as_ref().expect("stripe plan");
            let cost: usize = helpers.iter().map(|h| h.sub_chunks.len()).sum();
            let section = *stripe.lost_shard / q;
            let missing = down
                .iter()
                .filter(|&&position| {
                    slice_to_shard(slicer.strategy, N, stripe.stripe as usize, position) / q == section
                })
                .count();
            let expected = slicer.coder.d() * beta + (k - 1) * beta * missing;
            assert_eq!(cost, expected, "lost {lost} down {down:?} stripe {}", stripe.stripe);
        }
    }

    /// Insert the responses to `requests` from full slices, skipping `down`
    fn answer(plan: &RepairPlan, held: &mut HelperData, slices: &[Vec<u8>], down: &[usize]) {
        for request in plan.requests(held) {
            if down.contains(&*request.slice) {
                continue;
            }
            let data = extract_repair_data(&slices[*request.slice], plan, &request).expect("extract");
            held.insert(&request, data).expect("insert");
        }
    }

    fn metadata_of(slice: &[u8]) -> [u8; SliceMetadata::SIZE] {
        slice[slice.len() - SliceMetadata::SIZE..].try_into().expect("metadata")
    }

    // the coder plans and repairs one shard with every other shard available
    #[test]
    fn coder_direct() {
        let coder = test_coder();
        let original = mk(10_000);
        let chunks = coder.encode(&original).expect("encode");
        let chunk_size = chunks[0].len();
        let sub_chunk_size = chunk_size / coder.alpha();

        for lost in [0, 5, 19] {
            let available: Vec<SliceIndex> = (0..N)
                .filter(|&i| i != lost)
                .map(si)
                .collect();

            let plan = coder.plan_repair(si(lost), &available).expect("plan").expect("some plan");
            assert_eq!(plan.len(), coder.d());

            let mut helpers = Vec::new();
            let mut partial: Vec<Option<Vec<u8>>> = vec![None; N];
            for (shard, sub_chunks) in plan {
                let mut bytes = Vec::new();
                for &sc in &sub_chunks {
                    let start = sc as usize * sub_chunk_size;
                    bytes.extend_from_slice(&chunks[*shard][start..start + sub_chunk_size]);
                }
                partial[*shard] = Some(bytes);
                helpers.push(HelperPlan { slice: shard, shard, sub_chunks });
            }
            let slots: Vec<Option<&[u8]>> = partial.iter().map(|b| b.as_deref()).collect();

            let recovered = coder.repair(si(lost), &helpers, &slots, chunk_size).expect("repair");
            assert_eq!(recovered, chunks[lost], "repair failed for shard {lost}");
        }
    }

    // every slice repairs bit-exact on one stripe
    #[test]
    fn full_single() {
        let slicer = Slicer::with_stripe_cap(test_coder(), 100_000);
        let chunks = slicer.encode(&mk(10_000)).expect("encode");

        for lost in 0..N {
            let repaired = slicer.repair_full(si(lost), &helper_refs(&chunks, lost)).expect("repair");
            assert_eq!(repaired, chunks[lost], "repair failed for slice {lost}");
        }
    }

    // every slice repairs bit-exact across rotated stripes
    #[test]
    fn full_rotated() {
        let mut slicer = Slicer::with_rotation(test_coder());
        slicer.set_stripe_cap(2_000);
        let chunks = slicer.encode(&mk(10_000)).expect("encode");

        for lost in 0..N {
            let repaired = slicer.repair_full(si(lost), &helper_refs(&chunks, lost)).expect("repair");
            assert_eq!(repaired, chunks[lost], "repair failed for slice {lost}");
        }
    }

    // every stripe plans d helpers with every position available
    #[test]
    fn plan_helpers() {
        let slicer = Slicer::with_stripe_cap(test_coder(), 100_000);
        slicer.encode(&mk(10_000)).expect("encode");
        let available: Vec<SliceIndex> = (1..N).map(si).collect();

        let plan = slicer.repair_plan(si(0), &available, 10_000, slicer.stripe_size()).expect("plan");

        let d = slicer.coder.d();
        for stripe in &plan.stripes {
            let helpers = stripe.helpers.as_ref().expect("stripe plan");
            assert_eq!(helpers.len(), d, "expected d={d} helpers per stripe");
        }
    }

    // repair reads d*beta of the k*alpha sub-chunks a full decode needs
    #[test]
    fn plan_bandwidth() {
        let slicer = Slicer::with_stripe_cap(test_coder(), 100_000);
        let chunks = slicer.encode(&mk(50_000)).expect("encode");
        let available: Vec<SliceIndex> = (1..N).map(si).collect();

        let plan = slicer.repair_plan(si(0), &available, 50_000, slicer.stripe_size()).expect("plan");

        let mut repair_bytes = 0;
        for stripe in &plan.stripes {
            for helper in stripe.helpers.as_ref().expect("stripe plan") {
                repair_bytes += helper.sub_chunks.len() as u64 * plan.sub_chunk_size;
            }
        }
        let k = slicer.k() as u64;
        let d = slicer.coder.d() as u64;
        let alpha = slicer.coder.alpha() as u64;
        let beta = slicer.coder.beta() as u64;
        let full_decode_bytes = k * chunks[0].len() as u64;
        assert!(
            repair_bytes * k * alpha <= full_decode_bytes * d * beta,
            "repair bytes ({repair_bytes}) exceed d*beta/(k*alpha) of full decode ({full_decode_bytes})"
        );
    }

    // the lost shard moves across stripes under rotation
    #[test]
    fn plan_rotation() {
        let (slicer, _) = track();
        let available: Vec<SliceIndex> = (1..N).map(si).collect();
        let blob_len = TRACK_STRIPES * slicer.coder.stripe_alignment();

        let plan = slicer.repair_plan(si(0), &available, blob_len, slicer.stripe_size()).expect("plan");

        assert_eq!(plan.stripes.len(), TRACK_STRIPES);
        let shards: HashSet<usize> = plan.stripes.iter().map(|s| *s.lost_shard).collect();
        assert_eq!(shards.len(), N);
    }

    // exactly d helpers repair the slice
    #[test]
    fn exactly_d() {
        let slicer = Slicer::with_stripe_cap(test_coder(), 100_000);
        let d = slicer.coder.d();
        let chunks = slicer.encode(&mk(10_000)).expect("encode");
        let helpers: Vec<(SliceIndex, &[u8])> = chunks
            .iter()
            .enumerate()
            .skip(1)
            .take(d)
            .map(|(i, c)| (si(i), c.as_slice()))
            .collect();

        let repaired = slicer.repair_full(si(0), &helpers).expect("repair");

        assert_eq!(repaired, chunks[0]);
    }

    // k - 1 positions give no stripe a plan, and k positions repair every stripe
    #[test]
    fn below_k() {
        let (slicer, slices) = track();
        let k = slicer.k();
        let all_stripes: Vec<u32> = (0..TRACK_STRIPES as u32).collect();

        let few: Vec<usize> = (k..N).collect();
        let result = repair_without(&slicer, &slices, 0, &few);
        assert_eq!(result, Err(RepairError::ShortStripes(all_stripes)));

        let enough: Vec<usize> = (k + 1..N).collect();
        let repaired = repair_without(&slicer, &slices, 0, &enough).expect("repair");
        assert_eq!(repaired, slices[0]);
    }

    // one position down at every distance from the lost one
    #[test]
    fn one_down() {
        let (slicer, slices) = track();

        for lost in [0, 7] {
            for distance in 1..N {
                let down = (lost + distance) % N;
                assert_cost(&slicer, lost, &[down]);
                let repaired = repair_without(&slicer, &slices, lost, &[down]).expect("repair");
                assert_eq!(repaired, slices[lost], "lost {lost} down {down}");
            }
        }
    }

    // two positions down at every pair of distances from the lost one
    #[test]
    fn two_down() {
        let (slicer, slices) = track();

        for first in 1..N {
            for second in first + 1..N {
                assert_cost(&slicer, 0, &[first, second]);
                let repaired = repair_without(&slicer, &slices, 0, &[first, second]).expect("repair");
                assert_eq!(repaired, slices[0], "down {first} and {second}");
            }
        }
    }

    // with a virtual node in the lost section, a stripe whose plan needs the
    // down node has no plan, the others plan, and a later plan completes it
    #[test]
    fn no_plan() {
        // (n, k, d) = (8, 4, 6) holds one virtual node beside shards 3 and 4
        let slicer = unit_slicer(ClayCoder::new(8, 4, 6));
        let unit = slicer.coder.stripe_alignment();
        let blob_len = 16 * unit;
        let slices = slicer.encode(&mk(blob_len)).expect("encode");
        let available: Vec<SliceIndex> = (2..8).map(si).collect();

        let mut plan = slicer.repair_plan(si(0), &available, blob_len, slicer.stripe_size()).expect("plan");
        let mut held = HelperData::new(&plan);
        answer(&plan, &mut held, &slices, &[]);

        // in stripes where 3 * stripe = 5 mod 8, the lost position holds
        // shard 3 and the down one shard 4
        assert_eq!(plan.short_stripes(&held), vec![7, 15]);
        let metadata = metadata_of(&slices[0]);
        assert_eq!(
            slicer.repair(&plan, &held, &metadata),
            Err(RepairError::ShortStripes(vec![7, 15])),
        );

        let every: Vec<SliceIndex> = (1..8).map(si).collect();
        slicer.replan(&mut plan, &every, &[7, 15]).expect("replan");
        answer(&plan, &mut held, &slices, &[]);
        assert_eq!(slicer.repair(&plan, &held, &metadata).expect("repair"), slices[0]);
    }

    // the plan keeps the caller's order and takes the first k other-section helpers in it
    #[test]
    fn caller_order() {
        let (slicer, _) = track();
        let q = slicer.coder.d() - slicer.k() + 1;
        let order: Vec<SliceIndex> = (1..N).rev().map(si).collect();
        let blob_len = TRACK_STRIPES * slicer.coder.stripe_alignment();

        let plan = slicer.repair_plan(si(0), &order, blob_len, slicer.stripe_size()).expect("plan");

        for stripe in &plan.stripes {
            let helpers = stripe.helpers.as_ref().expect("stripe plan");
            let planned: Vec<SliceIndex> = helpers.iter().map(|h| h.slice).collect();
            let kept: Vec<SliceIndex> = order
                .iter()
                .copied()
                .filter(|s| planned.contains(s))
                .collect();
            assert_eq!(planned, kept, "stripe {}", stripe.stripe);

            let section = *stripe.lost_shard / q;
            let other: Vec<SliceIndex> = order
                .iter()
                .copied()
                .filter(|s| slice_to_shard(slicer.strategy, N, stripe.stripe as usize, **s) / q != section)
                .take(slicer.k())
                .collect();
            let chosen: Vec<SliceIndex> = helpers
                .iter()
                .filter(|h| *h.shard / q != section)
                .map(|h| h.slice)
                .collect();
            assert_eq!(chosen, other, "stripe {}", stripe.stripe);
        }
    }

    // a response of the wrong length is refused, and a held one leaves the requests
    #[test]
    fn wrong_length() {
        let slicer = Slicer::with_stripe_cap(test_coder(), 100_000);
        let slices = slicer.encode(&mk(10_000)).expect("encode");
        let available: Vec<SliceIndex> = (1..N).map(si).collect();
        let plan = slicer.repair_plan(si(0), &available, 10_000, slicer.stripe_size()).expect("plan");
        let mut held = HelperData::new(&plan);
        let request = plan.requests(&held).remove(0);
        let data = extract_repair_data(&slices[*request.slice], &plan, &request).expect("extract");

        let short = data[..data.len() - 1].to_vec();
        let mut long = data.clone();
        long.push(0);

        assert!(matches!(held.insert(&request, short), Err(RepairError::HelperLength { .. })));
        assert!(matches!(held.insert(&request, long), Err(RepairError::HelperLength { .. })));
        assert_eq!(plan.requests(&held).len(), slicer.coder.d());
        held.insert(&request, data).expect("insert");
        let left = plan.requests(&held);
        assert_eq!(left.len(), slicer.coder.d() - 1);
        assert!(left.iter().all(|r| r.slice != request.slice));
    }

    // a helper that fails in the first wave is planned around in a second,
    // which asks only for sub-chunks the attempt lacks
    #[test]
    fn second_wave() {
        let (slicer, slices) = track();
        let failed = 3;
        let blob_len = TRACK_STRIPES * slicer.coder.stripe_alignment();
        let every: Vec<SliceIndex> = (1..N).map(si).collect();
        let mut plan = slicer.repair_plan(si(0), &every, blob_len, slicer.stripe_size()).expect("plan");
        let mut held = HelperData::new(&plan);
        answer(&plan, &mut held, &slices, &[failed]);

        let short = plan.short_stripes(&held);
        assert!(!short.is_empty());
        let answered: Vec<SliceIndex> = (1..N)
            .filter(|&i| i != failed)
            .map(si)
            .collect();
        slicer.replan(&mut plan, &answered, &short).expect("replan");

        let wave = plan.requests(&held);
        let mut is_split = false;
        for request in &wave {
            assert_ne!(*request.slice, failed);
            for stripe in &request.stripes {
                for &sub_chunk in &stripe.sub_chunks {
                    assert!(!held.has(request.slice, stripe.stripe, sub_chunk));
                }
                let helpers = plan.stripes[stripe.stripe as usize].helpers.as_ref().expect("plan");
                let helper = helpers.iter().find(|h| h.slice == request.slice).expect("helper");
                is_split |= helper.sub_chunks.iter().any(|&sc| held.has(request.slice, stripe.stripe, sc));
            }
        }
        assert!(is_split, "no stripe drew one helper's sub-chunks from both waves");

        answer(&plan, &mut held, &slices, &[]);
        let repaired = slicer.repair(&plan, &held, &metadata_of(&slices[0])).expect("repair");
        assert_eq!(repaired, slices[0]);
    }

    // a position past the group's width is refused
    #[test]
    fn past_width() {
        let slicer = unit_slicer(ClayCoder::new(8, 4, 6));

        let result = slicer.repair_plan(si(0), &[si(1), si(8)], 1, 1);

        assert_eq!(result.err(), Some(RepairError::InvalidSlice));
    }
}
