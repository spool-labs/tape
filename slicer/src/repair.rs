//! Repair plan types for bandwidth-optimal single-slice repair.
//!
//! A `RepairPlan` describes exactly which sub-chunks to fetch from which
//! helper nodes, enabling repair at ~1/d bandwidth cost vs full decode.

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
