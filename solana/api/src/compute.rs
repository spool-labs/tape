//! CU limits for tapedrive ix, used by ix builders to pre-set
//! `ComputeBudgetInstruction::set_compute_unit_limit`. Solana's per-ix
//! default is 200,000; the BLS-aggregate and iteration-heavy handlers
//! must bump above that.
//!
//! Values include ~50%+ headroom over the worst case observed in SBF
//! tests to absorb production-scale state.

// BLS aggregate verify is the load. ~250k CU observed in SBF tests.
pub const CERTIFY_TRACK_CU:    u32 = 400_000;
pub const INVALIDATE_TRACK_CU: u32 = 400_000;
pub const FINALIZE_SNAPSHOT_CU: u32 = 400_000;
pub const FINALIZE_GROUP_CU:    u32 = 400_000;
pub const VOTE_SNAPSHOT_CU:    u32 = 400_000;
pub const VOTE_ASSIGNMENT_CU:  u32 = 400_000;

// Iteration-heavy: AdvanceEpoch scans Committee(N+1) x PeerSet for
// preference aggregation.
pub const ADVANCE_EPOCH_CU:    u32 = 400_000;

// CPI-bearing: stake / unstake call into the staking program and SPL Token.
pub const STAKE_WITH_POOL_CU:  u32 = 150_000;
pub const UNSTAKE_FROM_POOL_CU: u32 = 150_000;

// Mid-weight on-chain logic (merkle proofs, multi-account writes).
pub const TRACK_WRITE_CU:      u32 = 100_000;
pub const COMMIT_EPOCH_CU:     u32 =  50_000;
pub const ADVANCE_POOL_CU:     u32 =  50_000;
pub const SETTLE_SPOOL_CU:     u32 =  50_000;
pub const SYNC_SPOOL_CU:       u32 =  30_000;
pub const JOIN_COMMITTEE_CU:   u32 =  50_000;
pub const CREATE_EPOCH_CU:     u32 =  30_000;
pub const CREATE_COMMITTEE_CU: u32 =  30_000;
pub const RESIZE_COMMITTEE_CU: u32 =  50_000;
pub const RESIZE_PEER_SET_CU:  u32 =  30_000;
pub const PROPOSE_SNAPSHOT_CU: u32 =  30_000;
pub const PROPOSE_ASSIGNMENT_CU: u32 =  30_000;
pub const RESIZE_ARCHIVE_CU:   u32 =  30_000;

// Lightweight: small mutations + scheduled-state writes.
pub const REQUEST_STAKE_UNLOCK_CU: u32 = 25_000;
pub const CLAIM_COMMISSION_CU:     u32 = 15_000;
pub const SET_COMMISSION_RATE_CU:  u32 = 10_000;
