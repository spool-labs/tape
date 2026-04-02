/// Compute unit budgets for Tapedrive instructions.
///
/// Instructions that exceed the Solana default (200K CU) must include a
/// `ComputeBudgetInstruction::set_compute_unit_limit` in the transaction.
/// Callers should prepend the budget instruction before the program instruction.
///
/// Instructions not listed here fit within the default 200K budget.

/// AdvancePool — iterates `committee_prev × SPOOL_COUNT` to compute reward
/// weights via `get_committee_score`, plus pool schedule processing.
pub const ADVANCE_POOL_CU: u32 = 400_000;

/// JoinNetwork — reads committee state and updates spool assignments.
pub const JOIN_NETWORK_CU: u32 = 400_000;

/// SyncEpoch — iterates spool list and updates per-member sync weight.
pub const SYNC_EPOCH_CU: u32 = 400_000;

/// RequestStakeUnlock — reads pool state and schedules share reduction.
pub const REQUEST_STAKE_UNLOCK_CU: u32 = 400_000;

/// UnstakeFromPool — processes pending withdrawal and transfers tokens.
pub const UNSTAKE_FROM_POOL_CU: u32 = 400_000;

/// SetCommissionRate — reads pool state and schedules commission change.
pub const SET_COMMISSION_RATE_CU: u32 = 400_000;

/// ClaimCommission — reads pool state and transfers accumulated commission.
pub const CLAIM_COMMISSION_CU: u32 = 400_000;

/// InitSnapshotEpoch — creates the per-epoch manifest and snapshot tape.
pub const INIT_SNAPSHOT_EPOCH_CU: u32 = 700_000;

/// StakeWithPool — creates/resizes ATA, transfers tokens, updates pool state.
pub const STAKE_WITH_POOL_CU: u32 = 1_400_000;

/// AdvanceEpoch — rotates committees, reassigns spools, resets epoch state.
pub const ADVANCE_EPOCH_CU: u32 = 1_400_000;

/// CertifyTrack — BLS aggregate signature verification over committee bitmap.
pub const CERTIFY_TRACK_CU: u32 = 1_400_000;

/// InvalidateTrack — BLS aggregate signature verification for fraud proof.
pub const INVALIDATE_TRACK_CU: u32 = 1_400_000;

/// CertifySnapshotGroup — BLS aggregate signature verification for snapshot cert.
pub const CERTIFY_SNAPSHOT_GROUP_CU: u32 = 1_400_000;

/// FinalizeSnapshotEpoch — verifies manifest completion and advances the tail.
pub const FINALIZE_SNAPSHOT_EPOCH_CU: u32 = 400_000;
