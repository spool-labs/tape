/// Compute unit budgets for Tapedrive instructions.
///
/// Instructions that exceed the Solana default (200K CU) must include a
/// `ComputeBudgetInstruction::set_compute_unit_limit` in the transaction.
/// Callers should prepend the budget instruction before the program instruction.
///
/// Instructions not listed here fit within the default 200K budget.

pub const ADVANCE_POOL_CU: u32 = 400_000;
pub const JOIN_NETWORK_CU: u32 = 400_000;
pub const SYNC_EPOCH_CU: u32 = 400_000;
pub const REQUEST_STAKE_UNLOCK_CU: u32 = 400_000;
pub const UNSTAKE_FROM_POOL_CU: u32 = 400_000;
pub const SET_COMMISSION_RATE_CU: u32 = 400_000;
pub const CLAIM_COMMISSION_CU: u32 = 400_000;
pub const STAKE_WITH_POOL_CU: u32 = 1_400_000;
pub const ADVANCE_EPOCH_CU: u32 = 1_400_000;
pub const CERTIFY_TRACK_CU: u32 = 1_400_000;
pub const INVALIDATE_TRACK_CU: u32 = 1_400_000;
pub const TRACK_WRITE_CU: u32 = 1_400_000;
pub const RESERVE_SNAPSHOT_CU: u32 = 400_000;
pub const WRITE_SNAPSHOT_CU: u32 = 400_000;
pub const SIGN_SNAPSHOT_CU: u32 = 400_000;
