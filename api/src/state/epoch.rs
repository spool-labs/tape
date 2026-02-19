use tape_solana::*;
use tape_core::prelude::*;
use super::AccountType;

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Pod, Zeroable)]
pub struct Epoch {
    /// The current epoch number.
    pub id: EpochNumber,

    /// The state of the current epoch.
    pub state: EpochState,

    /// The timestamp (in seconds) of the last epoch.
    pub last_epoch: i64,

    /// Randomness seed captured from SlotHashes at epoch advance.
    pub nonce: Hash,
}

tape_solana::state!(AccountType, Epoch);
