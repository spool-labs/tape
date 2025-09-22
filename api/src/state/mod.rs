mod core;
mod staking;
mod data;

pub use core::*;
pub use staking::*;
pub use data::*;

use steel::*;

#[repr(u8)]
#[derive(Clone, Copy, Debug, Eq, PartialEq, IntoPrimitive, TryFromPrimitive)]
pub enum AccountType {
    Unknown = 0,

    // Core
    System,
    Treasury,
    Archive,
    Epoch,

    // Staking
    StakingPool,
    StakedTape,

    // Data
    StorageResource,
    Blob,
}
