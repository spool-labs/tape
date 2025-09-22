mod program;
mod staking;
mod data;

pub use program::*;
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
    StorageNode,
    StakedTape,

    // Data
    StorageResource,
    Blob,
}
