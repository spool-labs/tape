mod data;
mod program;
mod staking;
mod exchange;

pub use program::*;
pub use staking::*;
pub use data::*;
pub use exchange::*;

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

    // Exchange
    Exchange,

    // Staking
    StorageNode,
    StakedTape,

    // Data
    StorageResource,
    Blob,
}
