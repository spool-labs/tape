mod system;
mod exchange;
mod operator;
mod staking;
mod storage;
mod blob;

pub use system::*;
pub use exchange::*;
pub use operator::*;
pub use staking::*;
pub use storage::*;
pub use blob::*;

use steel::*;

#[repr(u8)]
#[derive(Clone, Copy, Debug, Eq, PartialEq, IntoPrimitive, TryFromPrimitive)]
pub enum AccountType {
    Unknown = 0,

    System,
    Epoch,
    Archive,
    Treasury,
    Feature,

    Exchange,
    StorageNode,
    StakedTape,
    TapeResource,
    Blob,
}
