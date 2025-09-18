mod system;
mod treasury;
mod archive;
mod epoch;
mod pool;
mod blob;
mod stake;

pub use system::*;
pub use treasury::*;
pub use archive::*;
pub use epoch::*;
pub use pool::*;
pub use blob::*;
pub use stake::*;

use steel::*;

#[repr(u8)]
#[derive(Clone, Copy, Debug, Eq, PartialEq, IntoPrimitive, TryFromPrimitive)]
pub enum AccountType {
    Unknown = 0,
    System,
    Treasury,
    Archive,
    Epoch,
    Pool,
    Stake,
    Blob,
}
