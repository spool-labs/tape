mod archive;
mod epoch;
mod block;
mod tape;
mod treasury;
mod writer;
mod miner;

pub use archive::*;
pub use epoch::*;
pub use block::*;
pub use tape::*;
pub use treasury::*;
pub use writer::*;
pub use miner::*;

use steel::*;

#[repr(u8)]
#[derive(Clone, Copy, Debug, Eq, PartialEq, IntoPrimitive, TryFromPrimitive)]
pub enum AccountType {
    Unknown = 0,
    Archive,
    Writer,
    Tape,
    Miner,
    Epoch,
    Block,
    Treasury,
}
