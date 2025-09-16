mod system;
mod treasury;
mod archive;
// mod epoch;
// mod block;
// mod tape;
// mod writer;
// mod miner;
// mod spool;
mod member;

pub use system::*;
pub use treasury::*;
pub use archive::*;
// pub use epoch::*;
// pub use block::*;
// pub use tape::*;
// pub use writer::*;
// pub use miner::*;
// pub use spool::*;
pub use member::*;

use steel::*;

#[repr(u8)]
#[derive(Clone, Copy, Debug, Eq, PartialEq, IntoPrimitive, TryFromPrimitive)]
pub enum AccountType {
    Unknown = 0,
    System,
    Treasury,

    Archive,
    Spool,
    Tape,
    Blob,

    Epoch,
    Block,

    Committee,
    Member,
    Pool,
}
