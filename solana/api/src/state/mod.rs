mod archive;
mod epoch;
mod exchange;
mod history;
mod node;
mod snapshot;
mod stake;
mod system;
mod tape;
mod treasury;

pub use archive::*;
pub use epoch::*;
pub use exchange::*;
pub use history::*;
pub use node::*;
pub use snapshot::*;
pub use stake::*;
pub use system::*;
pub use tape::*;
pub use treasury::*;

use tape_solana::*;

#[repr(u8)]
#[derive(Clone, Copy, Debug, Eq, PartialEq, IntoPrimitive, TryFromPrimitive)]
pub enum AccountType {
    Unknown = 0,

    Treasury,

    System,
    Epoch,
    Archive,

    Exchange,
    Node,
    History,
    Stake,

    Tape,

    Snapshot,
}
