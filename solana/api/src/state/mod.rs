mod archive;
mod committee;
mod epoch;
mod exchange;
mod group;
mod node;
mod peer;
mod stake;
mod system;
mod tape;
mod treasury;
mod vote;

pub use archive::*;
pub use committee::*;
pub use epoch::*;
pub use exchange::*;
pub use group::*;
pub use node::*;
pub use peer::*;
pub use stake::*;
pub use system::*;
pub use tape::*;
pub use treasury::*;
pub use vote::*;

use tape_solana::*;

#[repr(u8)]
#[derive(Clone, Copy, Debug, Eq, PartialEq, IntoPrimitive, TryFromPrimitive)]
pub enum AccountType {
    Unknown = 0,
    Archive,
    Committee,
    Epoch,
    Exchange,
    Group,
    Node,
    PeerSet,
    Stake,
    System,
    Tape,
    Treasury,
    Vote,
}
