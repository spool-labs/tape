mod archive;
mod blob;
mod committee;
mod epoch;
mod exchange;
mod feature;
mod node;
mod stake;
mod system;
mod tape;

pub use archive::*;
pub use blob::*;
pub use committee::*;
pub use epoch::*;
pub use exchange::*;
pub use feature::*;
pub use node::*;
pub use stake::*;
pub use system::*;
pub use tape::*;

use steel::*;

#[repr(u8)]
#[derive(Clone, Copy, Debug, Eq, PartialEq, IntoPrimitive, TryFromPrimitive)]
pub enum AccountType {
    Unknown = 0,

    System,
    Epoch,
    Committee,
    Archive,
    Feature,

    Exchange,
    Node,
    Stake,
    Tape,
    Blob,
}
