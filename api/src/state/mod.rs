mod archive;
mod epoch;
mod exchange;
mod feature;
mod history;
mod node;
mod stake;
mod system;
mod tape;
mod track;
mod treasury;

pub use archive::*;
pub use epoch::*;
pub use exchange::*;
pub use history::*;
pub use node::*;
pub use stake::*;
pub use system::*;
pub use tape::*;
pub use track::*;
pub use treasury::*;

use steel::*;

#[repr(u8)]
#[derive(Clone, Copy, Debug, Eq, PartialEq, IntoPrimitive, TryFromPrimitive)]
pub enum AccountType {
    Unknown = 0,

    Treasury,

    System,
    Epoch,
    Archive,
    Feature,

    Exchange,
    Node,
    History,
    Stake,

    Tape,
    Track,
}
