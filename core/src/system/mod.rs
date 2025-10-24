mod archive;
mod blob;
mod committee;
mod epoch;
mod exchange;
mod node;
//mod pool;
mod staking;
mod utils;
//mod value;

pub use archive::*;
pub use blob::*;
pub use committee::*;
pub use epoch::*;
pub use exchange::*;
pub use node::*;
//pub use pool::*;
pub use staking::*;
pub use utils::*;
//pub use value::*;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SystemError {
    EpochInPast,
    EpochTooFar,
    IndexOutOfBounds,
    StartNotAfterBase,
    EndNotAfterStart,
    RangeTooLarge,
    ExceedsFutureEpochs,
    Overflow,
    Underflow,
}
