mod accounting;
mod blob;
mod committee;
mod epoch;
mod exchange;
mod node;
mod utils;

pub use accounting::*;
pub use blob::*;
pub use committee::*;
pub use epoch::*;
pub use exchange::*;
pub use node::*;
pub use utils::*;

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
