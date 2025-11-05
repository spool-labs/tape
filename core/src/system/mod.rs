mod accounting;
mod blacklist;
mod committee;
mod epoch;
mod exchange;
mod node;
mod rewards;
mod utils;

pub use accounting::*;
pub use blacklist::*;
pub use committee::*;
pub use epoch::*;
pub use exchange::*;
pub use node::*;
pub use rewards::*;
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
