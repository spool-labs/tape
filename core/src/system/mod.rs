mod blob;
mod committee;
mod epoch;
mod exchange;
mod operator;
mod pool;
mod reward;
mod staking;
mod storage;
mod utils;
mod value;

pub use blob::*;
pub use committee::*;
pub use epoch::*;
pub use exchange::*;
pub use operator::*;
pub use pool::*;
pub use reward::*;
pub use staking::*;
pub use storage::*;
pub use utils::*;
pub use value::*;

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
