mod committee;
mod epoch;
mod exchange;
mod pool;
mod reward;
mod staking;
mod storage;
mod utils;
mod value;

pub use committee::*;
pub use exchange::*;
pub use epoch::*;
pub use reward::*;
pub use pool::*;
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
