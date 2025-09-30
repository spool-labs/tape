mod exchange;
mod reward;
mod pool;
mod staking;
mod storage;
mod utils;
mod value;

pub use exchange::*;
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
