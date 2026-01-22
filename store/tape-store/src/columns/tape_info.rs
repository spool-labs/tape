//! TapeInfo column family for tape metadata
//!
//! Stores information about storage allocations (tapes).

use crate::types::{Pubkey, TapeInfo};
use store::Column;

/// Tape info indexed by tape address
///
/// Key: Pubkey (tape_address, 32 bytes)
/// Value: TapeInfo (active/expiry epoch, authority)
pub struct TapeInfoCol;

impl Column for TapeInfoCol {
    const CF_NAME: &'static str = "tape_info";
    type Key = Pubkey;
    type Value = TapeInfo;
}
