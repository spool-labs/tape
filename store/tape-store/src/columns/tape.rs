//! Tape column family for tape metadata

use crate::types::{Pubkey, TapeInfo};
use store::Column;

/// Tape info indexed by tape address
///
/// Key: Pubkey (tape_address, 32 bytes)
/// Value: TapeInfo (end_epoch)
pub struct TapeCol;

impl Column for TapeCol {
    const CF_NAME: &'static str = "tape";
    type Key = Pubkey;
    type Value = TapeInfo;
}
