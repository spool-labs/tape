//! Tape column family for tape metadata

use store::Column;
use tape_crypto::address::Address;

use crate::types::TapeInfo;

/// Tape info indexed by tape address
///
/// Key: Address (tape_address, 32 bytes)
/// Value: TapeInfo (end_epoch)
pub struct TapeCol;

impl Column for TapeCol {
    const CF_NAME: &'static str = "tape";
    type Key = Address;
    type Value = TapeInfo;
}
