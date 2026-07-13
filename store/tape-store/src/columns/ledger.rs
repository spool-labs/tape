//! Accounting-ledger column families for the S3 write-authorization subsystem.

use store::Column;
use tape_crypto::address::Address;

use crate::types::{LedgerEntry, LedgerReservation, LedgerReservationKey};

/// Per-principal accounting ledger, keyed by owner authority.
pub struct LedgerCol;

impl Column for LedgerCol {
    const CF_NAME: &'static str = "ledger";
    type Key = Address;
    type Value = LedgerEntry;
}

/// Outstanding budget reservations.
pub struct LedgerReservationCol;

impl Column for LedgerReservationCol {
    const CF_NAME: &'static str = "ledger_reservation";
    type Key = LedgerReservationKey;
    type Value = LedgerReservation;
}
