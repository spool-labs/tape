//! What the funder and publisher read from and do on chain

use std::future::Future;

use tape_core::types::coin::{SOL, TAPE};
use tape_core::types::{EpochNumber, StorageUnits, TrackNumber};
use tape_crypto::address::Address;
use tape_sdk::keys::tape_key::TapeKey;

use crate::chain::error::ChainError;
use crate::records::rows::TapeObservation;
use crate::types::ids::TrackCount;

/// A tape's on-chain state
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TapeStatus {
    pub capacity: StorageUnits,
    pub used: StorageUnits,
    pub expiry_epoch: EpochNumber,
    /// Every track ever written on the tape is below this number
    pub next_track: TrackNumber,
    /// Tracks written and not deleted
    pub present_tracks: TrackCount,
}

/// The payer's balances
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PayerBalances {
    pub sol: SOL,
    pub tape: TAPE,
}

/// Chain reads and paid operations on the rack and blacklist tapes
pub trait TapeChain {
    /// The tape's state, `None` when no such tape exists
    fn tape_status(
        &self,
        tape: Address,
    ) -> impl Future<Output = Result<Option<TapeStatus>, ChainError>> + Send;

    fn current_epoch(&self) -> impl Future<Output = Result<EpochNumber, ChainError>> + Send;

    fn payer_balances(&self) -> impl Future<Output = Result<PayerBalances, ChainError>> + Send;

    fn extend_expiry(
        &self,
        tape: Address,
        epochs: EpochNumber,
    ) -> impl Future<Output = Result<(), ChainError>> + Send;

    fn extend_capacity(
        &self,
        tape: Address,
        extra: StorageUnits,
    ) -> impl Future<Output = Result<(), ChainError>> + Send;

    fn reserve(
        &self,
        key: &TapeKey,
        capacity: StorageUnits,
        epochs: EpochNumber,
    ) -> impl Future<Output = Result<(), ChainError>> + Send;
}

impl TapeStatus {
    pub fn observation(&self) -> TapeObservation {
        TapeObservation {
            next_track: self.next_track,
            expiry_epoch: self.expiry_epoch,
            capacity: self.capacity,
            used: self.used,
        }
    }
}
