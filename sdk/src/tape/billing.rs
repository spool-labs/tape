use rpc::Rpc;
use tape_core::tape::{remaining_tape_epochs, tape_reservation_cost};
use tape_core::types::coin::{Coin, TAPE};
use tape_core::types::{EpochNumber, StorageUnits};
use tape_protocol::Api;

use crate::error::TapedriveError;
use crate::tapedrive::Tapedrive;

impl<Blockchain: Rpc, Cluster: Api> Tapedrive<Blockchain, Cluster> {
    /// Estimate the token cost of reserving a tape.
    pub async fn estimate_cost(
        &self,
        capacity: StorageUnits,
        epochs: u64,
    ) -> Result<Coin<TAPE>, TapedriveError> {
        let archive = self.rpc().get_archive().await?;
        reservation_cost(archive.storage_price, capacity, epochs)
    }
}

/// Calculate the token cost of reserving a tape with the given parameters.
pub fn reservation_cost(
    price_per_unit: Coin<TAPE>,
    capacity: StorageUnits,
    epochs: u64,
) -> Result<Coin<TAPE>, TapedriveError> {
    tape_reservation_cost(price_per_unit, capacity, epochs)
        .ok_or_else(|| TapedriveError::InvalidArgument("tape reservation cost overflow".to_string()))
}

/// Calculate the remaining epochs until a tape expires, given the current epoch, tape activation
/// epoch, and tape expiry epoch.
pub fn remaining_epochs(
    current: EpochNumber,
    active: EpochNumber,
    expiry: EpochNumber,
) -> Result<u64, TapedriveError> {
    remaining_tape_epochs(current, active, expiry)
        .ok_or_else(|| TapedriveError::InvalidArgument("tape is expired".to_string()))
}

