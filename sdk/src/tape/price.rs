use tape_rpc::Rpc;
use solana_program::program_pack::Pack;
use tape_api::state::Tape;
use tape_core::tape::{remaining_tape_epochs, tape_reservation_cost};
use tape_core::types::coin::{Coin, SOL, TAPE};
use tape_core::types::{EpochNumber, StorageUnits};
use tape_protocol::Api;

use crate::balance::{get_rent, rent_exempt_minimum};
use crate::error::TapedriveError;
use crate::tapedrive::Tapedrive;

/// Solana's base fee for each transaction signature, in lamports.
pub const LAMPORTS_PER_SIGNATURE: u64 = 5_000;

/// `reserve` is signed by the payer and the new tape key.
const RESERVE_SIGNATURES: u64 = 2;

impl<Blockchain: Rpc, Cluster: Api> Tapedrive<Blockchain, Cluster> {
    /// Estimate the token cost of reserving a tape. The estimate covers the
    /// epochs `reserve` would get: the expiry rounded up to the next row of
    /// the archive schedule, stopping at the network's longest reservation.
    pub async fn estimate_cost(
        &self,
        capacity: StorageUnits,
        epochs: u64,
    ) -> Result<Coin<TAPE>, TapedriveError> {
        let archive = self.rpc().get_archive().await?;
        let activation = archive.schedule.current_epoch();
        let requested = activation.saturating_add(EpochNumber(epochs));
        let expiry = archive
            .expiry_for(requested)
            .map_err(|error| TapedriveError::InvalidArgument(format!("archive schedule: {error:?}")))?;
        let reserved_epochs = expiry.saturating_sub(activation).as_u64();
        reservation_cost(archive.storage_price, capacity, reserved_epochs)
    }

    /// Estimate the token cost of adding epochs to a tape's expiry. The
    /// estimate covers the epochs `extend_expiry` would add, with the new
    /// expiry rounded as `reserve` rounds it.
    pub async fn estimate_extend_expiry_cost(
        &self,
        tape: &Tape,
        extra_epochs: u64,
    ) -> Result<Coin<TAPE>, TapedriveError> {
        let archive = self.rpc().get_archive().await?;
        let requested = tape.expiry_epoch.saturating_add(EpochNumber(extra_epochs));
        let new_expiry = archive
            .expiry_for(requested)
            .map_err(|error| TapedriveError::InvalidArgument(format!("archive schedule: {error:?}")))?;
        let added_epochs = new_expiry.saturating_sub(tape.expiry_epoch).as_u64();
        reservation_cost(archive.storage_price, tape.capacity, added_epochs)
    }

    /// Estimate the token cost of adding capacity for the rest of a tape's
    /// reservation. Epochs already past were paid for at the old capacity.
    pub async fn estimate_extend_capacity_cost(
        &self,
        tape: &Tape,
        extra: StorageUnits,
    ) -> Result<Coin<TAPE>, TapedriveError> {
        let system = self.rpc().get_system().await?;
        let archive = self.rpc().get_archive().await?;
        let epochs = remaining_tape_epochs(system.current_epoch, tape.active_epoch, tape.expiry_epoch)
            .ok_or_else(|| TapedriveError::InvalidArgument("tape has expired".to_string()))?;
        reservation_cost(archive.storage_price, extra, epochs)
    }

    /// Estimate the lamports `reserve` spends: rent for the tape account and
    /// the tape key's token account, plus signature fees.
    pub async fn estimate_reserve_lamports(&self) -> Result<Coin<SOL>, TapedriveError> {
        let rent = get_rent(self.rpc()).await?;
        let tape_rent = rent_exempt_minimum(&rent, Tape::get_size())?;
        let token_rent = rent_exempt_minimum(&rent, spl_token::state::Account::LEN)?;
        Ok(SOL(tape_rent.lamports() + token_rent.lamports() + RESERVE_SIGNATURES * LAMPORTS_PER_SIGNATURE))
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
