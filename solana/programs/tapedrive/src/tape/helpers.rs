use tape_api::event::{TapeDestroyed, TapeReserved};
use tape_api::program::prelude::*;
use tape_core::tape::tape_reservation_cost;

#[derive(Clone, Copy)]
pub struct TapeSpec {
    pub address: Address,
    pub authority: Address,
    pub capacity: StorageUnits,
    pub active_epoch: EpochNumber,
    pub expiry_epoch: EpochNumber,
}

#[derive(Clone, Copy)]
pub struct TapeReservation {
    pub spec: TapeSpec,
    pub id: TapeNumber,
    pub cost: Coin<TAPE>,
}

pub fn reserve_archive(
    system: &System,
    archive: &mut Archive,
    spec: TapeSpec,
) -> Result<TapeReservation, ProgramError> {
    if spec.active_epoch < current_epoch(system) {
        return Err(ProgramError::InvalidArgument);
    }
    if spec.expiry_epoch <= spec.active_epoch {
        return Err(ProgramError::InvalidArgument);
    }

    let epoch_count = spec
        .expiry_epoch
        .checked_sub(spec.active_epoch)
        .ok_or(ProgramError::InvalidArgument)?;

    let fee_per_epoch = tape_reservation_cost(archive.storage_price, spec.capacity, 1)
        .ok_or(ProgramError::InvalidArgument)?;
    let cost = tape_reservation_cost(archive.storage_price, spec.capacity, epoch_count.as_u64())
        .ok_or(ProgramError::InvalidArgument)?;

    let current_epoch = current_epoch(system);
    if archive.schedule.current_epoch() != current_epoch {
        return Err(TapeError::UnexpectedState.into());
    }

    if !archive.schedule.has_capacity_for(
        spec.capacity,
        archive.storage_capacity,
        spec.active_epoch,
        spec.expiry_epoch,
    ) {
        return Err(TapeError::NoCapacity.into());
    }

    archive
        .schedule
        .reserve_capacity(spec.capacity, fee_per_epoch, spec.active_epoch, spec.expiry_epoch)
        .map_err(|_| TapeError::UnexpectedState)?;

    let id = TapeNumber(
        archive
            .tape_count
            .checked_add(1)
            .ok_or(ProgramError::ArithmeticOverflow)?,
    );

    archive.tape_count = id.as_u64();

    Ok(TapeReservation { spec, id, cost })
}

pub fn create_tape_account<'account_info>(
    tape_info: &AccountInfo<'account_info>,
    system_program_info: &AccountInfo<'account_info>,
    fee_payer_info: &AccountInfo<'account_info>,
    seeds: &[&[u8]],
    reservation: TapeReservation,
) -> ProgramResult {
    create_program_account::<Tape>(
        tape_info,
        system_program_info,
        fee_payer_info,
        &tapedrive::ID,
        seeds,
    )?;

    let tape = tape_info.as_account_mut::<Tape>(&tapedrive::ID)?;
    tape.id = reservation.id;
    tape.authority = reservation.spec.authority;
    tape.active_epoch = reservation.spec.active_epoch;
    tape.expiry_epoch = reservation.spec.expiry_epoch;
    tape.capacity = reservation.spec.capacity;
    tape.used = StorageUnits::zero();

    TapeReserved {
        tape: reservation.spec.address,
        authority: reservation.spec.authority,
        capacity: reservation.spec.capacity,
        active_epoch: reservation.spec.active_epoch,
        expiry_epoch: reservation.spec.expiry_epoch,
        cost: reservation.cost.as_u64().to_le_bytes(),
    }
    .log();

    Ok(())
}

pub fn destroy_expired<'account_info>(
    tape_info: &AccountInfo<'account_info>,
    fee_payer_info: &AccountInfo<'account_info>,
    system: &System,
    tape_address: Address,
    authority: Address,
    expiry_epoch: EpochNumber,
) -> ProgramResult {
    if current_epoch(system) < expiry_epoch {
        return Err(TapeError::NotExpired.into());
    }

    TapeDestroyed {
        tape: tape_address,
        authority,
    }
    .log();

    close_account(tape_info, fee_payer_info)?;

    Ok(())
}
