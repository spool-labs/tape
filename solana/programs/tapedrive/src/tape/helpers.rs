use tape_api::event::{TapeDestroyed, TapeReserved};
use tape_api::program::prelude::*;
use tape_core::tape::{tape_reservation_cost, user_tape_number};

#[derive(Clone, Copy)]
pub struct TapeSpec {
    pub address: Address,
    pub authority: Address,
    pub flags: u64,
    pub capacity: StorageUnits,
    pub active_epoch: EpochNumber,
    pub expiry_epoch: EpochNumber,
}

#[derive(Clone, Copy)]
pub struct TapeReservation {
    pub spec: TapeSpec,
    pub id: TapeNumber,
    pub cost: Coin<TAPE>,
    pub burned: Coin<TAPE>,
    pub scheduled: Coin<TAPE>,
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

    let cost = tape_reservation_cost(archive.storage_price, spec.capacity, epoch_count.as_u64())
        .ok_or(ProgramError::InvalidArgument)?;
    let policy_burn = bps_amount(cost, archive.burn_fee_bps)?;
    let rewards = cost
        .checked_sub(policy_burn)
        .ok_or(ProgramError::ArithmeticOverflow)?;
    let reward_per_epoch = rewards
        .checked_div(TAPE(epoch_count.as_u64()))
        .ok_or(ProgramError::ArithmeticOverflow)?;
    let scheduled = reward_per_epoch
        .checked_mul(TAPE(epoch_count.as_u64()))
        .ok_or(ProgramError::ArithmeticOverflow)?;
    let dust = rewards
        .checked_sub(scheduled)
        .ok_or(ProgramError::ArithmeticOverflow)?;
    let burned = policy_burn
        .checked_add(dust)
        .ok_or(ProgramError::ArithmeticOverflow)?;

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
        .reserve_capacity(spec.capacity, reward_per_epoch, spec.active_epoch, spec.expiry_epoch)
        .map_err(|_| TapeError::UnexpectedState)?;

    let next_count = archive
        .tape_count
        .checked_add(1)
        .ok_or(ProgramError::ArithmeticOverflow)?;
    let tape_id = user_tape_number(next_count)
        .ok_or(ProgramError::ArithmeticOverflow)?;

    archive.tape_count = next_count;

    Ok(TapeReservation { spec, id: tape_id, cost, burned, scheduled })
}

fn bps_amount(amount: Coin<TAPE>, bps: BasisPoints) -> Result<Coin<TAPE>, ProgramError> {
    if !bps.is_valid() {
        return Err(ProgramError::InvalidArgument);
    }

    let raw = amount
        .as_u128()
        .checked_mul(bps.as_u128())
        .ok_or(ProgramError::ArithmeticOverflow)?
        / BasisPoints::MAX as u128;

    if raw > u64::MAX as u128 {
        return Err(ProgramError::ArithmeticOverflow);
    }

    Ok(TAPE(raw as u64))
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
    tape.flags = reservation.spec.flags;
    tape.authority = reservation.spec.authority;
    tape.active_epoch = reservation.spec.active_epoch;
    tape.expiry_epoch = reservation.spec.expiry_epoch;
    tape.capacity = reservation.spec.capacity;
    tape.used = StorageUnits::zero();

    TapeReserved {
        tape: reservation.spec.address,
        id: reservation.id,
        flags: reservation.spec.flags,
        authority: reservation.spec.authority,
        capacity: reservation.spec.capacity,
        active_epoch: reservation.spec.active_epoch,
        expiry_epoch: reservation.spec.expiry_epoch,
        cost: reservation.cost,
        burned: reservation.burned,
        scheduled: reservation.scheduled,
    }
    .log();

    Ok(())
}

pub fn verified_tape_address(
    tape_info: &AccountInfo<'_>,
    tape: &Tape,
) -> Result<Address, ProgramError> {
    let (tape_address, _) = tape_pda(tape.authority);
    if tape_address != (*tape_info.key).into() {
        return Err(ProgramError::InvalidAccountData);
    }

    Ok(tape_address)
}

pub fn authorize_tape_authority(tape: &Tape, signer: Address) -> ProgramResult {
    if tape.authority != signer {
        return Err(ProgramError::InvalidAccountData);
    }

    Ok(())
}

pub fn authorize_tape_operator(tape: &Tape, signer: Address) -> ProgramResult {
    if !tape.is_operator(signer) {
        return Err(ProgramError::InvalidAccountData);
    }

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
