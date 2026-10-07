use tape_api::event::TapeReserved;
use tape_api::program::prelude::*;
use tape_core::tape::{tape_reservation_cost, user_tape_number};

#[derive(Clone, Copy)]
pub struct TapeSpec {
    pub address: Address,
    pub authority: Address,
    pub flags: u64,
    pub capacity: StorageUnits,
    pub expiry_epoch: EpochNumber,
}

#[derive(Clone, Copy)]
pub struct TapeReservation {
    pub spec: TapeSpec,
    pub id: TapeNumber,
    pub active_epoch: EpochNumber,
    pub payment: SchedulePayment,
}

#[derive(Clone, Copy)]
pub struct SchedulePayment {
    pub cost: Coin<TAPE>,
    pub burned: Coin<TAPE>,
    pub scheduled: Coin<TAPE>,
}

pub fn reserve_archive(
    system: &System,
    archive: &mut Archive,
    spec: TapeSpec,
) -> Result<TapeReservation, ProgramError> {
    let (spec, payment) = schedule_reservation(system, archive, spec)?;

    let next_count = archive
        .tape_count
        .checked_add(1)
        .ok_or(ProgramError::ArithmeticOverflow)?;
    let tape_id = user_tape_number(next_count)
        .ok_or(ProgramError::ArithmeticOverflow)?;

    archive.tape_count = next_count;

    Ok(TapeReservation {
        spec,
        id: tape_id,
        active_epoch: current_epoch(system),
        payment,
    })
}

/// Schedule a reservation's capacity from the current epoch. The spec comes
/// back with the expiry the tape gets.
pub fn schedule_reservation(
    system: &System,
    archive: &mut Archive,
    spec: TapeSpec,
) -> Result<(TapeSpec, SchedulePayment), ProgramError> {
    // A request past the longest reservation the network takes is cut back
    // to it, and the tape pays for the epochs it gets.
    let latest_expiry = archive.latest_expiry().map_err(schedule_error)?;
    let spec = TapeSpec {
        expiry_epoch: spec.expiry_epoch.min(latest_expiry),
        ..spec
    };

    let payment = schedule_capacity(
        system,
        archive,
        spec.capacity,
        current_epoch(system),
        spec.expiry_epoch,
    )?;

    Ok((spec, payment))
}

pub fn schedule_capacity(
    system: &System,
    archive: &mut Archive,
    units: StorageUnits,
    start_epoch: EpochNumber,
    end_epoch: EpochNumber,
) -> Result<SchedulePayment, ProgramError> {
    // A zero unit or empty window reservation would be free; free
    // reservations pad the schedule and block rent reclamation.
    if units.is_zero() || end_epoch <= start_epoch {
        return Err(ProgramError::InvalidArgument);
    }

    let epoch_count = end_epoch
        .checked_sub(start_epoch)
        .ok_or(ProgramError::InvalidArgument)?;

    let cost = tape_reservation_cost(archive.storage_price, units, epoch_count.as_u64())
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

    if archive.schedule.current_epoch() != current_epoch(system) {
        return Err(TapeError::UnexpectedState.into());
    }

    let has_capacity = archive
        .schedule
        .has_capacity_for(units, archive.storage_capacity, start_epoch, end_epoch)
        .map_err(schedule_error)?;
    if !has_capacity {
        return Err(TapeError::NoCapacity.into());
    }

    archive
        .schedule
        .reserve_capacity(units, reward_per_epoch, start_epoch, end_epoch)
        .map_err(schedule_error)?;

    Ok(SchedulePayment { cost, burned, scheduled })
}

pub fn schedule_error(error: EpochScheduleError) -> ProgramError {
    match error {
        EpochScheduleError::MidRow => TapeError::MidRow.into(),
        EpochScheduleError::EpochTooFar => TapeError::NoCapacity.into(),
        EpochScheduleError::EpochInPast
        | EpochScheduleError::EndNotAfterStart
        | EpochScheduleError::Overflow
        | EpochScheduleError::Underflow => TapeError::UnexpectedState.into(),
    }
}

pub fn collect_payment<'account_info>(
    payer_info: &AccountInfo<'account_info>,
    payer_ata_info: &AccountInfo<'account_info>,
    archive_ata_info: &AccountInfo<'account_info>,
    mint_info: &AccountInfo<'account_info>,
    token_program_info: &AccountInfo<'account_info>,
    payment: SchedulePayment,
) -> ProgramResult {
    if !payment.burned.is_zero() {
        burn(
            payer_ata_info,
            mint_info,
            payer_info,
            token_program_info,
            payment.burned.as_u64(),
        )?;
    }

    if !payment.scheduled.is_zero() {
        transfer(
            payer_info,
            payer_ata_info,
            archive_ata_info,
            token_program_info,
            payment.scheduled.as_u64(),
        )?;
    }

    Ok(())
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
    slot_hash: Hash,
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
    tape.active_epoch = reservation.active_epoch;
    tape.expiry_epoch = reservation.spec.expiry_epoch;
    tape.capacity = reservation.spec.capacity;
    tape.used = StorageUnits::zero();
    tape.seed_nonce(slot_hash);

    TapeReserved {
        tape: reservation.spec.address,
        id: reservation.id,
        flags: reservation.spec.flags,
        authority: reservation.spec.authority,
        capacity: reservation.spec.capacity,
        active_epoch: reservation.active_epoch,
        expiry_epoch: reservation.spec.expiry_epoch,
        cost: reservation.payment.cost,
        burned: reservation.payment.burned,
        scheduled: reservation.payment.scheduled,
        nonce: tape.nonce,
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

