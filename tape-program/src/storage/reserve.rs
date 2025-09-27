use tape_api::prelude::*;
use steel::*;

pub fn process_reserve_tape(accounts: &[AccountInfo<'_>], data: &[u8]) -> ProgramResult {
    let args = ReserveTape::try_from_bytes(data)?;
    let [
        signer_info,
        signer_ata_info,
        resource_info,

        epoch_info,
        archive_info,
        treasury_info,
        treasury_ata_info,

        token_program_info,
        system_program_info, 
        rent_info,
    ] = accounts else {
        return Err(ProgramError::NotEnoughAccountKeys);
    };

    signer_info
        .is_signer()?;

    signer_ata_info
        .is_writable()?
        .as_token_account()?
        .assert(|t| t.owner() == *signer_info.key)?
        .assert(|t| t.mint() == MINT_ADDRESS)?;

    treasury_ata_info
        .is_writable()?
        .has_address(&TREASURY_ATA)?
        .as_token_account()?
        .assert(|t| t.owner() == TREASURY_ADDRESS)?
        .assert(|t| t.mint() == MINT_ADDRESS)?;

    let (resource_address, _)  = resource_pda(*signer_info.key);

    resource_info
        .is_empty()?
        .is_writable()?
        .has_address(&resource_address)?;

    let epoch = epoch_info
        .is_tape_epoch()?
        .as_account::<Epoch>(&tape_api::ID)?;

    let archive = archive_info
        .is_writable()?
        .is_tape_archive()?
        .as_account_mut::<Archive>(&tape_api::ID)?;

    let treasury = treasury_info
        .is_writable()?
        .is_tape_treasury()?
        .as_account_mut::<Treasury>(&tape_api::ID)?;

    token_program_info
        .is_program(&spl_token::ID)?;
    system_program_info
        .is_program(&system_program::ID)?;
    rent_info
        .is_sysvar(&sysvar::rent::ID)?;

    let start_epoch = EpochNumber::unpack(args.start_epoch);
    let end_epoch = EpochNumber::unpack(args.end_epoch);

    if start_epoch <= current_epoch(epoch) {
        return Err(ProgramError::InvalidArgument);
    }
    if end_epoch <= start_epoch {
        return Err(ProgramError::InvalidArgument);
    }

    let num_epochs = end_epoch
        .checked_sub(start_epoch)
        .ok_or(ProgramError::InvalidArgument)?;

    let total_units = StorageUnits::unpack(args.storage_units);

    let price_per_unit = archive.storage_price_per_unit
        .as_u64();

    let single_epoch_price = price_per_unit
        .checked_mul(total_units.as_u64())
        .ok_or(ProgramError::InvalidArgument)?;

    let total_cost = single_epoch_price
        .checked_mul(num_epochs.as_u64())
        .ok_or(ProgramError::InvalidArgument)?;

    let current_epoch = current_epoch(epoch);
    let current_capacity = archive.storage_capacity;
    let future_usage = &mut archive.storage_used;
    let future_rewards = &mut treasury.fees_collected;
    let fee_per_epoch = TAPE(single_epoch_price);

    if !has_capacity_for(
        total_units, start_epoch, end_epoch, current_capacity, current_epoch, future_usage) {
        return Err(TapeError::InsufficientCapacity.into());
    }

    reserve_capacity(
        total_units,
        start_epoch,
        end_epoch,
        current_epoch,
        fee_per_epoch,
        future_usage,
        future_rewards
    ).map_err(|_| TapeError::UnexpectedState)?;

    create_program_account::<TapeResource>(
        resource_info,
        system_program_info,
        signer_info,
        &tape_api::ID,
        &[RESOURCE, signer_info.key.as_ref()],
    )?;

    let tape = resource_info.as_account_mut::<TapeResource>(&tape_api::ID)?;
    tape.authority = *signer_info.key;
    tape.active_epoch = start_epoch;
    tape.expiry_epoch = end_epoch;
    tape.capacity = total_units;
    tape.used = StorageUnits::zero();

    transfer(
        signer_info,
        signer_ata_info,
        treasury_ata_info,
        token_program_info,
        total_cost,
    )?;

    Ok(())
}



