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

    let total_units = StorageUnits::unpack(args.storage_units)
        .as_u64();

    let price_per_unit = archive.storage_price_per_unit
        .as_u64();

    let single_epoch_price = price_per_unit
        .checked_mul(total_units)
        .ok_or(ProgramError::InvalidArgument)?;

    let total_cost = single_epoch_price
        .checked_mul(num_epochs.as_u64())
        .ok_or(ProgramError::InvalidArgument)?;

    // Update the storage used in the archive for each epoch covered by this reservation
    let current_epoch = current_epoch(epoch);
    for epoch_index in start_epoch.as_u64()..end_epoch.as_u64() {

        // Calculate the relative index in the RingBuffer
        let relative_epoch = epoch_index
            .checked_sub(current_epoch.as_u64())
            .ok_or(TapeError::Underflow)?;
        
        // Check if the epoch is beyond the buffer's capacity
        if relative_epoch >= archive.storage_used.capacity() as u64 {
            return Err(ProgramError::InvalidArgument)
        }

        // Get the storage used for this epoch, or 0 if not set
        let storage_used = archive.storage_used
            .get(relative_epoch as usize)
            .copied()
            .unwrap_or(StorageUnits::zero());

        // Check if adding total_units exceeds capacity
        let new_storage_used = storage_used
            .checked_add(StorageUnits::new(total_units))
            .ok_or(TapeError::Overflow)?;

        if new_storage_used > archive.storage_capacity {
            return Err(TapeError::InsufficientCapacity.into());
        }

        // Update the storage used for this epoch
        archive.storage_used
            .get_mut(relative_epoch as usize)
            .map(|su| *su = new_storage_used)
            .ok_or(TapeError::UnexpectedState)?;
    }

    // TODO: spread the total cost over the covered epochs in the archive's fees_collected
    // RingBuffer

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
    tape.capacity = StorageUnits::new(total_units);
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
