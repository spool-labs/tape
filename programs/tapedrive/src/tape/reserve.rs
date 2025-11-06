use tape_api::prelude::*;
use steel::*;

pub fn process_reserve_tape(accounts: &[AccountInfo<'_>], data: &[u8]) -> ProgramResult {
    let args = ReserveTape::try_from_bytes(data)?;
    let [
        signer_info,
        signer_ata_info,

        tape_info,
        epoch_info,
        archive_info,
        archive_ata_info,

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

    token_program_info
        .is_program(&spl_token::ID)?;
    system_program_info
        .is_program(&system_program::ID)?;
    rent_info
        .is_sysvar(&sysvar::rent::ID)?;

    let epoch = epoch_info
        .is_epoch()?
        .as_account::<Epoch>(&tapedrive::ID)?;

    let archive = archive_info
        .is_writable()?
        .is_archive()?
        .as_account_mut::<Archive>(&tapedrive::ID)?;

    archive_ata_info
        .is_writable()?
        .is_archive_ata()?;

    let (tape_address, _)  = tape_pda(*signer_info.key);

    tape_info
        .is_empty()?
        .is_writable()?
        .has_address(&tape_address)?;

    let start_epoch = EpochNumber::unpack(args.activation_epoch);
    let end_epoch = EpochNumber::unpack(args.expiry_epoch);

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

    let price_per_unit = archive.storage_price
        .as_u64();

    let single_epoch_price = price_per_unit
        .checked_mul(total_units.as_u64())
        .ok_or(ProgramError::InvalidArgument)?;

    let total_cost = single_epoch_price
        .checked_mul(num_epochs.as_u64())
        .ok_or(ProgramError::InvalidArgument)?;

    let current_epoch = current_epoch(epoch);
    let current_capacity = archive.storage_capacity;
    let fee_per_epoch = TAPE(single_epoch_price);

    if archive.schedule.current_epoch() != current_epoch {
        return Err(ProgramError::Custom(0));
        //return Err(TapeError::UnexpectedState.into());
    }

    if !archive.schedule.has_capacity_for(
        total_units, current_capacity, start_epoch, end_epoch) {
        return Err(ProgramError::Custom(1));
        //return Err(TapeError::InsufficientCapacity.into());
    }
    
    archive.schedule
        .reserve_capacity(total_units, fee_per_epoch, start_epoch, end_epoch)
        .map_err(|_| ProgramError::Custom(2))?;
        //.map_err(|_| TapeError::UnexpectedState)?;


    create_program_account::<Tape>(
        tape_info,
        system_program_info,
        signer_info,
        &tapedrive::ID,
        &[RESOURCE, signer_info.key.as_ref()],
    )?;

    let tape = tape_info.as_account_mut::<Tape>(&tapedrive::ID)?;

    tape.authority = *signer_info.key;
    tape.active_epoch = start_epoch;
    tape.expiry_epoch = end_epoch;
    tape.capacity = total_units;
    tape.used = StorageUnits::zero();

    transfer(
        signer_info,
        signer_ata_info,
        archive_ata_info,
        token_program_info,
        total_cost,
    )?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tape_test::*;

    #[test]
    fn test_reserve_tape() {
        let signer = Pubkey::new_unique();

        let storage_units = StorageUnits(100);     // 100 MB
        let start_epoch = EpochNumber(43);         // In the future
        let end_epoch = EpochNumber(45);           // Two epochs duration
        let price_per_unit = TAPE::from("0.0001"); // 0.0001 TAPE per MB

        let instruction = build_reserve_tape_ix(
            signer, storage_units, start_epoch, end_epoch);

        let (epoch_address, _) = epoch_pda();
        let (archive_address, _) = archive_pda();
        let (archive_ata, _) = archive_ata();
        let (tape_address, _) = tape_pda(signer);
        let signer_ata = ata_address(&signer);

        // Setup existing accounts

        let epoch = Epoch::zeroed();

        let archive = Archive {
            storage_capacity: StorageUnits(1000), // 1000 MB capacity
            storage_price: price_per_unit,
            schedule: EpochSchedule::new(),
            ..Archive::zeroed()
        };

        // Calculate expected cost and state
        let num_epochs = (end_epoch - start_epoch).as_u64(); // 2 epochs
        let single_epoch_price = price_per_unit.as_u64() * storage_units.as_u64(); // 0.0001 * 100 = 0.01 TAPE
        let total_cost = single_epoch_price * num_epochs; // 0.01 * 2 = 0.02 TAPE
        let fee_per_epoch = TAPE(single_epoch_price);

        // Simulate reserve_capacity and add_rewards

        let mut expected_archive = archive.clone();
        expected_archive
            .schedule
            .reserve_capacity(storage_units, fee_per_epoch, start_epoch, end_epoch)
            .unwrap();

        let initial_token_balance: u64 = 1_000_000;

        let accounts = vec![
            sol(signer, 1_000_000_000),
            token(signer_ata, signer, initial_token_balance),

            empty(tape_address),
            pda(epoch_address, epoch.pack(), tapedrive::ID),
            pda(archive_address, archive.pack(), tapedrive::ID),
            token(archive_ata, archive_address, 0),

            token_program(),
            system_program(),
            rent_sysvar(),
        ];

        let env = test_env();
        env.process_instruction(
            &instruction,
            &accounts,
            &[
                Check::success(),
                Check::account(&tape_address).data(
                    Tape {
                        authority: signer,
                        capacity: storage_units,
                        used: StorageUnits::zero(),
                        active_epoch: start_epoch,
                        expiry_epoch: end_epoch,
                        ..Tape::zeroed()
                    }.pack().as_ref()
                ).build(),
                Check::account(&archive_address).data(
                    expected_archive.pack().as_ref()
                ).build(),
                Check::account(&signer_ata).data(
                    token(signer_ata, signer, initial_token_balance - total_cost).1.data.as_ref()
                ).build(),
                Check::account(&archive_ata).data(
                    token(archive_ata, archive_address, total_cost).1.data.as_ref()
                ).build(),
            ]
        );
    }
}
