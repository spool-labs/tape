use tape_api::program::prelude::*;

pub fn process_split_tape_by_epoch(accounts: &[AccountInfo<'_>], data: &[u8]) -> ProgramResult {
    let args = SplitTapeByEpoch::try_from_bytes(data)?;
    let [
        fee_payer_info,
        source_authority_info,
        dest_authority_info,

        source_tape_info,
        dest_tape_info,
        archive_info,

        system_program_info,
        rent_info,
    ] = accounts else {
        return Err(ProgramError::NotEnoughAccountKeys);
    };

    fee_payer_info
        .is_signer()?
        .is_writable()?;

    source_authority_info
        .is_signer()?;
    dest_authority_info
        .is_signer()?;

    system_program_info
        .is_program(&system_program::ID)?;
    rent_info
        .is_sysvar(&sysvar::rent::ID)?;

    let archive = archive_info
        .is_writable()?
        .is_archive()?
        .as_account_mut::<Archive>(&tapedrive::ID)?;

    // Splitting creates an additional tape (1 becomes 2)
    archive.tape_count = archive.tape_count
        .checked_add(1)
        .ok_or(ProgramError::ArithmeticOverflow)?;

    // Derive PDAs
    let (source_tape_address, _) = tape_pda((*source_authority_info.key).into());
    let (dest_tape_address, _) = tape_pda((*dest_authority_info.key).into());

    // Validate source tape
    source_tape_info
        .has_address(&source_tape_address.into())?
        .is_writable()?
        .is_type::<Tape>(&tapedrive::ID)?;
    let source_tape = source_tape_info.as_account_mut::<Tape>(&tapedrive::ID)?;

    if source_tape.authority != (*source_authority_info.key).into() {
        return Err(ProgramError::InvalidAccountData);
    }

    // Destination must be empty PDA for recipient
    dest_tape_info
        .has_address(&dest_tape_address.into())?
        .is_writable()?
        .is_empty()?;

    let split_epoch = EpochNumber::unpack(args.epoch);

    // Require split strictly inside (active, expiry)
    if !(split_epoch > source_tape.active_epoch && split_epoch < source_tape.expiry_epoch) {
        return Err(ProgramError::InvalidArgument);
    }

    // Create destination Tape account (dest authority)
    create_program_account::<Tape>(
        dest_tape_info,
        system_program_info,
        fee_payer_info,
        &tapedrive::ID,
        &[CASSETTE, dest_authority_info.key.as_ref()],
    )?;

    let dest_tape = dest_tape_info.as_account_mut::<Tape>(&tapedrive::ID)?;

    // Initialize destination: later time slice with same capacity; used starts at zero
    dest_tape.authority = (*dest_authority_info.key).into();
    dest_tape.active_epoch = split_epoch;
    dest_tape.expiry_epoch = source_tape.expiry_epoch;
    dest_tape.capacity     = source_tape.capacity;
    dest_tape.used         = StorageUnits::zero();

    // Update source to earlier slice
    source_tape.expiry_epoch = split_epoch;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tape_test::*;

    #[test]
    fn test_split_tape_by_epoch() {
        let fee_payer = Pubkey::new_unique();
        let source_authority = Pubkey::new_unique();
        let dest_authority = Pubkey::new_unique();

        let (source_tape_address, _) = tape_pda(source_authority.into());
        let (dest_tape_address, _)   = tape_pda(dest_authority.into());
        let (archive_address, _)     = archive_pda();

        // Source: 500 capacity, used 123, epochs [40, 50)
        let source_tape = Tape {
            authority: source_authority.into(),
            capacity: StorageUnits::mb(500),
            used: StorageUnits::mb(123),
            active_epoch: EpochNumber(40),
            expiry_epoch: EpochNumber(50),
            ..Tape::zeroed()
        };

        let archive = Archive {
            tape_count: 100,
            ..Archive::zeroed()
        };

        let split_epoch = EpochNumber(45);
        let instruction = build_split_tape_by_epoch_ix(fee_payer.into(), source_authority.into(), dest_authority.into(), split_epoch);

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            sol(source_authority, 0),
            sol(dest_authority, 0),

            pda(source_tape_address, source_tape.pack(), tapedrive::ID),
            empty(dest_tape_address),
            pda(archive_address, archive.pack(), tapedrive::ID),

            system_program(),
            rent_sysvar(),
        ];

        let expected_dest = Tape {
            authority: dest_authority.into(),
            capacity: StorageUnits::mb(500),
            used: StorageUnits(0), // starts zero for future slice
            active_epoch: EpochNumber(45),
            expiry_epoch: EpochNumber(50),
            ..Tape::zeroed()
        };
        let expected_source = Tape {
            authority: source_authority.into(),
            capacity: StorageUnits::mb(500),
            used: StorageUnits::mb(123),
            active_epoch: EpochNumber(40),
            expiry_epoch: EpochNumber(45),
            ..Tape::zeroed()
        };
        let expected_archive = Archive {
            tape_count: 101, // split adds 1 tape (1 becomes 2)
            ..archive
        };

        let env = test_env();
        env.process_instruction(
            &instruction,
            &accounts,
            &[
                Check::success(),
                Check::account(&Pubkey::from(dest_tape_address))
                    .data(expected_dest.pack().as_ref()).build(),
                Check::account(&Pubkey::from(source_tape_address))
                    .data(expected_source.pack().as_ref()).build(),
                Check::account(&Pubkey::from(archive_address))
                    .data(expected_archive.pack().as_ref())
                    .build(),
            ],
        );
    }
}
