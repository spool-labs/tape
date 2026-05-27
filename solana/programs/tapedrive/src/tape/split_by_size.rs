use tape_solana::*;
use tape_api::program::prelude::*;

pub fn process_split_tape_by_size(accounts: &[AccountInfo<'_>], data: &[u8]) -> ProgramResult {
    let args = SplitTapeBySize::try_from_bytes(data)?;
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

    archive_info
        .is_writable()?
        .is_archive()?;

    let archive = archive_info.as_account_mut::<Archive>(&tapedrive::ID)?;

    // Allocate a new monotonic tape ID for the destination tape.
    let dest_tape_id = TapeNumber(
        archive.tape_count
            .checked_add(1)
            .ok_or(ProgramError::ArithmeticOverflow)?,
    );
    archive.tape_count = dest_tape_id.as_u64();

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

    let split_size = args.size;
    if split_size.is_zero() || split_size >= source_tape.capacity {
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

    // Compute used distribution
    let used_for_dest = StorageUnits(source_tape.used.as_u64().min(split_size.as_u64()));
    let used_for_source = source_tape
        .used
        .checked_sub(used_for_dest)
        .ok_or(TapeError::UnexpectedState)?;

    // Initialize destination tape
    dest_tape.id = dest_tape_id;
    dest_tape.authority = (*dest_authority_info.key).into();
    dest_tape.active_epoch = source_tape.active_epoch;
    dest_tape.expiry_epoch = source_tape.expiry_epoch;
    dest_tape.capacity     = split_size;
    dest_tape.used         = used_for_dest;

    // Update source
    source_tape.capacity = source_tape
        .capacity
        .checked_sub(split_size)
        .ok_or(TapeError::UnexpectedState)?;
    source_tape.used = used_for_source;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tape_test::*;

    #[test]
    fn test_split_tape_by_size() {
        let fee_payer = Pubkey::new_unique();
        let source_authority = Pubkey::new_unique();
        let dest_authority = Pubkey::new_unique();

        let (source_tape_address, _) = tape_pda(source_authority.into());
        let (dest_tape_address, _)   = tape_pda(dest_authority.into());
        let (archive_address, _)     = archive_pda();

        // Source: 1000 capacity, used 250, epochs [10, 20)
        let source_tape = Tape {
            authority: source_authority.into(),
            capacity: StorageUnits::mb(1000),
            used: StorageUnits::mb(250),
            active_epoch: EpochNumber(10),
            expiry_epoch: EpochNumber(20),
            ..Tape::zeroed()
        };

        let archive = Archive {
            tape_count: 100,
            ..Archive::zeroed()
        };

        let split_size = StorageUnits::mb(200);
        let instruction = build_split_tape_by_size_ix(fee_payer.into(), source_authority.into(), dest_authority.into(), split_size);

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
            id: TapeNumber(101),
            authority: dest_authority.into(),
            capacity: split_size,
            used: StorageUnits::mb(200), // min(250, 200)
            active_epoch: EpochNumber(10),
            expiry_epoch: EpochNumber(20),
            ..Tape::zeroed()
        };

        let expected_source = Tape {
            authority: source_authority.into(),
            capacity: StorageUnits::mb(800),
            used: StorageUnits::mb(50), // 250 - 200
            active_epoch: EpochNumber(10),
            expiry_epoch: EpochNumber(20),
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
