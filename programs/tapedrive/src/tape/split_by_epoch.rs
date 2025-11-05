use tape_api::prelude::*;
use steel::*;

pub fn process_split_tape_by_epoch(accounts: &[AccountInfo<'_>], data: &[u8]) -> ProgramResult {
    let args = SplitTapeByEpoch::try_from_bytes(data)?;
    let [
        signer_info,
        recipient_info,

        source_tape_info,
        dest_tape_info,

        system_program_info,
        rent_info,
    ] = accounts else {
        return Err(ProgramError::NotEnoughAccountKeys);
    };

    signer_info.is_signer()?;
    recipient_info.is_signer()?;

    system_program_info.is_program(&system_program::ID)?;
    rent_info.is_sysvar(&sysvar::rent::ID)?;

    // Derive PDAs
    let (source_tape_address, _) = tape_pda(*signer_info.key);
    let (dest_tape_address, _)   = tape_pda(*recipient_info.key);

    // Validate source tape
    source_tape_info
        .has_address(&source_tape_address)?
        .is_writable()?
        .is_type::<Tape>(&tapedrive::ID)?;
    let source_tape = source_tape_info.as_account_mut::<Tape>(&tapedrive::ID)?;

    if source_tape.authority != *signer_info.key {
        return Err(ProgramError::InvalidAccountData);
    }

    // Destination must be empty PDA for recipient
    dest_tape_info
        .has_address(&dest_tape_address)?
        .is_writable()?
        .is_empty()?;

    let split_epoch = EpochNumber::unpack(args.epoch);

    // Require split strictly inside (active, expiry)
    if !(split_epoch > source_tape.active_epoch && split_epoch < source_tape.expiry_epoch) {
        return Err(ProgramError::InvalidArgument);
    }

    // Create destination Tape account (recipient authority)
    create_program_account::<Tape>(
        dest_tape_info,
        system_program_info,
        signer_info,
        &tapedrive::ID,
        &[RESOURCE, recipient_info.key.as_ref()],
    )?;

    let dest_tape = dest_tape_info.as_account_mut::<Tape>(&tapedrive::ID)?;

    // Initialize destination: later time slice with same capacity; used starts at zero
    dest_tape.authority    = *recipient_info.key;
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
        let signer = Pubkey::new_unique();
        let recipient = Pubkey::new_unique();

        let (source_tape_address, _) = tape_pda(signer);
        let (dest_tape_address, _)   = tape_pda(recipient);

        // Source: 500 capacity, used 123, epochs [40, 50)
        let source_tape = Tape {
            authority: signer,
            capacity: StorageUnits(500),
            used: StorageUnits(123),
            active_epoch: EpochNumber(40),
            expiry_epoch: EpochNumber(50),
            ..Tape::zeroed()
        };

        let split_epoch = EpochNumber(45);
        let instruction = build_split_tape_by_epoch_ix(signer, recipient, split_epoch);

        let accounts = vec![
            sol(signer, 1_000_000_000),
            sol(recipient, 0),

            pda(source_tape_address, source_tape.pack(), tapedrive::ID),
            empty(dest_tape_address),

            system_program(),
            rent_sysvar(),
        ];

        let expected_dest = Tape {
            authority: recipient,
            capacity: StorageUnits(500),
            used: StorageUnits(0), // starts zero for future slice
            active_epoch: EpochNumber(45),
            expiry_epoch: EpochNumber(50),
            ..Tape::zeroed()
        };
        let expected_source = Tape {
            authority: signer,
            capacity: StorageUnits(500),
            used: StorageUnits(123),
            active_epoch: EpochNumber(40),
            expiry_epoch: EpochNumber(45),
            ..Tape::zeroed()
        };

        let env = test_env();
        env.process_instruction(
            &instruction,
            &accounts,
            &[
                Check::success(),
                Check::account(&dest_tape_address)
                    .data(expected_dest.pack().as_ref()).build(),
                Check::account(&source_tape_address)
                    .data(expected_source.pack().as_ref()).build(),
            ],
        );
    }
}
