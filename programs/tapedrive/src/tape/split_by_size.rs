use tape_api::prelude::*;
use steel::*;

pub fn process_split_tape_by_size(accounts: &[AccountInfo<'_>], data: &[u8]) -> ProgramResult {
    let args = SplitTapeBySize::try_from_bytes(data)?;
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

    let split_size = StorageUnits::unpack(args.size);
    if split_size.is_zero() || split_size >= source_tape.capacity {
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

    // Compute used distribution
    let used_for_dest = StorageUnits(source_tape.used.as_u64().min(split_size.as_u64()));
    let used_for_source = source_tape
        .used
        .checked_sub(used_for_dest)
        .ok_or(ProgramError::Custom(3))?;

    // Initialize destination tape
    dest_tape.authority    = *recipient_info.key;
    dest_tape.active_epoch = source_tape.active_epoch;
    dest_tape.expiry_epoch = source_tape.expiry_epoch;
    dest_tape.capacity     = split_size;
    dest_tape.used         = used_for_dest;

    // Update source
    source_tape.capacity = source_tape
        .capacity
        .checked_sub(split_size)
        .ok_or(ProgramError::Custom(4))?;
    source_tape.used = used_for_source;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tape_test::*;

    #[test]
    fn test_split_tape_by_size() {
        let signer = Pubkey::new_unique();
        let recipient = Pubkey::new_unique();

        let (source_tape_address, _) = tape_pda(signer);
        let (dest_tape_address, _)   = tape_pda(recipient);

        // Source: 1000 capacity, used 250, epochs [10, 20)
        let source_tape = Tape {
            authority: signer,
            capacity: StorageUnits(1000),
            used: StorageUnits(250),
            active_epoch: EpochNumber(10),
            expiry_epoch: EpochNumber(20),
            ..Tape::zeroed()
        };

        let split_size = StorageUnits(200);
        let instruction = build_split_tape_by_size_ix(signer, recipient, split_size);

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
            capacity: split_size,
            used: StorageUnits(200), // min(250, 200)
            active_epoch: EpochNumber(10),
            expiry_epoch: EpochNumber(20),
            ..Tape::zeroed()
        };

        let expected_source = Tape {
            authority: signer,
            capacity: StorageUnits(800),
            used: StorageUnits(50), // 250 - 200
            active_epoch: EpochNumber(10),
            expiry_epoch: EpochNumber(20),
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
