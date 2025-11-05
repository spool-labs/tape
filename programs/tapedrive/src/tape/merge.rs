use tape_api::prelude::*;
use steel::*;

pub fn process_merge_tape(accounts: &[AccountInfo<'_>], data: &[u8]) -> ProgramResult {
    let _args = MergeTape::try_from_bytes(data)?;
    let [
        signer_info,
        recipient_info,

        source_tape_info,
        dest_tape_info,

        system_program_info,
    ] = accounts else {
        return Err(ProgramError::NotEnoughAccountKeys);
    };

    signer_info
        .is_signer()?;
    recipient_info
        .is_signer()?;

    system_program_info
        .is_program(&system_program::ID)?;

    let (source_tape_address, _) = tape_pda(*signer_info.key);
    let (dest_tape_address, _)   = tape_pda(*recipient_info.key);

    let source_tape = source_tape_info
        .is_writable()?
        .has_address(&source_tape_address)?
        .as_account_mut::<Tape>(&tapedrive::ID)?;

    let dest_tape   = dest_tape_info
        .is_writable()?
        .has_address(&dest_tape_address)?
        .as_account_mut::<Tape>(&tapedrive::ID)?;

    // Require correct authorities
    if source_tape.authority != *signer_info.key || 
       dest_tape.authority != *recipient_info.key {
        return Err(ProgramError::InvalidAccountData);
    }

    // Merge tapes together if possible
    let Some((new_active_epoch, new_expiry_epoch, new_capacity, new_used)) =
        merge_tapes(
            source_tape.active_epoch,
            source_tape.expiry_epoch,
            source_tape.capacity,
            source_tape.used,
            dest_tape.active_epoch,
            dest_tape.expiry_epoch,
            dest_tape.capacity,
            dest_tape.used,
        )
    else {
        return Err(ProgramError::Custom(20));
    };

    // Apply common result
    dest_tape.active_epoch = new_active_epoch;
    dest_tape.expiry_epoch = new_expiry_epoch;
    dest_tape.capacity     = new_capacity;
    dest_tape.used         = new_used;

    // Close source (common)
    close_account(source_tape_info, signer_info)?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tape_test::*;

    #[test]
    fn test_merge_tape() {
        let signer = Pubkey::new_unique();
        let recipient = Pubkey::new_unique();

        let (source_tape_address, _) = tape_pda(signer);
        let (dest_tape_address, _)   = tape_pda(recipient);

        // Two tapes with identical epochs
        let e0 = EpochNumber(100);
        let e1 = EpochNumber(110);

        let source_tape = Tape {
            authority: signer,
            capacity: StorageUnits(200),
            used: StorageUnits(30),
            active_epoch: e0,
            expiry_epoch: e1,
            ..Tape::zeroed()
        };
        let dest_tape = Tape {
            authority: recipient,
            capacity: StorageUnits(100),
            used: StorageUnits(20),
            active_epoch: e0,
            expiry_epoch: e1,
            ..Tape::zeroed()
        };

        let instruction = build_merge_tape_ix(signer, recipient);

        let accounts = vec![
            sol(signer, 1_000_000_000),
            sol(recipient, 0),

            pda(source_tape_address, source_tape.pack(), tapedrive::ID),
            pda(dest_tape_address, dest_tape.pack(), tapedrive::ID),

            system_program(),
        ];

        let expected_dest = Tape {
            authority: recipient,
            capacity: StorageUnits(300),
            used: StorageUnits(50),
            active_epoch: e0,
            expiry_epoch: e1,
            ..Tape::zeroed()
        };

        let env = test_env();
        env.process_instruction(
            &instruction,
            &accounts,
            &[
                Check::success(),
                Check::account(&dest_tape_address).data(expected_dest.pack().as_ref()).build(),
                Check::account(&source_tape_address).lamports(0).closed().build(),
            ],
        );
    }
}
