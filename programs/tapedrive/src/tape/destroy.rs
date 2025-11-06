use tape_api::prelude::*;
use steel::*;

pub fn process_destroy_tape(accounts: &[AccountInfo<'_>], data: &[u8]) -> ProgramResult {
    let _args = DestroyTape::try_from_bytes(data)?;
    let [
        signer_info,

        tape_info,
        epoch_info,

        system_program_info,
    ] = accounts else {
        return Err(ProgramError::NotEnoughAccountKeys);
    };

    signer_info
        .is_signer()?;

    system_program_info
        .is_program(&system_program::ID)?;

    let tape = tape_info
        .is_writable()?
        .as_account_mut::<Tape>(&tapedrive::ID)?;

    if tape.authority != *signer_info.key {
        return Err(ProgramError::InvalidAccountData);
    }

    // Require the tape to be expired
    let epoch = epoch_info
        .is_epoch()?
        .as_account::<Epoch>(&tapedrive::ID)?;

    let now = current_epoch(epoch);

    if now < tape.expiry_epoch {
        return Err(ProgramError::Custom(30)); // not expired
    }

    if !tape.used.is_zero() {
        return Err(ProgramError::Custom(31)); // still used
    }

    close_account(tape_info, signer_info)?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tape_test::*;

    #[test]
    fn test_destroy_tape() {
        let signer = Pubkey::new_unique();
        let (tape_address, _) = tape_pda(signer);
        let (epoch_address, _) = epoch_pda();

        // Tape expired at 50, used = 0
        let tape = Tape {
            authority: signer,
            capacity: StorageUnits(123),
            used: StorageUnits(0),
            active_epoch: EpochNumber(40),
            expiry_epoch: EpochNumber(50),
            ..Tape::zeroed()
        };

        let mut epoch = Epoch::zeroed();
        epoch.id = EpochNumber(60);

        let instruction = build_destroy_tape_ix(signer);

        let accounts = vec![
            sol(signer, 1_000_000_000),

            pda(tape_address, tape.pack(), tapedrive::ID),
            pda(epoch_address, epoch.pack(), tapedrive::ID),

            system_program(),
        ];

        let env = test_env();
        env.process_instruction(
            &instruction,
            &accounts,
            &[
                Check::success(),
                Check::account(&signer)
                    .lamports(1_000_000_000 + rent(Tape::get_size()))
                    .build(),
                Check::account(&tape_address)
                    .lamports(0)
                    .closed()
                    .build(),
            ],
        );
    }
}
