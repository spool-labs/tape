use tape_solana::*;
use tape_api::prelude::*;
use tape_api::event::TapeDestroyed;
use crate::error::*;

pub fn process_destroy_tape(accounts: &[AccountInfo<'_>], data: &[u8]) -> ProgramResult {
    let _args = DestroyTape::try_from_bytes(data)?;
    let [
        fee_payer_info,
        authority_info,

        tape_info,
        epoch_info,
        archive_info,

        system_program_info,
    ] = accounts else {
        return Err(ProgramError::NotEnoughAccountKeys);
    };

    fee_payer_info
        .is_signer()?
        .is_writable()?;

    authority_info
        .is_signer()?;

    system_program_info
        .is_program(&system_program::ID)?;

    let archive = archive_info
        .is_writable()?
        .is_archive()?
        .as_account_mut::<Archive>(&tapedrive::ID)?;

    archive.tape_count = archive.tape_count
        .checked_sub(1)
        .ok_or(ProgramError::ArithmeticOverflow)?;

    let tape = tape_info
        .is_writable()?
        .as_account_mut::<Tape>(&tapedrive::ID)?;

    if tape.authority != *authority_info.key {
        return Err(ProgramError::InvalidAccountData);
    }

    // Require the tape to be expired
    let epoch = epoch_info
        .is_epoch()?
        .as_account::<Epoch>(&tapedrive::ID)?;

    let now = current_epoch(epoch);

    if now < tape.expiry_epoch {
        return Err(TapeError::NotExpired.into());
    }

    if !tape.used.is_zero() {
        return Err(TapeError::NotEmpty.into());
    }

    TapeDestroyed {
        tape: *tape_info.key,
        authority: *authority_info.key,
    }.log();

    close_account(tape_info, fee_payer_info)?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tape_test::*;

    #[test]
    fn test_destroy_tape() {
        let fee_payer = Pubkey::new_unique();
        let authority = Pubkey::new_unique();
        let (tape_address, _) = tape_pda(authority);
        let (epoch_address, _) = epoch_pda();
        let (archive_address, _) = archive_pda();

        // Tape expired at 50, used = 0
        let tape = Tape {
            authority: authority,
            capacity: StorageUnits::mb(123),
            used: StorageUnits(0),
            active_epoch: EpochNumber(40),
            expiry_epoch: EpochNumber(50),
            ..Tape::zeroed()
        };

        let mut epoch = Epoch::zeroed();
        epoch.id = EpochNumber(60);

        let archive = Archive {
            tape_count: 100,
            ..Archive::zeroed()
        };

        let instruction = build_destroy_tape_ix(fee_payer, authority);

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            sol(authority, 0),

            pda(tape_address, tape.pack(), tapedrive::ID),
            pda(epoch_address, epoch.pack(), tapedrive::ID),
            pda(archive_address, archive.pack(), tapedrive::ID),

            system_program(),
        ];

        let expected_archive = Archive {
            tape_count: 99,
            ..archive
        };

        let env = test_env();
        env.process_instruction(
            &instruction,
            &accounts,
            &[
                Check::success(),
                Check::account(&fee_payer)
                    .lamports(1_000_000_000 + rent(Tape::get_size()))
                    .build(),
                Check::account(&tape_address)
                    .lamports(0)
                    .closed()
                    .build(),
                Check::account(&archive_address)
                    .data(expected_archive.pack().as_ref())
                    .build(),
            ],
        );
    }
}
