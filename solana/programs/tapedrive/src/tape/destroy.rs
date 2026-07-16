use tape_solana::*;
use tape_api::program::prelude::*;

use crate::tape::helpers::destroy_expired;

pub fn process_destroy_tape(accounts: &[AccountInfo<'_>], data: &[u8]) -> ProgramResult {
    let _args = DestroyTape::try_from_bytes(data)?;
    let [
        fee_payer_info,
        authority_info,

        tape_info,
        system_info,

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

    let tape = tape_info
        .is_writable()?
        .as_account_mut::<Tape>(&tapedrive::ID)?;

    if tape.authority != (*authority_info.key).into() {
        return Err(ProgramError::InvalidAccountData);
    }

    let system = system_info
        .is_system()?
        .as_account::<System>(&tapedrive::ID)?;

    destroy_expired(
        tape_info,
        fee_payer_info,
        system,
        (*tape_info.key).into(),
        (*authority_info.key).into(),
        tape.expiry_epoch,
    )?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tape_test::*;

    #[test]
    fn destroy_tape() {
        let fee_payer = Pubkey::new_unique();
        let authority = Pubkey::new_unique();
        let (tape_address, _) = tape_pda(authority.into());
        let (system_address, _) = system_pda();

        // Tape expired at 50. Destroy is allowed even when used is non-zero;
        // expired tape metadata cleanup is independent of track cleanup.
        let tape = Tape {
            authority: authority.into(),
            capacity: StorageUnits::mb(123),
            used: StorageUnits::mb(12),
            active_epoch: EpochNumber(40),
            expiry_epoch: EpochNumber(50),
            ..Tape::zeroed()
        };

        let system = System {
            current_epoch: EpochNumber(60),
            ..System::zeroed()
        };

        let instruction = build_destroy_tape_ix(fee_payer.into(), authority.into());

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            sol(authority, 0),

            pda(tape_address, tape.pack(), tapedrive::ID),
            pda(system_address, system.pack(), tapedrive::ID),

            system_program(),
        ];

        let env = test_env();
        env.process_instruction(
            &instruction,
            &accounts,
            &[
                Check::success(),
                Check::account(&fee_payer)
                    .lamports(1_000_000_000 + rent(Tape::get_size()))
                    .build(),
                Check::account(&Pubkey::from(tape_address))
                    .lamports(0)
                    .closed()
                    .build(),
            ],
        );
    }
}
