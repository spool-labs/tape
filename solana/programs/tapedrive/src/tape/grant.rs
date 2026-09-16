use tape_api::program::prelude::*;

use crate::tape::helpers::{
    authorize_tape_operator,
    verified_tape_address,
};

pub fn process_bump_grant_generation(accounts: &[AccountInfo<'_>], data: &[u8]) -> ProgramResult {
    let _args = BumpGrantGeneration::try_from_bytes(data)?;
    let [
        fee_payer_info,
        signer_info,
        tape_info,
    ] = accounts else {
        return Err(ProgramError::NotEnoughAccountKeys);
    };

    fee_payer_info
        .is_signer()?
        .is_writable()?;

    signer_info
        .is_signer()?;

    let tape = tape_info
        .is_writable()?
        .as_account_mut::<Tape>(&tapedrive::ID)?;

    if tape.is_system() {
        return Err(TapeError::UnexpectedState.into());
    }

    verified_tape_address(tape_info, tape)?;
    authorize_tape_operator(tape, (*signer_info.key).into())?;

    tape.bump_grant_generation()?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tape_test::*;

    // the delegate can bump the generation counter
    #[test]
    fn bump_generation() {
        let fee_payer = Pubkey::new_unique();
        let authority = Pubkey::new_unique();
        let delegate = Pubkey::new_unique();
        let (tape_address, _) = tape_pda(authority.into());

        let tape = Tape {
            authority: authority.into(),
            delegate: delegate.into(),
            capacity: StorageUnits::mb(1000),
            grant_generation: 4,
            ..Tape::zeroed()
        };

        let instruction = build_bump_grant_generation_ix(
            fee_payer.into(),
            delegate.into(),
            tape_address,
        );

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            sol(delegate, 0),
            pda(tape_address, tape.pack(), tapedrive::ID),
        ];

        let env = test_env();
        env.process_instruction(
            &instruction,
            &accounts,
            &[
                Check::success(),
                Check::account(&Pubkey::from(tape_address))
                    .data(Tape {
                        grant_generation: 5,
                        ..tape
                    }.pack().as_ref())
                    .build(),
            ],
        );
    }

    // an unrelated signer cannot bump the generation counter
    #[test]
    fn bump_rejects_stranger() {
        let fee_payer = Pubkey::new_unique();
        let authority = Pubkey::new_unique();
        let stranger = Pubkey::new_unique();
        let (tape_address, _) = tape_pda(authority.into());

        let tape = Tape {
            authority: authority.into(),
            capacity: StorageUnits::mb(1000),
            ..Tape::zeroed()
        };

        let instruction = build_bump_grant_generation_ix(
            fee_payer.into(),
            stranger.into(),
            tape_address,
        );

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            sol(stranger, 0),
            pda(tape_address, tape.pack(), tapedrive::ID),
        ];

        let env = test_env();
        env.process_instruction(
            &instruction,
            &accounts,
            &[Check::err(ProgramError::InvalidAccountData)],
        );
    }
}
