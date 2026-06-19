use tape_api::program::prelude::*;

use crate::tape::helpers::{
    authorize_tape_authority,
    verified_tape_address,
};

pub fn process_set_tape_delegate(accounts: &[AccountInfo<'_>], data: &[u8]) -> ProgramResult {
    let args = SetTapeDelegate::try_from_bytes(data)?;
    let [
        fee_payer_info,
        authority_info,
        tape_info,
    ] = accounts else {
        return Err(ProgramError::NotEnoughAccountKeys);
    };

    fee_payer_info
        .is_signer()?
        .is_writable()?;

    authority_info
        .is_signer()?;

    let tape = tape_info
        .is_writable()?
        .as_account_mut::<Tape>(&tapedrive::ID)?;

    if tape.is_system() {
        return Err(TapeError::UnexpectedState.into());
    }

    verified_tape_address(tape_info, tape)?;
    authorize_tape_authority(tape, (*authority_info.key).into())?;

    tape.delegate = args.delegate;

    Ok(())
}

pub fn process_revoke_tape_delegate(accounts: &[AccountInfo<'_>], data: &[u8]) -> ProgramResult {
    let _args = RevokeTapeDelegate::try_from_bytes(data)?;
    let [
        fee_payer_info,
        authority_info,
        tape_info,
    ] = accounts else {
        return Err(ProgramError::NotEnoughAccountKeys);
    };

    fee_payer_info
        .is_signer()?
        .is_writable()?;

    authority_info
        .is_signer()?;

    let tape = tape_info
        .is_writable()?
        .as_account_mut::<Tape>(&tapedrive::ID)?;

    if tape.is_system() {
        return Err(TapeError::UnexpectedState.into());
    }

    verified_tape_address(tape_info, tape)?;
    authorize_tape_authority(tape, (*authority_info.key).into())?;

    tape.delegate = Address::default();

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tape_test::*;

    #[test]
    fn set_tape_delegate() {
        let fee_payer = Pubkey::new_unique();
        let authority = Pubkey::new_unique();
        let delegate = Pubkey::new_unique();
        let (tape_address, _) = tape_pda(authority.into());

        let tape = Tape {
            authority: authority.into(),
            capacity: StorageUnits::mb(1000),
            ..Tape::zeroed()
        };

        let instruction = build_set_tape_delegate_ix(
            fee_payer.into(),
            authority.into(),
            tape_address,
            delegate.into(),
        );

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            sol(authority, 0),
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
                        delegate: delegate.into(),
                        ..tape
                    }.pack().as_ref())
                    .build(),
            ],
        );
    }

    #[test]
    fn revoke_tape_delegate() {
        let fee_payer = Pubkey::new_unique();
        let authority = Pubkey::new_unique();
        let delegate = Pubkey::new_unique();
        let (tape_address, _) = tape_pda(authority.into());

        let tape = Tape {
            authority: authority.into(),
            delegate: delegate.into(),
            capacity: StorageUnits::mb(1000),
            ..Tape::zeroed()
        };

        let instruction = build_revoke_tape_delegate_ix(
            fee_payer.into(),
            authority.into(),
            tape_address,
        );

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            sol(authority, 0),
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
                        delegate: Address::default(),
                        ..tape
                    }.pack().as_ref())
                    .build(),
            ],
        );
    }
}
