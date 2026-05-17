use tape_solana::*;
use tape_api::program::prelude::*;

pub fn process_create_system(accounts: &[AccountInfo<'_>], data: &[u8]) -> ProgramResult {
    let _args = CreateSystem::try_from_bytes(data)?;
    let [
        fee_payer_info,
        authority_info,
        system_info,
        system_program_info,
        rent_sysvar_info,
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
    rent_sysvar_info
        .is_sysvar(&sysvar::rent::ID)?;

    let (system_address, _) = system_pda();

    system_info
        .is_empty()?
        .is_writable()?
        .has_address(&system_address.into())?;

    create_program_account::<System>(
        system_info,
        system_program_info,
        fee_payer_info,
        &tapedrive::ID,
        &[SYSTEM],
    )?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tape_test::*;

    #[test]
    fn create_system() {
        let fee_payer = Pubkey::new_unique();
        let authority = Pubkey::new_unique();

        let instruction = build_create_system_ix(fee_payer.into(), authority.into());
        let (system_address, _) = system_pda();

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            sol(authority, 0),
            empty(system_address),

            system_program(),
            rent_sysvar(),
        ];

        let env = test_env();
        env.process_instruction(
            &instruction,
            &accounts,
            &[
                Check::success(),
                Check::account(&Pubkey::from(system_address))
                    .space(System::get_size())
                    .owner(&tapedrive::ID)
                    .data_slice(0, &[System::discriminator()])
                    .build(),
            ]
        );
    }
}
