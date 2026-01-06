use steel::*;
use tape_api::prelude::*;
use solana_program::entrypoint::MAX_PERMITTED_DATA_INCREASE;

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
        .has_address(&system_address)?;

    let size = MAX_PERMITTED_DATA_INCREASE
        .min(System::get_size());

    create_account_with_size::<System>(
        system_info,
        system_program_info,
        fee_payer_info,
        size,
        &tapedrive::ID,
        &[SYSTEM],
        SYSTEM_BUMP,
    )?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tape_test::*;

    #[test]
    fn test_system_create() {
        let fee_payer = Pubkey::new_unique();
        let authority = Pubkey::new_unique();

        let instruction = build_create_system_ix(fee_payer, authority);
        let (system_address, _) = system_pda();

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            sol(authority, 0),
            empty(system_address),

            system_program(),
            rent_sysvar(),
        ];

        let size = MAX_PERMITTED_DATA_INCREASE
            .min(System::get_size());

        let env = test_env();
        env.process_instruction(
            &instruction,
            &accounts,
            &[
                Check::success(),
                Check::account(&system_address)
                    .space(size)
                    .owner(&tapedrive::ID)
                    .data_slice(0, &[System::discriminator()])
                    .build(),
            ]
        );
    }
}
