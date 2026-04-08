use tape_solana::*;
use tape_api::program::prelude::*;
use solana_program::entrypoint::MAX_PERMITTED_DATA_INCREASE;

pub fn process_expand_system(accounts: &[AccountInfo<'_>], data: &[u8]) -> ProgramResult {
    let _args = ExpandSystem::try_from_bytes(data)?;
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
        .is_type::<System>(&tapedrive::ID)?
        .is_writable()?
        .has_address(&system_address.into())?;

    let current_size = system_info.data_len();
    let required_size = System::get_size();

    if current_size == 0 {
        return Err(ProgramError::UninitializedAccount);
    }

    if current_size >= required_size {
        return Err(ProgramError::AccountAlreadyInitialized);
    }

    let new_size = current_size
        .saturating_add(MAX_PERMITTED_DATA_INCREASE)
        .min(required_size);

    resize_account(
        system_info,
        system_program_info,
        fee_payer_info,
        new_size,
    )?;

    Ok(())
}

 #[cfg(test)]
 mod tests {
     use super::*;
     use tape_test::*;

     #[test]
     fn test_system_expand() {
         let fee_payer = Pubkey::new_unique();
         let authority = Pubkey::new_unique();

         let instruction = build_expand_system_ix(fee_payer.into(), authority.into());
         let (system_address, _) = system_pda();

         // Create a system account that is one byte short.
         let partial_account = System::zeroed()
             .pack()[..System::get_size()-1].to_vec();

         let accounts = vec![
             sol(fee_payer, 1_000_000_000),
             sol(authority, 0),
             pda(system_address, partial_account, tapedrive::ID),

             system_program(),
             rent_sysvar(),
         ];

         let env = test_env();
         env.process_instruction(
             &instruction,
             &accounts,
             &[
                 Check::success(),
                 Check::account(&Pubkey::from(system_address)).data(
                     System {
                         ..System::zeroed()
                     }.pack().as_ref()
                 ).build(),
             ]
         );
     }

    #[test]
    fn test_system_partial_expand() {
        let fee_payer = Pubkey::new_unique();
        let authority = Pubkey::new_unique();
        let (system_address, _) = system_pda();
        let instruction = build_expand_system_ix(fee_payer.into(), authority.into());

        // Create a system account with minimal size (1 byte)
        let initial_size = 1;
        let partial_account = System::zeroed()
            .pack()[0..initial_size].to_vec();

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            sol(authority, 0),
            pda(system_address, partial_account, tapedrive::ID),
            system_program(),
            rent_sysvar(),
        ];

        let env = test_env();

        // Calculate expected size after one expansion
        let required_size = System::get_size();
        let expected_size = initial_size
            .saturating_add(MAX_PERMITTED_DATA_INCREASE)
            .min(required_size);

        assert!(expected_size > initial_size, "Expected size should be greater than initial size");

        env.process_instruction(
            &instruction,
            &accounts,
            &[
                Check::success(),
                Check::account(&Pubkey::from(system_address))
                    .space(expected_size)
                    .build(),
            ],
        );
    }
 }
