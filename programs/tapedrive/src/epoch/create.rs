use steel::*;
use tape_api::prelude::*;
use solana_program::entrypoint::MAX_PERMITTED_DATA_INCREASE;

pub fn process_create_epoch(accounts: &[AccountInfo<'_>], data: &[u8]) -> ProgramResult {
    let _args = CreateEpoch::try_from_bytes(data)?;
    let [
        signer_info, 
        epoch_info,
        system_program_info, 
        rent_sysvar_info,
    ] = accounts else {
        return Err(ProgramError::NotEnoughAccountKeys);
    };

    system_program_info
        .is_program(&system_program::ID)?;
    rent_sysvar_info
        .is_sysvar(&sysvar::rent::ID)?;

    let (epoch_address, _) = epoch_pda();

    epoch_info
        .is_empty()?
        .is_writable()?
        .has_address(&epoch_address)?;

    let size = MAX_PERMITTED_DATA_INCREASE
        .min(Epoch::get_size());
    
    create_account_with_size::<Epoch>(
        epoch_info,
        system_program_info,
        signer_info,
        size,
        &tapedrive::ID,
        &[EPOCH],
        EPOCH_BUMP,
    )?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tape_test::*;

    #[test]
    fn test_epoch_create() {
        let signer = Pubkey::new_unique();

        let instruction = build_create_epoch_ix(signer);
        let (epoch_address, _) = epoch_pda();

        let accounts = vec![
            sol(signer, 1_000_000_000),
            empty(epoch_address),

            system_program(),
            rent_sysvar(),
        ];

        let size = MAX_PERMITTED_DATA_INCREASE
            .min(Epoch::get_size());

        let env = test_env();
        env.process_instruction(
            &instruction, 
            &accounts,
            &[
                Check::success(),
                Check::account(&epoch_address)
                    .space(size)
                    .owner(&tapedrive::ID)
                    .data_slice(0, &[Epoch::discriminator()])
                    .build(),
            ]
        );
    }
}
