use steel::*;
use tape_api::prelude::*;

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

    create_program_account::<Epoch>(
        epoch_info,
        system_program_info,
        signer_info,
        &tapedrive::ID,
        &[EPOCH],
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

        let env = test_env();
        env.process_instruction(
            &instruction, 
            &accounts,
            &[
                Check::success(),

                Check::account(&epoch_address).data(
                    Epoch { 
                        id: EpochNumber(0),
                        ..Epoch::zeroed()
                    }.pack().as_ref()
                ).build(),
            ]
        );
    }
}
