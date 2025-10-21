use steel::*;
use tape_api::prelude::*;

pub fn process_initialize(accounts: &[AccountInfo<'_>], _data: &[u8]) -> ProgramResult {
    let [
        signer_info, 

        system_info,
        system_ata_info,
        epoch_info, 
        mint_info, 

        system_program_info, 
        token_program_info, 
        associated_token_program_info,
        rent_sysvar_info,
    ] = accounts else {
        return Err(ProgramError::NotEnoughAccountKeys);
    };

    mint_info
        .is_mint()?;

    system_info
        .is_writable()?
        .is_system()?;

    epoch_info
        .is_writable()?
        .is_epoch()?;

    // Check programs and sysvars.

    system_program_info
        .is_program(&system_program::ID)?;
    token_program_info
        .is_program(&spl_token::ID)?;
    rent_sysvar_info
        .is_sysvar(&sysvar::rent::ID)?;

    // Start at epoch 1, previous epoch is 0.
    let epoch_number = EpochNumber(1);
    //let prev_epoch_number = EpochNumber(0);

    let system = system_info.as_account_mut::<System>(&tapedrive::ID)?;
    system.total_nodes = 0;
    //system.storage_capacity = StorageUnits(1000); // 1Gb
    //system.storage_price = TAPE::from("0.0001"); // 1 TAPE per 1Mb
    //system.future_usage = FutureUsage::new_at(epoch_number);
    //system.future_rewards = FutureRewards::new_at(epoch_number);

    let epoch = epoch_info.as_account_mut::<Epoch>(&tapedrive::ID)?;
    epoch.id = epoch_number;
    epoch.last_epoch_ms = 0;

    // Create the system_ata token account.
    create_associated_token_account(
        signer_info,
        system_info,
        system_ata_info,
        mint_info,
        system_program_info,
        token_program_info,
        associated_token_program_info,
    )?;

    Ok(())
}


#[cfg(test)]
mod tests {
    use super::*;
    use tape_test::*;

    #[test]
    fn test_initialize() {
        let signer = Pubkey::new_unique();
        let signer_ata = ata_address(&signer);

        let instruction = build_initialize_ix(signer);

        let (system_address, _) = system_pda();
        let (system_ata, _) = system_ata();
        let (epoch_address, _) = epoch_pda();

        let system = System::zeroed();
        let epoch = Epoch::zeroed();

        let accounts = vec![
            sol(signer, 1_000_000_000),
            empty(signer_ata),

            pda(system_address, system.pack(), tapedrive::ID),
            pda(epoch_address, epoch.pack(), tapedrive::ID),
            empty(system_ata),
            mint(MAX_SUPPLY),

            system_program(),
            token_program(),
            ata_program(),
            rent_sysvar(),
        ];

        let env = test_env();
        env.process_instruction(
            &instruction, 
            &accounts,
            &[
                Check::success(),
                Check::account(&system_address).data(
                    System { 
                        total_nodes: 0,
                        //storage_capacity: StorageUnits(1000),
                        //storage_price: TAPE::from("0.0001"),
                        //future_rewards: FutureRewards::new_at(EpochNumber(1)),
                        //future_usage: FutureUsage::new_at(EpochNumber(1)),
                        ..system
                    }.pack().as_ref()
                ).build(),
                Check::account(&epoch_address).data(
                    Epoch {
                        id: EpochNumber(1),
                        ..epoch
                    }.pack().as_ref()
                ).build(),
                Check::account(&system_ata).data(
                    token(system_ata, system_address, 0).1.data.as_ref()
                ).build(),
            ]
        );
    }
}
