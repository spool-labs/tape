use steel::*;
use tape_api::prelude::*;

pub fn process_initialize(accounts: &[AccountInfo<'_>], _data: &[u8]) -> ProgramResult {
    let [
        signer_info, 

        system_info,
        epoch_info, 
        archive_info,
        archive_ata_info,
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

    archive_info
        .is_writable()?
        .is_archive()?;

    // Check programs and sysvars.

    system_program_info
        .is_program(&system_program::ID)?;
    token_program_info
        .is_program(&spl_token::ID)?;
    rent_sysvar_info
        .is_sysvar(&sysvar::rent::ID)?;

    let system = system_info.as_account_mut::<System>(&tapedrive::ID)?;
    system.total_nodes = 0;

    let epoch = epoch_info.as_account_mut::<Epoch>(&tapedrive::ID)?;
    epoch.id = EpochNumber(1);
    epoch.last_epoch_ms = 0;

    let archive = archive_info.as_account_mut::<Archive>(&tapedrive::ID)?;
    archive.storage_capacity = StorageUnits(1000); // 1Gb
    archive.storage_price = TAPE::from("0.0001");  // 1 TAPE per 1Mb
    archive.capacity_used = FutureUsage::new_at(epoch.id);
    archive.fees_collected = FutureRewards::new_at(epoch.id);

    // Create the system_ata token account.
    create_associated_token_account(
        signer_info,
        archive_info,
        archive_ata_info,
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
        let (epoch_address, _) = epoch_pda();
        let (archive_address, _) = archive_pda();
        let (archive_ata, _) = archive_ata();

        let system = System::zeroed();
        let epoch = Epoch::zeroed();
        let archive = Archive::zeroed();

        let accounts = vec![
            sol(signer, 1_000_000_000),
            empty(signer_ata),

            pda(system_address, system.pack(), tapedrive::ID),
            pda(epoch_address, epoch.pack(), tapedrive::ID),
            pda(archive_address, archive.pack(), tapedrive::ID),
            empty(archive_ata),
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
                        ..system
                    }.pack().as_ref()
                ).build(),
                Check::account(&epoch_address).data(
                    Epoch {
                        id: EpochNumber(1),
                        ..epoch
                    }.pack().as_ref()
                ).build(),
                Check::account(&archive_address).data(
                    Archive {
                        storage_capacity: StorageUnits(1000),
                        storage_price: TAPE::from("0.0001"),
                        fees_collected: FutureRewards::new_at(EpochNumber(1)),
                        capacity_used: FutureUsage::new_at(EpochNumber(1)),
                        ..archive
                    }.pack().as_ref()
                ).build(),
                Check::account(&archive_ata).data(
                    token(archive_ata, archive_address, 0).1.data.as_ref()
                ).build(),
            ]
        );
    }
}
