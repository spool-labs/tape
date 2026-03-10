use tape_solana::*;
use tape_api::prelude::*;

pub fn process_initialize(accounts: &[AccountInfo<'_>], data: &[u8]) -> ProgramResult {
    let _args = Initialize::try_from_bytes(data)?;
    let [
        fee_payer_info,
        authority_info,

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

    fee_payer_info
        .is_signer()?
        .is_writable()?;
    authority_info
        .is_signer()?;

    system_program_info
        .is_program(&system_program::ID)?;
    token_program_info
        .is_program(&spl_token::ID)?;
    associated_token_program_info
        .is_program(&spl_associated_token_account::ID)?;
    rent_sysvar_info
        .is_sysvar(&sysvar::rent::ID)?;

    // System must already exist and be writable
    system_info
        .is_writable()?
        .is_system()?;

    // Expect Epoch/Archive PDAs and their ATA addresses
    let (epoch_address, _) = epoch_pda();
    let (archive_address, _) = archive_pda();
    let (archive_ata_address, _) = archive_ata();

    epoch_info
        .is_empty()?
        .is_writable()?
        .has_address(&epoch_address)?;

    archive_info
        .is_empty()?
        .is_writable()?
        .has_address(&archive_address)?;

    archive_ata_info
        .is_empty()?
        .is_writable()?
        .has_address(&archive_ata_address)?;

    // Mint must be the program's TAPE mint
    mint_info
        .is_mint()?;

    // Create Epoch and Archive PDAs
    create_program_account::<Epoch>(
        epoch_info,
        system_program_info,
        fee_payer_info,
        &tapedrive::ID,
        &[EPOCH],
    )?;

    create_program_account::<Archive>(
        archive_info,
        system_program_info,
        fee_payer_info,
        &tapedrive::ID,
        &[ARCHIVE],
    )?;

    // Create Archive ATA
    create_associated_token_account(
        fee_payer_info,
        archive_info,
        archive_ata_info,
        mint_info,
        system_program_info,
        token_program_info,
        associated_token_program_info,
    )?;

    // Initialize state
    let system = system_info.as_account_mut::<System>(&tapedrive::ID)?;
    system.total_nodes = 0;

    let epoch = epoch_info.as_account_mut::<Epoch>(&tapedrive::ID)?;
    epoch.id = EpochNumber(1);
    epoch.state = EpochState::active();  // Enable low-quorum mode from the start
    epoch.last_epoch = 0;

    let archive = archive_info.as_account_mut::<Archive>(&tapedrive::ID)?;
    archive.storage_capacity = StorageUnits::mb(1000); // 1Gb
    archive.storage_price = TAPE::from("0.0001");  // 1 TAPE per 1Mb
    archive.schedule = EpochSchedule::new_at(epoch.id);

    Ok(())
}



#[cfg(test)]
mod tests {
    use super::*;
    use tape_crypto::Hash;
    use tape_test::*;

    #[test]
    fn test_initialize() {
        let fee_payer = Pubkey::new_unique();
        let authority = Pubkey::new_unique();

        // PDAs
        let (system_address, _) = system_pda();
        let (epoch_address, _) = epoch_pda();
        let (archive_address, _) = archive_pda();
        let (archive_ata, _) = archive_ata();

        let system = System::zeroed();

        let instruction = build_initialize_ix(fee_payer, authority);

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            sol(authority, 0),

            // existing system account
            pda(system_address, system.pack(), tapedrive::ID),

            // to be created
            empty(epoch_address),
            empty(archive_address),
            empty(archive_ata),

            // mint and programs
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

                // System initialized
                Check::account(&system_address).data(
                    System {
                        total_nodes: 0,
                        ..system
                    }.pack().as_ref()
                ).build(),

                // Epoch created + initialized
                Check::account(&epoch_address).data(
                    Epoch {
                        id: EpochNumber(1),
                        state: EpochState::active(),
                        last_epoch: 0,
                        nonce: Hash::default(),
                    }.pack().as_ref()
                ).build(),

                // Archive created + initialized
                Check::account(&archive_address).data(
                    Archive {
                        storage_capacity: StorageUnits::mb(1000),
                        storage_price: TAPE::from("0.0001"),
                        schedule: EpochSchedule::new_at(EpochNumber(1)),
                        ..Archive::zeroed()
                    }.pack().as_ref()
                ).build(),

                // Archive ATA created
                Check::account(&archive_ata).data(
                    token(archive_ata, archive_address, 0).1.data.as_ref()
                ).build(),
            ],
        );
    }
}
