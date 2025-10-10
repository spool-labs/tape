use steel::*;
use tape_api::prelude::*;

pub fn process_create_archive(accounts: &[AccountInfo<'_>], data: &[u8]) -> ProgramResult {
    solana_program::msg!("1");
    let args = CreateArchive::try_from_bytes(data)?;
    let [
        signer_info, 
        signer_ata_info,

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

    signer_info
        .is_signer()?;

    solana_program::msg!("1");
    signer_ata_info
        .is_writable()?
        .as_token_account()?
        .assert(|t| t.owner() == *signer_info.key)?
        .assert(|t| t.mint() == MINT_ADDRESS)?;

    let system = system_info
        .is_writable()?
        .is_system()?
        .as_account_mut::<System>(&tape_api::ID)?;
    solana_program::msg!("1");

    let epoch = epoch_info
        .is_epoch()?
        .as_account::<Epoch>(&tape_api::ID)?;

    // Empty accounts

    let archive_number = ArchiveNumber::unpack(args.id);
    let (archive_address, _) = archive_pda(archive_number);
    let (archive_ata_address, _) = archive_ata(archive_address);

    solana_program::msg!("1");
    archive_info
        .is_empty()?
        .is_writable()?
        .has_address(&archive_address)?;

    archive_ata_info
        .is_empty()?
        .has_address(&archive_ata_address)?;

    solana_program::msg!("1");
    // Check programs and sysvars.

    token_program_info
        .is_program(&spl_token::ID)?;
    system_program_info
        .is_program(&system_program::ID)?;
    rent_sysvar_info
        .is_sysvar(&sysvar::rent::ID)?;

    system.total_archives = system.total_archives + 1;

    if archive_number != system.total_archives.into() {
        return Err(ProgramError::InvalidArgument);
    }

    // Create new accounts.

    solana_program::msg!("1");
    create_program_account::<Archive>(
        archive_info,
        system_program_info,
        signer_info,
        &tape_api::ID,
        &[ARCHIVE, &archive_number.pack()],
    )?;


    let archive = archive_info.as_account_mut::<Archive>(&tape_api::ID)?;
    // Default to 1Tb storage, 0.0001 TAPE per Mb.
    archive.id = archive_number;
    archive.storage_capacity = StorageUnits(1000); // 1Gb
    archive.storage_price = TAPE::from("0.0001"); // 1 TAPE per 1Mb

    // TODO: fast forward epoch to the epoch
    archive.future_storage = StorageAccounting::new();
    archive.future_rewards = RewardAccounting::new();

    // TODO: Calculate cost based on some formula.
    let total_cost = TAPE::from("1.0").as_u64(); // TAPE to create an archive.

    solana_program::msg!("1");
    // Create the archive_ata token account.
    create_associated_token_account(
        signer_info,
        archive_info,
        archive_ata_info,
        mint_info,
        system_program_info,
        token_program_info,
        associated_token_program_info,
    )?;

    solana_program::msg!("1");
    transfer(
        signer_info,
        signer_ata_info,
        archive_ata_info,
        token_program_info,
        total_cost,
    )?;

    Ok(())
}


#[cfg(test)]
mod tests {
    use super::*;
    use tape_test::*;

    #[test]
    fn test_create_archive() {
        let signer = Pubkey::new_unique();
        let signer_ata = ata_address(&signer);

        let archive_number = ArchiveNumber(10);
        let instruction = build_create_archive_ix(signer, archive_number);

        let (system_address, _) = system_pda();
        let (archive_address, _) = archive_pda(archive_number);
        let (archive_ata_address, _) = archive_ata(archive_address);
        let (epoch_address, _) = epoch_pda();
        let (mint_address, _) = mint_pda();

        // Setup existing accounts

        let system = System { 
            total_archives: 9,
            total_nodes: 0,
        };

        let epoch = Epoch { 
            id: EpochNumber(42),
            state: EpochState::zeroed(),
            last_epoch_ms: 0,
            leaders: LeaderSet::zeroed(),
        };

        let initial_token_balance = 1_000_000;

        let accounts = vec![
            sol(signer, 1_000_000_000),
            token(signer_ata, signer, initial_token_balance),

            pda(system_address, system.pack()),
            pda(epoch_address, epoch.pack()),

            empty(archive_address),
            empty(archive_ata_address),
            mint(MAX_SUPPLY),

            system_program(),
            token_program(),
            ata_program(),
            rent_sysvar(),
        ];

        let env = test_env("tape".to_string());
        env.process_instruction(
            &instruction, 
            &accounts,
            &[
                Check::success(),
                Check::account(&system_address).data(
                    System { 
                        total_archives: 10,
                        total_nodes: 0,
                    }.pack().as_ref()
                ).build(),
                Check::account(&epoch_address).data(
                    epoch.pack().as_ref()
                ).build(),
                Check::account(&archive_address).data(
                    Archive { 
                        id: archive_number,
                        storage_capacity: StorageUnits(1000),
                        storage_price: TAPE::from("0.0001"),
                        future_storage: StorageAccounting::new(),
                        future_rewards: RewardAccounting::new(),
                    }.pack().as_ref()
                ).build(),
                Check::account(&signer_ata).data(
                    token(signer_ata, signer, initial_token_balance - TAPE::from("1.0").as_u64()).1.data.as_ref()
                ).build(),
                Check::account(&archive_ata_address).data(
                    token(archive_ata_address, archive_address, TAPE::from("1.0").into()).1.data.as_ref()
                ).build(),
                Check::account(&mint_address).data(
                    mint(MAX_SUPPLY).1.data.as_ref()
                ).build(),
            ]
        );
    }
}
