use steel::*;
use solana_program::program_pack::Pack;
use spl_token::state::Mint;
use tape_api::prelude::*;

pub fn process_initialize(accounts: &[AccountInfo<'_>], _data: &[u8]) -> ProgramResult {
    let [
        signer_info, 
        signer_ata_info,

        system_info,
        epoch_info, 
        committee_info,
        previous_committee_info,
        archive_info, 
        treasury_info,
        treasury_ata_info,
        mint_info, 
        metadata_info, 

        system_program_info, 
        token_program_info, 
        associated_token_program_info,
        metadata_program_info, 
        rent_sysvar_info,
    ] = accounts else {
        return Err(ProgramError::NotEnoughAccountKeys);
    };

    // Empty accounts

    system_info
        .is_empty()?
        .is_writable()?
        .has_address(&SYSTEM_ADDRESS)?;

    archive_info
        .is_empty()?
        .is_writable()?
        .has_address(&ARCHIVE_ADDRESS)?;

    treasury_info
        .is_empty()?
        .is_writable()?
        .has_address(&TREASURY_ADDRESS)?;

    mint_info
        .is_empty()?
        .is_writable()?
        .has_address(&MINT_ADDRESS)?;

    metadata_info
        .is_empty()?
        .is_writable()?
        .has_address(&METADATA_ADDRESS)?;

    // Existing accounts

    epoch_info
        .is_writable()?
        .is_epoch()?;

    committee_info
        .is_writable()?
        .is_current_committee()?;

    previous_committee_info
        .is_writable()?
        .is_previous_committee()?;

    // Check programs and sysvars.

    system_program_info
        .is_program(&system_program::ID)?;
    token_program_info
        .is_program(&spl_token::ID)?;
    metadata_program_info
        .is_program(&mpl_token_metadata::ID)?;
    rent_sysvar_info
        .is_sysvar(&sysvar::rent::ID)?;

    // Create new accounts.

    create_program_account::<System>(
        system_info,
        system_program_info,
        signer_info,
        &tape_api::ID,
        &[SYSTEM],
    )?;

    create_program_account::<Archive>(
        archive_info,
        system_program_info,
        signer_info,
        &tape_api::ID,
        &[ARCHIVE],
    )?;

    create_program_account::<Treasury>(
        treasury_info,
        system_program_info,
        signer_info,
        &tape_api::ID,
        &[TREASURY],
    )?;

    // Start at epoch 1, previous epoch is 0.
    let epoch_number = EpochNumber(1);
    let prev_epoch_number = EpochNumber(0);

    let system = system_info.as_account_mut::<System>(&tape_api::ID)?;
    system.total_nodes = 0;

    let epoch = epoch_info.as_account_mut::<Epoch>(&tape_api::ID)?;
    epoch.id = epoch_number;
    epoch.last_epoch_ms = 0;

    let committee = committee_info.as_account_mut::<Committee>(&tape_api::ID)?;
    committee.epoch = epoch_number;

    let prev_committee = previous_committee_info.as_account_mut::<Committee>(&tape_api::ID)?;
    prev_committee.epoch = prev_epoch_number;

    // Default to 1Tb storage, 0.0001 TAPE per Mb.

    let archive = archive_info.as_account_mut::<Archive>(&tape_api::ID)?;
    archive.storage_capacity = StorageUnits(1000); // 1Gb
    archive.storage_price_per_unit = TAPE::from("0.0001"); // 1 TAPE per 1Mb
    archive.future_usage = StorageAccounting::new();

    let treasury = treasury_info.as_account_mut::<Treasury>(&tape_api::ID)?;
    treasury.future_rewards = RewardAccounting::new();


    // Initialize mint.
    allocate_account_with_bump(
        mint_info,
        system_program_info,
        signer_info,
        Mint::LEN,
        &spl_token::ID,
        &[MINT, MINT_SEED],
        MINT_BUMP,
    )?;

    // Set mint authority
    initialize_mint_signed_with_bump(
        mint_info, 
        treasury_info,
        None,
        token_program_info,
        rent_sysvar_info,
        TOKEN_DECIMALS,
        &[TREASURY],
        TREASURY_BUMP,
    )?;

    // Initialize mint metadata.
    mpl_token_metadata::instructions::CreateMetadataAccountV3Cpi {
        __program: metadata_program_info,
        metadata: metadata_info,
        mint: mint_info,
        mint_authority: treasury_info,
        payer: signer_info,
        update_authority: (signer_info, true),
        system_program: system_program_info,
        rent: Some(rent_sysvar_info),
        __args: mpl_token_metadata::instructions::CreateMetadataAccountV3InstructionArgs {
            data: mpl_token_metadata::types::DataV2 {
                name: METADATA_NAME.to_string(),
                symbol: METADATA_SYMBOL.to_string(),
                uri: METADATA_URI.to_string(),
                seller_fee_basis_points: 0,
                creators: None,
                collection: None,
                uses: None,
            },
            is_mutable: true,
            collection_details: None,
        },
    }
    .invoke_signed(&[&[TREASURY, &[TREASURY_BUMP]]])?;

    // Create the treasury_ata token account.
    create_associated_token_account(
        signer_info,
        treasury_info,
        treasury_ata_info,
        mint_info,
        system_program_info,
        token_program_info,
        associated_token_program_info,
    )?;

    // Create signer_ata token account.
    create_associated_token_account(
        signer_info,
        signer_info,
        signer_ata_info,
        mint_info,
        system_program_info,
        token_program_info,
        associated_token_program_info,
    )?;

    // Mint max supply to signer_ata.
    mint_to_signed(
        mint_info,
        signer_ata_info,
        treasury_info,
        token_program_info,
        MAX_SUPPLY,
        &[TREASURY],
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
        let (treasury_address, _) = treasury_pda();
        let (treasury_ata_address, _) = treasury_ata();
        let (archive_address, _) = archive_pda();
        let (epoch_address, _) = epoch_pda();
        let (mint_address, _) = mint_pda();
        let (metadata_address, _) = metadata_pda();
        let (committee_address, _) = current_committee_pda();
        let (prev_committee_address, _) = previous_committee_pda();

        // Setup existing accounts
        // (assuming created and expanded)

        let epoch_data = Epoch { 
            id: EpochNumber::zero(),
            state: EpochState::zeroed(),
            last_epoch_ms: 0,
            leaders: LeaderSet::zeroed(),
        }.pack();

        let committee_data = Committee { 
            id: CommitteeNumber::current(),
            epoch: EpochNumber::zero(),
            inner: AppointedSet::zeroed(),
        }.pack();

        let prev_committee_data = Committee { 
            id: CommitteeNumber::previous(),
            epoch: EpochNumber::zero(),
            inner: AppointedSet::zeroed(),
        }.pack();

        let accounts = vec![
            sol(signer, 1_000_000_000),
            empty(signer_ata),

            empty(system_address),
            pda(epoch_address, epoch_data),
            pda(committee_address, committee_data),
            pda(prev_committee_address, prev_committee_data),
            empty(archive_address),
            empty(treasury_address),
            empty(treasury_ata_address),
            empty(mint_address),
            empty(metadata_address),

            system_program(),
            token_program(),
            ata_program(),
            mpl_program(),
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
                        total_nodes: 0,
                    }.pack().as_ref()
                ).build(),
                Check::account(&epoch_address).data(
                    Epoch { 
                        id: EpochNumber(1),
                        state: EpochState::zeroed(),
                        last_epoch_ms: 0,
                        leaders: LeaderSet::zeroed(),
                    }.pack().as_ref()
                ).build(),
                Check::account(&committee_address).data(
                    Committee { 
                        id: CommitteeNumber::current(),
                        epoch: EpochNumber(1),
                        inner: AppointedSet::zeroed(),
                    }.pack().as_ref()
                ).build(),
                Check::account(&prev_committee_address).data(
                    Committee { 
                        id: CommitteeNumber::previous(),
                        epoch: EpochNumber(0),
                        inner: AppointedSet::zeroed(),
                    }.pack().as_ref()
                ).build(),
                Check::account(&archive_address).data(
                    Archive { 
                        storage_capacity: StorageUnits(1000),
                        storage_price_per_unit: TAPE::from("0.0001"),
                        future_usage: StorageAccounting::new(),
                    }.pack().as_ref()
                ).build(),
                Check::account(&treasury_address).data(
                    Treasury { 
                        future_rewards: RewardAccounting::new(),
                    }.pack().as_ref()
                ).build(),
                Check::account(&signer_ata).data(
                    token(signer_ata, signer, MAX_SUPPLY).1.data.as_ref()
                ).build(),
                Check::account(&treasury_ata_address).data(
                    token(treasury_ata_address, treasury_address, 0).1.data.as_ref()
                ).build(),
                Check::account(&mint_address).data(
                    mint(MAX_SUPPLY).1.data.as_ref()
                ).build(),
            ]
        );
    }
}
