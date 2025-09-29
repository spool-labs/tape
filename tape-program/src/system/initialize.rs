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

    system_info
        .is_empty()?
        .is_writable()?
        .has_address(&SYSTEM_ADDRESS)?;

    epoch_info
        .is_empty()?
        .is_writable()?
        .has_address(&EPOCH_ADDRESS)?;

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

    // Check programs and sysvars.
    system_program_info
        .is_program(&system_program::ID)?;
    token_program_info
        .is_program(&spl_token::ID)?;
    metadata_program_info
        .is_program(&mpl_token_metadata::ID)?;
    rent_sysvar_info
        .is_sysvar(&sysvar::rent::ID)?;

    create_program_account::<System>(
        system_info,
        system_program_info,
        signer_info,
        &tape_api::ID,
        &[SYSTEM],
    )?;

    create_program_account::<Epoch>(
        epoch_info,
        system_program_info,
        signer_info,
        &tape_api::ID,
        &[EPOCH],
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

    let system = system_info.as_account_mut::<System>(&tape_api::ID)?;
    system.total_staked = TAPE::zero();
    system.total_nodes = 0;

    let epoch = epoch_info.as_account_mut::<Epoch>(&tape_api::ID)?;
    epoch.id = EpochNumber::zero();
    epoch.last_epoch_at = 0;

    let archive = archive_info.as_account_mut::<Archive>(&tape_api::ID)?;
    archive.storage_capacity = StorageUnits(1000); // 1Gb
    archive.write_price_per_unit = TAPE::from("0.0001"); // 1 TAPE per 1Mb
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
