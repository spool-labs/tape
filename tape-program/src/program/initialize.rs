use steel::*;
use solana_program::program_pack::Pack;
use spl_token::state::Mint;
use tape_api::prelude::*;

pub fn process_initialize(accounts: &[AccountInfo<'_>], _data: &[u8]) -> ProgramResult {
    solana_program::msg!("num accounts: {}", accounts.len());
    let [
        signer_info, 
        system_info,
        archive_info, 
        epoch_info, 
        metadata_info, 
        mint_info, 
        treasury_info, 
        treasury_ata_info, 
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
        .has_seeds(&[SYSTEM], &tape_api::ID)?;

    archive_info
        .is_empty()?
        .is_writable()?
        .has_seeds(&[ARCHIVE], &tape_api::ID)?;

    epoch_info
        .is_empty()?
        .is_writable()?
        .has_seeds(&[EPOCH], &tape_api::ID)?;

    // Check mint, metadata, treasury
    let (mint_address, mint_bump) = mint_pda();
    let (treasury_address, treasury_bump) = treasury_pda();
    let (metadata_address, _metadata_bump) = metadata_pda(mint_address);

    assert_eq!(mint_bump, MINT_BUMP);
    assert_eq!(treasury_bump, TREASURY_BUMP);

    mint_info
        .is_empty()?
        .is_writable()?
        .has_address(&mint_address)?;

    metadata_info
        .is_empty()?
        .is_writable()?
        .has_address(&metadata_address)?;

    treasury_info
        .is_empty()?
        .is_writable()?
        .has_address(&treasury_address)?;

    treasury_ata_info
        .is_empty()?
        .is_writable()?;

    // Check programs and sysvars.
    system_program_info
        .is_program(&system_program::ID)?;
    token_program_info
        .is_program(&spl_token::ID)?;
    associated_token_program_info
        .is_program(&spl_associated_token_account::ID)?;

    metadata_program_info
        .is_program(&mpl_token_metadata::ID)?;
    rent_sysvar_info
        .is_sysvar(&sysvar::rent::ID)?;

    // Initialize system.
    create_program_account::<System>(
        system_info,
        system_program_info,
        signer_info,
        &tape_api::ID,
        &[SYSTEM],
    )?;

    let system = system_info.as_account_mut::<System>(&tape_api::ID)?;
    system.total_nodes = 0;

    // Initialize epoch.
    create_program_account::<Epoch>(
        epoch_info,
        system_program_info,
        signer_info,
        &tape_api::ID,
        &[EPOCH],
    )?;

    let epoch = epoch_info.as_account_mut::<Epoch>(&tape_api::ID)?;

    epoch.id            = EpochNumber::zero();
    epoch.last_epoch_at = 0;

    // Initialize archive.
    create_program_account::<Archive>(
        archive_info,
        system_program_info,
        signer_info,
        &tape_api::ID,
        &[ARCHIVE],
    )?;

    let _archive = archive_info.as_account_mut::<Archive>(&tape_api::ID)?;

    // Initialize treasury.
    create_program_account::<Treasury>(
        treasury_info,
        system_program_info,
        signer_info,
        &tape_api::ID,
        &[TREASURY],
    )?;

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
    initialize_mint_signed_with_bump(
        mint_info,
        treasury_info,
        None,
        token_program_info,
        rent_sysvar_info,
        TOKEN_DECIMALS,
        &[MINT, MINT_SEED],
        MINT_BUMP,
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

    // Initialize treasury token account.
    create_associated_token_account(
        signer_info,
        treasury_info,
        treasury_ata_info,
        mint_info,
        system_program_info,
        token_program_info,
        associated_token_program_info,
    )?;

    // Fund the treasury token account.
    mint_to_signed(
        mint_info,
        treasury_ata_info,
        treasury_info,
        token_program_info,
        MAX_SUPPLY,
        &[TREASURY],
    )?;

    Ok(())
}
