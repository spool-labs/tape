use steel::*;
use solana_program::{program_pack::Pack, program::{invoke, invoke_signed}};
use spl_token::state::Mint;
use tape_api::prelude::*;
use tape_api::instruction::tape::{
    build_create_ix, 
    build_write_ix, 
    build_subsidize_ix, 
    build_finalize_ix,
};
use crate::mine::get_base_rate;

pub fn process_initialize(accounts: &[AccountInfo<'_>], _data: &[u8]) -> ProgramResult {
    solana_program::msg!("num accounts: {}", accounts.len());
    let [
        signer_info, 
        archive_info, 
        epoch_info, 
        block_info,
        metadata_info, 
        mint_info, 
        treasury_info, 
        treasury_ata_info, 
        tape_info,
        writer_info,
        _tape_program_info,
        system_program_info, 
        token_program_info, 
        associated_token_program_info, 
        metadata_program_info, 
        slot_hashes_info,
        rent_sysvar_info,
        clock_info
    ] = accounts else {
        return Err(ProgramError::NotEnoughAccountKeys);
    };

    let (archive_address, archive_bump) = archive_pda();
    let (epcoh_address, epoch_bump) = epoch_pda();
    let (block_address, block_bump) = block_pda();
    let (mint_address, mint_bump) = mint_pda();
    let (treasury_address, treasury_bump) = treasury_pda();
    let (metadata_address, _metadata_bump) = metadata_find_pda(mint_address);

    archive_info
        .is_empty()?
        .has_address(archive_address)?;

    epoch_info
        .is_empty()?
        .has_address(epcoh_address)?;

    block_info
        .is_empty()?
        .has_address(block_address)?;

    mint_info
        .is_empty()?
        .has_address(mint_address)?;

    metadata_info
        .is_empty()?
        .has_address(&metadata_address)?;

    treasury_info
        .is_empty()?
        .has_address(treasury_address)?;

    treasury_ata_info
        .is_empty()?;

    // Initialize epoch.
    create_program_account_with_bump::<Epoch>(
        epoch_info,
        system_program_info,
        signer_info,
        &tape_api::ID,
        &[EPOCH],
        epoch_bump
    )?;

    let epoch = epoch_info.as_account_mut::<Epoch>(&tape_api::ID)?;

    epoch.number               = 1;
    epoch.progress             = 0;
    epoch.target_participation = MIN_PARTICIPATION_TARGET;
    epoch.mining_difficulty    = MIN_MINING_DIFFICULTY;
    epoch.packing_difficulty   = MIN_PACKING_DIFFICULTY;
    epoch.reward_rate          = get_base_rate(1);
    epoch.duplicates           = 0;
    epoch.last_epoch_at        = 0;

    // Initialize block.
    create_program_account_with_bump::<Block>(
        block_info,
        system_program_info,
        signer_info,
        &tape_api::ID,
        &[BLOCK],
        block_bump
    )?;

    let block = block_info.as_account_mut::<Block>(&tape_api::ID)?;

    block.number            = 1;
    block.progress          = 0;
    block.last_proof_at     = 0;
    block.last_block_at     = 0;

    let next_challenge = compute_next_challenge(
        &block_address.to_bytes(),
        slot_hashes_info
    );

    block.challenge = next_challenge;
    block.challenge_set = 1;

    // Initialize archive.
    create_program_account_with_bump::<Archive>(
        archive_info,
        system_program_info,
        signer_info,
        &tape_api::ID,
        &[ARCHIVE],
        archive_bump
    )?;

    let archive = archive_info.as_account_mut::<Archive>(&tape_api::ID)?;

    archive.tapes_stored      = 0;
    archive.segments_stored   = 0;

    // Initialize treasury.
    create_program_account_with_bump::<Treasury>(
        treasury_info,
        system_program_info,
        signer_info,
        &tape_api::ID,
        &[TREASURY],
        treasury_bump
    )?;

    // Initialize mint.
    allocate_account_with_bump(
        mint_info,
        system_program_info,
        signer_info,
        Mint::LEN,
        &spl_token::ID,
        &[MINT, MINT_SEED],
        mint_bump
    )?;

    initialize_mint_signed_with_bump(
        mint_info,
        treasury_info,
        None,
        token_program_info,
        rent_sysvar_info,
        TOKEN_DECIMALS,
        &[MINT, MINT_SEED],
        mint_bump
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
    mint_to_signed_with_bump(
        mint_info,
        treasury_ata_info,
        treasury_info,
        token_program_info,
        MAX_SUPPLY,
        &[TREASURY],
        treasury_bump
    )?;

    // Create the genesis tape

    let name = "genesis";
    let (tape_address, _tape_bump) = tape_find_pda(signer_info.key, &to_name(name));
    let (writer_address, _writer_bump) = writer_find_pda(&tape_address);

    // Create the tape
    invoke(
        &build_create_ix(
            *signer_info.key,
            name,
        ),
        &[
            signer_info.clone(),
            tape_info.clone(),
            writer_info.clone(),
            system_program_info.clone(),
            clock_info.clone(),
        ],
    )?;

    // Write "hello, world" to the tape
    invoke(
        &build_write_ix(
            *signer_info.key,
            tape_address,
            writer_address,
            b"hello, world",
        ),
        &[
            signer_info.clone(),
            tape_info.clone(),
            writer_info.clone(),
            clock_info.clone()
        ],
    )?;

    // Subsidize the tape for 1 block
    invoke_signed(
        &build_subsidize_ix(
            *treasury_info.key,
            *treasury_ata_info.key,
            tape_address,
            min_finalization_rent(1),
        ),
        &[
            treasury_info.clone(),
            tape_info.clone(),
            treasury_ata_info.clone(),
        ],
        &[&[TREASURY, &[TREASURY_BUMP]]]
    )?;

    // Finalize the tape
    invoke(
        &build_finalize_ix(
            *signer_info.key,
            tape_address,
            writer_address,
        ),
        &[
            signer_info.clone(),
            tape_info.clone(),
            writer_info.clone(),
            archive_info.clone(),
        ],
    )?;

    Ok(())
}
