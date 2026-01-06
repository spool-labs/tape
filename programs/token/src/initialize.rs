use steel::*;
use solana_program::program_pack::Pack;
use spl_token::state::Mint;
use tape_api::prelude::*;

pub fn process_initialize_mint(accounts: &[AccountInfo<'_>], _data: &[u8]) -> ProgramResult {
    let [
        fee_payer_info,
        authority_info,
        authority_ata_info,

        mint_info,
        metadata_info,
        treasury_info,

        system_program_info,
        token_program_info,
        associated_token_program_info,
        metadata_program_info,
        rent_sysvar_info,
    ] = accounts else {
        return Err(ProgramError::NotEnoughAccountKeys);
    };

    fee_payer_info
        .is_signer()?
        .is_writable()?;

    authority_info
        .is_signer()?;

    // Empty accounts

    mint_info
        .is_empty()?
        .is_writable()?
        .has_address(&MINT_ADDRESS)?;

    metadata_info
        .is_empty()?
        .is_writable()?
        .has_address(&METADATA_ADDRESS)?;

    treasury_info
        .is_empty()?
        .is_writable()?
        .has_address(&TREASURY_ADDRESS)?;


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

    create_program_account::<Treasury>(
        treasury_info,
        system_program_info,
        fee_payer_info,
        &tape_api::program::token::ID,
        &[TREASURY],
    )?;

    // Initialize mint.
    allocate_account_with_bump(
        mint_info,
        system_program_info,
        fee_payer_info,
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
        SYSTEM_BUMP,
    )?;

    // Initialize mint metadata.
    mpl_token_metadata::instructions::CreateMetadataAccountV3Cpi {
        __program: metadata_program_info,
        metadata: metadata_info,
        mint: mint_info,
        mint_authority: treasury_info,
        payer: fee_payer_info,
        update_authority: (authority_info, true),
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

    // Create authority_ata token account.
    create_associated_token_account(
        fee_payer_info,
        authority_info,
        authority_ata_info,
        mint_info,
        system_program_info,
        token_program_info,
        associated_token_program_info,
    )?;

    // Mint max supply to authority_ata.
    mint_to_signed(
        mint_info,
        authority_ata_info,
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
        let fee_payer = Pubkey::new_unique();
        let authority = Pubkey::new_unique();
        let authority_ata = ata_address(&authority);

        let instruction = build_initialize_mint_ix(fee_payer, authority);

        let (treasury_address, _) = treasury_pda();
        let (mint_address, _) = mint_pda();
        let (metadata_address, _) = metadata_pda();

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            sol(authority, 0),
            empty(authority_ata),

            empty(mint_address),
            empty(metadata_address),
            empty(treasury_address),

            system_program(),
            token_program(),
            ata_program(),
            mpl_program(),
            rent_sysvar(),
        ];

        let env = test_env();
        env.process_instruction(
            &instruction,
            &accounts,
            &[
                Check::success(),
                Check::account(&treasury_address).data(
                    Treasury {
                    }.pack().as_ref()
                ).build(),
                Check::account(&authority_ata).data(
                    token(authority_ata, authority, MAX_SUPPLY).1.data.as_ref()
                ).build(),
                Check::account(&mint_address).data(
                    mint(MAX_SUPPLY).1.data.as_ref()
                ).build(),
            ]
        );
    }
}
