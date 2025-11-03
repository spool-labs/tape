use steel::*;
use tape_api::prelude::*;

pub fn process_create_archive(accounts: &[AccountInfo<'_>], data: &[u8]) -> ProgramResult {
    let _args = CreateArchive::try_from_bytes(data)?;
    let [
        signer_info, 
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

    // Empty accounts

    let (archive_address, _) = archive_pda();
    let (archive_ata, _) = archive_ata();

    archive_info
        .is_empty()?
        .is_writable()?
        .has_address(&archive_address)?;

    archive_ata_info
        .is_empty()?
        .is_writable()?
        .has_address(&archive_ata)?;

    mint_info
        .is_mint()?;

    // Check programs and sysvars.

    system_program_info
        .is_program(&system_program::ID)?;
    token_program_info
        .is_program(&spl_token::ID)?;
    associated_token_program_info
        .is_program(&spl_associated_token_account::ID)?;
    rent_sysvar_info
        .is_sysvar(&sysvar::rent::ID)?;

    // Create new accounts.

    create_program_account::<Archive>(
        archive_info,
        system_program_info,
        signer_info,
        &tapedrive::ID,
        &[ARCHIVE],
    )?;

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
    fn test_create_archive() {
        let signer = Pubkey::new_unique();

        let instruction = build_create_archive_ix(signer);

        let (archive_address, _) = archive_pda();
        let (archive_ata, _) = archive_ata();

        // Setup existing accounts

        let accounts = vec![
            sol(signer, 1_000_000_000),
            empty(archive_address),
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
                Check::account(&archive_address).data(
                    Archive::zeroed().pack().as_ref()
                ).build(),
                Check::account(&archive_ata).data(
                    token(archive_ata, archive_address, 0).1.data.as_ref()
                ).build(),
            ]
        );
    }
}
