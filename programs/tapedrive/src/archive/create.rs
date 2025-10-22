use steel::*;
use tape_api::prelude::*;

pub fn process_create_archive(accounts: &[AccountInfo<'_>], data: &[u8]) -> ProgramResult {
    let _args = CreateArchive::try_from_bytes(data)?;
    let [
        signer_info, 
        archive_info, 
        system_program_info, 
        rent_sysvar_info,
    ] = accounts else {
        return Err(ProgramError::NotEnoughAccountKeys);
    };

    signer_info
        .is_signer()?;

    // Empty accounts

    let (archive_address, _) = archive_pda();
    archive_info
        .is_empty()?
        .is_writable()?
        .has_address(&archive_address)?;

    // Check programs and sysvars.

    system_program_info
        .is_program(&system_program::ID)?;
    rent_sysvar_info
        .is_sysvar(&sysvar::rent::ID)?;

    // Create new accounts.

    create_program_account::<Archive>(
        archive_info,
        system_program_info,
        signer_info,
        &tape_api::ID,
        &[ARCHIVE],
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

        // Setup existing accounts

        let accounts = vec![
            sol(signer, 1_000_000_000),
            empty(archive_address),
            system_program(),
            rent_sysvar(),
        ];

        let env = test_env("tape".to_string());
        env.process_instruction(
            &instruction, 
            &accounts,
            &[
                Check::success(),
                Check::account(&archive_address).data(
                    Archive::zeroed().pack().as_ref()
                ).build(),
            ]
        );
    }
}
