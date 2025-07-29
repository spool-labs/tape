use tape_api::prelude::*;
use steel::*;

pub fn process_bin_destroy(accounts: &[AccountInfo<'_>], _data: &[u8]) -> ProgramResult {
    let [
        signer_info, 
        bin_info, 
        system_program_info,
    ] = accounts else {
        return Err(ProgramError::NotEnoughAccountKeys);
    };

    signer_info.is_signer()?;

    system_program_info
        .is_program(&system_program::ID)?;

    bin_info
        .is_writable()?
        .as_account::<Bin>(&tape_api::ID)?
        .assert_err(
            |p| p.authority == *signer_info.key,
            ProgramError::MissingRequiredSignature,
        )?;

    // Return rent to signer.
    bin_info.close(signer_info)?;

    Ok(())
}
