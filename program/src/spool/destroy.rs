use tape_api::prelude::*;
use steel::*;

pub fn process_spool_destroy(accounts: &[AccountInfo<'_>], _data: &[u8]) -> ProgramResult {
    let [
        signer_info, 
        spool_info, 
    ] = accounts else {
        return Err(ProgramError::NotEnoughAccountKeys);
    };

    signer_info.is_signer()?;

    spool_info
        .as_account::<Spool>(&tape_api::ID)?
        .assert_err(
            |p| p.authority == *signer_info.key,
            ProgramError::MissingRequiredSignature,
        )?;

    // Return rent to signer.
    spool_info.close(signer_info)?;

    Ok(())
}
