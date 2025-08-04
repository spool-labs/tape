use tape_api::prelude::*;
use steel::*;

pub fn process_unregister(accounts: &[AccountInfo<'_>]) -> ProgramResult {
    let [
        signer_info, 
        miner_info, 
    ] = accounts else {
        return Err(ProgramError::NotEnoughAccountKeys);
    };

    signer_info.is_signer()?;

    miner_info
        .is_writable()?
        .as_account::<Miner>(&tape_api::ID)?
        .assert_err(
            |p| p.authority == *signer_info.key,
            ProgramError::MissingRequiredSignature,
        )?
        .assert(|p| p.unclaimed_rewards == 0)?;

    // Return rent to signer.
    miner_info.close(signer_info)?;

    Ok(())
}
