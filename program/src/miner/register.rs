use tape_api::prelude::*;
use steel::*;

pub fn process_register(accounts: &[AccountInfo<'_>], data: &[u8]) -> ProgramResult {
    let current_time = Clock::get()?.unix_timestamp;
    let args = Register::try_from_bytes(data)?;
    let [
        signer_info,
        miner_info,
        system_program_info, 
        rent_info,
        slot_hashes_info
    ] = accounts else {
        return Err(ProgramError::NotEnoughAccountKeys);
    };

    signer_info.is_signer()?;

    let (miner_pda, _bump) = miner_pda(*signer_info.key, args.name);

    miner_info
        .is_empty()?
        .is_writable()?
        .has_address(&miner_pda)?;

    system_program_info.is_program(&system_program::ID)?;
    rent_info.is_sysvar(&sysvar::rent::ID)?;
    slot_hashes_info.is_sysvar(&sysvar::slot_hashes::ID)?;

    // Register miner.
    create_program_account::<Miner>(
        miner_info,
        system_program_info,
        signer_info,
        &tape_api::ID,
        &[MINER, signer_info.key.as_ref(), args.name.as_ref()],
    )?;

    let miner = miner_info.as_account_mut::<Miner>(&tape_api::ID)?;

    miner.authority         = *signer_info.key;
    miner.name              = args.name;

    miner.multiplier        = 0;
    miner.last_proof_at     = current_time;
    miner.total_proofs      = 0;
    miner.total_rewards     = 0;
    miner.unclaimed_rewards = 0;

    let next_challenge = compute_next_challenge(
        &miner_info.key.to_bytes(),
        slot_hashes_info
    );

    miner.challenge = next_challenge;

    Ok(())
}
