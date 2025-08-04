use tape_api::prelude::*;
use tape_api::instruction::miner::Register;
use steel::*;

pub fn process_register(accounts: &[AccountInfo<'_>], data: &[u8]) -> ProgramResult {
  
    let args = Register::try_from_bytes(data)?;
    let [
        signer_info,
        miner_info,
        system_program_info, 
        slot_hashes_info,
        clock_info
    ] = accounts else {
        return Err(ProgramError::NotEnoughAccountKeys);
    };

    let current_time = Clock::from_account_info(clock_info)?.unix_timestamp;

    signer_info.is_signer()?;

    let (miner_pda, bump) = miner_find_pda(*signer_info.key, args.name);

    miner_info
        .is_empty()?
        .is_writable()?
        .has_address(&miner_pda)?;

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
    miner.pda_bump          = bump as u64;

    let next_challenge = compute_next_challenge(
        &miner_info.key.to_bytes(),
        slot_hashes_info
    );

    miner.challenge = next_challenge;

    Ok(())
}
