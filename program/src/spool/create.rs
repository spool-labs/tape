use tape_api::prelude::*;
use tape_api::instruction::spool::Create;
use steel::*;

pub fn process_spool_create(accounts: &[AccountInfo<'_>], data: &[u8]) -> ProgramResult {
    let args = Create::try_from_bytes(data)?;
    let [
        signer_info,
        miner_info,
        spool_info,
        system_program_info,
        clock_info
    ] = accounts else {
        return Err(ProgramError::NotEnoughAccountKeys);
    };

    signer_info.is_signer()?;

    let current_time = Clock::from_account_info(clock_info)?.unix_timestamp;

    let spool_number = u64::from_le_bytes(args.number);
    let (spool_pda, spool_bump) = spool_find_pda(miner_info.key, spool_number);

    spool_info
        .is_empty()?
        .has_address(&spool_pda)?;

    miner_info
        .as_account::<Miner>(&tape_api::ID)?
        .assert_err(
            |p| p.authority == *signer_info.key,
            ProgramError::MissingRequiredSignature,
        )?;

    // Create spool account.
    create_program_account_with_bump::<Spool>(
        spool_info,
        system_program_info,
        signer_info,
        &tape_api::ID,
        &[SPOOL, miner_info.key.as_ref(), &args.number],
        spool_bump
    )?;

    let spool = spool_info.as_account_mut::<Spool>(&tape_api::ID)?;

    spool.number            = spool_number;
    spool.authority         = *signer_info.key;
    spool.last_proof_at     = current_time;
    spool.last_proof_block  = 0;
    spool.state             = TapeTree::new(&[spool_info.key.as_ref()]);
    spool.contains          = [0; 32];
    spool.total_tapes       = 0;
    spool.pda_bump          = spool_bump as u64;

    Ok(())
}
