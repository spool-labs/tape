use tape_api::prelude::*;
use steel::*;

pub fn process_reserve_tape(accounts: &[AccountInfo<'_>], data: &[u8]) -> ProgramResult {
    let args = StakeWithNode::try_from_bytes(data)?;
    let [
        signer_info,
        signer_ata_info,
        resource_info,
        epoch_info,
        system_info,
        token_program_info,
        system_program_info, 
        rent_info,
    ] = accounts else {
        return Err(ProgramError::NotEnoughAccountKeys);
    };

    signer_info
        .is_signer()?;

    signer_ata_info
        .is_writable()?
        .as_token_account()?
        .assert(|t| t.owner() == *signer_info.key)?
        .assert(|t| t.mint() == MINT_ADDRESS)?;

    let (resource_address, _)  = resource_pda(*signer_info.key);

    resource_info
        .is_empty()?
        .is_writable()?
        .has_address(&resource_address)?;

    let system = system_info
        .is_writable()?
        .is_tape_system()?
        .as_account_mut::<System>(&tape_api::ID)?;

    let epoch = epoch_info
        .is_tape_epoch()?
        .as_account::<Epoch>(&tape_api::ID)?;

    token_program_info
        .is_program(&spl_token::ID)?;
    system_program_info
        .is_program(&system_program::ID)?;
    rent_info
        .is_sysvar(&sysvar::rent::ID)?;

    // TODO: get the current price from the system

    let amount = u64::from_le_bytes(args.amount);
    if amount == 0 {
        return Err(ProgramError::InvalidArgument);
    }

    create_program_account::<TapeResource>(
        resource_info,
        system_program_info,
        signer_info,
        &tape_api::ID,
        &[RESOURCE, signer_info.key.as_ref()],
    )?;

    let tape = resource_info.as_account_mut::<TapeResource>(&tape_api::ID)?;
    tape.authority       = *signer_info.key;

    // system.total_staked = system.total_staked
    //     .checked_add(stake.amount)
    //     .ok_or(TapeError::Overflow)?;
    //
    // node.pool.total_staked = node.pool.total_staked
    //     .checked_add(stake.amount)
    //     .ok_or(TapeError::Overflow)?;


    transfer(
        signer_info,
        signer_ata_info,
        stake_ata_info,
        token_program_info,
        amount,
    )?;

    // TODO: Emit event, check if the stake puts the pool into the active set for the next epoch.

    Ok(())
}
