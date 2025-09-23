use tape_api::prelude::*;
use steel::*;

//pub fn process_stake(accounts: &[AccountInfo<'_>], data: &[u8]) -> ProgramResult {
//    let args = Stake::try_from_bytes(data)?;
//    let [
//        signer_info,
//        signer_ata_info,
//
//        system_info,
//        epoch_info,
//        node_info,
//        stake_info,
//
//        treasury_info,
//        treasury_ata_info,
//        token_program_info,
//        system_program_info, 
//        rent_info,
//    ] = accounts else {
//        return Err(ProgramError::NotEnoughAccountKeys);
//    };
//
//    signer_info.is_signer()?;
//
//    let (stake_address, _) = staked_tape_pda(*signer_info.key, *node_info.key);
//    stake_info
//        .is_empty()?
//        .is_writable()?
//        .has_address(&stake_address)?;
//
//    let epoch = epoch_info
//        .is_epoch()?
//        .as_account::<Epoch>(&tape_api::ID)?;
//
//    let _system = system_info
//        .is_system()?
//        .as_account_mut::<System>(&tape_api::ID)?;
//
//    let node = node_info
//        .is_writable()?
//        .as_account_mut::<StorageNode>(&tape_api::ID)?;
//
//    let treasury = treasury_info
//        .is_treasury()?
//        .is_writable()?
//        .as_account_mut::<Treasury>(&tape_api::ID)?;
//
//    treasury_ata_info
//        .is_writable()?
//        .is_treasury_ata()?;
//
//    token_program_info
//        .is_program(&spl_token::ID)?;
//
//    signer_ata_info
//        .is_writable()?
//        .as_token_account()?
//        .assert(|t| t.owner() == *signer_info.key)?
//        .assert(|t| t.mint() == MINT_ADDRESS)?;
//
//    system_program_info.is_program(&system_program::ID)?;
//    rent_info.is_sysvar(&sysvar::rent::ID)?;
//
//    let amount = u64::from_le_bytes(args.amount);
//    if amount == 0 {
//        return Err(ProgramError::InvalidArgument);
//    }
//
//    transfer(
//        signer_info,
//        signer_ata_info,
//        treasury_ata_info,
//        token_program_info,
//        amount,
//    )?;
//
//    create_program_account::<StakedTape>(
//        stake_info,
//        system_program_info,
//        signer_info,
//        &tape_api::ID,
//        &[STAKE, signer_info.key.as_ref(), node_info.key.as_ref()],
//    )?;
//
//    let stake = stake_info.as_account_mut::<StakedTape>(&tape_api::ID)?;
//
//    stake.authority            = *signer_info.key;
//    stake.node                 = *node_info.key;
//    stake.state                = StakeState::Active.into();
//    stake.amount               = TAPE::new(amount);
//    stake.activated_epoch      = current_epoch(epoch);
//    stake.unstake_epoch        = EpochNumber::zero();
//
//    treasury.total_stake = treasury
//        .total_stake
//        .checked_add(stake.amount)
//        .ok_or(TapeError::UnexpectedState)?;
//
//    node.pool.total_stake.increment();
//
//    // TODO: Emit event, check if the stake puts the pool into the active set for the next epoch.
//
//    Ok(())
//}
