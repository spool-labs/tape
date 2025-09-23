use tape_api::prelude::*;
use steel::*;

pub fn process_stake(accounts: &[AccountInfo<'_>], data: &[u8]) -> ProgramResult {
    let args = Stake::try_from_bytes(data)?;
    let [
        signer_info,
        signer_ata_info,
        stake_info,
        stake_ata_info,
        system_info,
        epoch_info,
        node_info,
        mint_info,
        token_program_info,
        associated_token_program_info,
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

    let (stake_address, _)     = staked_tape_pda(*signer_info.key, *node_info.key);
    let (stake_ata_address, _) = staked_tape_ata(stake_address);

    stake_info
        .is_empty()?
        .is_writable()?
        .has_address(&stake_address)?;

    stake_ata_info
        .is_empty()?
        .is_writable()?
        .has_address(&stake_ata_address)?;

    let system = system_info
        .is_writable()?
        .is_tape_system()?
        .as_account_mut::<System>(&tape_api::ID)?;

    let epoch = epoch_info
        .is_tape_epoch()?
        .as_account::<Epoch>(&tape_api::ID)?;

    let node = node_info
        .is_writable()?
        .as_account_mut::<StorageNode>(&tape_api::ID)?;

    mint_info
        .is_tape_mint()?;

    token_program_info
        .is_program(&spl_token::ID)?;
    associated_token_program_info
        .is_program(&spl_associated_token_account::ID)?;
    system_program_info
        .is_program(&system_program::ID)?;
    rent_info
        .is_sysvar(&sysvar::rent::ID)?;

    let amount = u64::from_le_bytes(args.amount);
    if amount == 0 {
        return Err(ProgramError::InvalidArgument);
    }

    create_program_account::<StakedTape>(
        stake_info,
        system_program_info,
        signer_info,
        &tape_api::ID,
        &[STAKE, signer_info.key.as_ref(), node_info.key.as_ref()],
    )?;

    let stake = stake_info.as_account_mut::<StakedTape>(&tape_api::ID)?;

    stake.authority       = *signer_info.key;
    stake.node            = *node_info.key;
    stake.state           = StakeState::Active.into();
    stake.amount          = TAPE::new(amount);
    stake.activated_epoch = current_epoch(epoch);
    stake.unstake_epoch   = EpochNumber::zero();

    system.total_staked = system.total_staked
        .checked_add(stake.amount)
        .ok_or(TapeError::Overflow)?;

    node.pool.total_staked = node.pool.total_staked
        .checked_add(stake.amount)
        .ok_or(TapeError::Overflow)?;


    create_associated_token_account(
        signer_info,
        stake_info,
        stake_ata_info,
        mint_info,
        system_program_info,
        token_program_info,
        associated_token_program_info,
    )?;

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
