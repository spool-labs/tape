use tape_api::prelude::*;
use tape_api::instruction::pool::Register;
use steel::*;

pub fn process_register(accounts: &[AccountInfo<'_>], data: &[u8]) -> ProgramResult {
    solana_program::msg!("1");

    let args = Register::try_from_bytes(data)?;
    let [
        signer_info,
        system_info,
        epoch_info,
        pool_info,
        system_program_info, 
        rent_info,
    ] = accounts else {
        return Err(ProgramError::NotEnoughAccountKeys);
    };

    solana_program::msg!("1");
    signer_info.is_signer()?;

    let (pool_address, _bump) = pool_pda(*signer_info.key);
    pool_info
        .is_empty()?
        .is_writable()?
        .has_address(&pool_address)?;

    let epoch = epoch_info
        .is_epoch()?
        .as_account::<Epoch>(&tape_api::ID)?;
    solana_program::msg!("1");

    let system = system_info
        .is_system()?
        .is_writable()?
        .as_account_mut::<System>(&tape_api::ID)?;
    solana_program::msg!("1");

    system_program_info.is_program(&system_program::ID)?;
    rent_info.is_sysvar(&sysvar::rent::ID)?;
    solana_program::msg!("1");

    create_program_account::<Pool>(
        pool_info,
        system_program_info,
        signer_info,
        &tape_api::ID,
        &[POOL, signer_info.key.as_ref()],
    )?;
    solana_program::msg!("1");

    let pool = pool_info.as_account_mut::<Pool>(&tape_api::ID)?;

    pool.id                   = PoolNumber::new(system.total_pools);
    pool.authority            = *signer_info.key;
    pool.total_stake          = TAPE::zero();
    pool.commission_rate      = BasisPoints::unpack(args.commission_rate);
    pool.registered_epoch     = current_epoch(epoch);

    pool.storage_node = StorageNode {
        name: args.name,
        storage_capacity: 0,
        storage_used: 0,
        network_address: args.network_address,
        network_tls: args.network_tls,
    };

    system.total_pools = system
        .total_pools
        .checked_add(1)
        .ok_or(TapeError::UnexpectedState)?;

    Ok(())
}
