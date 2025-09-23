use tape_api::prelude::*;
use steel::*;

//pub fn process_register_node(accounts: &[AccountInfo<'_>], data: &[u8]) -> ProgramResult {
//    let args = RegisterNode::try_from_bytes(data)?;
//    let [
//        signer_info,
//        system_info,
//        epoch_info,
//        node_info,
//        system_program_info, 
//        rent_info,
//    ] = accounts else {
//        return Err(ProgramError::NotEnoughAccountKeys);
//    };
//
//    signer_info.is_signer()?;
//
//    let (pool_address, _bump) = storage_node_pda(*signer_info.key);
//    node_info
//        .is_empty()?
//        .is_writable()?
//        .has_address(&pool_address)?;
//
//    let epoch = epoch_info
//        .is_epoch()?
//        .as_account::<Epoch>(&tape_api::ID)?;
//
//    let system = system_info
//        .is_system()?
//        .is_writable()?
//        .as_account_mut::<System>(&tape_api::ID)?;
//
//    system_program_info.is_program(&system_program::ID)?;
//    rent_info.is_sysvar(&sysvar::rent::ID)?;
//
//    create_program_account::<StorageNode>(
//        node_info,
//        system_program_info,
//        signer_info,
//        &tape_api::ID,
//        &[NODE, signer_info.key.as_ref()],
//    )?;
//
//    let node = node_info.as_account_mut::<StorageNode>(&tape_api::ID)?;
//
//    node.id                   = NodeId::new(system.total_nodes);
//    node.authority            = *signer_info.key;
//    node.registered_epoch     = current_epoch(epoch);
//
//    node.pool = StakingPool {
//        total_stake : TAPE::zero(),
//        commission_rate : BasisPoints::unpack(args.commission_rate)
//    };
//
//    node.metadata = NodeMetadata {
//        name: args.name,
//        storage_capacity: 0,
//        storage_used: 0,
//        network_address: args.network_address,
//        network_tls: args.network_tls,
//    };
//
//    system.total_nodes = system.total_nodes
//        .checked_add(1)
//        .ok_or(TapeError::UnexpectedState)?;
//
//    Ok(())
//}
