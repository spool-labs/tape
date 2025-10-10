use tape_api::prelude::*;
use steel::*;

pub fn process_register_node(accounts: &[AccountInfo<'_>], data: &[u8]) -> ProgramResult {
    let args = RegisterNode::try_from_bytes(data)?;
    let [
        signer_info,
        system_info,
        epoch_info,
        node_info,
        system_program_info, 
        rent_info,
    ] = accounts else {
        return Err(ProgramError::NotEnoughAccountKeys);
    };

    signer_info.is_signer()?;

    let (node_address, _bump) = node_pda(*signer_info.key);
    node_info
        .is_empty()?
        .is_writable()?
        .has_address(&node_address)?;

    let epoch = epoch_info
        .is_epoch()?
        .as_account::<Epoch>(&tape_api::ID)?;

    let system = system_info
        .is_writable()?
        .is_system()?
        .as_account_mut::<System>(&tape_api::ID)?;

    system_program_info.is_program(&system_program::ID)?;
    rent_info.is_sysvar(&sysvar::rent::ID)?;

    let bls_pubkey = args.bls_pubkey;
    let bls_signature = args.bls_pop;
    if !bls_pubkey.is_valid(bls_signature) {
        solana_program::msg!("Invalid BLS proof of possession");
        return Err(ProgramError::InvalidArgument);
    }

    create_program_account::<Node>(
        node_info,
        system_program_info,
        signer_info,
        &tape_api::ID,
        &[NODE, signer_info.key.as_ref()],
    )?;

    let node = node_info.as_account_mut::<Node>(&tape_api::ID)?;

    node.id                   = NodeId::new(system.total_nodes);
    node.authority            = *signer_info.key;
    node.registered_epoch     = current_epoch(epoch);

    let commission_rate = BasisPoints::unpack(args.commission_rate);
    node.pool = StakingPool::new(commission_rate);

    node.metadata = NodeMetadata {
        name: args.name,
        storage_capacity: 0,
        storage_used: 0,
        network_address: args.network_address,
        network_tls: args.network_tls,
        bls_pubkey: args.bls_pubkey,
    };

    system.total_nodes = system.total_nodes
        .checked_add(1)
        .ok_or(TapeError::Overflow)?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tape_test::*;

    #[test]
    fn test_register_node() {
        let signer = Pubkey::new_unique();
        let commission_rate = BasisPoints(100); // 1%
        let name = to_name("hello, world");
        let network_address = NetworkAddress::default();
        let network_tls = Pubkey::new_unique();

        let secret = BlsPrivateKey::from_random();
        let bls_pubkey = secret.public_key().expect("pubkey");
        let bls_pop = secret.proof_of_possession().expect("pop");

        let instruction = build_register_node_ix(
            signer,
            name,
            commission_rate,
            network_address,
            network_tls,
            bls_pubkey,
            bls_pop,
        );

        let (system_address, _) = system_pda();
        let (epoch_address, _) = epoch_pda();
        let (node_address, _) = node_pda(signer);

        // Setup existing accounts

        let system = System {
            total_nodes: 0,
        };

        let epoch = Epoch {
            id: EpochNumber(42),
            state: EpochState::new(),
            last_epoch_ms: 0,
            leaders: LeaderSet::zeroed(),
        };

        let accounts = vec![
            sol(signer, 1_000_000_000),
            pda(system_address, system.pack()),
            pda(epoch_address, epoch.pack()),
            empty(node_address),
            system_program(),
            rent_sysvar(),
        ];

        let env = test_env("tape".to_string());
        env.process_instruction(
            &instruction,
            &accounts,
            &[
                Check::success(),
                Check::account(&system_address).data(
                    System {
                        total_nodes: 1,
                    }.pack().as_ref()
                ).build(),
                Check::account(&epoch_address).data(
                    epoch.pack().as_ref()
                ).build(),
                Check::account(&node_address).data(
                    Node {
                        id: NodeId::new(0),
                        authority: signer,
                        pool: StakingPool::new(commission_rate),
                        metadata: NodeMetadata {
                            name,
                            storage_capacity: 0,
                            storage_used: 0,
                            network_address,
                            network_tls,
                            bls_pubkey,
                        },
                        registered_epoch: epoch.id,
                    }.pack().as_ref()
                ).build(),
            ]
        );
    }
}
