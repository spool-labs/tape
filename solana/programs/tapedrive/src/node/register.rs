use tape_solana::*;
use tape_api::program::prelude::*;
use tape_api::event::NodeRegistered;

pub fn process_register_node(accounts: &[AccountInfo<'_>], data: &[u8]) -> ProgramResult {
    let args = RegisterNode::try_from_bytes(data)?;
    let [
        fee_payer_info,
        authority_info,

        system_info,
        archive_info,
        node_info,
        history_info,

        system_program_info,
        rent_sysvar_info,
    ] = accounts else {
        return Err(ProgramError::NotEnoughAccountKeys);
    };

    fee_payer_info
        .is_signer()?
        .is_writable()?;
    authority_info
        .is_signer()?;

    let (node_address, _) = node_pda((*authority_info.key).into());
    let (history_address, _) = history_pda(node_address);

    node_info
        .is_empty()?
        .is_writable()?
        .has_address(&node_address.into())?;

    history_info
        .is_empty()?
        .is_writable()?
        .has_address(&history_address.into())?;

    let system = system_info
        .is_writable()?
        .is_system()?
        .as_account_mut::<System>(&tapedrive::ID)?;

    archive_info.is_archive()?;
    let archive = archive_info.as_account::<Archive>(&tapedrive::ID)?;

    system_program_info
        .is_program(&system_program::ID)?;
    rent_sysvar_info
        .is_sysvar(&sysvar::rent::ID)?;

    let bls_pubkey = args.bls_pubkey;
    let bls_signature = args.bls_pop;
    if !bls_pubkey.is_valid(bls_signature) {
        return Err(TapeError::BadBlsProof.into());
    }

    create_program_account::<Node>(
        node_info,
        system_program_info,
        fee_payer_info,
        &tapedrive::ID,
        &[NODE, authority_info.key.as_ref()],
    )?;

    let node_number = system.total_nodes;
    system.total_nodes = system.total_nodes
        .checked_add(1)
        .ok_or(ProgramError::ArithmeticOverflow)?;

    let current = current_epoch(system);
    let commission_rate = BasisPoints::unpack(args.commission_rate);
    if !commission_rate.is_valid() {
        return Err(ProgramError::InvalidArgument);
    }

    let node = node_info.as_account_mut::<Node>(&tapedrive::ID)?;

    node.id                   = node_number.into();
    node.authority            = (*authority_info.key).into();
    node.registered_epoch     = current;
    node.latest_sync_epoch    = current;
    node.latest_advance_epoch = current;

    node.pool = StakingPool::new(commission_rate);

    node.metadata = NodeMetadata {
        name: args.name,
        network_address: args.network_address,
        network_tls: args.network_tls,
        bls_pubkey: args.bls_pubkey,
    };

    node.preferences = NodePreferences {
        storage_price: archive.storage_price,
        storage_capacity: archive.storage_capacity,
        committee_size: system.committee_size,
        spool_groups: system.target_group_count,
        min_version: system.min_version,
    };

    create_program_account::<History>(
        history_info,
        system_program_info,
        fee_payer_info,
        &tapedrive::ID,
        &[HISTORY, node_address.as_ref()],
    )?;

    let history = history_info.as_account_mut::<History>(&tapedrive::ID)?;

    history.node              = node_address;
    history.registered_epoch  = node.registered_epoch;
    history.latest_epoch      = node.latest_advance_epoch;
    history.inner             = PoolHistory::new();

    // Initial flat rate so stakes that activate immediately have a valid
    // rate_at(activation_epoch) lookup during unlock.
    history.inner.push(node.registered_epoch, ExchangeRate::flat());

    NodeRegistered {
        node: node_address,
        id: node.id,
        authority: (*authority_info.key).into(),
        epoch: current,
    }.log();

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tape_test::*;

    #[test]
    fn register() {
        let fee_payer = Pubkey::new_unique();
        let authority = Pubkey::new_unique();

        let name = to_name("hello, world");
        let commission_rate = BasisPoints(100); // 1%
        let network_address = NetworkAddress::default();
        let network_tls = NetworkTlsPubkey::new_unique();

        let secret = BlsPrivateKey::from_random();
        let bls_pubkey = secret.public_key().expect("pubkey");
        let bls_pop = secret.proof_of_possession().expect("pop");

        let instruction = build_register_node_ix(
            fee_payer.into(),
            authority.into(),
            name,
            commission_rate,
            network_address,
            network_tls,
            bls_pubkey,
            bls_pop,
        );

        let (system_address, _) = system_pda();
        let (archive_address, _) = archive_pda();
        let (node_address, _) = node_pda(authority.into());
        let (history_address, _) = history_pda(node_address);

        let system = System {
            current_epoch: EpochNumber(42),
            ..System::zeroed()
        };
        let archive = Archive {
            storage_price: TAPE(100),
            storage_capacity: StorageUnits::mb(1_000_000),
            ..Archive::zeroed()
        };

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            sol(authority, 0),

            pda(system_address, system.pack(), tapedrive::ID),
            pda(archive_address, archive.pack(), tapedrive::ID),
            empty(node_address),
            empty(history_address),

            system_program(),
            rent_sysvar(),
        ];

        let env = test_env();
        env.process_instruction(
            &instruction,
            &accounts,
            &[
                Check::success(),
                Check::account(&Pubkey::from(system_address)).data(
                    System {
                        total_nodes: 1,
                        ..system
                    }.pack().as_ref()
                ).build(),
                Check::account(&Pubkey::from(node_address)).data(
                    Node {
                        id: NodeId::new(0),
                        authority: authority.into(),
                        pool: StakingPool::new(commission_rate),
                        metadata: NodeMetadata {
                            name,
                            network_address,
                            network_tls,
                            bls_pubkey,
                        },
                        preferences: NodePreferences {
                            storage_price: TAPE(100),
                            storage_capacity: StorageUnits::mb(1_000_000),
                            committee_size: system.committee_size,
                            spool_groups: system.target_group_count,
                            min_version: system.min_version,
                        },
                        registered_epoch: system.current_epoch,
                        latest_sync_epoch: system.current_epoch,
                        latest_advance_epoch: system.current_epoch,
                        ..Node::zeroed()
                    }.pack().as_ref()
                ).build(),
                Check::account(&Pubkey::from(history_address)).data({
                    let mut expected_history = History {
                        node: node_address,
                        registered_epoch: system.current_epoch,
                        latest_epoch: system.current_epoch,
                        inner: PoolHistory::new(),
                    };
                    expected_history.inner.push(system.current_epoch, ExchangeRate::flat());
                    expected_history
                }.pack().as_ref()
                ).build(),
            ]
        );
    }

    #[test]
    fn register_rejects_invalid_commission_rate() {
        let fee_payer = Pubkey::new_unique();
        let authority = Pubkey::new_unique();

        let secret = BlsPrivateKey::from_random();
        let bls_pubkey = secret.public_key().expect("pubkey");
        let bls_pop = secret.proof_of_possession().expect("pop");

        let instruction = build_register_node_ix(
            fee_payer.into(),
            authority.into(),
            to_name("node"),
            BasisPoints(BasisPoints::MAX + 1),
            NetworkAddress::default(),
            NetworkTlsPubkey::new_unique(),
            bls_pubkey,
            bls_pop,
        );

        let (system_address, _) = system_pda();
        let (archive_address, _) = archive_pda();
        let (node_address, _) = node_pda(authority.into());
        let (history_address, _) = history_pda(node_address);

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            sol(authority, 0),

            pda(system_address, System::zeroed().pack(), tapedrive::ID),
            pda(archive_address, Archive::zeroed().pack(), tapedrive::ID),
            empty(node_address),
            empty(history_address),

            system_program(),
            rent_sysvar(),
        ];

        let env = test_env();
        env.process_instruction(
            &instruction,
            &accounts,
            &[Check::err(ProgramError::InvalidArgument)]
        );
    }
}
