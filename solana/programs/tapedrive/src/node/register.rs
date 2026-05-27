use tape_solana::*;
use tape_api::program::prelude::*;
use tape_api::event::NodeRegistered;

pub fn process_register_node(accounts: &[AccountInfo<'_>], data: &[u8]) -> ProgramResult {
    let args = RegisterNode::try_from_bytes(data)?;
    let [
        fee_payer_info,
        authority_info,

        system_info,
        node_info,
        history_info,
        blacklist_info,

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
    let (blacklist_address, _) = blacklist_pda(node_address);

    node_info
        .is_empty()?
        .is_writable()?
        .has_address(&node_address.into())?;

    history_info
        .is_empty()?
        .is_writable()?
        .has_address(&history_address.into())?;

    blacklist_info
        .is_empty()?
        .is_writable()?
        .has_address(&blacklist_address.into())?;

    let system = system_info
        .is_writable()?
        .is_system()?
        .as_account_mut::<System>(&tapedrive::ID)?;

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
    let commission_rate = args.commission_rate;
    if !commission_rate.is_valid() {
        return Err(ProgramError::InvalidArgument);
    }

    let node = node_info.as_account_mut::<Node>(&tapedrive::ID)?;

    node.id                   = node_number.into();
    node.authority            = (*authority_info.key).into();
    node.registered_epoch     = current;
    node.latest_sync_epoch    = current;
    node.latest_advance_epoch = current;
    node.rate_span_start      = current;

    node.pool = StakingPool::new(commission_rate);

    node.metadata = NodeMetadata {
        name: args.name,
        network_address: args.network_address,
        network_tls: args.network_tls,
        bls_pubkey: args.bls_pubkey,
    };

    node.preferences = args.preferences;

    create_program_account::<Tape>(
        history_info,
        system_program_info,
        fee_payer_info,
        &tapedrive::ID,
        &[HISTORY, node_address.as_ref()],
    )?;

    let history_tape = history_info
        .as_account_mut::<Tape>(&tapedrive::ID)?;
    *history_tape = Tape::history(node.id, current);

    create_program_account::<Tape>(
        blacklist_info,
        system_program_info,
        fee_payer_info,
        &tapedrive::ID,
        &[BLACKLIST, node_address.as_ref()],
    )?;

    let blacklist_tape = blacklist_info
        .as_account_mut::<Tape>(&tapedrive::ID)?;
    *blacklist_tape = Tape::blacklist(node.id, current);

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

        let preferences = NodePreferences::from(&GenesisConfig::local());

        let instruction = build_register_node_ix(
            fee_payer.into(),
            authority.into(),
            name,
            commission_rate,
            network_address,
            network_tls,
            bls_pubkey,
            bls_pop,
            preferences,
        );

        let (system_address, _) = system_pda();
        let (node_address, _) = node_pda(authority.into());
        let (history_address, _) = history_pda(node_address);
        let (blacklist_address, _) = blacklist_pda(node_address);

        let system = System {
            current_epoch: EpochNumber(42),
            ..System::zeroed()
        };

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            sol(authority, 0),

            pda(system_address, system.pack(), tapedrive::ID),
            empty(node_address),
            empty(history_address),
            empty(blacklist_address),

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
                        preferences,
                        registered_epoch: system.current_epoch,
                        latest_sync_epoch: system.current_epoch,
                        latest_advance_epoch: system.current_epoch,
                        rate_span_start: system.current_epoch,
                        ..Node::zeroed()
                    }.pack().as_ref()
                ).build(),
                Check::account(&Pubkey::from(history_address)).data(
                    Tape::history(NodeId::new(0), system.current_epoch)
                        .pack()
                        .as_ref()
                ).build(),
                Check::account(&Pubkey::from(blacklist_address)).data(
                    Tape::blacklist(NodeId::new(0), system.current_epoch)
                        .pack()
                        .as_ref()
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
            NodePreferences::from(&GenesisConfig::local()),
        );

        let (system_address, _) = system_pda();
        let (node_address, _) = node_pda(authority.into());
        let (history_address, _) = history_pda(node_address);
        let (blacklist_address, _) = blacklist_pda(node_address);

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            sol(authority, 0),

            pda(system_address, System::zeroed().pack(), tapedrive::ID),
            empty(node_address),
            empty(history_address),
            empty(blacklist_address),

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
