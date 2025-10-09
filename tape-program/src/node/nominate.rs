use tape_api::prelude::*;
use steel::*;

pub fn process_nominate_node(accounts: &[AccountInfo<'_>], data: &[u8]) -> ProgramResult {
    let _args = NominateNode::try_from_bytes(data)?;
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

    let _system = system_info
        .is_writable()?
        .is_system()?
        .as_account_mut::<System>(&tape_api::ID)?;

    let epoch = epoch_info
        .is_epoch()?
        .is_writable()?
        .as_account_mut::<Epoch>(&tape_api::ID)?;

    let node = node_info
        .is_writable()?
        .as_account_mut::<Node>(&tape_api::ID)?
        .assert(|n| n.authority.eq(signer_info.key))?;

    system_program_info.is_program(&system_program::ID)?;
    rent_info.is_sysvar(&sysvar::rent::ID)?;

    let activation_epoch = current_epoch(epoch)
        .checked_add(EpochNumber(2))
        .ok_or(TapeError::Overflow)?;

    // Find the stake balance at activation epoch (2 epochs from now)
    let balance = node.pool.tape_balance_at_epoch(activation_epoch);

    solana_program::msg!("threshold {:?}", epoch.leaders.threshold_stake());
    solana_program::msg!("count {:?}", epoch.leaders.size());
    solana_program::msg!("balance {:?}", balance);

    let res = epoch.leaders.insert_or_update(
        CommitteeMember {
            id: node.id,
            key: node.metadata.bls_pubkey,
        }, 
        balance
    );

    if res {
        solana_program::msg!("Node nominated: {:?} with stake {:?}", node.id, balance);
    } else {
        solana_program::msg!("Failed to nominate node: {:?}", node.id);
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tape_test::*;

    #[test]
    fn test_nominate_node() {
        let signer = Pubkey::new_unique();
        let instruction = build_nominate_node_ix(signer);

        let (system_address, _) = system_pda();
        let (epoch_address, _) = epoch_pda();
        let (node_address, _) = node_pda(signer);

        // Setup existing accounts

        let system = System {
            total_nodes: 1,
        };

        let mut epoch = Epoch {
            id: EpochNumber(42),
            state: EpochState::zeroed(),
            last_epoch_ms: 0,
            leaders: CandidateSet::zeroed(),
        };

        epoch.leaders = CandidateSet {
            member_count: COMMITTEE_SIZE as u64 - 5,
            members: [CommitteeMember::zeroed(); COMMITTEE_SIZE],
            //members: (0..COMMITTEE_SIZE as u64)
            //    .map(|i| CommitteeMember { id: i.into(), key: BlsPubkey::zeroed() })
            //    .collect::<Vec<_>>()
            //    .try_into()
            //    .unwrap(),
            stakes: [TAPE(100000); COMMITTEE_SIZE],
        };

        //let res = epoch.leaders.insert_or_update(
        //    CommitteeMember { id: NodeId::new(9000), key: BlsPubkey::zeroed() },
        //    TAPE(1901)
        //);
        //println!("Initial epoch leaders: {:?}", epoch.leaders);
        //println!("res: {:?}", res);
        //assert!(false);

        let commission_rate = BasisPoints(100);
        let mut node = Node {
            id: NodeId::new(99),
            authority: signer,
            pool: StakingPool::new(commission_rate),
            metadata: NodeMetadata::zeroed(),
            registered_epoch: EpochNumber(1),
        };

        let e0: EpochNumber = epoch.id;
        let e1: EpochNumber = e0 + EpochNumber(1);
        let e2: EpochNumber = e1 + EpochNumber(1);
        let e3: EpochNumber = e2 + EpochNumber(1);
        let e4: EpochNumber = e3 + EpochNumber(1);

        node.pool.pending_stake.0 = FixedMap {
            length: 2,
            keys: [e2, e3],
            values: [1000, 200],
        };

        node.pool.pre_active_withdrawals.0 = FixedMap {
            length: 2,
            keys: [e2, e3],
            values: [100, 50],
        };

        // Simulate pending stake on the pool
        //node.pool.stake_with_pool(e0, TAPE(1000)).expect("schedule stake"); // activation at e2
        //node.pool.stake_with_pool(e1, TAPE(200)).expect("schedule stake");  // activation at e3
        //
        //assert_eq!(node.pool.tape_balance_at_epoch(e0), TAPE(0));           // before activation
        //assert_eq!(node.pool.tape_balance_at_epoch(e1), TAPE(0));           // before activation
        //assert_eq!(node.pool.tape_balance_at_epoch(e2), TAPE(1000));
        //assert_eq!(node.pool.tape_balance_at_epoch(e3), TAPE(1200));
        //assert_eq!(node.pool.tape_balance_at_epoch(e4), TAPE(1200));

        let accounts = vec![
            sol(signer, 1_000_000_000),
            pda(system_address, system.pack()),
            pda(epoch_address, epoch.pack()),
            pda(node_address, node.pack()),
            system_program(),
            rent_sysvar(),
        ];

        let env = test_env("tape".to_string());
        env.process_instruction(
            &instruction,
            &accounts,
            &[
                Check::success(),
                //Check::account(&epoch_address).data(
                //    Epoch {
                //        leaders: {
                //            let mut leaders = CandidateSet::zeroed();
                //            leaders.insert_or_update(
                //                CommitteeMember {
                //                    id: node.id,
                //                    key: node.metadata.bls_pubkey,
                //                },
                //                TAPE(1000) // balance at e4
                //            );
                //            leaders
                //        },
                //        ..epoch
                //    }.pack().as_ref()
                //).build(),
            ]
        );
    }

    //#[test]
    //fn test_register_node() {
    //    let signer = Pubkey::new_unique();
    //    let commission_rate = BasisPoints(100); // 1%
    //    let name = to_name("hello, world");
    //    let network_address = NetworkAddress::default();
    //    let network_tls = Pubkey::new_unique();
    //
    //    let secret = BlsPrivateKey::from_random();
    //    let bls_pubkey = secret.public_key().expect("pubkey");
    //    let bls_pop = secret.proof_of_possession().expect("pop");
    //
    //    let instruction = build_register_node_ix(
    //        signer,
    //        name,
    //        commission_rate,
    //        network_address,
    //        network_tls,
    //        bls_pubkey,
    //        bls_pop,
    //    );
    //
    //    let (system_address, _) = system_pda();
    //    let (epoch_address, _) = epoch_pda();
    //    let (node_address, _) = node_pda(signer);
    //
    //    // Setup existing accounts
    //
    //    let system = System {
    //        total_nodes: 0,
    //    };
    //
    //    let epoch = Epoch {
    //        id: EpochNumber(42),
    //        state: EpochState::new(),
    //        last_epoch_ms: 0,
    //        leaders: CandidateSet::zeroed(),
    //    };
    //
    //    let accounts = vec![
    //        sol(signer, 1_000_000_000),
    //        pda(system_address, system.pack()),
    //        pda(epoch_address, epoch.pack()),
    //        empty(node_address),
    //        system_program(),
    //        rent_sysvar(),
    //    ];
    //
    //    let env = test_env("tape".to_string());
    //    env.process_instruction(
    //        &instruction,
    //        &accounts,
    //        &[
    //            Check::success(),
    //            Check::account(&system_address).data(
    //                System {
    //                    total_nodes: 1,
    //                }.pack().as_ref()
    //            ).build(),
    //            Check::account(&node_address).data(
    //                Node {
    //                    id: NodeId::new(0),
    //                    authority: signer,
    //                    pool: StakingPool::new(commission_rate),
    //                    metadata: NodeMetadata {
    //                        name,
    //                        storage_capacity: 0,
    //                        storage_used: 0,
    //                        network_address,
    //                        network_tls,
    //                        bls_pubkey,
    //                    },
    //                    registered_epoch: epoch.id,
    //                }.pack().as_ref()
    //            ).build(),
    //        ]
    //    );
    //}
}
