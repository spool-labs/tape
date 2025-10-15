use tape_api::prelude::*;
use steel::*;

pub fn process_join_network(accounts: &[AccountInfo<'_>], data: &[u8]) -> ProgramResult {
    let _args = JoinNetwork::try_from_bytes(data)?;
    let [
        signer_info,
        epoch_info,
        node_info,
        system_program_info, 
        rent_info,
    ] = accounts else {
        return Err(ProgramError::NotEnoughAccountKeys);
    };

    signer_info.is_signer()?;

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

    // Find the stake balance at activation epoch (1 epoch from now)
    let activation_epoch = next_epoch(epoch);
    let balance = node.pool.tape_balance_at_epoch(activation_epoch);

    epoch.leaders
        .try_join(&node.id, balance)
        .map_err(|_| TapeError::UnexpectedState)?;
    
    epoch.leaders
        .set_bls_key(&node.id, node.metadata.bls_pubkey)
        .map_err(|_| TapeError::UnexpectedState)?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tape_test::*;

    #[test]
    fn test_join_network() {
        let signer = Pubkey::new_unique();
        let instruction = build_join_network_ix(signer);

        let (epoch_address, _) = epoch_pda();
        let (node_address, _) = node_pda(signer);

        // Setup existing accounts

        let mut epoch = Epoch {
            id: EpochNumber(42),
            state: EpochState::zeroed(),
            leaders: LeaderSet::zeroed(),
            last_epoch_ms: 0,
        };

        epoch.leaders = LeaderSet {
            member_count: COMMITTEE_SIZE as u64,
            members: [NodeId::zeroed(); COMMITTEE_SIZE],
            keys: [BlsPubkey::zeroed(); COMMITTEE_SIZE],
            stakes: (0..COMMITTEE_SIZE as u64)
                .map(|i| TAPE(1000 - i))
                .collect::<Vec<_>>()
                .try_into()
                .unwrap()
        };

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

        node.pool.tape_balance = TAPE(5000);
        node.pool.pending_stake.0 = FixedMap {
            length: 2,
            keys: [e1, e2],
            values: [1000, 200],
        };
        node.pool.pre_active_withdrawals.0 = FixedMap {
            length: 2,
            keys: [e1, e2],
            values: [100, 50],
        };

        assert_eq!(node.pool.tape_balance_at_epoch(e0), TAPE(5000));
        assert_eq!(node.pool.tape_balance_at_epoch(e1), TAPE(5900));
        assert_eq!(node.pool.tape_balance_at_epoch(e2), TAPE(6050));

        // println!("leaders {:?}", epoch.leaders);

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
                Check::account(&epoch_address).data(
                    Epoch {
                        leaders: {
                            // Same as before, but with our node evicting the lowest stake node
                            let mut leaders = epoch.leaders;

                            // Nudge all values over by one
                            let last_index = (COMMITTEE_SIZE - 1) as usize;
                            for i in (1..COMMITTEE_SIZE).rev() {
                                leaders.stakes[i] = leaders.stakes[i - 1];
                                leaders.members[i] = leaders.members[i - 1];
                            }

                            leaders.members[0] = node.id;
                            leaders.stakes[0] = TAPE(5900);
                            leaders.keys[0] = node.metadata.bls_pubkey;

                            leaders
                        },
                        ..epoch
                    }.pack().as_ref()
                ).build(),
            ]
        );
    }

}
