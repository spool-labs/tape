use tape_solana::*;
use tape_api::prelude::*;
use crate::error::*;

/// Calculate total stake including all scheduled additions.
/// Used during low-quorum mode to bypass E+2 activation delay.
fn calculate_total_pending_stake<const N: usize>(pool: &StakingPool<N>) -> Coin<TAPE> {
    pool.stake
        .saturating_add(pool.schedule.total_incoming())
        .saturating_sub(pool.schedule.total_outgoing())
}

pub fn process_join_network(accounts: &[AccountInfo<'_>], data: &[u8]) -> ProgramResult {
    let _args = JoinNetwork::try_from_bytes(data)?;
    let [
        fee_payer_info,
        authority_info,
        system_info,
        epoch_info,
        node_info,
    ] = accounts else {
        return Err(ProgramError::NotEnoughAccountKeys);
    };

    fee_payer_info
        .is_signer()?
        .is_writable()?;
    authority_info
        .is_signer()?;

    let system = system_info
        .is_writable()?
        .is_system()?
        .as_account_mut::<System>(&tapedrive::ID)?;

    let epoch = epoch_info
        .is_epoch()?
        .as_account::<Epoch>(&tapedrive::ID)?;

    let node = node_info
        .as_account::<Node>(&tapedrive::ID)?;

    if node.authority != *authority_info.key {
        return Err(ProgramError::InvalidAccountData);
    }

    // During low-quorum mode, include all scheduled stake (bypass E+2 delay)
    // In normal mode, use the stake balance at activation epoch (1 epoch from now)
    let balance = if system.is_low_quorum() {
        calculate_total_pending_stake(&node.pool)
    } else {
        let activation_epoch = next_epoch(epoch);
        node.pool.calculate_stake_at(activation_epoch)
    };

    if balance.is_zero() {
        return Err(TapeError::UnexpectedState.into());
    }

    let member = CommitteeMember {
        id: node.id,
        stake: balance,
        key: node.metadata.bls_pubkey,
        blacklist: node.blacklist.total_size(),
        preferences: node.preferences.clone(),
        weight: 0,
    };

    system.committee_next
        .try_join(&member)
        .map_err(|_| TapeError::UnexpectedState)?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tape_test::*;

    fn member(id: u64, stake: u64) -> CommitteeMember {
        CommitteeMember::new(NodeId(id), TAPE(stake))
    }

    #[test]
    fn test_node_pack_schedule() {
        // Verify pack/unpack preserves schedule data
        let mut node = Node::zeroed();

        node.pool.stake = TAPE(0);
        node.pool.shares = ShareAmount(0);
        node.pool.schedule.stake(EpochNumber(44), TAPE(2000)).unwrap();
        node.pool.schedule.stake(EpochNumber(45), TAPE(500)).unwrap();

        // Verify before pack
        assert_eq!(node.pool.schedule.incoming_tokens.len(), 2);
        assert_eq!(node.pool.schedule.total_incoming(), TAPE(2500));

        // Pack and unpack
        let packed = node.pack();
        let unpacked = Node::unpack(&packed[8..]).unwrap();

        // Verify after unpack
        assert_eq!(unpacked.pool.schedule.incoming_tokens.len(), 2);
        assert_eq!(unpacked.pool.schedule.total_incoming(), TAPE(2500));
        assert_eq!(calculate_total_pending_stake(&unpacked.pool), TAPE(2500));
    }

    #[test]
    fn test_join_network() {
        let fee_payer = Pubkey::new_unique();
        let authority = Pubkey::new_unique();

        let (node_address, _) = node_pda(authority);
        let (system_address, _) = system_pda();
        let (epoch_address, _) = epoch_pda();

        let instruction = build_join_network_ix(fee_payer, authority, node_address);

        // Setup existing accounts
        let mut system = System::zeroed();
        let mut epoch = Epoch::zeroed();
        let mut node = Node::zeroed();

        system.committee_next = Committee::from_members(&[
            member(3, 3_500),
            member(4, 2_100),
        ]);

        epoch.id = EpochNumber(42);

        node.id = NodeId(5);
        node.authority = authority;

        // Minimal pool setup to produce a non-zero activation balance
        node.pool.stake = TAPE(1_000);
        node.pool.shares = ShareAmount(1_000);
        node.preferences = NodePreferences {
            storage_price: TAPE(10),
            storage_capacity: StorageUnits(1_000_000),
        };

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            sol(authority, 0),
            pda(system_address, system.pack(), tapedrive::ID),
            pda(epoch_address, epoch.pack(), tapedrive::ID),
            pda(node_address, node.pack(), tapedrive::ID),
        ];

        // Expected state after instruction
        let e0: EpochNumber = epoch.id;
        let e1: EpochNumber = e0 + EpochNumber(1);

        let balance = node.pool.calculate_stake_at(e1);

        let member = CommitteeMember {
            id: node.id,
            stake: balance,
            key: node.metadata.bls_pubkey,
            blacklist: node.blacklist.total_size(),
            preferences: node.preferences.clone(),
            ..CommitteeMember::zeroed()
        };

        system
            .committee_next
            .try_join(&member)
            .expect("join committee");

        let env = test_env();
        env.process_instruction(
            &instruction,
            &accounts,
            &[
                Check::success(),
                Check::account(&system_address)
                    .data(system.pack().as_ref())
                    .build(),
                Check::account(&epoch_address) // unchanged
                    .data(epoch.pack().as_ref())
                    .build(),
                Check::account(&node_address) // unchanged
                    .data(node.pack().as_ref())
                    .build(),
            ],
        );
    }

    #[test]
    fn test_join_low_quorum_pending_stake() {
        // Test that in low-quorum mode, pending stake is included
        let fee_payer = Pubkey::new_unique();
        let authority = Pubkey::new_unique();

        let (node_address, _) = node_pda(authority);
        let (system_address, _) = system_pda();
        let (epoch_address, _) = epoch_pda();

        let instruction = build_join_network_ix(fee_payer, authority, node_address);

        // Setup existing accounts
        let mut system = System::zeroed();
        let mut epoch = Epoch::zeroed();
        let mut node = Node::zeroed();

        // System has only 1 member - low-quorum mode
        system.committee = Committee::from_members(&[
            member(99, 1_000),
        ]);

        epoch.id = EpochNumber(42);

        node.id = NodeId(5);
        node.authority = authority;

        // Pool has no active stake, only scheduled stake
        node.pool.stake = TAPE(0);
        node.pool.shares = ShareAmount(0);

        // Schedule 2000 for epoch 44 and 500 for epoch 45
        node.pool.schedule.stake(EpochNumber(44), TAPE(2000)).unwrap();
        node.pool.schedule.stake(EpochNumber(45), TAPE(500)).unwrap();

        node.preferences = NodePreferences {
            storage_price: TAPE(10),
            storage_capacity: StorageUnits(1_000_000),
        };

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            sol(authority, 0),
            pda(system_address, system.pack(), tapedrive::ID),
            pda(epoch_address, epoch.pack(), tapedrive::ID),
            pda(node_address, node.pack(), tapedrive::ID),
        ];

        // In low-quorum mode, balance should include all pending stake
        // total_incoming = 2000 + 500 = 2500
        let total_pending = calculate_total_pending_stake(&node.pool);
        assert_eq!(total_pending, TAPE(2500));

        let member = CommitteeMember {
            id: node.id,
            stake: total_pending,
            key: node.metadata.bls_pubkey,
            blacklist: node.blacklist.total_size(),
            preferences: node.preferences.clone(),
            ..CommitteeMember::zeroed()
        };

        system
            .committee_next
            .try_join(&member)
            .expect("join committee");

        let env = test_env();
        env.process_instruction(
            &instruction,
            &accounts,
            &[
                Check::success(),
                Check::account(&system_address)
                    .data(system.pack().as_ref())
                    .build(),
            ],
        );
    }

    #[test]
    fn test_join_zero_stake_fails() {
        // Test that joining with zero stake fails
        let fee_payer = Pubkey::new_unique();
        let authority = Pubkey::new_unique();

        let (node_address, _) = node_pda(authority);
        let (system_address, _) = system_pda();
        let (epoch_address, _) = epoch_pda();

        let instruction = build_join_network_ix(fee_payer, authority, node_address);

        let mut system = System::zeroed();
        let mut epoch = Epoch::zeroed();
        let mut node = Node::zeroed();

        // Low-quorum mode
        system.committee = Committee::from_members(&[
            member(99, 1_000),
        ]);

        epoch.id = EpochNumber(42);

        node.id = NodeId(5);
        node.authority = authority;
        // No stake at all
        node.pool.stake = TAPE(0);
        node.pool.shares = ShareAmount(0);

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            sol(authority, 0),
            pda(system_address, system.pack(), tapedrive::ID),
            pda(epoch_address, epoch.pack(), tapedrive::ID),
            pda(node_address, node.pack(), tapedrive::ID),
        ];

        let env = test_env();
        env.process_instruction(
            &instruction,
            &accounts,
            &[
                Check::err(TapeError::UnexpectedState.into()),
            ],
        );
    }

    #[test]
    fn test_wrong_authority() {
        // Test that wrong authority signer fails
        let fee_payer = Pubkey::new_unique();
        let authority = Pubkey::new_unique();
        let wrong_authority = Pubkey::new_unique();

        let (node_address, _) = node_pda(authority);
        let (system_address, _) = system_pda();
        let (epoch_address, _) = epoch_pda();

        // Build instruction with wrong_authority as signer
        let instruction = build_join_network_ix(fee_payer, wrong_authority, node_address);

        let mut system = System::zeroed();
        let mut epoch = Epoch::zeroed();
        let mut node = Node::zeroed();

        // Normal mode (>= 24 members in committee)
        system.committee = Committee::from_members(&(0..24).map(|i| member(i, 1000)).collect::<Vec<_>>());

        epoch.id = EpochNumber(42);

        node.id = NodeId(99);
        node.authority = authority; // Node expects authority, not wrong_authority
        node.pool.stake = TAPE(1_000);
        node.pool.shares = ShareAmount(1_000);

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            sol(wrong_authority, 0), // Wrong signer
            pda(system_address, system.pack(), tapedrive::ID),
            pda(epoch_address, epoch.pack(), tapedrive::ID),
            pda(node_address, node.pack(), tapedrive::ID),
        ];

        let env = test_env();
        env.process_instruction(
            &instruction,
            &accounts,
            &[
                Check::err(ProgramError::InvalidAccountData),
            ],
        );
    }

    #[test]
    fn test_already_in_committee() {
        // Test that joining when already in committee_next fails
        let fee_payer = Pubkey::new_unique();
        let authority = Pubkey::new_unique();

        let (node_address, _) = node_pda(authority);
        let (system_address, _) = system_pda();
        let (epoch_address, _) = epoch_pda();

        let instruction = build_join_network_ix(fee_payer, authority, node_address);

        let mut system = System::zeroed();
        let mut epoch = Epoch::zeroed();
        let mut node = Node::zeroed();

        // Normal mode
        system.committee = Committee::from_members(&(0..24).map(|i| member(i, 1000)).collect::<Vec<_>>());

        // Node is already in committee_next
        system.committee_next = Committee::from_members(&[
            member(5, 2000), // Same NodeId as node below
        ]);

        epoch.id = EpochNumber(42);

        node.id = NodeId(5); // Same as in committee_next
        node.authority = authority;
        node.pool.stake = TAPE(3_000);
        node.pool.shares = ShareAmount(3_000);

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            sol(authority, 0),
            pda(system_address, system.pack(), tapedrive::ID),
            pda(epoch_address, epoch.pack(), tapedrive::ID),
            pda(node_address, node.pack(), tapedrive::ID),
        ];

        let env = test_env();
        env.process_instruction(
            &instruction,
            &accounts,
            &[
                Check::err(TapeError::UnexpectedState.into()),
            ],
        );
    }

    #[test]
    fn test_full_committee_higher_stake() {
        // Test that joining with higher stake works when committee has existing members.
        // We use a smaller committee to avoid BPF VM memory limits.
        // The stake comparison and insertion logic is tested with fewer members.
        let fee_payer = Pubkey::new_unique();
        let authority = Pubkey::new_unique();

        let (node_address, _) = node_pda(authority);
        let (system_address, _) = system_pda();
        let (epoch_address, _) = epoch_pda();

        let instruction = build_join_network_ix(fee_payer, authority, node_address);

        let mut system = System::zeroed();
        let mut epoch = Epoch::zeroed();
        let mut node = Node::zeroed();

        // Normal mode (>= 24 members)
        let committee_members: Vec<CommitteeMember> = (0..30)
            .map(|i| member(i, 1000))
            .collect();
        system.committee = Committee::from_members(&committee_members);

        // Committee_next has members with varying stakes, including a low-stake one
        let mut next_members: Vec<CommitteeMember> = (0..29)
            .map(|i| member(i, 1000))
            .collect();
        next_members.push(member(29, 500)); // Lowest stake member
        system.committee_next = Committee::from_members(&next_members);

        epoch.id = EpochNumber(42);

        node.id = NodeId(200); // New node
        node.authority = authority;
        node.pool.stake = TAPE(2_000); // Higher than lowest (500)
        node.pool.shares = ShareAmount(2_000);

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            sol(authority, 0),
            pda(system_address, system.pack(), tapedrive::ID),
            pda(epoch_address, epoch.pack(), tapedrive::ID),
            pda(node_address, node.pack(), tapedrive::ID),
        ];

        // Calculate expected state
        let balance = node.pool.calculate_stake_at(EpochNumber(43));
        let new_member = CommitteeMember {
            id: node.id,
            stake: balance,
            key: node.metadata.bls_pubkey,
            blacklist: node.blacklist.total_size(),
            preferences: node.preferences.clone(),
            ..CommitteeMember::zeroed()
        };

        system.committee_next.try_join(&new_member).expect("should join");

        // New member should be in the committee
        assert!(system.committee_next.contains(&NodeId(200)));

        let env = test_env();
        env.process_instruction(
            &instruction,
            &accounts,
            &[
                Check::success(),
                Check::account(&system_address)
                    .data(system.pack().as_ref())
                    .build(),
            ],
        );
    }

    #[test]
    fn test_full_committee_lower_stake() {
        // Test that joining full committee with lower/equal stake fails
        let fee_payer = Pubkey::new_unique();
        let authority = Pubkey::new_unique();

        let (node_address, _) = node_pda(authority);
        let (system_address, _) = system_pda();
        let (epoch_address, _) = epoch_pda();

        let instruction = build_join_network_ix(fee_payer, authority, node_address);

        let mut system = System::zeroed();
        let mut epoch = Epoch::zeroed();
        let mut node = Node::zeroed();

        // Normal mode (>= 24 members)
        let committee_members: Vec<CommitteeMember> = (0..30)
            .map(|i| member(i, 1000))
            .collect();
        system.committee = Committee::from_members(&committee_members);

        // Committee_next is full with all stakes at 1000
        let members: Vec<CommitteeMember> = (0..128)
            .map(|i| member(i, 1000))
            .collect();
        system.committee_next = Committee::from_members(&members);

        epoch.id = EpochNumber(42);

        node.id = NodeId(200);
        node.authority = authority;
        node.pool.stake = TAPE(500); // Lower than lowest (1000)
        node.pool.shares = ShareAmount(500);

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            sol(authority, 0),
            pda(system_address, system.pack(), tapedrive::ID),
            pda(epoch_address, epoch.pack(), tapedrive::ID),
            pda(node_address, node.pack(), tapedrive::ID),
        ];

        let env = test_env();
        env.process_instruction(
            &instruction,
            &accounts,
            &[
                Check::err(TapeError::UnexpectedState.into()),
            ],
        );
    }

    #[test]
    fn test_future_stake_normal_mode() {
        // Test that stake activating in E+2 doesn't count in normal mode
        // because calculate_stake_at(E+1) won't see stake scheduled for E+2
        let fee_payer = Pubkey::new_unique();
        let authority = Pubkey::new_unique();

        let (node_address, _) = node_pda(authority);
        let (system_address, _) = system_pda();
        let (epoch_address, _) = epoch_pda();

        let instruction = build_join_network_ix(fee_payer, authority, node_address);

        let mut system = System::zeroed();
        let mut epoch = Epoch::zeroed();
        let mut node = Node::zeroed();

        // Normal mode (>= 24 members)
        system.committee = Committee::from_members(&(0..24).map(|i| member(i, 1000)).collect::<Vec<_>>());

        epoch.id = EpochNumber(42);

        node.id = NodeId(99);
        node.authority = authority;

        // No current stake
        node.pool.stake = TAPE(0);
        node.pool.shares = ShareAmount(0);

        // Schedule stake for E+2 (epoch 44) - won't be seen at E+1 (epoch 43)
        node.pool.schedule.stake(EpochNumber(44), TAPE(5000)).unwrap();

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            sol(authority, 0),
            pda(system_address, system.pack(), tapedrive::ID),
            pda(epoch_address, epoch.pack(), tapedrive::ID),
            pda(node_address, node.pack(), tapedrive::ID),
        ];

        // In normal mode, calculate_stake_at(43) = 0 because stake scheduled for 44
        let balance = node.pool.calculate_stake_at(EpochNumber(43));
        assert_eq!(balance, TAPE(0));

        let env = test_env();
        env.process_instruction(
            &instruction,
            &accounts,
            &[
                Check::err(TapeError::UnexpectedState.into()),
            ],
        );
    }

    #[test]
    fn test_zero_stake_normal_mode() {
        // Test zero stake in normal mode (not just low-quorum)
        let fee_payer = Pubkey::new_unique();
        let authority = Pubkey::new_unique();

        let (node_address, _) = node_pda(authority);
        let (system_address, _) = system_pda();
        let (epoch_address, _) = epoch_pda();

        let instruction = build_join_network_ix(fee_payer, authority, node_address);

        let mut system = System::zeroed();
        let mut epoch = Epoch::zeroed();
        let mut node = Node::zeroed();

        // Normal mode
        system.committee = Committee::from_members(&(0..24).map(|i| member(i, 1000)).collect::<Vec<_>>());

        epoch.id = EpochNumber(42);

        node.id = NodeId(99);
        node.authority = authority;
        node.pool.stake = TAPE(0);
        node.pool.shares = ShareAmount(0);

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            sol(authority, 0),
            pda(system_address, system.pack(), tapedrive::ID),
            pda(epoch_address, epoch.pack(), tapedrive::ID),
            pda(node_address, node.pack(), tapedrive::ID),
        ];

        let env = test_env();
        env.process_instruction(
            &instruction,
            &accounts,
            &[
                Check::err(TapeError::UnexpectedState.into()),
            ],
        );
    }

    #[test]
    fn test_preferences_stored() {
        // Test that node preferences are stored correctly in CommitteeMember
        let fee_payer = Pubkey::new_unique();
        let authority = Pubkey::new_unique();

        let (node_address, _) = node_pda(authority);
        let (system_address, _) = system_pda();
        let (epoch_address, _) = epoch_pda();

        let instruction = build_join_network_ix(fee_payer, authority, node_address);

        let mut system = System::zeroed();
        let mut epoch = Epoch::zeroed();
        let mut node = Node::zeroed();

        // Normal mode
        system.committee = Committee::from_members(&(0..24).map(|i| member(i, 1000)).collect::<Vec<_>>());

        epoch.id = EpochNumber(42);

        node.id = NodeId(99);
        node.authority = authority;
        node.pool.stake = TAPE(5_000);
        node.pool.shares = ShareAmount(5_000);

        // Custom preferences
        node.preferences = NodePreferences {
            storage_price: TAPE(999),
            storage_capacity: StorageUnits(12345678),
        };

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            sol(authority, 0),
            pda(system_address, system.pack(), tapedrive::ID),
            pda(epoch_address, epoch.pack(), tapedrive::ID),
            pda(node_address, node.pack(), tapedrive::ID),
        ];

        // Expected member
        let balance = node.pool.calculate_stake_at(EpochNumber(43));
        let expected_member = CommitteeMember {
            id: node.id,
            stake: balance,
            key: node.metadata.bls_pubkey,
            blacklist: node.blacklist.total_size(),
            preferences: node.preferences.clone(),
            weight: 0,
        };

        system.committee_next.try_join(&expected_member).expect("join");

        // Verify preferences are correct
        let (stored_member, _) = system.committee_next.get_member(&NodeId(99)).unwrap();
        assert_eq!(stored_member.preferences.storage_price, TAPE(999));
        assert_eq!(stored_member.preferences.storage_capacity, StorageUnits(12345678));

        let env = test_env();
        env.process_instruction(
            &instruction,
            &accounts,
            &[
                Check::success(),
                Check::account(&system_address)
                    .data(system.pack().as_ref())
                    .build(),
            ],
        );
    }

    #[test]
    fn test_node_id_stored() {
        // Test that node.id is correctly stored in CommitteeMember
        let fee_payer = Pubkey::new_unique();
        let authority = Pubkey::new_unique();

        let (node_address, _) = node_pda(authority);
        let (system_address, _) = system_pda();
        let (epoch_address, _) = epoch_pda();

        let instruction = build_join_network_ix(fee_payer, authority, node_address);

        let mut system = System::zeroed();
        let mut epoch = Epoch::zeroed();
        let mut node = Node::zeroed();

        // Normal mode
        system.committee = Committee::from_members(&(0..24).map(|i| member(i, 1000)).collect::<Vec<_>>());

        epoch.id = EpochNumber(42);

        node.id = NodeId(42424242); // Specific ID to verify
        node.authority = authority;
        node.pool.stake = TAPE(1_000);
        node.pool.shares = ShareAmount(1_000);

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            sol(authority, 0),
            pda(system_address, system.pack(), tapedrive::ID),
            pda(epoch_address, epoch.pack(), tapedrive::ID),
            pda(node_address, node.pack(), tapedrive::ID),
        ];

        let balance = node.pool.calculate_stake_at(EpochNumber(43));
        let expected_member = CommitteeMember {
            id: NodeId(42424242),
            stake: balance,
            key: node.metadata.bls_pubkey,
            blacklist: node.blacklist.total_size(),
            preferences: node.preferences.clone(),
            weight: 0,
        };

        system.committee_next.try_join(&expected_member).expect("join");
        assert!(system.committee_next.contains(&NodeId(42424242)));

        let env = test_env();
        env.process_instruction(
            &instruction,
            &accounts,
            &[
                Check::success(),
                Check::account(&system_address)
                    .data(system.pack().as_ref())
                    .build(),
            ],
        );
    }

    #[test]
    fn test_ordering_by_stake() {
        // Test that committee_next is ordered by stake (descending)
        let fee_payer = Pubkey::new_unique();
        let authority = Pubkey::new_unique();

        let (node_address, _) = node_pda(authority);
        let (system_address, _) = system_pda();
        let (epoch_address, _) = epoch_pda();

        let instruction = build_join_network_ix(fee_payer, authority, node_address);

        let mut system = System::zeroed();
        let mut epoch = Epoch::zeroed();
        let mut node = Node::zeroed();

        // Normal mode
        system.committee = Committee::from_members(&(0..24).map(|i| member(i, 1000)).collect::<Vec<_>>());

        // Start with some members in committee_next
        system.committee_next = Committee::from_members(&[
            member(1, 5000),
            member(2, 3000),
            member(3, 1000),
        ]);

        epoch.id = EpochNumber(42);

        node.id = NodeId(99);
        node.authority = authority;
        node.pool.stake = TAPE(4_000); // Should be inserted between 5000 and 3000
        node.pool.shares = ShareAmount(4_000);

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            sol(authority, 0),
            pda(system_address, system.pack(), tapedrive::ID),
            pda(epoch_address, epoch.pack(), tapedrive::ID),
            pda(node_address, node.pack(), tapedrive::ID),
        ];

        let balance = node.pool.calculate_stake_at(EpochNumber(43));
        let new_member = CommitteeMember {
            id: node.id,
            stake: balance,
            key: node.metadata.bls_pubkey,
            blacklist: node.blacklist.total_size(),
            preferences: node.preferences.clone(),
            weight: 0,
        };

        system.committee_next.try_join(&new_member).expect("join");

        // Verify ordering: 5000, 4000, 3000, 1000
        let stakes: Vec<u64> = system.committee_next.active_stakes()
            .iter()
            .map(|s| s.as_u64())
            .collect();
        assert_eq!(stakes, vec![5000, 4000, 3000, 1000]);

        let env = test_env();
        env.process_instruction(
            &instruction,
            &accounts,
            &[
                Check::success(),
                Check::account(&system_address)
                    .data(system.pack().as_ref())
                    .build(),
            ],
        );
    }

    #[test]
    fn test_bls_pubkey_stored() {
        // Test that BLS pubkey from node metadata is stored in CommitteeMember
        let fee_payer = Pubkey::new_unique();
        let authority = Pubkey::new_unique();

        let (node_address, _) = node_pda(authority);
        let (system_address, _) = system_pda();
        let (epoch_address, _) = epoch_pda();

        let instruction = build_join_network_ix(fee_payer, authority, node_address);

        let mut system = System::zeroed();
        let mut epoch = Epoch::zeroed();
        let mut node = Node::zeroed();

        // Normal mode
        system.committee = Committee::from_members(&(0..24).map(|i| member(i, 1000)).collect::<Vec<_>>());

        epoch.id = EpochNumber(42);

        node.id = NodeId(99);
        node.authority = authority;
        node.pool.stake = TAPE(1_000);
        node.pool.shares = ShareAmount(1_000);

        // Use a unique BLS pubkey
        let bls_key = BlsPubkey::new_unique();
        node.metadata.bls_pubkey = bls_key;

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            sol(authority, 0),
            pda(system_address, system.pack(), tapedrive::ID),
            pda(epoch_address, epoch.pack(), tapedrive::ID),
            pda(node_address, node.pack(), tapedrive::ID),
        ];

        let balance = node.pool.calculate_stake_at(EpochNumber(43));
        let expected_member = CommitteeMember {
            id: node.id,
            stake: balance,
            key: bls_key,
            blacklist: node.blacklist.total_size(),
            preferences: node.preferences.clone(),
            weight: 0,
        };

        system.committee_next.try_join(&expected_member).expect("join");

        // Verify BLS key is stored (should match the unique key we set)
        let (stored, _) = system.committee_next.get_member(&NodeId(99)).unwrap();
        assert_eq!(stored.key, bls_key);

        let env = test_env();
        env.process_instruction(
            &instruction,
            &accounts,
            &[
                Check::success(),
                Check::account(&system_address)
                    .data(system.pack().as_ref())
                    .build(),
            ],
        );
    }

    #[test]
    fn test_blacklist_size_stored() {
        // Test that blacklist total_size is stored in CommitteeMember
        let fee_payer = Pubkey::new_unique();
        let authority = Pubkey::new_unique();

        let (node_address, _) = node_pda(authority);
        let (system_address, _) = system_pda();
        let (epoch_address, _) = epoch_pda();

        let instruction = build_join_network_ix(fee_payer, authority, node_address);

        let mut system = System::zeroed();
        let mut epoch = Epoch::zeroed();
        let mut node = Node::zeroed();

        // Normal mode
        system.committee = Committee::from_members(&(0..24).map(|i| member(i, 1000)).collect::<Vec<_>>());

        epoch.id = EpochNumber(42);

        node.id = NodeId(99);
        node.authority = authority;
        node.pool.stake = TAPE(1_000);
        node.pool.shares = ShareAmount(1_000);

        // Set blacklist with some entries
        node.blacklist = Blacklist::new();
        // Note: Blacklist starts empty, so total_size() should be 0

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            sol(authority, 0),
            pda(system_address, system.pack(), tapedrive::ID),
            pda(epoch_address, epoch.pack(), tapedrive::ID),
            pda(node_address, node.pack(), tapedrive::ID),
        ];

        let balance = node.pool.calculate_stake_at(EpochNumber(43));
        let expected_member = CommitteeMember {
            id: node.id,
            stake: balance,
            key: node.metadata.bls_pubkey,
            blacklist: node.blacklist.total_size(),
            preferences: node.preferences.clone(),
            weight: 0,
        };

        system.committee_next.try_join(&expected_member).expect("join");

        // Verify blacklist size is stored
        let (stored, _) = system.committee_next.get_member(&NodeId(99)).unwrap();
        assert_eq!(stored.blacklist, StorageUnits(0));

        let env = test_env();
        env.process_instruction(
            &instruction,
            &accounts,
            &[
                Check::success(),
                Check::account(&system_address)
                    .data(system.pack().as_ref())
                    .build(),
            ],
        );
    }

    #[test]
    fn test_weight_initialized_zero() {
        // Test that weight is initialized to 0 when joining
        let fee_payer = Pubkey::new_unique();
        let authority = Pubkey::new_unique();

        let (node_address, _) = node_pda(authority);
        let (system_address, _) = system_pda();
        let (epoch_address, _) = epoch_pda();

        let instruction = build_join_network_ix(fee_payer, authority, node_address);

        let mut system = System::zeroed();
        let mut epoch = Epoch::zeroed();
        let mut node = Node::zeroed();

        // Normal mode
        system.committee = Committee::from_members(&(0..24).map(|i| member(i, 1000)).collect::<Vec<_>>());

        epoch.id = EpochNumber(42);

        node.id = NodeId(99);
        node.authority = authority;
        node.pool.stake = TAPE(1_000);
        node.pool.shares = ShareAmount(1_000);

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            sol(authority, 0),
            pda(system_address, system.pack(), tapedrive::ID),
            pda(epoch_address, epoch.pack(), tapedrive::ID),
            pda(node_address, node.pack(), tapedrive::ID),
        ];

        let balance = node.pool.calculate_stake_at(EpochNumber(43));
        let expected_member = CommitteeMember {
            id: node.id,
            stake: balance,
            key: node.metadata.bls_pubkey,
            blacklist: node.blacklist.total_size(),
            preferences: node.preferences.clone(),
            weight: 0,
        };

        system.committee_next.try_join(&expected_member).expect("join");

        // Verify weight is 0
        let (stored, _) = system.committee_next.get_member(&NodeId(99)).unwrap();
        assert_eq!(stored.weight, 0);

        let env = test_env();
        env.process_instruction(
            &instruction,
            &accounts,
            &[
                Check::success(),
                Check::account(&system_address)
                    .data(system.pack().as_ref())
                    .build(),
            ],
        );
    }

    #[test]
    fn test_scheduled_withdrawal_reduces_stake() {
        // Test that scheduled withdrawals reduce stake calculation
        let fee_payer = Pubkey::new_unique();
        let authority = Pubkey::new_unique();

        let (node_address, _) = node_pda(authority);
        let (system_address, _) = system_pda();
        let (epoch_address, _) = epoch_pda();

        let instruction = build_join_network_ix(fee_payer, authority, node_address);

        let mut system = System::zeroed();
        let mut epoch = Epoch::zeroed();
        let mut node = Node::zeroed();

        // Low-quorum mode to use total_pending_stake
        system.committee = Committee::from_members(&[member(1, 1000)]);

        epoch.id = EpochNumber(42);

        node.id = NodeId(99);
        node.authority = authority;
        node.pool.stake = TAPE(10_000);
        node.pool.shares = ShareAmount(10_000);

        // Schedule incoming and outgoing
        node.pool.schedule.stake(EpochNumber(44), TAPE(5000)).unwrap();
        node.pool.schedule.cancel(EpochNumber(44), TAPE(2000)).unwrap();

        // total_pending = 10000 + 5000 - 2000 = 13000
        let expected_balance = calculate_total_pending_stake(&node.pool);
        assert_eq!(expected_balance, TAPE(13000));

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            sol(authority, 0),
            pda(system_address, system.pack(), tapedrive::ID),
            pda(epoch_address, epoch.pack(), tapedrive::ID),
            pda(node_address, node.pack(), tapedrive::ID),
        ];

        let expected_member = CommitteeMember {
            id: node.id,
            stake: expected_balance,
            key: node.metadata.bls_pubkey,
            blacklist: node.blacklist.total_size(),
            preferences: node.preferences.clone(),
            weight: 0,
        };

        system.committee_next.try_join(&expected_member).expect("join");

        let env = test_env();
        env.process_instruction(
            &instruction,
            &accounts,
            &[
                Check::success(),
                Check::account(&system_address)
                    .data(system.pack().as_ref())
                    .build(),
            ],
        );
    }

    #[test]
    fn test_large_stake_value() {
        // Test with a large stake value near u64 max
        let fee_payer = Pubkey::new_unique();
        let authority = Pubkey::new_unique();

        let (node_address, _) = node_pda(authority);
        let (system_address, _) = system_pda();
        let (epoch_address, _) = epoch_pda();

        let instruction = build_join_network_ix(fee_payer, authority, node_address);

        let mut system = System::zeroed();
        let mut epoch = Epoch::zeroed();
        let mut node = Node::zeroed();

        // Normal mode
        system.committee = Committee::from_members(&(0..24).map(|i| member(i, 1000)).collect::<Vec<_>>());

        epoch.id = EpochNumber(42);

        node.id = NodeId(99);
        node.authority = authority;
        // Large stake value
        let large_stake = u64::MAX / 2;
        node.pool.stake = TAPE(large_stake);
        node.pool.shares = ShareAmount(large_stake);

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            sol(authority, 0),
            pda(system_address, system.pack(), tapedrive::ID),
            pda(epoch_address, epoch.pack(), tapedrive::ID),
            pda(node_address, node.pack(), tapedrive::ID),
        ];

        let balance = node.pool.calculate_stake_at(EpochNumber(43));
        let expected_member = CommitteeMember {
            id: node.id,
            stake: balance,
            key: node.metadata.bls_pubkey,
            blacklist: node.blacklist.total_size(),
            preferences: node.preferences.clone(),
            weight: 0,
        };

        system.committee_next.try_join(&expected_member).expect("join");

        let env = test_env();
        env.process_instruction(
            &instruction,
            &accounts,
            &[
                Check::success(),
                Check::account(&system_address)
                    .data(system.pack().as_ref())
                    .build(),
            ],
        );
    }

    #[test]
    fn test_stake_at_e1_includes_e1() {
        // Test that stake scheduled for exactly E+1 is included
        let fee_payer = Pubkey::new_unique();
        let authority = Pubkey::new_unique();

        let (node_address, _) = node_pda(authority);
        let (system_address, _) = system_pda();
        let (epoch_address, _) = epoch_pda();

        let instruction = build_join_network_ix(fee_payer, authority, node_address);

        let mut system = System::zeroed();
        let mut epoch = Epoch::zeroed();
        let mut node = Node::zeroed();

        // Normal mode
        system.committee = Committee::from_members(&(0..24).map(|i| member(i, 1000)).collect::<Vec<_>>());

        epoch.id = EpochNumber(42); // E+1 = 43

        node.id = NodeId(99);
        node.authority = authority;
        node.pool.stake = TAPE(1_000);
        node.pool.shares = ShareAmount(1_000);

        // Schedule stake for exactly E+1 (epoch 43)
        node.pool.schedule.stake(EpochNumber(43), TAPE(2000)).unwrap();

        // calculate_stake_at(43) should include 1000 + 2000 = 3000
        let balance = node.pool.calculate_stake_at(EpochNumber(43));
        assert_eq!(balance, TAPE(3000));

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            sol(authority, 0),
            pda(system_address, system.pack(), tapedrive::ID),
            pda(epoch_address, epoch.pack(), tapedrive::ID),
            pda(node_address, node.pack(), tapedrive::ID),
        ];

        let expected_member = CommitteeMember {
            id: node.id,
            stake: balance,
            key: node.metadata.bls_pubkey,
            blacklist: node.blacklist.total_size(),
            preferences: node.preferences.clone(),
            weight: 0,
        };

        system.committee_next.try_join(&expected_member).expect("join");

        let env = test_env();
        env.process_instruction(
            &instruction,
            &accounts,
            &[
                Check::success(),
                Check::account(&system_address)
                    .data(system.pack().as_ref())
                    .build(),
            ],
        );
    }

    #[test]
    fn test_empty_committee_is_low_quorum() {
        // Test that empty committee (0 members) is low-quorum
        let fee_payer = Pubkey::new_unique();
        let authority = Pubkey::new_unique();

        let (node_address, _) = node_pda(authority);
        let (system_address, _) = system_pda();
        let (epoch_address, _) = epoch_pda();

        let instruction = build_join_network_ix(fee_payer, authority, node_address);

        let mut system = System::zeroed();
        let mut epoch = Epoch::zeroed();
        let mut node = Node::zeroed();

        // Empty committee = low-quorum mode
        // system.committee is zeroed (empty)
        assert!(system.is_low_quorum());

        epoch.id = EpochNumber(0);

        node.id = NodeId(1);
        node.authority = authority;

        // Only pending stake (no active stake)
        node.pool.stake = TAPE(0);
        node.pool.shares = ShareAmount(0);
        node.pool.schedule.stake(EpochNumber(2), TAPE(5000)).unwrap();

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            sol(authority, 0),
            pda(system_address, system.pack(), tapedrive::ID),
            pda(epoch_address, epoch.pack(), tapedrive::ID),
            pda(node_address, node.pack(), tapedrive::ID),
        ];

        // In low-quorum mode, should use total_pending_stake
        let balance = calculate_total_pending_stake(&node.pool);
        assert_eq!(balance, TAPE(5000));

        let expected_member = CommitteeMember {
            id: node.id,
            stake: balance,
            key: node.metadata.bls_pubkey,
            blacklist: node.blacklist.total_size(),
            preferences: node.preferences.clone(),
            weight: 0,
        };

        system.committee_next.try_join(&expected_member).expect("join");

        let env = test_env();
        env.process_instruction(
            &instruction,
            &accounts,
            &[
                Check::success(),
                Check::account(&system_address)
                    .data(system.pack().as_ref())
                    .build(),
            ],
        );
    }

    #[test]
    fn test_exactly_24_is_normal_mode() {
        // Test that exactly 24 members means NOT low-quorum (normal mode)
        let fee_payer = Pubkey::new_unique();
        let authority = Pubkey::new_unique();

        let (node_address, _) = node_pda(authority);
        let (system_address, _) = system_pda();
        let (epoch_address, _) = epoch_pda();

        let instruction = build_join_network_ix(fee_payer, authority, node_address);

        let mut system = System::zeroed();
        let mut epoch = Epoch::zeroed();
        let mut node = Node::zeroed();

        // Exactly 24 members = normal mode (not low-quorum)
        system.committee = Committee::from_members(&(0..24).map(|i| member(i, 1000)).collect::<Vec<_>>());
        assert!(!system.is_low_quorum());

        epoch.id = EpochNumber(42);

        node.id = NodeId(99);
        node.authority = authority;
        node.pool.stake = TAPE(5_000);
        node.pool.shares = ShareAmount(5_000);

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            sol(authority, 0),
            pda(system_address, system.pack(), tapedrive::ID),
            pda(epoch_address, epoch.pack(), tapedrive::ID),
            pda(node_address, node.pack(), tapedrive::ID),
        ];

        // Normal mode uses calculate_stake_at
        let balance = node.pool.calculate_stake_at(EpochNumber(43));

        let expected_member = CommitteeMember {
            id: node.id,
            stake: balance,
            key: node.metadata.bls_pubkey,
            blacklist: node.blacklist.total_size(),
            preferences: node.preferences.clone(),
            weight: 0,
        };

        system.committee_next.try_join(&expected_member).expect("join");

        let env = test_env();
        env.process_instruction(
            &instruction,
            &accounts,
            &[
                Check::success(),
                Check::account(&system_address)
                    .data(system.pack().as_ref())
                    .build(),
            ],
        );
    }

    #[test]
    fn test_23_is_low_quorum() {
        // Test that 23 members means low-quorum mode
        let fee_payer = Pubkey::new_unique();
        let authority = Pubkey::new_unique();

        let (node_address, _) = node_pda(authority);
        let (system_address, _) = system_pda();
        let (epoch_address, _) = epoch_pda();

        let instruction = build_join_network_ix(fee_payer, authority, node_address);

        let mut system = System::zeroed();
        let mut epoch = Epoch::zeroed();
        let mut node = Node::zeroed();

        // 23 members = low-quorum mode
        system.committee = Committee::from_members(&(0..23).map(|i| member(i, 1000)).collect::<Vec<_>>());
        assert!(system.is_low_quorum());

        epoch.id = EpochNumber(42);

        node.id = NodeId(99);
        node.authority = authority;

        // Only pending stake - would fail in normal mode but works in low-quorum
        node.pool.stake = TAPE(0);
        node.pool.shares = ShareAmount(0);
        node.pool.schedule.stake(EpochNumber(50), TAPE(3000)).unwrap();

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            sol(authority, 0),
            pda(system_address, system.pack(), tapedrive::ID),
            pda(epoch_address, epoch.pack(), tapedrive::ID),
            pda(node_address, node.pack(), tapedrive::ID),
        ];

        // Low-quorum mode uses total_pending_stake
        let balance = calculate_total_pending_stake(&node.pool);
        assert_eq!(balance, TAPE(3000));

        let expected_member = CommitteeMember {
            id: node.id,
            stake: balance,
            key: node.metadata.bls_pubkey,
            blacklist: node.blacklist.total_size(),
            preferences: node.preferences.clone(),
            weight: 0,
        };

        system.committee_next.try_join(&expected_member).expect("join");

        let env = test_env();
        env.process_instruction(
            &instruction,
            &accounts,
            &[
                Check::success(),
                Check::account(&system_address)
                    .data(system.pack().as_ref())
                    .build(),
            ],
        );
    }

    #[test]
    fn test_full_equal_stake_rejected() {
        // Test that joining full committee with equal stake to minimum is rejected
        let fee_payer = Pubkey::new_unique();
        let authority = Pubkey::new_unique();

        let (node_address, _) = node_pda(authority);
        let (system_address, _) = system_pda();
        let (epoch_address, _) = epoch_pda();

        let instruction = build_join_network_ix(fee_payer, authority, node_address);

        let mut system = System::zeroed();
        let mut epoch = Epoch::zeroed();
        let mut node = Node::zeroed();

        // Normal mode (>= 24 members)
        let committee_members: Vec<CommitteeMember> = (0..30)
            .map(|i| member(i, 1000))
            .collect();
        system.committee = Committee::from_members(&committee_members);

        // Full committee with minimum stake of 1000
        let members: Vec<CommitteeMember> = (0..128)
            .map(|i| member(i, 1000))
            .collect();
        system.committee_next = Committee::from_members(&members);

        epoch.id = EpochNumber(42);

        node.id = NodeId(200);
        node.authority = authority;
        node.pool.stake = TAPE(1000); // Equal to minimum (not strictly greater)
        node.pool.shares = ShareAmount(1000);

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            sol(authority, 0),
            pda(system_address, system.pack(), tapedrive::ID),
            pda(epoch_address, epoch.pack(), tapedrive::ID),
            pda(node_address, node.pack(), tapedrive::ID),
        ];

        let env = test_env();
        env.process_instruction(
            &instruction,
            &accounts,
            &[
                Check::err(TapeError::UnexpectedState.into()),
            ],
        );
    }
}
