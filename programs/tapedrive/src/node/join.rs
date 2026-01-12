use tape_solana::*;
use tape_api::prelude::*;
use tape_api::event::NodeJoinedCommittee;
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

    // Check if node is already in the current committee (re-join path)
    let in_current_committee = system.committee.index_of(&node.id).is_some();

    let balance = if in_current_committee {
        // RE-JOIN: Must have advanced pool this epoch to ensure fresh stake
        if node.latest_advance_epoch != epoch.id {
            return Err(TapeError::NodeStale.into());
        }
        node.pool.stake
    } else {
        // NEW JOIN: Use projected stake
        // During low-quorum mode, include all scheduled stake (bypass E+2 delay)
        // In normal mode, use the stake balance at activation epoch (1 epoch from now)
        if system.is_low_quorum() {
            calculate_total_pending_stake(&node.pool)
        } else {
            let activation_epoch = next_epoch(epoch);
            node.pool.calculate_stake_at(activation_epoch)
        }
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

    NodeJoinedCommittee {
        node: *node_info.key,
        id: node.id,
        stake: balance.as_u64().to_le_bytes(),
        activation_epoch: next_epoch(epoch),
    }.log();

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
    fn test_rejoin_requires_advance_pool() {
        // Test that re-joining requires AdvancePool to be called first
        // Node IS in current committee, but latest_advance_epoch is stale
        let fee_payer = Pubkey::new_unique();
        let authority = Pubkey::new_unique();

        let (node_address, _) = node_pda(authority);
        let (system_address, _) = system_pda();
        let (epoch_address, _) = epoch_pda();

        let instruction = build_join_network_ix(fee_payer, authority, node_address);

        let mut system = System::zeroed();
        let mut epoch = Epoch::zeroed();
        let mut node = Node::zeroed();

        epoch.id = EpochNumber(10);

        node.id = NodeId(5);
        node.authority = authority;
        node.pool.stake = TAPE(3_000);
        node.pool.shares = ShareAmount(3_000);
        // STALE: latest_advance_epoch is N-1, but current epoch is N
        node.latest_advance_epoch = EpochNumber(9);
        node.preferences = NodePreferences {
            storage_price: TAPE(10),
            storage_capacity: StorageUnits(1_000_000),
        };

        // Node IS in current committee
        system.committee = Committee::from_members(&[
            member(node.id.as_u64(), 3_000),
            member(6, 2_000),
        ]);

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
                Check::err(TapeError::NodeStale.into()),
            ],
        );
    }

    #[test]
    fn test_rejoin_after_advance_pool() {
        // Test that re-joining succeeds after AdvancePool
        // Node IS in current committee, latest_advance_epoch == epoch.id
        let fee_payer = Pubkey::new_unique();
        let authority = Pubkey::new_unique();

        let (node_address, _) = node_pda(authority);
        let (system_address, _) = system_pda();
        let (epoch_address, _) = epoch_pda();

        let instruction = build_join_network_ix(fee_payer, authority, node_address);

        let mut system = System::zeroed();
        let mut epoch = Epoch::zeroed();
        let mut node = Node::zeroed();

        epoch.id = EpochNumber(10);

        node.id = NodeId(5);
        node.authority = authority;
        // Fresh stake from AdvancePool
        node.pool.stake = TAPE(5_000);
        node.pool.shares = ShareAmount(5_000);
        // CURRENT: AdvancePool was called this epoch
        node.latest_advance_epoch = EpochNumber(10);
        node.preferences = NodePreferences {
            storage_price: TAPE(10),
            storage_capacity: StorageUnits(1_000_000),
        };

        // Node IS in current committee
        system.committee = Committee::from_members(&[
            member(5, 3_000),  // Our node (old stake in committee)
            member(6, 2_000),
        ]);

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            sol(authority, 0),
            pda(system_address, system.pack(), tapedrive::ID),
            pda(epoch_address, epoch.pack(), tapedrive::ID),
            pda(node_address, node.pack(), tapedrive::ID),
        ];

        // Expected state: committee_next has node with stake = 5000
        let joined_member = CommitteeMember {
            id: node.id,
            stake: TAPE(5_000),
            key: node.metadata.bls_pubkey,
            blacklist: node.blacklist.total_size(),
            preferences: node.preferences.clone(),
            ..CommitteeMember::zeroed()
        };

        system
            .committee_next
            .try_join(&joined_member)
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
    fn test_rejoin_uses_fresh_stake() {
        // Test that re-join uses fresh pool stake, not stale committee stake
        // Node in committee with old stake, but pool has fresh stake from rewards
        let fee_payer = Pubkey::new_unique();
        let authority = Pubkey::new_unique();

        let (node_address, _) = node_pda(authority);
        let (system_address, _) = system_pda();
        let (epoch_address, _) = epoch_pda();

        let instruction = build_join_network_ix(fee_payer, authority, node_address);

        let mut system = System::zeroed();
        let mut epoch = Epoch::zeroed();
        let mut node = Node::zeroed();

        epoch.id = EpochNumber(10);

        node.id = NodeId(5);
        node.authority = authority;
        // Fresh stake including rewards from AdvancePool
        node.pool.stake = TAPE(6_000);
        node.pool.shares = ShareAmount(6_000);
        // CURRENT: AdvancePool was called this epoch
        node.latest_advance_epoch = EpochNumber(10);
        node.preferences = NodePreferences {
            storage_price: TAPE(10),
            storage_capacity: StorageUnits(1_000_000),
        };

        // Node IS in current committee with STALE stake of 1000
        system.committee = Committee::from_members(&[
            member(5, 1_000),  // Our node with stale stake
            member(6, 2_000),
        ]);

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            sol(authority, 0),
            pda(system_address, system.pack(), tapedrive::ID),
            pda(epoch_address, epoch.pack(), tapedrive::ID),
            pda(node_address, node.pack(), tapedrive::ID),
        ];

        // Expected: committee_next has node with fresh stake = 6000 (not stale 1000)
        let joined_member = CommitteeMember {
            id: node.id,
            stake: TAPE(6_000),  // Fresh stake from pool, not 1000 from committee
            key: node.metadata.bls_pubkey,
            blacklist: node.blacklist.total_size(),
            preferences: node.preferences.clone(),
            ..CommitteeMember::zeroed()
        };

        system
            .committee_next
            .try_join(&joined_member)
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
    fn test_returning_node_treated_as_new() {
        // Test that a returning node (NOT in current committee) takes NEW JOIN path
        // Should succeed even with stale latest_advance_epoch
        let fee_payer = Pubkey::new_unique();
        let authority = Pubkey::new_unique();

        let (node_address, _) = node_pda(authority);
        let (system_address, _) = system_pda();
        let (epoch_address, _) = epoch_pda();

        let instruction = build_join_network_ix(fee_payer, authority, node_address);

        let mut system = System::zeroed();
        let mut epoch = Epoch::zeroed();
        let mut node = Node::zeroed();

        epoch.id = EpochNumber(10);

        node.id = NodeId(5);
        node.authority = authority;
        node.pool.stake = TAPE(2_000);
        node.pool.shares = ShareAmount(2_000);
        // OLD: Node hasn't called AdvancePool in a while
        node.latest_advance_epoch = EpochNumber(7);
        // Schedule some stake for activation
        node.pool.schedule.stake(EpochNumber(11), TAPE(500)).unwrap();
        node.preferences = NodePreferences {
            storage_price: TAPE(10),
            storage_capacity: StorageUnits(1_000_000),
        };

        // Node was in previous committee but NOT in current committee
        system.committee_prev = Committee::from_members(&[
            member(5, 2_000),  // Our node was here
            member(6, 1_500),
        ]);
        // Current committee does NOT include our node
        system.committee = Committee::from_members(&[
            member(7, 3_000),
            member(8, 2_500),
        ]);

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            sol(authority, 0),
            pda(system_address, system.pack(), tapedrive::ID),
            pda(epoch_address, epoch.pack(), tapedrive::ID),
            pda(node_address, node.pack(), tapedrive::ID),
        ];

        // Expected: NEW JOIN path - uses calculate_stake_at(activation_epoch)
        // activation_epoch = 11, scheduled stake activates at E+2 = 12
        // So stake at epoch 11 = current stake only = 2000
        let activation_epoch = EpochNumber(11);
        let balance = node.pool.calculate_stake_at(activation_epoch);

        let joined_member = CommitteeMember {
            id: node.id,
            stake: balance,
            key: node.metadata.bls_pubkey,
            blacklist: node.blacklist.total_size(),
            preferences: node.preferences.clone(),
            ..CommitteeMember::zeroed()
        };

        system
            .committee_next
            .try_join(&joined_member)
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
}
