use tape_solana::*;
use tape_api::prelude::*;
use crate::error::*;

pub fn process_advance_pool(accounts: &[AccountInfo<'_>], data: &[u8]) -> ProgramResult {
    let _args = AdvancePool::try_from_bytes(data)?;
    let [
        fee_payer_info,
        authority_info,

        system_info,
        archive_info,
        epoch_info,
        node_info,
        history_info,
    ] = accounts else {
        return Err(ProgramError::NotEnoughAccountKeys);
    };

    // fee_payer does not need to be the pool authority
    fee_payer_info
        .is_signer()?
        .is_writable()?;
    authority_info
        .is_signer()?;

    let system = system_info
        .is_system()?
        .as_account::<System>(&tapedrive::ID)?;

    let archive = archive_info
        .is_writable()?
        .is_archive()?
        .as_account_mut::<Archive>(&tapedrive::ID)?;

    let epoch = epoch_info
        .is_writable()?
        .is_epoch()?
        .as_account_mut::<Epoch>(&tapedrive::ID)?;

    let node = node_info
        .is_writable()?
        .as_account_mut::<Node>(&tapedrive::ID)?;

    let history = history_info
        .is_writable()?
        .as_account_mut::<History>(&tapedrive::ID)?
        .assert_mut(|h| h.node == *node_info.key)?;

    // Skip syncing check during low-quorum mode
    if !system.is_low_quorum() && epoch.state.is_syncing() {
        return Err(TapeError::BadEpochState.into());
    }

    // If this pool is already updated for this epoch, can't advance again
    if node.latest_epoch >= epoch.id {
        return Err(TapeError::AlreadyAdvanced.into());
    }

    // Rotate BLS key if needed
    if node.metadata.bls_pubkey.ne(&node.metadata.next_bls_pubkey) {
        node.metadata.bls_pubkey = node.metadata.next_bls_pubkey;
    }

    // No rewards if prev committee is empty (first pool / first epoch)
    let rewards_owed = if system.committee_prev_empty() {
        TAPE::zero()
    } else {
        calc_rewards(
            node.id,
            archive.recent_usage,
            &system.committee_prev,
            &system.spools_prev,
            archive.rewards_pool
        )
    };

    // Only error on zero rewards if prev committee exists and node was in it
    if rewards_owed.is_zero() && !system.committee_prev_empty() {
        if system.committee_prev.index_of(&node.id).is_some() {
            return Err(TapeError::NoRewards.into());
        }
    }

    // Update node

    node.latest_epoch = current_epoch(epoch);
    node.pool
        .advance_epoch(current_epoch(epoch), rewards_owed)
        .map_err(|_| ProgramError::Custom(1))?;

    // Update history

    let new_rate = node.pool
        .get_current_rate();

    history.latest_epoch = node.latest_epoch;
    history.inner.push(current_epoch(epoch), new_rate);

    // Archive Reward Tracking
    if !system.committee_prev_empty() && !rewards_owed.is_zero() {
        let rewards_paid = archive.rewards_paid
            .saturating_add(rewards_owed.into());

        if rewards_paid > archive.rewards_pool {
            return Err(TapeError::RewardsOverflow.into());
        }

        archive.rewards_paid = rewards_paid;
    }

    // State Transition
    if epoch.state.is_active() {
        if system.committee_prev_empty() {
            // First epoch: immediately transition to next_ready
            epoch.state.set_next_ready();
        } else if let Some(member_index) = system.committee_prev.index_of(&node.id) {
            let weight = system.spools_prev.spools_for_member(member_index).len() as u64;
            epoch.state.add_advanced_weight(weight, SLICE_COUNT as u64);
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tape_test::*;

    fn member(id: u64, stake: u64, bl: u64) -> CommitteeMember {
        CommitteeMember {
            id: NodeId(id),
            stake: TAPE(stake),
            blacklist: StorageUnits(bl),
            ..CommitteeMember::zeroed()
        }
    }

    #[test]
    fn test_advance_pool() {
        let fee_payer = Pubkey::new_unique();
        let authority = Pubkey::new_unique();
        let pool_owner = Pubkey::new_unique();

        let (system_address, _) = system_pda();
        let (archive_address, _) = archive_pda();
        let (epoch_address, _) = epoch_pda();
        let (pool_address, _) = node_pda(pool_owner);
        let (history_address, _) = history_pda(pool_address);

        let instruction = build_advance_pool_ix(fee_payer, authority, pool_address);

        let mut system = System::zeroed();
        let mut archive = Archive::zeroed();
        let mut epoch = Epoch::zeroed();

        epoch.id = EpochNumber(7);
        epoch.state.set_active();

        // Minimal pool setup: non-zero stake/shares so rewards can be applied
        let mut node = Node {
            id: NodeId(2),
            authority: pool_owner,
            pool: StakingPool {
                stake: TAPE(1_000),
                shares: ShareAmount(1_000),
                commission_rate: BasisPoints(0),
                ..StakingPool::zeroed()
            },
            metadata: NodeMetadata {
                bls_pubkey: BlsPubkey::new_unique(),
                next_bls_pubkey: BlsPubkey::new_unique(),
                ..NodeMetadata::zeroed()
            },
            latest_epoch: EpochNumber(6),
            ..Node::zeroed()
        };
        let mut history = History {
            node: pool_address,
            latest_epoch: EpochNumber(6),
            inner: PoolHistory::new(),
            ..History::zeroed()
        };

        // Previous committee/spools used by calc_rewards
        system.committee_prev = Committee::from_members(&[
            member(node.id.into(), 3_000, 0),
            member(3, 2_000, 0),
            member(5, 1_000, 0),
        ]);

        system.spools_prev = SpoolAssignment::try_from_counts(
            &[500, 300, 224]
        ).unwrap();

        archive.rewards_pool = TAPE(10_000);
        archive.recent_usage = StorageUnits(1_000);
        archive.rewards_paid = TAPE(0);

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            sol(authority, 0),
            pda(system_address, system.pack(), tapedrive::ID),
            pda(archive_address, archive.pack(), tapedrive::ID),
            pda(epoch_address, epoch.pack(), tapedrive::ID),
            pda(pool_address, node.pack(), tapedrive::ID),
            pda(history_address, history.pack(), tapedrive::ID),
        ];

        // Expected state after instruction
        let e0 = epoch.id;

        let rewards_owed = calc_rewards(
            node.id,
            archive.recent_usage,
            &system.committee_prev,
            &system.spools_prev,
            archive.rewards_pool,
        );

        archive.rewards_paid = archive
            .rewards_paid
            .saturating_add(rewards_owed.into());

        node.latest_epoch = e0;
        node.pool
            .advance_epoch(e0, rewards_owed)
            .expect("advance epoch");

        let new_rate = node.pool.get_current_rate();
        node.metadata.bls_pubkey = node.metadata.next_bls_pubkey;

        history.inner.push(e0, new_rate);
        history.latest_epoch = node.latest_epoch;

        let env = test_env();
        env.process_instruction(
            &instruction,
            &accounts,
            &[
                Check::success(),
                Check::account(&archive_address)
                    .data(archive.pack().as_ref())
                    .build(),
                Check::account(&pool_address)
                    .data(node.pack().as_ref())
                    .build(),
                Check::account(&history_address)
                    .data(history.pack().as_ref())
                    .build(),
            ],
        );
    }

    #[test]
    fn active_to_next() {
        let fee_payer = Pubkey::new_unique();
        let authority = Pubkey::new_unique();
        let pool_owner = Pubkey::new_unique();

        let (system_address, _) = system_pda();
        let (archive_address, _) = archive_pda();
        let (epoch_address, _) = epoch_pda();
        let (pool_address, _) = node_pda(pool_owner);
        let (history_address, _) = history_pda(pool_address);

        let instruction = build_advance_pool_ix(fee_payer, authority, pool_address);

        let mut system = System::zeroed();
        let mut archive = Archive::zeroed();
        let mut epoch = Epoch::zeroed();

        epoch.id = EpochNumber(7);
        epoch.state.set_active();

        let mut node = Node {
            id: NodeId(2),
            authority: pool_owner,
            pool: StakingPool {
                stake: TAPE(1_000),
                shares: ShareAmount(1_000),
                commission_rate: BasisPoints(0),
                ..StakingPool::zeroed()
            },
            metadata: NodeMetadata {
                bls_pubkey: BlsPubkey::new_unique(),
                next_bls_pubkey: BlsPubkey::new_unique(),
                ..NodeMetadata::zeroed()
            },
            latest_epoch: EpochNumber(6),
            ..Node::zeroed()
        };

        let mut history = History {
            node: pool_address,
            latest_epoch: EpochNumber(6),
            inner: PoolHistory::new(),
            ..History::zeroed()
        };

        // Give node enough spools to trigger transition (needs > 683 for supermajority)
        system.committee_prev = Committee::from_members(&[
            member(node.id.into(), 3_000, 0),
            member(3, 2_000, 0),
            member(5, 1_000, 0),
        ]);
        // Node (id=2) gets 700 spools, others get less - node at index 0 after sort
        system.spools_prev = SpoolAssignment::try_from_counts(&[700, 200, 124]).unwrap();

        archive.rewards_pool = TAPE(10_000);
        archive.recent_usage = StorageUnits(1_000);

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            sol(authority, 0),
            pda(system_address, system.pack(), tapedrive::ID),
            pda(archive_address, archive.pack(), tapedrive::ID),
            pda(epoch_address, epoch.pack(), tapedrive::ID),
            pda(pool_address, node.pack(), tapedrive::ID),
            pda(history_address, history.pack(), tapedrive::ID),
        ];

        // Expected state after instruction
        let e0 = epoch.id;
        let rewards_owed = calc_rewards(
            node.id,
            archive.recent_usage,
            &system.committee_prev,
            &system.spools_prev,
            archive.rewards_pool,
        );

        archive.rewards_paid = rewards_owed.into();

        // 700 spools > 683 threshold, so should transition
        epoch.state.set_next_ready();

        node.latest_epoch = e0;
        node.pool.advance_epoch(e0, rewards_owed).unwrap();
        node.metadata.bls_pubkey = node.metadata.next_bls_pubkey;

        history.inner.push(e0, node.pool.get_current_rate());
        history.latest_epoch = node.latest_epoch;

        let env = test_env();
        env.process_instruction(
            &instruction,
            &accounts,
            &[
                Check::success(),
                Check::account(&epoch_address)
                    .data(epoch.pack().as_ref())
                    .build(),
                Check::account(&archive_address)
                    .data(archive.pack().as_ref())
                    .build(),
                Check::account(&pool_address)
                    .data(node.pack().as_ref())
                    .build(),
                Check::account(&history_address)
                    .data(history.pack().as_ref())
                    .build(),
            ],
        );
    }

    #[test]
    fn test_first_epoch_advance() {
        // Test that in the first epoch (empty committee_prev), we skip rewards
        // and immediately transition to next_ready
        let fee_payer = Pubkey::new_unique();
        let authority = Pubkey::new_unique();
        let pool_owner = Pubkey::new_unique();

        let (system_address, _) = system_pda();
        let (archive_address, _) = archive_pda();
        let (epoch_address, _) = epoch_pda();
        let (pool_address, _) = node_pda(pool_owner);
        let (history_address, _) = history_pda(pool_address);

        let instruction = build_advance_pool_ix(fee_payer, authority, pool_address);

        let mut system = System::zeroed();
        let mut archive = Archive::zeroed();
        let mut epoch = Epoch::zeroed();

        epoch.id = EpochNumber(2);
        epoch.state.set_active();

        // Empty previous committee (first epoch after bootstrap)
        system.committee_prev = Committee::new();
        // Current committee has only this node (low-quorum)
        system.committee = Committee::from_members(&[
            member(2, 1_000, 0),
        ]);

        let mut node = Node {
            id: NodeId(2),
            authority: pool_owner,
            pool: StakingPool {
                stake: TAPE(1_000),
                shares: ShareAmount(1_000),
                commission_rate: BasisPoints(0),
                ..StakingPool::zeroed()
            },
            metadata: NodeMetadata {
                bls_pubkey: BlsPubkey::new_unique(),
                next_bls_pubkey: BlsPubkey::new_unique(),
                ..NodeMetadata::zeroed()
            },
            latest_epoch: EpochNumber(1),
            ..Node::zeroed()
        };

        let mut history = History {
            node: pool_address,
            latest_epoch: EpochNumber(1),
            inner: PoolHistory::new(),
            ..History::zeroed()
        };

        // Even though there's a rewards pool, we should not pay out
        // because committee_prev is empty
        archive.rewards_pool = TAPE(10_000);
        archive.recent_usage = StorageUnits(1_000);
        archive.rewards_paid = TAPE(0);

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            sol(authority, 0),
            pda(system_address, system.pack(), tapedrive::ID),
            pda(archive_address, archive.pack(), tapedrive::ID),
            pda(epoch_address, epoch.pack(), tapedrive::ID),
            pda(pool_address, node.pack(), tapedrive::ID),
            pda(history_address, history.pack(), tapedrive::ID),
        ];

        // Expected state after instruction
        let e0 = epoch.id;

        // rewards_owed should be zero (committee_prev empty)
        let rewards_owed = TAPE::zero();

        node.latest_epoch = e0;
        node.pool.advance_epoch(e0, rewards_owed).unwrap();
        node.metadata.bls_pubkey = node.metadata.next_bls_pubkey;

        history.inner.push(e0, node.pool.get_current_rate());
        history.latest_epoch = node.latest_epoch;

        // Epoch should transition to next_ready immediately
        epoch.state.set_next_ready();

        // Archive should NOT have rewards_paid updated (empty committee_prev)

        // Archive rewards_paid should remain 0 since committee_prev is empty
        let expected_archive = Archive {
            rewards_pool: TAPE(10_000),
            rewards_paid: TAPE(0),  // Unchanged - no rewards paid (committee_prev empty)
            recent_usage: StorageUnits(1_000),
            ..Archive::zeroed()
        };

        let env = test_env();
        env.process_instruction(
            &instruction,
            &accounts,
            &[
                Check::success(),
                Check::account(&epoch_address)
                    .data(epoch.pack().as_ref())
                    .build(),
                Check::account(&pool_address)
                    .data(node.pack().as_ref())
                    .build(),
                Check::account(&archive_address)
                    .data(expected_archive.pack().as_ref())
                    .build(),
                Check::account(&history_address)
                    .data(history.pack().as_ref())
                    .build(),
            ],
        );
    }

    #[test]
    fn test_low_quorum_syncing_allowed() {
        // Test that in low-quorum mode, advancing during syncing state is allowed
        let fee_payer = Pubkey::new_unique();
        let authority = Pubkey::new_unique();
        let pool_owner = Pubkey::new_unique();

        let (system_address, _) = system_pda();
        let (archive_address, _) = archive_pda();
        let (epoch_address, _) = epoch_pda();
        let (pool_address, _) = node_pda(pool_owner);
        let (history_address, _) = history_pda(pool_address);

        let instruction = build_advance_pool_ix(fee_payer, authority, pool_address);

        let mut system = System::zeroed();
        let mut archive = Archive::zeroed();
        let mut epoch = Epoch::zeroed();

        epoch.id = EpochNumber(5);
        epoch.state = EpochState::syncing(); // Normally would block advance_pool

        // Small committee (low-quorum mode)
        system.committee = Committee::from_members(&[
            member(2, 1_000, 0),
        ]);
        // Small prev committee
        system.committee_prev = Committee::from_members(&[
            member(2, 1_000, 0),
        ]);
        system.spools_prev = SpoolAssignment::try_from_counts(&[SLICE_COUNT as u16]).unwrap();

        let node = Node {
            id: NodeId(2),
            authority: pool_owner,
            pool: StakingPool {
                stake: TAPE(1_000),
                shares: ShareAmount(1_000),
                commission_rate: BasisPoints(0),
                ..StakingPool::zeroed()
            },
            metadata: NodeMetadata {
                bls_pubkey: BlsPubkey::new_unique(),
                next_bls_pubkey: BlsPubkey::new_unique(),
                ..NodeMetadata::zeroed()
            },
            latest_epoch: EpochNumber(4),
            ..Node::zeroed()
        };

        let history = History {
            node: pool_address,
            latest_epoch: EpochNumber(4),
            inner: PoolHistory::new(),
            ..History::zeroed()
        };

        archive.rewards_pool = TAPE(10_000);
        archive.recent_usage = StorageUnits(1_000);
        archive.rewards_paid = TAPE(0);

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            sol(authority, 0),
            pda(system_address, system.pack(), tapedrive::ID),
            pda(archive_address, archive.pack(), tapedrive::ID),
            pda(epoch_address, epoch.pack(), tapedrive::ID),
            pda(pool_address, node.pack(), tapedrive::ID),
            pda(history_address, history.pack(), tapedrive::ID),
        ];

        // Expected state after instruction
        let rewards_owed = calc_rewards(
            node.id,
            archive.recent_usage,
            &system.committee_prev,
            &system.spools_prev,
            archive.rewards_pool,
        );

        let mut expected_node = node.clone();
        expected_node.latest_epoch = EpochNumber(5);
        expected_node.pool.advance_epoch(EpochNumber(5), rewards_owed).unwrap();
        expected_node.metadata.bls_pubkey = expected_node.metadata.next_bls_pubkey;

        let mut expected_archive = archive.clone();
        expected_archive.rewards_paid = rewards_owed.into();

        let mut expected_history = history.clone();
        expected_history.latest_epoch = EpochNumber(5);
        expected_history.inner.push(EpochNumber(5), expected_node.pool.get_current_rate());

        // In low-quorum mode, syncing check is skipped
        let env = test_env();
        env.process_instruction(
            &instruction,
            &accounts,
            &[
                Check::success(),
                Check::account(&pool_address)
                    .data(expected_node.pack().as_ref())
                    .build(),
                Check::account(&archive_address)
                    .data(expected_archive.pack().as_ref())
                    .build(),
                Check::account(&history_address)
                    .data(expected_history.pack().as_ref())
                    .build(),
            ],
        );
    }

    #[test]
    fn test_not_in_prev_no_rewards() {
        // Node NOT in committee_prev should succeed but with no rewards
        let fee_payer = Pubkey::new_unique();
        let authority = Pubkey::new_unique();
        let pool_owner = Pubkey::new_unique();

        let (system_address, _) = system_pda();
        let (archive_address, _) = archive_pda();
        let (epoch_address, _) = epoch_pda();
        let (pool_address, _) = node_pda(pool_owner);
        let (history_address, _) = history_pda(pool_address);

        let instruction = build_advance_pool_ix(fee_payer, authority, pool_address);

        let mut system = System::zeroed();
        let mut archive = Archive::zeroed();
        let mut epoch = Epoch::zeroed();

        epoch.id = EpochNumber(7);
        epoch.state.set_active();

        // Node is NOT in committee_prev (node.id = 99, but prev has ids 2, 3, 5)
        let mut node = Node {
            id: NodeId(99),
            authority: pool_owner,
            pool: StakingPool {
                stake: TAPE(1_000),
                shares: ShareAmount(1_000),
                commission_rate: BasisPoints(0),
                ..StakingPool::zeroed()
            },
            metadata: NodeMetadata {
                bls_pubkey: BlsPubkey::new_unique(),
                next_bls_pubkey: BlsPubkey::new_unique(),
                ..NodeMetadata::zeroed()
            },
            latest_epoch: EpochNumber(6),
            ..Node::zeroed()
        };

        let mut history = History {
            node: pool_address,
            latest_epoch: EpochNumber(6),
            inner: PoolHistory::new(),
            ..History::zeroed()
        };

        // committee_prev does not include node 99
        system.committee_prev = Committee::from_members(&[
            member(2, 3_000, 0),
            member(3, 2_000, 0),
            member(5, 1_000, 0),
        ]);
        system.spools_prev = SpoolAssignment::try_from_counts(&[500, 300, 224]).unwrap();

        archive.rewards_pool = TAPE(10_000);
        archive.recent_usage = StorageUnits(1_000);
        archive.rewards_paid = TAPE(0);

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            sol(authority, 0),
            pda(system_address, system.pack(), tapedrive::ID),
            pda(archive_address, archive.pack(), tapedrive::ID),
            pda(epoch_address, epoch.pack(), tapedrive::ID),
            pda(pool_address, node.pack(), tapedrive::ID),
            pda(history_address, history.pack(), tapedrive::ID),
        ];

        // Expected: node advances with zero rewards, no epoch state change
        let e0 = epoch.id;
        let rewards_owed = TAPE::zero(); // Not in committee_prev

        node.latest_epoch = e0;
        node.pool.advance_epoch(e0, rewards_owed).unwrap();
        node.metadata.bls_pubkey = node.metadata.next_bls_pubkey;

        history.inner.push(e0, node.pool.get_current_rate());
        history.latest_epoch = node.latest_epoch;

        // archive.rewards_paid should remain 0 (no rewards paid)

        let env = test_env();
        env.process_instruction(
            &instruction,
            &accounts,
            &[
                Check::success(),
                Check::account(&pool_address)
                    .data(node.pack().as_ref())
                    .build(),
                Check::account(&archive_address)
                    .data(archive.pack().as_ref())
                    .build(),
                Check::account(&history_address)
                    .data(history.pack().as_ref())
                    .build(),
            ],
        );
    }

    #[test]
    fn test_in_prev_zero_spools() {
        // Node in committee_prev but with 0 spools should fail with NoRewards
        let fee_payer = Pubkey::new_unique();
        let authority = Pubkey::new_unique();
        let pool_owner = Pubkey::new_unique();

        let (system_address, _) = system_pda();
        let (archive_address, _) = archive_pda();
        let (epoch_address, _) = epoch_pda();
        let (pool_address, _) = node_pda(pool_owner);
        let (history_address, _) = history_pda(pool_address);

        let instruction = build_advance_pool_ix(fee_payer, authority, pool_address);

        let mut system = System::zeroed();
        let archive = Archive::zeroed();
        let mut epoch = Epoch::zeroed();

        epoch.id = EpochNumber(7);
        epoch.state.set_active();

        let node = Node {
            id: NodeId(2),
            authority: pool_owner,
            pool: StakingPool {
                stake: TAPE(1_000),
                shares: ShareAmount(1_000),
                commission_rate: BasisPoints(0),
                ..StakingPool::zeroed()
            },
            metadata: NodeMetadata {
                bls_pubkey: BlsPubkey::new_unique(),
                next_bls_pubkey: BlsPubkey::new_unique(),
                ..NodeMetadata::zeroed()
            },
            latest_epoch: EpochNumber(6),
            ..Node::zeroed()
        };

        let history = History {
            node: pool_address,
            latest_epoch: EpochNumber(6),
            inner: PoolHistory::new(),
            ..History::zeroed()
        };

        // Node 2 is at index 0 in committee, but gets 0 spools
        system.committee_prev = Committee::from_members(&[
            member(2, 1_000, 0),  // node at index 0
            member(3, 5_000, 0),
        ]);
        // All 1024 spools go to index 1 (node 3), node 2 gets 0
        system.spools_prev = SpoolAssignment::try_from_counts(&[0, SLICE_COUNT as u16]).unwrap();

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            sol(authority, 0),
            pda(system_address, system.pack(), tapedrive::ID),
            pda(archive_address, archive.pack(), tapedrive::ID),
            pda(epoch_address, epoch.pack(), tapedrive::ID),
            pda(pool_address, node.pack(), tapedrive::ID),
            pda(history_address, history.pack(), tapedrive::ID),
        ];

        let env = test_env();
        env.process_instruction(
            &instruction,
            &accounts,
            &[
                Check::err(TapeError::NoRewards.into()),
            ],
        );
    }

    #[test]
    fn test_next_ready_state() {
        // Advance when epoch state is NextReady (not Active) should succeed
        let fee_payer = Pubkey::new_unique();
        let authority = Pubkey::new_unique();
        let pool_owner = Pubkey::new_unique();

        let (system_address, _) = system_pda();
        let (archive_address, _) = archive_pda();
        let (epoch_address, _) = epoch_pda();
        let (pool_address, _) = node_pda(pool_owner);
        let (history_address, _) = history_pda(pool_address);

        let instruction = build_advance_pool_ix(fee_payer, authority, pool_address);

        let mut system = System::zeroed();
        let mut archive = Archive::zeroed();
        let mut epoch = Epoch::zeroed();

        epoch.id = EpochNumber(7);
        epoch.state.set_next_ready(); // NextReady state

        let mut node = Node {
            id: NodeId(2),
            authority: pool_owner,
            pool: StakingPool {
                stake: TAPE(1_000),
                shares: ShareAmount(1_000),
                commission_rate: BasisPoints(0),
                ..StakingPool::zeroed()
            },
            metadata: NodeMetadata {
                bls_pubkey: BlsPubkey::new_unique(),
                next_bls_pubkey: BlsPubkey::new_unique(),
                ..NodeMetadata::zeroed()
            },
            latest_epoch: EpochNumber(6),
            ..Node::zeroed()
        };

        let mut history = History {
            node: pool_address,
            latest_epoch: EpochNumber(6),
            inner: PoolHistory::new(),
            ..History::zeroed()
        };

        // Full committee (>= 24 members) so not low-quorum
        let mut members = Vec::new();
        for i in 0..24 {
            members.push(member(i + 1, 1_000, 0));
        }
        // Add our node (id=2 is already in at index 1)
        system.committee = Committee::from_members(&members);
        system.committee_prev = Committee::from_members(&[
            member(2, 3_000, 0),
            member(3, 2_000, 0),
            member(5, 1_000, 0),
        ]);
        system.spools_prev = SpoolAssignment::try_from_counts(&[500, 300, 224]).unwrap();

        archive.rewards_pool = TAPE(10_000);
        archive.recent_usage = StorageUnits(1_000);
        archive.rewards_paid = TAPE(0);

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            sol(authority, 0),
            pda(system_address, system.pack(), tapedrive::ID),
            pda(archive_address, archive.pack(), tapedrive::ID),
            pda(epoch_address, epoch.pack(), tapedrive::ID),
            pda(pool_address, node.pack(), tapedrive::ID),
            pda(history_address, history.pack(), tapedrive::ID),
        ];

        let e0 = epoch.id;
        let rewards_owed = calc_rewards(
            node.id,
            archive.recent_usage,
            &system.committee_prev,
            &system.spools_prev,
            archive.rewards_pool,
        );

        archive.rewards_paid = rewards_owed.into();

        node.latest_epoch = e0;
        node.pool.advance_epoch(e0, rewards_owed).unwrap();
        node.metadata.bls_pubkey = node.metadata.next_bls_pubkey;

        history.inner.push(e0, node.pool.get_current_rate());
        history.latest_epoch = node.latest_epoch;

        // Epoch state should NOT change (weight only added in Active state)

        let env = test_env();
        env.process_instruction(
            &instruction,
            &accounts,
            &[
                Check::success(),
                Check::account(&pool_address)
                    .data(node.pack().as_ref())
                    .build(),
                Check::account(&archive_address)
                    .data(archive.pack().as_ref())
                    .build(),
                Check::account(&history_address)
                    .data(history.pack().as_ref())
                    .build(),
            ],
        );
    }

    #[test]
    fn test_syncing_normal_fails() {
        // Advance when epoch state is Syncing in normal mode should fail
        let fee_payer = Pubkey::new_unique();
        let authority = Pubkey::new_unique();
        let pool_owner = Pubkey::new_unique();

        let (system_address, _) = system_pda();
        let (archive_address, _) = archive_pda();
        let (epoch_address, _) = epoch_pda();
        let (pool_address, _) = node_pda(pool_owner);
        let (history_address, _) = history_pda(pool_address);

        let instruction = build_advance_pool_ix(fee_payer, authority, pool_address);

        let mut system = System::zeroed();
        let archive = Archive::zeroed();
        let mut epoch = Epoch::zeroed();

        epoch.id = EpochNumber(7);
        epoch.state = EpochState::syncing();

        let node = Node {
            id: NodeId(2),
            authority: pool_owner,
            pool: StakingPool {
                stake: TAPE(1_000),
                shares: ShareAmount(1_000),
                ..StakingPool::zeroed()
            },
            latest_epoch: EpochNumber(6),
            ..Node::zeroed()
        };

        let history = History {
            node: pool_address,
            latest_epoch: EpochNumber(6),
            inner: PoolHistory::new(),
            ..History::zeroed()
        };

        // Full committee (>= 24 members) so NOT low-quorum
        let mut members = Vec::new();
        for i in 0..24 {
            members.push(member(i + 1, 1_000, 0));
        }
        system.committee = Committee::from_members(&members);

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            sol(authority, 0),
            pda(system_address, system.pack(), tapedrive::ID),
            pda(archive_address, archive.pack(), tapedrive::ID),
            pda(epoch_address, epoch.pack(), tapedrive::ID),
            pda(pool_address, node.pack(), tapedrive::ID),
            pda(history_address, history.pack(), tapedrive::ID),
        ];

        let env = test_env();
        env.process_instruction(
            &instruction,
            &accounts,
            &[
                Check::err(TapeError::BadEpochState.into()),
            ],
        );
    }

    #[test]
    fn test_already_advanced() {
        // Node tries to advance when already advanced this epoch
        let fee_payer = Pubkey::new_unique();
        let authority = Pubkey::new_unique();
        let pool_owner = Pubkey::new_unique();

        let (system_address, _) = system_pda();
        let (archive_address, _) = archive_pda();
        let (epoch_address, _) = epoch_pda();
        let (pool_address, _) = node_pda(pool_owner);
        let (history_address, _) = history_pda(pool_address);

        let instruction = build_advance_pool_ix(fee_payer, authority, pool_address);

        let system = System::zeroed();
        let archive = Archive::zeroed();
        let mut epoch = Epoch::zeroed();

        epoch.id = EpochNumber(7);
        epoch.state.set_active();

        // Node already advanced this epoch
        let node = Node {
            id: NodeId(2),
            authority: pool_owner,
            pool: StakingPool {
                stake: TAPE(1_000),
                shares: ShareAmount(1_000),
                ..StakingPool::zeroed()
            },
            latest_epoch: EpochNumber(7), // Already at epoch 7
            ..Node::zeroed()
        };

        let history = History {
            node: pool_address,
            latest_epoch: EpochNumber(7),
            inner: PoolHistory::new(),
            ..History::zeroed()
        };

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            sol(authority, 0),
            pda(system_address, system.pack(), tapedrive::ID),
            pda(archive_address, archive.pack(), tapedrive::ID),
            pda(epoch_address, epoch.pack(), tapedrive::ID),
            pda(pool_address, node.pack(), tapedrive::ID),
            pda(history_address, history.pack(), tapedrive::ID),
        ];

        let env = test_env();
        env.process_instruction(
            &instruction,
            &accounts,
            &[
                Check::err(TapeError::AlreadyAdvanced.into()),
            ],
        );
    }

    #[test]
    fn test_bls_key_no_rotation() {
        // Node where bls_pubkey == next_bls_pubkey (no rotation needed)
        let fee_payer = Pubkey::new_unique();
        let authority = Pubkey::new_unique();
        let pool_owner = Pubkey::new_unique();

        let (system_address, _) = system_pda();
        let (archive_address, _) = archive_pda();
        let (epoch_address, _) = epoch_pda();
        let (pool_address, _) = node_pda(pool_owner);
        let (history_address, _) = history_pda(pool_address);

        let instruction = build_advance_pool_ix(fee_payer, authority, pool_address);

        let mut system = System::zeroed();
        let mut archive = Archive::zeroed();
        let mut epoch = Epoch::zeroed();

        epoch.id = EpochNumber(7);
        epoch.state.set_active();

        let same_bls_key = BlsPubkey::new_unique();

        let mut node = Node {
            id: NodeId(2),
            authority: pool_owner,
            pool: StakingPool {
                stake: TAPE(1_000),
                shares: ShareAmount(1_000),
                commission_rate: BasisPoints(0),
                ..StakingPool::zeroed()
            },
            metadata: NodeMetadata {
                bls_pubkey: same_bls_key,
                next_bls_pubkey: same_bls_key, // Same key - no rotation
                ..NodeMetadata::zeroed()
            },
            latest_epoch: EpochNumber(6),
            ..Node::zeroed()
        };

        let mut history = History {
            node: pool_address,
            latest_epoch: EpochNumber(6),
            inner: PoolHistory::new(),
            ..History::zeroed()
        };

        system.committee_prev = Committee::from_members(&[
            member(node.id.into(), 3_000, 0),
            member(3, 2_000, 0),
            member(5, 1_000, 0),
        ]);
        system.spools_prev = SpoolAssignment::try_from_counts(&[500, 300, 224]).unwrap();

        archive.rewards_pool = TAPE(10_000);
        archive.recent_usage = StorageUnits(1_000);

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            sol(authority, 0),
            pda(system_address, system.pack(), tapedrive::ID),
            pda(archive_address, archive.pack(), tapedrive::ID),
            pda(epoch_address, epoch.pack(), tapedrive::ID),
            pda(pool_address, node.pack(), tapedrive::ID),
            pda(history_address, history.pack(), tapedrive::ID),
        ];

        let e0 = epoch.id;
        let rewards_owed = calc_rewards(
            node.id,
            archive.recent_usage,
            &system.committee_prev,
            &system.spools_prev,
            archive.rewards_pool,
        );

        archive.rewards_paid = rewards_owed.into();

        node.latest_epoch = e0;
        node.pool.advance_epoch(e0, rewards_owed).unwrap();
        // bls_pubkey should remain the same (no rotation)

        history.inner.push(e0, node.pool.get_current_rate());
        history.latest_epoch = node.latest_epoch;

        let env = test_env();
        env.process_instruction(
            &instruction,
            &accounts,
            &[
                Check::success(),
                Check::account(&pool_address)
                    .data(node.pack().as_ref())
                    .build(),
            ],
        );
    }

    #[test]
    fn test_zero_rewards_pool() {
        // Test with 0 rewards_pool - should still succeed but with 0 rewards
        let fee_payer = Pubkey::new_unique();
        let authority = Pubkey::new_unique();
        let pool_owner = Pubkey::new_unique();

        let (system_address, _) = system_pda();
        let (archive_address, _) = archive_pda();
        let (epoch_address, _) = epoch_pda();
        let (pool_address, _) = node_pda(pool_owner);
        let (history_address, _) = history_pda(pool_address);

        let instruction = build_advance_pool_ix(fee_payer, authority, pool_address);

        let mut system = System::zeroed();
        let archive = Archive::zeroed();
        let mut epoch = Epoch::zeroed();

        epoch.id = EpochNumber(7);
        epoch.state.set_active();

        let node = Node {
            id: NodeId(2),
            authority: pool_owner,
            pool: StakingPool {
                stake: TAPE(1_000),
                shares: ShareAmount(1_000),
                ..StakingPool::zeroed()
            },
            metadata: NodeMetadata {
                bls_pubkey: BlsPubkey::new_unique(),
                next_bls_pubkey: BlsPubkey::new_unique(),
                ..NodeMetadata::zeroed()
            },
            latest_epoch: EpochNumber(6),
            ..Node::zeroed()
        };

        let history = History {
            node: pool_address,
            latest_epoch: EpochNumber(6),
            inner: PoolHistory::new(),
            ..History::zeroed()
        };

        system.committee_prev = Committee::from_members(&[
            member(node.id.into(), 3_000, 0),
            member(3, 2_000, 0),
        ]);
        system.spools_prev = SpoolAssignment::try_from_counts(&[500, 524]).unwrap();

        // Zero rewards pool and zero usage - calc_rewards returns 0
        // archive.rewards_pool = TAPE(0); // Already zeroed
        // archive.recent_usage = StorageUnits(0); // Already zeroed

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            sol(authority, 0),
            pda(system_address, system.pack(), tapedrive::ID),
            pda(archive_address, archive.pack(), tapedrive::ID),
            pda(epoch_address, epoch.pack(), tapedrive::ID),
            pda(pool_address, node.pack(), tapedrive::ID),
            pda(history_address, history.pack(), tapedrive::ID),
        ];

        // calc_rewards with 0 allocated returns 0, which triggers NoRewards for members in prev
        let env = test_env();
        env.process_instruction(
            &instruction,
            &accounts,
            &[
                Check::err(TapeError::NoRewards.into()),
            ],
        );
    }

    #[test]
    fn test_mismatched_history_node() {
        // History.node doesn't match the node account key
        let fee_payer = Pubkey::new_unique();
        let authority = Pubkey::new_unique();
        let pool_owner = Pubkey::new_unique();
        let wrong_node = Pubkey::new_unique();

        let (system_address, _) = system_pda();
        let (archive_address, _) = archive_pda();
        let (epoch_address, _) = epoch_pda();
        let (pool_address, _) = node_pda(pool_owner);
        let (history_address, _) = history_pda(pool_address);

        let instruction = build_advance_pool_ix(fee_payer, authority, pool_address);

        let system = System::zeroed();
        let archive = Archive::zeroed();
        let mut epoch = Epoch::zeroed();

        epoch.id = EpochNumber(7);
        epoch.state.set_active();

        let node = Node {
            id: NodeId(2),
            authority: pool_owner,
            pool: StakingPool {
                stake: TAPE(1_000),
                shares: ShareAmount(1_000),
                ..StakingPool::zeroed()
            },
            latest_epoch: EpochNumber(6),
            ..Node::zeroed()
        };

        // History points to a different node
        let history = History {
            node: wrong_node, // Wrong node!
            latest_epoch: EpochNumber(6),
            inner: PoolHistory::new(),
            ..History::zeroed()
        };

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            sol(authority, 0),
            pda(system_address, system.pack(), tapedrive::ID),
            pda(archive_address, archive.pack(), tapedrive::ID),
            pda(epoch_address, epoch.pack(), tapedrive::ID),
            pda(pool_address, node.pack(), tapedrive::ID),
            pda(history_address, history.pack(), tapedrive::ID),
        ];

        let env = test_env();
        // assert_mut returns InvalidAccountData when history.node != node_info.key
        env.process_instruction(
            &instruction,
            &accounts,
            &[
                Check::err(ProgramError::InvalidAccountData),
            ],
        );
    }

    #[test]
    fn test_weight_below_supermajority() {
        // Weight accumulation just below supermajority (no transition)
        let fee_payer = Pubkey::new_unique();
        let authority = Pubkey::new_unique();
        let pool_owner = Pubkey::new_unique();

        let (system_address, _) = system_pda();
        let (archive_address, _) = archive_pda();
        let (epoch_address, _) = epoch_pda();
        let (pool_address, _) = node_pda(pool_owner);
        let (history_address, _) = history_pda(pool_address);

        let instruction = build_advance_pool_ix(fee_payer, authority, pool_address);

        let mut system = System::zeroed();
        let mut archive = Archive::zeroed();
        let mut epoch = Epoch::zeroed();

        epoch.id = EpochNumber(7);
        epoch.state.set_active();

        let mut node = Node {
            id: NodeId(2),
            authority: pool_owner,
            pool: StakingPool {
                stake: TAPE(1_000),
                shares: ShareAmount(1_000),
                commission_rate: BasisPoints(0),
                ..StakingPool::zeroed()
            },
            metadata: NodeMetadata {
                bls_pubkey: BlsPubkey::new_unique(),
                next_bls_pubkey: BlsPubkey::new_unique(),
                ..NodeMetadata::zeroed()
            },
            latest_epoch: EpochNumber(6),
            ..Node::zeroed()
        };

        let mut history = History {
            node: pool_address,
            latest_epoch: EpochNumber(6),
            inner: PoolHistory::new(),
            ..History::zeroed()
        };

        // Node gets 300 spools - below supermajority threshold (683)
        // Stakes sorted descending: NodeId(2) at 5000 (index 0), NodeId(3) at 3000 (index 1), NodeId(5) at 1000 (index 2)
        system.committee_prev = Committee::from_members(&[
            member(node.id.into(), 5_000, 0),  // highest stake -> index 0
            member(3, 3_000, 0),
            member(5, 1_000, 0),
        ]);
        system.spools_prev = SpoolAssignment::try_from_counts(&[300, 500, 224]).unwrap();

        archive.rewards_pool = TAPE(10_000);
        archive.recent_usage = StorageUnits(1_000);

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            sol(authority, 0),
            pda(system_address, system.pack(), tapedrive::ID),
            pda(archive_address, archive.pack(), tapedrive::ID),
            pda(epoch_address, epoch.pack(), tapedrive::ID),
            pda(pool_address, node.pack(), tapedrive::ID),
            pda(history_address, history.pack(), tapedrive::ID),
        ];

        let e0 = epoch.id;
        let rewards_owed = calc_rewards(
            node.id,
            archive.recent_usage,
            &system.committee_prev,
            &system.spools_prev,
            archive.rewards_pool,
        );

        archive.rewards_paid = rewards_owed.into();

        // Epoch state should remain Active with weight = 300
        epoch.state = EpochState {
            phase: EpochPhase::Active.into(),
            weight: 300,
        };

        node.latest_epoch = e0;
        node.pool.advance_epoch(e0, rewards_owed).unwrap();
        node.metadata.bls_pubkey = node.metadata.next_bls_pubkey;

        history.inner.push(e0, node.pool.get_current_rate());
        history.latest_epoch = node.latest_epoch;

        let env = test_env();
        env.process_instruction(
            &instruction,
            &accounts,
            &[
                Check::success(),
                Check::account(&epoch_address)
                    .data(epoch.pack().as_ref())
                    .build(),
                Check::account(&pool_address)
                    .data(node.pack().as_ref())
                    .build(),
            ],
        );
    }

    #[test]
    fn test_rewards_overflow() {
        // Test rewards_paid overflow protection
        let fee_payer = Pubkey::new_unique();
        let authority = Pubkey::new_unique();
        let pool_owner = Pubkey::new_unique();

        let (system_address, _) = system_pda();
        let (archive_address, _) = archive_pda();
        let (epoch_address, _) = epoch_pda();
        let (pool_address, _) = node_pda(pool_owner);
        let (history_address, _) = history_pda(pool_address);

        let instruction = build_advance_pool_ix(fee_payer, authority, pool_address);

        let mut system = System::zeroed();
        let mut archive = Archive::zeroed();
        let mut epoch = Epoch::zeroed();

        epoch.id = EpochNumber(7);
        epoch.state.set_active();

        let node = Node {
            id: NodeId(2),
            authority: pool_owner,
            pool: StakingPool {
                stake: TAPE(1_000),
                shares: ShareAmount(1_000),
                ..StakingPool::zeroed()
            },
            metadata: NodeMetadata {
                bls_pubkey: BlsPubkey::new_unique(),
                next_bls_pubkey: BlsPubkey::new_unique(),
                ..NodeMetadata::zeroed()
            },
            latest_epoch: EpochNumber(6),
            ..Node::zeroed()
        };

        let history = History {
            node: pool_address,
            latest_epoch: EpochNumber(6),
            inner: PoolHistory::new(),
            ..History::zeroed()
        };

        system.committee_prev = Committee::from_members(&[
            member(node.id.into(), 3_000, 0),
        ]);
        system.spools_prev = SpoolAssignment::try_from_counts(&[SLICE_COUNT as u16]).unwrap();

        // Set rewards_paid to just under rewards_pool, so new rewards would overflow
        archive.rewards_pool = TAPE(10_000);
        archive.rewards_paid = TAPE(9_999); // Almost at limit
        archive.recent_usage = StorageUnits(1_000);

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            sol(authority, 0),
            pda(system_address, system.pack(), tapedrive::ID),
            pda(archive_address, archive.pack(), tapedrive::ID),
            pda(epoch_address, epoch.pack(), tapedrive::ID),
            pda(pool_address, node.pack(), tapedrive::ID),
            pda(history_address, history.pack(), tapedrive::ID),
        ];

        // calc_rewards should return ~1000 (all rewards to single node)
        // Adding to 9999 would exceed 10000, triggering overflow error
        let env = test_env();
        env.process_instruction(
            &instruction,
            &accounts,
            &[
                Check::err(TapeError::RewardsOverflow.into()),
            ],
        );
    }

    #[test]
    fn test_second_node_triggers_transition() {
        // Second node advancing triggers supermajority transition
        let fee_payer = Pubkey::new_unique();
        let authority = Pubkey::new_unique();
        let pool_owner = Pubkey::new_unique();

        let (system_address, _) = system_pda();
        let (archive_address, _) = archive_pda();
        let (epoch_address, _) = epoch_pda();
        let (pool_address, _) = node_pda(pool_owner);
        let (history_address, _) = history_pda(pool_address);

        let instruction = build_advance_pool_ix(fee_payer, authority, pool_address);

        let mut system = System::zeroed();
        let mut archive = Archive::zeroed();
        let mut epoch = Epoch::zeroed();

        epoch.id = EpochNumber(7);
        // Start with some weight accumulated from first node
        epoch.state = EpochState {
            phase: EpochPhase::Active.into(),
            weight: 400, // First node already advanced with 400 spools
        };

        let mut node = Node {
            id: NodeId(2),
            authority: pool_owner,
            pool: StakingPool {
                stake: TAPE(1_000),
                shares: ShareAmount(1_000),
                commission_rate: BasisPoints(0),
                ..StakingPool::zeroed()
            },
            metadata: NodeMetadata {
                bls_pubkey: BlsPubkey::new_unique(),
                next_bls_pubkey: BlsPubkey::new_unique(),
                ..NodeMetadata::zeroed()
            },
            latest_epoch: EpochNumber(6),
            ..Node::zeroed()
        };

        let mut history = History {
            node: pool_address,
            latest_epoch: EpochNumber(6),
            inner: PoolHistory::new(),
            ..History::zeroed()
        };

        // Node 2 at index 0 gets 300 spools; 400 + 300 = 700 > 683 threshold
        system.committee_prev = Committee::from_members(&[
            member(node.id.into(), 2_000, 0),
            member(3, 3_000, 0),
            member(5, 1_000, 0),
        ]);
        system.spools_prev = SpoolAssignment::try_from_counts(&[300, 500, 224]).unwrap();

        archive.rewards_pool = TAPE(10_000);
        archive.recent_usage = StorageUnits(1_000);

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            sol(authority, 0),
            pda(system_address, system.pack(), tapedrive::ID),
            pda(archive_address, archive.pack(), tapedrive::ID),
            pda(epoch_address, epoch.pack(), tapedrive::ID),
            pda(pool_address, node.pack(), tapedrive::ID),
            pda(history_address, history.pack(), tapedrive::ID),
        ];

        let e0 = epoch.id;
        let rewards_owed = calc_rewards(
            node.id,
            archive.recent_usage,
            &system.committee_prev,
            &system.spools_prev,
            archive.rewards_pool,
        );

        archive.rewards_paid = rewards_owed.into();

        // Should transition to NextReady (400 + 300 = 700 > 683)
        epoch.state.set_next_ready();

        node.latest_epoch = e0;
        node.pool.advance_epoch(e0, rewards_owed).unwrap();
        node.metadata.bls_pubkey = node.metadata.next_bls_pubkey;

        history.inner.push(e0, node.pool.get_current_rate());
        history.latest_epoch = node.latest_epoch;

        let env = test_env();
        env.process_instruction(
            &instruction,
            &accounts,
            &[
                Check::success(),
                Check::account(&epoch_address)
                    .data(epoch.pack().as_ref())
                    .build(),
                Check::account(&pool_address)
                    .data(node.pack().as_ref())
                    .build(),
            ],
        );
    }

    #[test]
    fn test_exact_supermajority_threshold() {
        // Transition at exact supermajority threshold (683 for 1024)
        let fee_payer = Pubkey::new_unique();
        let authority = Pubkey::new_unique();
        let pool_owner = Pubkey::new_unique();

        let (system_address, _) = system_pda();
        let (archive_address, _) = archive_pda();
        let (epoch_address, _) = epoch_pda();
        let (pool_address, _) = node_pda(pool_owner);
        let (history_address, _) = history_pda(pool_address);

        let instruction = build_advance_pool_ix(fee_payer, authority, pool_address);

        let mut system = System::zeroed();
        let mut archive = Archive::zeroed();
        let mut epoch = Epoch::zeroed();

        epoch.id = EpochNumber(7);
        epoch.state.set_active();

        let mut node = Node {
            id: NodeId(2),
            authority: pool_owner,
            pool: StakingPool {
                stake: TAPE(1_000),
                shares: ShareAmount(1_000),
                commission_rate: BasisPoints(0),
                ..StakingPool::zeroed()
            },
            metadata: NodeMetadata {
                bls_pubkey: BlsPubkey::new_unique(),
                next_bls_pubkey: BlsPubkey::new_unique(),
                ..NodeMetadata::zeroed()
            },
            latest_epoch: EpochNumber(6),
            ..Node::zeroed()
        };

        let mut history = History {
            node: pool_address,
            latest_epoch: EpochNumber(6),
            inner: PoolHistory::new(),
            ..History::zeroed()
        };

        // Give node exactly 683 spools (supermajority threshold for 1024)
        // 3 * 683 = 2049 >= 2 * 1024 + 1 = 2049 (exactly at threshold)
        system.committee_prev = Committee::from_members(&[
            member(node.id.into(), 3_000, 0),
            member(3, 2_000, 0),
        ]);
        system.spools_prev = SpoolAssignment::try_from_counts(&[683, 341]).unwrap();

        archive.rewards_pool = TAPE(10_000);
        archive.recent_usage = StorageUnits(1_000);

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            sol(authority, 0),
            pda(system_address, system.pack(), tapedrive::ID),
            pda(archive_address, archive.pack(), tapedrive::ID),
            pda(epoch_address, epoch.pack(), tapedrive::ID),
            pda(pool_address, node.pack(), tapedrive::ID),
            pda(history_address, history.pack(), tapedrive::ID),
        ];

        let e0 = epoch.id;
        let rewards_owed = calc_rewards(
            node.id,
            archive.recent_usage,
            &system.committee_prev,
            &system.spools_prev,
            archive.rewards_pool,
        );

        archive.rewards_paid = rewards_owed.into();

        // Should transition to NextReady at exact threshold
        epoch.state.set_next_ready();

        node.latest_epoch = e0;
        node.pool.advance_epoch(e0, rewards_owed).unwrap();
        node.metadata.bls_pubkey = node.metadata.next_bls_pubkey;

        history.inner.push(e0, node.pool.get_current_rate());
        history.latest_epoch = node.latest_epoch;

        let env = test_env();
        env.process_instruction(
            &instruction,
            &accounts,
            &[
                Check::success(),
                Check::account(&epoch_address)
                    .data(epoch.pack().as_ref())
                    .build(),
                Check::account(&pool_address)
                    .data(node.pack().as_ref())
                    .build(),
            ],
        );
    }

    #[test]
    fn test_just_below_threshold() {
        // Weight just below supermajority (682 for 1024) - no transition
        let fee_payer = Pubkey::new_unique();
        let authority = Pubkey::new_unique();
        let pool_owner = Pubkey::new_unique();

        let (system_address, _) = system_pda();
        let (archive_address, _) = archive_pda();
        let (epoch_address, _) = epoch_pda();
        let (pool_address, _) = node_pda(pool_owner);
        let (history_address, _) = history_pda(pool_address);

        let instruction = build_advance_pool_ix(fee_payer, authority, pool_address);

        let mut system = System::zeroed();
        let mut archive = Archive::zeroed();
        let mut epoch = Epoch::zeroed();

        epoch.id = EpochNumber(7);
        epoch.state.set_active();

        let mut node = Node {
            id: NodeId(2),
            authority: pool_owner,
            pool: StakingPool {
                stake: TAPE(1_000),
                shares: ShareAmount(1_000),
                commission_rate: BasisPoints(0),
                ..StakingPool::zeroed()
            },
            metadata: NodeMetadata {
                bls_pubkey: BlsPubkey::new_unique(),
                next_bls_pubkey: BlsPubkey::new_unique(),
                ..NodeMetadata::zeroed()
            },
            latest_epoch: EpochNumber(6),
            ..Node::zeroed()
        };

        let mut history = History {
            node: pool_address,
            latest_epoch: EpochNumber(6),
            inner: PoolHistory::new(),
            ..History::zeroed()
        };

        // Give node exactly 682 spools (just below supermajority threshold)
        // 3 * 682 = 2046 < 2 * 1024 + 1 = 2049 (below threshold)
        system.committee_prev = Committee::from_members(&[
            member(node.id.into(), 3_000, 0),
            member(3, 2_000, 0),
        ]);
        system.spools_prev = SpoolAssignment::try_from_counts(&[682, 342]).unwrap();

        archive.rewards_pool = TAPE(10_000);
        archive.recent_usage = StorageUnits(1_000);

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            sol(authority, 0),
            pda(system_address, system.pack(), tapedrive::ID),
            pda(archive_address, archive.pack(), tapedrive::ID),
            pda(epoch_address, epoch.pack(), tapedrive::ID),
            pda(pool_address, node.pack(), tapedrive::ID),
            pda(history_address, history.pack(), tapedrive::ID),
        ];

        let e0 = epoch.id;
        let rewards_owed = calc_rewards(
            node.id,
            archive.recent_usage,
            &system.committee_prev,
            &system.spools_prev,
            archive.rewards_pool,
        );

        archive.rewards_paid = rewards_owed.into();

        // Should NOT transition - remain Active with weight = 682
        epoch.state = EpochState {
            phase: EpochPhase::Active.into(),
            weight: 682,
        };

        node.latest_epoch = e0;
        node.pool.advance_epoch(e0, rewards_owed).unwrap();
        node.metadata.bls_pubkey = node.metadata.next_bls_pubkey;

        history.inner.push(e0, node.pool.get_current_rate());
        history.latest_epoch = node.latest_epoch;

        let env = test_env();
        env.process_instruction(
            &instruction,
            &accounts,
            &[
                Check::success(),
                Check::account(&epoch_address)
                    .data(epoch.pack().as_ref())
                    .build(),
                Check::account(&pool_address)
                    .data(node.pack().as_ref())
                    .build(),
            ],
        );
    }

    #[test]
    fn test_huge_rewards_pool() {
        // Test with very large rewards_pool value
        let fee_payer = Pubkey::new_unique();
        let authority = Pubkey::new_unique();
        let pool_owner = Pubkey::new_unique();

        let (system_address, _) = system_pda();
        let (archive_address, _) = archive_pda();
        let (epoch_address, _) = epoch_pda();
        let (pool_address, _) = node_pda(pool_owner);
        let (history_address, _) = history_pda(pool_address);

        let instruction = build_advance_pool_ix(fee_payer, authority, pool_address);

        let mut system = System::zeroed();
        let mut archive = Archive::zeroed();
        let mut epoch = Epoch::zeroed();

        epoch.id = EpochNumber(7);
        epoch.state.set_active();

        let mut node = Node {
            id: NodeId(2),
            authority: pool_owner,
            pool: StakingPool {
                stake: TAPE(1_000_000_000_000), // Large stake
                shares: ShareAmount(1_000_000_000_000),
                commission_rate: BasisPoints(0),
                ..StakingPool::zeroed()
            },
            metadata: NodeMetadata {
                bls_pubkey: BlsPubkey::new_unique(),
                next_bls_pubkey: BlsPubkey::new_unique(),
                ..NodeMetadata::zeroed()
            },
            latest_epoch: EpochNumber(6),
            ..Node::zeroed()
        };

        let mut history = History {
            node: pool_address,
            latest_epoch: EpochNumber(6),
            inner: PoolHistory::new(),
            ..History::zeroed()
        };

        system.committee_prev = Committee::from_members(&[
            member(node.id.into(), 3_000, 0),
        ]);
        system.spools_prev = SpoolAssignment::try_from_counts(&[SLICE_COUNT as u16]).unwrap();

        // Very large rewards pool (max u64 would be ~18.4 quintillion)
        archive.rewards_pool = TAPE(1_000_000_000_000_000); // 1 quadrillion
        archive.recent_usage = StorageUnits(1_000_000);

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            sol(authority, 0),
            pda(system_address, system.pack(), tapedrive::ID),
            pda(archive_address, archive.pack(), tapedrive::ID),
            pda(epoch_address, epoch.pack(), tapedrive::ID),
            pda(pool_address, node.pack(), tapedrive::ID),
            pda(history_address, history.pack(), tapedrive::ID),
        ];

        let e0 = epoch.id;
        let rewards_owed = calc_rewards(
            node.id,
            archive.recent_usage,
            &system.committee_prev,
            &system.spools_prev,
            archive.rewards_pool,
        );

        archive.rewards_paid = rewards_owed.into();
        epoch.state.set_next_ready();

        node.latest_epoch = e0;
        node.pool.advance_epoch(e0, rewards_owed).unwrap();
        node.metadata.bls_pubkey = node.metadata.next_bls_pubkey;

        history.inner.push(e0, node.pool.get_current_rate());
        history.latest_epoch = node.latest_epoch;

        let env = test_env();
        env.process_instruction(
            &instruction,
            &accounts,
            &[
                Check::success(),
                Check::account(&pool_address)
                    .data(node.pack().as_ref())
                    .build(),
                Check::account(&archive_address)
                    .data(archive.pack().as_ref())
                    .build(),
            ],
        );
    }

    #[test]
    fn test_unknown_epoch_state() {
        // Advance when epoch state is Unknown (phase = 0)
        let fee_payer = Pubkey::new_unique();
        let authority = Pubkey::new_unique();
        let pool_owner = Pubkey::new_unique();

        let (system_address, _) = system_pda();
        let (archive_address, _) = archive_pda();
        let (epoch_address, _) = epoch_pda();
        let (pool_address, _) = node_pda(pool_owner);
        let (history_address, _) = history_pda(pool_address);

        let instruction = build_advance_pool_ix(fee_payer, authority, pool_address);

        let mut system = System::zeroed();
        let mut archive = Archive::zeroed();
        let mut epoch = Epoch::zeroed();

        epoch.id = EpochNumber(7);
        // epoch.state is zeroed = Unknown phase

        let mut node = Node {
            id: NodeId(2),
            authority: pool_owner,
            pool: StakingPool {
                stake: TAPE(1_000),
                shares: ShareAmount(1_000),
                commission_rate: BasisPoints(0),
                ..StakingPool::zeroed()
            },
            metadata: NodeMetadata {
                bls_pubkey: BlsPubkey::new_unique(),
                next_bls_pubkey: BlsPubkey::new_unique(),
                ..NodeMetadata::zeroed()
            },
            latest_epoch: EpochNumber(6),
            ..Node::zeroed()
        };

        let mut history = History {
            node: pool_address,
            latest_epoch: EpochNumber(6),
            inner: PoolHistory::new(),
            ..History::zeroed()
        };

        // Empty committee_prev (first epoch scenario)
        system.committee_prev = Committee::new();

        archive.rewards_pool = TAPE(10_000);
        archive.recent_usage = StorageUnits(1_000);

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            sol(authority, 0),
            pda(system_address, system.pack(), tapedrive::ID),
            pda(archive_address, archive.pack(), tapedrive::ID),
            pda(epoch_address, epoch.pack(), tapedrive::ID),
            pda(pool_address, node.pack(), tapedrive::ID),
            pda(history_address, history.pack(), tapedrive::ID),
        ];

        let e0 = epoch.id;
        let rewards_owed = TAPE::zero(); // committee_prev empty

        node.latest_epoch = e0;
        node.pool.advance_epoch(e0, rewards_owed).unwrap();
        node.metadata.bls_pubkey = node.metadata.next_bls_pubkey;

        history.inner.push(e0, node.pool.get_current_rate());
        history.latest_epoch = node.latest_epoch;

        // Unknown state is not Active, so no state transition happens
        // (epoch.state remains zeroed/Unknown)

        let env = test_env();
        env.process_instruction(
            &instruction,
            &accounts,
            &[
                Check::success(),
                Check::account(&pool_address)
                    .data(node.pack().as_ref())
                    .build(),
                Check::account(&history_address)
                    .data(history.pack().as_ref())
                    .build(),
            ],
        );
    }

    #[test]
    fn test_blacklisted_member() {
        // Node in committee_prev but fully blacklisted - should fail with NoRewards
        let fee_payer = Pubkey::new_unique();
        let authority = Pubkey::new_unique();
        let pool_owner = Pubkey::new_unique();

        let (system_address, _) = system_pda();
        let (archive_address, _) = archive_pda();
        let (epoch_address, _) = epoch_pda();
        let (pool_address, _) = node_pda(pool_owner);
        let (history_address, _) = history_pda(pool_address);

        let instruction = build_advance_pool_ix(fee_payer, authority, pool_address);

        let mut system = System::zeroed();
        let mut archive = Archive::zeroed();
        let mut epoch = Epoch::zeroed();

        epoch.id = EpochNumber(7);
        epoch.state.set_active();

        let node = Node {
            id: NodeId(2),
            authority: pool_owner,
            pool: StakingPool {
                stake: TAPE(1_000),
                shares: ShareAmount(1_000),
                ..StakingPool::zeroed()
            },
            metadata: NodeMetadata {
                bls_pubkey: BlsPubkey::new_unique(),
                next_bls_pubkey: BlsPubkey::new_unique(),
                ..NodeMetadata::zeroed()
            },
            latest_epoch: EpochNumber(6),
            ..Node::zeroed()
        };

        let history = History {
            node: pool_address,
            latest_epoch: EpochNumber(6),
            inner: PoolHistory::new(),
            ..History::zeroed()
        };

        // Node 2 is fully blacklisted (blacklist >= allocated)
        system.committee_prev = Committee::from_members(&[
            member(2, 3_000, 2_000), // blacklist = 2000 >= allocated = 1000
            member(3, 2_000, 0),
        ]);
        system.spools_prev = SpoolAssignment::try_from_counts(&[500, 524]).unwrap();

        archive.rewards_pool = TAPE(10_000);
        archive.recent_usage = StorageUnits(1_000); // allocated = 1000

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            sol(authority, 0),
            pda(system_address, system.pack(), tapedrive::ID),
            pda(archive_address, archive.pack(), tapedrive::ID),
            pda(epoch_address, epoch.pack(), tapedrive::ID),
            pda(pool_address, node.pack(), tapedrive::ID),
            pda(history_address, history.pack(), tapedrive::ID),
        ];

        // calc_rewards returns 0 when blacklist >= allocated
        let env = test_env();
        env.process_instruction(
            &instruction,
            &accounts,
            &[
                Check::err(TapeError::NoRewards.into()),
            ],
        );
    }

    #[test]
    fn test_partial_blacklist() {
        // Node in committee_prev with partial blacklist - reduced rewards
        let fee_payer = Pubkey::new_unique();
        let authority = Pubkey::new_unique();
        let pool_owner = Pubkey::new_unique();

        let (system_address, _) = system_pda();
        let (archive_address, _) = archive_pda();
        let (epoch_address, _) = epoch_pda();
        let (pool_address, _) = node_pda(pool_owner);
        let (history_address, _) = history_pda(pool_address);

        let instruction = build_advance_pool_ix(fee_payer, authority, pool_address);

        let mut system = System::zeroed();
        let mut archive = Archive::zeroed();
        let mut epoch = Epoch::zeroed();

        epoch.id = EpochNumber(7);
        epoch.state.set_active();

        let mut node = Node {
            id: NodeId(2),
            authority: pool_owner,
            pool: StakingPool {
                stake: TAPE(1_000),
                shares: ShareAmount(1_000),
                commission_rate: BasisPoints(0),
                ..StakingPool::zeroed()
            },
            metadata: NodeMetadata {
                bls_pubkey: BlsPubkey::new_unique(),
                next_bls_pubkey: BlsPubkey::new_unique(),
                ..NodeMetadata::zeroed()
            },
            latest_epoch: EpochNumber(6),
            ..Node::zeroed()
        };

        let mut history = History {
            node: pool_address,
            latest_epoch: EpochNumber(6),
            inner: PoolHistory::new(),
            ..History::zeroed()
        };

        // Node 2 has partial blacklist (500 < 1000 allocated)
        system.committee_prev = Committee::from_members(&[
            member(2, 3_000, 500), // partial blacklist
            member(3, 2_000, 0),
        ]);
        system.spools_prev = SpoolAssignment::try_from_counts(&[700, 324]).unwrap();

        archive.rewards_pool = TAPE(10_000);
        archive.recent_usage = StorageUnits(1_000);

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            sol(authority, 0),
            pda(system_address, system.pack(), tapedrive::ID),
            pda(archive_address, archive.pack(), tapedrive::ID),
            pda(epoch_address, epoch.pack(), tapedrive::ID),
            pda(pool_address, node.pack(), tapedrive::ID),
            pda(history_address, history.pack(), tapedrive::ID),
        ];

        let e0 = epoch.id;
        let rewards_owed = calc_rewards(
            node.id,
            archive.recent_usage,
            &system.committee_prev,
            &system.spools_prev,
            archive.rewards_pool,
        );

        archive.rewards_paid = rewards_owed.into();
        epoch.state.set_next_ready(); // 700 > 683 threshold

        node.latest_epoch = e0;
        node.pool.advance_epoch(e0, rewards_owed).unwrap();
        node.metadata.bls_pubkey = node.metadata.next_bls_pubkey;

        history.inner.push(e0, node.pool.get_current_rate());
        history.latest_epoch = node.latest_epoch;

        let env = test_env();
        env.process_instruction(
            &instruction,
            &accounts,
            &[
                Check::success(),
                Check::account(&pool_address)
                    .data(node.pack().as_ref())
                    .build(),
                Check::account(&archive_address)
                    .data(archive.pack().as_ref())
                    .build(),
            ],
        );
    }
}
