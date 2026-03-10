use tape_solana::*;
use tape_api::prelude::*;
use crate::error::*;

pub fn process_advance_pool(accounts: &[AccountInfo<'_>], data: &[u8]) -> ProgramResult {
    let _args = AdvancePool::try_from_bytes(data)?;
    let [
        fee_payer_info,
        _authority_info,  // Not required to be signer - permissionless

        system_info,
        archive_info,
        epoch_info,
        node_info,
        history_info,
    ] = accounts else {
        return Err(ProgramError::NotEnoughAccountKeys);
    };

    // AdvancePool is fully permissionless - anyone can advance any pool
    // This allows stake withdrawals to not be locked if a node goes offline
    // Safety: idempotent (AlreadyAdvanced), deterministic rewards from prior epoch state
    fee_payer_info
        .is_signer()?
        .is_writable()?;
    // authority_info is NOT required to be signer - permissionless advancement

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

    // Cannot advance pool during Syncing phase - wait for settling
    if epoch.state.is_syncing() {
        return Err(TapeError::BadEpochState.into());
    }

    // Check if already advanced this epoch
    if node.latest_advance_epoch >= epoch.id {
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

    // Validate rewards only when they should exist
    if rewards_owed.is_zero() {
        // Skip validation if no previous committee
        if system.committee_prev_empty() {
            // OK: First epoch, no rewards expected
        } else if archive.recent_usage.is_zero() {
            // OK: No usage, no rewards to distribute
        } else if system.committee_prev.index_of(&node.id).is_some() {
            // ERROR: Node was in committee with usage, should have rewards
            return Err(TapeError::NoRewards.into());
        }
        // Otherwise: Node wasn't in committee, zero rewards expected
    }

    // Update node

    node.latest_advance_epoch = current_epoch(epoch);
    node.pool
        .advance_epoch(current_epoch(epoch), rewards_owed)
        .map_err(|_| ProgramError::Custom(1))?;

    // Update history

    let new_rate = node.pool
        .get_current_rate();

    history.latest_epoch = current_epoch(epoch);
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
    if epoch.state.is_settling() {
        if system.committee_prev_empty() {
            // First epoch: immediately transition to active
            epoch.state.set_active();
        } else if let Some(member_index) = system.committee_prev.index_of(&node.id) {
            let weight = system.spools_prev.spools_for_member(member_index).len() as u64;
            epoch.state.add_advanced_weight(weight, SPOOL_COUNT as u64);
        }
    }

    PoolAdvanced {
        node: *node_info.key,
        id: node.id,
        epoch: current_epoch(epoch),
        phase: epoch.state.phase,
    }.log();

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
            blacklist: StorageUnits::mb(bl),
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
        epoch.state.set_settling();

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
            latest_advance_epoch: EpochNumber(6),
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
            &[500, 300, 200]
        ).unwrap();

        archive.rewards_pool = TAPE(10_000);
        archive.recent_usage = StorageUnits::mb(1_000);
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

        node.latest_advance_epoch = e0;
        node.pool
            .advance_epoch(e0, rewards_owed)
            .expect("advance epoch");

        let new_rate = node.pool.get_current_rate();
        node.metadata.bls_pubkey = node.metadata.next_bls_pubkey;

        history.inner.push(e0, new_rate);
        history.latest_epoch = e0;

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
        epoch.state.set_settling();

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
            latest_advance_epoch: EpochNumber(6),
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
        system.spools_prev = SpoolAssignment::try_from_counts(&[700, 200, 100]).unwrap();

        archive.rewards_pool = TAPE(10_000);
        archive.recent_usage = StorageUnits::mb(1_000);

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
        epoch.state.set_active();

        node.latest_advance_epoch = e0;
        node.pool.advance_epoch(e0, rewards_owed).unwrap();
        node.metadata.bls_pubkey = node.metadata.next_bls_pubkey;

        history.inner.push(e0, node.pool.get_current_rate());
        history.latest_epoch = e0;

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
        // and immediately transition to active
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
        epoch.state.set_settling();

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
            latest_advance_epoch: EpochNumber(1),
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
        archive.recent_usage = StorageUnits::mb(1_000);
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

        node.latest_advance_epoch = e0;
        node.pool.advance_epoch(e0, rewards_owed).unwrap();
        node.metadata.bls_pubkey = node.metadata.next_bls_pubkey;

        history.inner.push(e0, node.pool.get_current_rate());
        history.latest_epoch = node.latest_advance_epoch;

        // Epoch should transition to active immediately
        epoch.state.set_active();

        // Archive should NOT have rewards_paid updated (empty committee_prev)

        // Archive rewards_paid should remain 0 since committee_prev is empty
        let expected_archive = Archive {
            rewards_pool: TAPE(10_000),
            rewards_paid: TAPE(0),  // Unchanged - no rewards paid (committee_prev empty)
            recent_usage: StorageUnits::mb(1_000),
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
    fn test_syncing_blocks_advance() {
        // Test that advancing during syncing state is blocked (regardless of committee size)
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
        epoch.state = EpochState::syncing(); // Should always block advance_pool

        // Even with small committee, syncing blocks advancement
        system.committee = Committee::from_members(&[
            member(2, 1_000, 0),
        ]);
        system.committee_prev = Committee::from_members(&[
            member(2, 1_000, 0),
        ]);
        system.spools_prev = SpoolAssignment::try_from_counts(&[SPOOL_COUNT as u16]).unwrap();

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
            latest_advance_epoch: EpochNumber(4),
            ..Node::zeroed()
        };

        let history = History {
            node: pool_address,
            latest_epoch: EpochNumber(4),
            inner: PoolHistory::new(),
            ..History::zeroed()
        };

        archive.rewards_pool = TAPE(10_000);
        archive.recent_usage = StorageUnits::mb(1_000);
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

        // Syncing state should block advancement with BadEpochState error
        let env = test_env();
        env.process_instruction(
            &instruction,
            &accounts,
            &[
                Check::err(TapeError::BadEpochState.into()),
            ],
        );
    }
}
