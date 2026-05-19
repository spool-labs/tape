use tape_solana::*;
use tape_api::program::prelude::*;
use tape_api::event::SpoolSettled;

pub fn process_settle_spool(accounts: &[AccountInfo<'_>], data: &[u8]) -> ProgramResult {
    let args = SettleSpool::try_from_bytes(data)?;
    let [
        fee_payer_info,
        system_info,
        archive_info,
        curr_epoch_info,
        prev_epoch_info,
        prev_group_info,
        pool_info,
    ] = accounts else {
        return Err(ProgramError::NotEnoughAccountKeys);
    };

    fee_payer_info
        .is_signer()?
        .is_writable()?;

    let system = system_info
        .is_system()?
        .as_account::<System>(&tapedrive::ID)?;

    let curr = system.current_epoch;
    let prev = curr.saturating_sub(EpochNumber(1));

    archive_info
        .is_writable()?
        .is_archive()?;

    let archive = archive_info.as_account_mut::<Archive>(&tapedrive::ID)?;

    let epoch = curr_epoch_info
        .is_writable()?
        .is_epoch(curr)?
        .as_account_mut::<Epoch>(&tapedrive::ID)?;

    let prev_epoch = prev_epoch_info
        .is_epoch(prev)?
        .as_account::<Epoch>(&tapedrive::ID)?;

    let spool = SpoolIndex::unpack(args.spool);
    let group_id = GroupIndex::containing(spool);

    let group = prev_group_info
        .is_writable()?
        .is_group(prev, group_id)?
        .as_account_mut::<Group>(&tapedrive::ID)?;

    let slice = group_id
        .position_of(spool)
        .ok_or(TapeError::BadSpoolHash)?;
    let slice_idx = slice;

    if group.settled.is_set(slice_idx) {
        return Err(TapeError::AlreadySettled.into());
    }

    let pool_address: Address = (*pool_info.key).into();
    let spool_owner = group.spools[slice_idx].node;
    let spool_size = group.size;
    if spool_owner != pool_address {
        return Err(TapeError::NotInCommittee.into());
    }

    let node = pool_info
        .is_writable()?
        .as_account_mut::<Node>(&tapedrive::ID)?;

    // Per-spool reward share: group.size / prev.total_assigned * rewards_pool.
    // `Archive.rewards_pool` is fixed for the epoch; any portion left unpaid
    // (e.g. nodes that never got settled) carries forward when `advance_epoch`
    // rolls `rewards_pool - rewards_paid` into the next epoch's pool.
    let share = compute_spool_share(
        spool_size,
        prev_epoch.total_assigned,
        archive.rewards_pool,
    );

    // Sum of shares should equal rewards_pool by construction
    let next_paid = archive.rewards_paid.0.saturating_add(share.0);
    if next_paid > archive.rewards_pool.0 {
        return Err(TapeError::RewardsOverflow.into());
    }

    archive.rewards_paid = TAPE(next_paid);
    node.pool.credit_spool(share);

    let was_supermajority = is_supermajority(
        group.settled.count_ones() as u64,
        GROUP_SIZE as u64,
    );
    group.settled.set(slice_idx);
    let now_supermajority = is_supermajority(
        group.settled.count_ones() as u64,
        GROUP_SIZE as u64,
    );

    // Bump the counter on every group's first supermajority crossing, but
    // only flip the phase if we're currently in Settle. Late settles arriving
    // in Snapshot/Active still increment settled_count for completeness;
    // the phase machine itself never regresses.
    if !was_supermajority && now_supermajority {
        epoch.state.settled_count = epoch.state.settled_count.saturating_add(1);
        if epoch.state.phase == EpochPhase::Settle as u64
            && epoch.state.settled_count == prev_epoch.total_groups
        {
            epoch.state.phase = EpochPhase::Snapshot as u64;
        }
    }

    SpoolSettled {
        node: pool_address,
        epoch: prev,
        group: group_id,
        spool: args.spool,
        phase: epoch.state.phase,
    }.log();

    Ok(())
}

fn compute_spool_share(
    spool_size: StorageUnits,
    total_assigned: StorageUnits,
    rewards_pool: Coin<TAPE>,
) -> Coin<TAPE> {
    let total = total_assigned.0;
    if total == 0 {
        return TAPE::zero();
    }
    let raw = (spool_size.0 as u128)
        .saturating_mul(rewards_pool.0 as u128)
        / total as u128;
    TAPE(raw as u64)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tape_test::*;

    // Crosses 2/3 supermajority on this call (13 prior bits + this one = 14).
    #[test]
    fn settle() {
        let fee_payer = Pubkey::new_unique();
        let authority = Pubkey::new_unique();

        let curr = EpochNumber(10);
        let prev = EpochNumber(9);
        let group_id = GroupIndex(0);
        let slice_in_group = 7usize;
        let spool = group_id.spool_at(slice_in_group);

        let (system_address, _) = system_pda();
        let (archive_address, _) = archive_pda();
        let (curr_epoch_address, _) = epoch_pda(curr);
        let (prev_epoch_address, _) = epoch_pda(prev);
        let (prev_group_address, _) = group_pda(prev, group_id);
        let (node_address, _) = node_pda(authority.into());

        let system = System {
            current_epoch: curr,
            ..System::zeroed()
        };

        let archive = Archive {
            rewards_pool: TAPE(1_000),
            rewards_paid: TAPE::zero(),
            ..Archive::zeroed()
        };

        let epoch = Epoch {
            id: curr,
            state: EpochState {
                phase: EpochPhase::Settle as u64,
                ..EpochState::zeroed()
            },
            ..Epoch::zeroed()
        };

        let prev_epoch_data = Epoch {
            id: prev,
            total_assigned: StorageUnits::mb(1_000),
            total_groups: 1,
            ..Epoch::zeroed()
        };

        // Pre-set 13 bits at slots ≠ 7 so this call lifts the count to 14/20.
        let mut group = Group::zeroed();
        group.id = group_id;
        group.epoch = prev;
        group.size = StorageUnits::mb(50);
        group.spools[slice_in_group] = Spool {
            node: node_address,
            bls_pubkey: BlsPubkey::zeroed(),
        };
        let mut pre_set = 0usize;
        for i in 0..GROUP_SIZE {
            if i == slice_in_group { continue; }
            if pre_set == 13 { break; }
            group.settled.set(i);
            pre_set += 1;
        }

        let node = Node {
            authority: authority.into(),
            ..Node::zeroed()
        };

        let instruction = build_settle_spool_ix(
            fee_payer.into(),
            node_address,
            curr,
            spool,
        );

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            pda(system_address, system.pack(), tapedrive::ID),
            pda(archive_address, archive.pack(), tapedrive::ID),
            pda(curr_epoch_address, epoch.pack(), tapedrive::ID),
            pda(prev_epoch_address, prev_epoch_data.pack(), tapedrive::ID),
            pda(prev_group_address, group.pack(), tapedrive::ID),
            pda(node_address, node.pack(), tapedrive::ID),
        ];

        // share = 50 MB / 1000 MB * 1000 TAPE = 50 TAPE
        let expected_share = TAPE(50);

        let mut expected_group = group;
        expected_group.settled.set(slice_in_group);

        let mut expected_epoch = epoch;
        expected_epoch.state.settled_count = 1;
        expected_epoch.state.phase = EpochPhase::Snapshot as u64;

        let mut expected_node = node;
        expected_node.pool.pending_rewards = expected_share;
        expected_node.pool.pending_settled = 1;

        let expected_archive = Archive {
            rewards_paid: expected_share,
            ..archive
        };

        let env = test_env();
        env.process_instruction(
            &instruction,
            &accounts,
            &[
                Check::success(),
                Check::account(&Pubkey::from(prev_group_address))
                    .data(expected_group.pack().as_ref())
                    .build(),
                Check::account(&Pubkey::from(curr_epoch_address))
                    .data(expected_epoch.pack().as_ref())
                    .build(),
                Check::account(&Pubkey::from(node_address))
                    .data(expected_node.pack().as_ref())
                    .build(),
                Check::account(&Pubkey::from(archive_address))
                    .data(expected_archive.pack().as_ref())
                    .build(),
            ],
        );
    }

    // Test if the instruction correctly rejects a case where the share would overflow the rewards pool.
    #[test]
    fn rejects_overflow() {
        let fee_payer = Pubkey::new_unique();
        let authority = Pubkey::new_unique();

        let curr = EpochNumber(10);
        let prev = EpochNumber(9);
        let group_id = GroupIndex(0);
        let slice_in_group = 7usize;
        let spool = group_id.spool_at(slice_in_group);

        let (system_address, _) = system_pda();
        let (archive_address, _) = archive_pda();
        let (curr_epoch_address, _) = epoch_pda(curr);
        let (prev_epoch_address, _) = epoch_pda(prev);
        let (prev_group_address, _) = group_pda(prev, group_id);
        let (node_address, _) = node_pda(authority.into());

        let system = System {
            current_epoch: curr,
            ..System::zeroed()
        };

        // Pool already drained 900 of 1000; a 200-TAPE share would overdraw.
        let archive = Archive {
            rewards_pool: TAPE(1_000),
            rewards_paid: TAPE(900),
            ..Archive::zeroed()
        };

        let epoch = Epoch {
            id: curr,
            state: EpochState {
                phase: EpochPhase::Settle as u64,
                ..EpochState::zeroed()
            },
            ..Epoch::zeroed()
        };

        // 20 MB / 100 MB * 1000 TAPE = 200 TAPE; combined with rewards_paid=900
        // that's 1100 — overdraws the 1000-TAPE pool.
        let prev_epoch_data = Epoch {
            id: prev,
            total_assigned: StorageUnits::mb(100),
            total_groups: 1,
            ..Epoch::zeroed()
        };

        let mut group = Group::zeroed();
        group.id = group_id;
        group.epoch = prev;
        group.size = StorageUnits::mb(20);
        group.spools[slice_in_group] = Spool {
            node: node_address,
            bls_pubkey: BlsPubkey::zeroed(),
        };

        let node = Node {
            authority: authority.into(),
            ..Node::zeroed()
        };

        let instruction = build_settle_spool_ix(
            fee_payer.into(),
            node_address,
            curr,
            spool,
        );

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            pda(system_address, system.pack(), tapedrive::ID),
            pda(archive_address, archive.pack(), tapedrive::ID),
            pda(curr_epoch_address, epoch.pack(), tapedrive::ID),
            pda(prev_epoch_address, prev_epoch_data.pack(), tapedrive::ID),
            pda(prev_group_address, group.pack(), tapedrive::ID),
            pda(node_address, node.pack(), tapedrive::ID),
        ];

        let env = test_env();
        env.process_instruction(
            &instruction,
            &accounts,
            &[Check::err(TapeError::RewardsOverflow.into())],
        );
    }
}
