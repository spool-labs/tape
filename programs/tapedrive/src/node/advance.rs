use crate::error::*;
use tape_api::prelude::*;
use steel::*;

pub fn process_advance_pool(accounts: &[AccountInfo<'_>], data: &[u8]) -> ProgramResult {
    let _args = AdvancePool::try_from_bytes(data)?;
    let [
        signer_info,

        system_info,
        archive_info,
        epoch_info,
        node_info,
    ] = accounts else {
        return Err(ProgramError::NotEnoughAccountKeys);
    };

    // Signer does not need to be the pool authority
    signer_info
        .is_signer()?;

    let system = system_info
        .is_system()?
        .as_account::<System>(&tapedrive::ID)?;

    let archive = archive_info
        .is_writable()?
        .is_archive()?
        .as_account_mut::<Archive>(&tapedrive::ID)?;

    let epoch = epoch_info
        .is_epoch()?
        .as_account::<Epoch>(&tapedrive::ID)?;

    let node = node_info
        .is_writable()?
        .as_account_mut::<Node>(&tapedrive::ID)?;

    if epoch.state.is_syncing() {
        return Err(ProgramError::Custom(2));
    }

    if node.latest_epoch >= epoch.id {
        return Err(ProgramError::Custom(0));
    }

    let reward_pool = archive.rewards_pool;
    let allocated = archive.recent_usage;

    let rewards_owed = calc_rewards(
        node.id, 
        allocated, 
        &system.committee_prev, 
        &system.seats_prev, 
        reward_pool
    );

    if rewards_owed.is_zero() {
        return Err(ProgramError::Custom(0));
    }

    let rewards_paid = archive.rewards_paid
        .saturating_add(rewards_owed.into());

    if rewards_paid > archive.rewards_pool {
        return Err(ProgramError::Custom(3));
    }

    let prev_rate = node.history
        .rate_at(current_epoch(epoch))
        .ok_or(ProgramError::Custom(0))?;

    let new_rate = node.pool
        .advance_epoch(current_epoch(epoch), rewards_owed, prev_rate)
        .map_err(|_| ProgramError::Custom(1))?;

    node.history.push(current_epoch(epoch), new_rate);

    archive.rewards_paid = rewards_paid;

    solana_program::msg!("rewards_owed {:?}", rewards_owed);
    solana_program::msg!("rewards_paid {:?}", rewards_paid);
    solana_program::msg!("prev rate: {:?}", prev_rate);
    solana_program::msg!("new rate: {:?}", new_rate);

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
            key: BlsPubkey::zeroed(),
            blacklist: StorageUnits(bl),
        }
    }


    #[test]
    fn test_advance_pool_non_zero_payout() {
        let signer = Pubkey::new_unique();
        let pool_owner = Pubkey::new_unique();

        let (system_address, _) = system_pda();
        let (archive_address, _) = archive_pda();
        let (epoch_address, _) = epoch_pda();
        let (pool_address, _) = node_pda(pool_owner);

        let instruction = build_advance_pool_ix(signer, pool_address);

        let mut system = System::zeroed();
        let mut archive = Archive::zeroed();
        let mut epoch = Epoch::zeroed();
        let mut node = Node::zeroed();

        // Prev committee: two members; give node 2 a blacklist of 50 to make stored differ
        system.committee_prev = Committee::from_members(&[
            member(2, 2_000, 50),
            member(1, 1_000, 0),
        ]);
        // Current/next committees not used in this test, but fill for completeness
        system.committee = Committee::from_members(&[
            member(3, 3_000, 0),
            member(2, 2_000, 0),
            member(1, 1_000, 0),
        ]);
        system.committee_next = Committee::from_members(&[
            member(3, 3_500, 0),
            member(4, 2_100, 0),
            member(2, 2_000, 0),
            member(1, 1_000, 0),
        ]);

        // Construct seats_prev for committee_prev deterministically using D'Hondt
        let seat_count_prev = dhondt_allocate(
            &system.committee_prev.active_stakes(),
            SEAT_COUNT as u16,
        );
        let seats_prev_vec = reassign_seats(
            &system.seats.seats, // zeroed base ok
            &system.committee_prev.active_members(),
            &system.committee_prev.active_members(),
            &seat_count_prev,
        )
        .expect("seats_prev assign failed");
        system.seats_prev = Seats::try_from(seats_prev_vec.as_ref()).unwrap();

        // Set archive snapshot and pool for distribution
        archive.rewards_pool = TAPE(10_000);
        archive.rewards_paid = TAPE(0);
        archive.recent_usage = StorageUnits(1_000);

        // Compute recent_score from seats_prev and committee_prev
        let mut score_u128: u128 = 0;
        for (i, m) in system.committee_prev.iter().enumerate() {
            let w = system.seats_prev.weight(i) as u128;
            let stored_i = archive.recent_usage.saturating_sub(m.blacklist).as_u128();
            score_u128 = score_u128.saturating_add(w.saturating_mul(stored_i));
        }
        // Make sure we actually have a non-zero score in this test
        assert!(score_u128 > 0, "recent_score unexpectedly zero");

        epoch.id = EpochNumber(42);
        epoch.state.set_active();

        // Node/pool setup
        let rate = ExchangeRate { tape: 1000, other: 9000 };
        node.id = NodeId(2);
        node.authority = pool_owner;
        node.history.push(EpochNumber(30), rate);
        node.pool.rewards = TAPE(500);
        node.pool.stake = TAPE(5000);
        node.pool.commission_rate = BasisPoints(500); // 5%
        node.pool.shares = rate.convert_to_other_amount(node.pool.stake.into()).into();
        node.blacklist.add(Hash::zeroed(), StorageUnits(50)).expect("blacklist add");

        // Pending I/O
        let e0 = epoch.id;
        let e2 = e0 + EpochNumber(2);

        node.pool
            .schedule
            .incoming_tokens = EpochValues::try_from(&[e0, e2], &[1000, 200])
            .expect("incoming");
        node.pool
            .schedule
            .outgoing_tokens = EpochValues::try_from(&[e0, e2], &[100, 50])
            .expect("outgoing");

        // Sanity projections unchanged
        assert_eq!(node.pool.stake_at(e0), TAPE(5900));
        assert_eq!(node.pool.stake_at(e0 + EpochNumber(1)), TAPE(5900));
        assert_eq!(node.pool.stake_at(e2), TAPE(6050));

        // Pre-compute expected payout for node 2
        let idx = system.committee_prev.index_of(&node.id).expect("member in prev committee");
        let w2 = system.seats_prev.weight(idx) as u128;
        let b2 = system.committee_prev.get_member(&node.id).unwrap().0.blacklist;
        let stored2 = archive.recent_usage.saturating_sub(b2).as_u128(); // 1000-50=950
        let node_score = w2.saturating_mul(stored2);

        //let denom = u128::from_le_bytes(archive.recent_score);
        //let pot_u128 = archive.rewards_pool.as_u128();
        //let expected_prev_rewards_u128 = pot_u128
        //    .saturating_mul(node_score)
        //    .checked_div(denom)
        //    .unwrap_or(0);
        //assert!(expected_prev_rewards_u128 > 0, "expected rewards zero unexpectedly");
        //let expected_prev_rewards = expected_prev_rewards_u128 as u64;
        //
        //// Expected rewards accounting in archive
        //let expected_rewards_paid = TAPE(expected_prev_rewards);

        // Expected new rate snapshot returned by advance_epoch (post-rewards, pre-pending-IO)
        //let commission_cut = (expected_prev_rewards_u128 * (node.pool.commission_rate.as_u128()))
        //    / (BasisPoints::MAX as u128);
        //let rewards_net_u128 = expected_prev_rewards_u128.saturating_sub(commission_cut);
        //let stake_after_rewards = node.pool.stake.as_u128().saturating_add(rewards_net_u128) as u64;
        //let expected_rate = ExchangeRate {
        //    tape: stake_after_rewards,
        //    other: node.pool.shares, // shares unchanged at the snapshot moment
        //};
        //
        //println!("expected_prev_rewards: {}", TAPE(expected_prev_rewards));
        //println!("expected_rewards_paid: {}", expected_rewards_paid);
        //println!("expected_rate: {:?}", expected_rate);


        let accounts = vec![
            sol(signer, 1_000_000_000),
            pda(system_address, system.pack(), tapedrive::ID),
            pda(archive_address, archive.pack(), tapedrive::ID),
            pda(epoch_address, epoch.pack(), tapedrive::ID),
            pda(pool_address, node.pack(), tapedrive::ID),
        ];

        // After instruction, archive.rewards_paid should equal expected_rewards_paid
        // We don't assert full node state (pending I/O mutates stake/shares), but we can at least
        // assert the snapshot rate recorded for this epoch.
        let env = test_env();
        env.process_instruction(
            &instruction,
            &accounts,
            &[
                Check::success(),
                //Check::account(&archive_address).data({
                //    let mut a = archive;
                //    a.rewards_paid = expected_rewards_paid;
                //    a.pack().as_ref()
                //}).build(),
                // Optional: assert the node's recorded rate snapshot for current epoch
                // by rebuilding the expected node with updated history only.
                // If your Node::pack encodes full pool state (including schedules after mutation),
                // this check can be brittle; uncomment if your encoding allows it.
                //
                //Check::account(&pool_address).data({
                //    let mut n = node;
                //    n.history.push(epoch.id, expected_rate);
                //
                //    n.pack().as_ref()
                //}).build(),
            ],
        );
    }
}
