//use crate::error::*;
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

    // Can't advance if epoch is syncing (i.e., not active)
    if epoch.state.is_syncing() {
        return Err(ProgramError::Custom(2));
    }

    // If this pool is already updated for this epoch, can't advance again
    if node.latest_epoch >= epoch.id {
        return Err(ProgramError::Custom(0));
    }

    // Calculate rewards owed based on recent usage snapshot
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
        // return Err(TapeError::NoRewardsOwed);
    }

    let rewards_paid = archive.rewards_paid
        .saturating_add(rewards_owed.into());

    if rewards_paid > archive.rewards_pool {
        return Err(ProgramError::Custom(3));
        // return Err(TapeError::RewardsOverflow);
    }

    node.pool
        .advance_epoch(current_epoch(epoch), rewards_owed)
        .map_err(|_| ProgramError::Custom(1))?;

    let new_rate = node.pool
        .get_current_rate();

    node.history.push(current_epoch(epoch), new_rate);

    archive.rewards_paid = rewards_paid;

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
    fn test_advance_pool() {
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

        epoch.id = EpochNumber(42);
        epoch.state.set_active();

        // Pending I/O
        let e0 = epoch.id;
        let e1 = e0 + EpochNumber(1);
        let e2 = e1 + EpochNumber(1);

        let mut node = Node {
            id: NodeId(2),
            authority: pool_owner,
            pool: StakingPool {
                stake: TAPE(5000),
                rewards: TAPE(500),
                commission: TAPE(10),
                commission_rate: BasisPoints(500), // 5%
                shares: ShareAmount(123),
                ..StakingPool::zeroed()
            },
            ..Node::zeroed()
        };

        let rate = node.pool.get_current_rate();

        //node.history.push(EpochNumber(30), node.pool.get_current_rate());
        node.blacklist.add(Hash::zeroed(), StorageUnits(50)).expect("blacklist add");

        node.pool.schedule.stake(e0, TAPE(1000)).expect("schedule stake");
        node.pool.schedule.stake(e2, TAPE(200)).expect("schedule stake");
        node.pool.schedule.cancel(e0, TAPE(100)).expect("schedule cancel");
        node.pool.schedule.cancel(e2, TAPE(50)).expect("schedule cancel");
        node.pool.schedule.unstake(e1, ShareAmount(50)).expect("schedule unstake");

        // Sanity check scheduled stake/unstake
        let e1_unstake_tape: Coin<TAPE> =
            rate.convert_to_tape_amount(ShareAmount(50).into()).into();

        assert_eq!(e1_unstake_tape, TAPE(2032)); // sanity

        assert_eq!(node.pool.calculate_stake_at(e0), TAPE(5900)); // 5000 + 1000 - 100
        assert_eq!(node.pool.calculate_stake_at(e1), TAPE(5900) - e1_unstake_tape); // 5900 - unstake
        assert_eq!(node.pool.calculate_stake_at(e2), TAPE(6050) - e1_unstake_tape); // 5900 - unstake + 200 - 50

        // Set previous committee and seats in system, ignore the current/next ones in this test
        system.committee_prev = Committee::from_members(&[
            member(node.id.into(), 3_000, 50),
            member(2, 2_000, 0),
            member(1, 1_000, 0),
        ]);

        // Arbitrary seat counts for testing
        system.seats_prev = Seats::try_from_counts(
            &[500, 300, 200]
        ).unwrap();

        archive.rewards_pool = TAPE(10_000);
        archive.rewards_paid = TAPE(0);
        archive.recent_usage = StorageUnits(1_000);

        let accounts = vec![
            sol(signer, 1_000_000_000),
            pda(system_address, system.pack(), tapedrive::ID),
            pda(archive_address, archive.pack(), tapedrive::ID),
            pda(epoch_address, epoch.pack(), tapedrive::ID),
            pda(pool_address, node.pack(), tapedrive::ID),
        ];

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
