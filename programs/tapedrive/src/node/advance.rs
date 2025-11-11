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

    // Rotate BLS key if needed
    if node.metadata.bls_pubkey.ne(&node.metadata.next_bls_pubkey) {
        node.metadata.bls_pubkey = node.metadata.next_bls_pubkey;
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

    node.pool
        .advance_epoch(current_epoch(epoch), rewards_owed)
        .map_err(|_| ProgramError::Custom(1))?;

    let new_rate = node.pool
        .get_current_rate();

    node.history.push(current_epoch(epoch), new_rate);

    let rewards_paid = archive.rewards_paid
        .saturating_add(rewards_owed.into());

    if rewards_paid > archive.rewards_pool {
        return Err(ProgramError::Custom(3));
        // return Err(TapeError::RewardsOverflow);
    }

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
            blacklist: StorageUnits(bl),
            ..CommitteeMember::zeroed()
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
            ..Node::zeroed()
        };

        // Previous committee/seats used by calc_rewards
        system.committee_prev = Committee::from_members(&[
            member(node.id.into(), 3_000, 0),
            member(3, 2_000, 0),
            member(5, 1_000, 0),
        ]);

        system.seats_prev = Seats::try_from_counts(
            &[500, 300, 200]
        ).unwrap();

        archive.rewards_pool = TAPE(10_000);
        archive.recent_usage = StorageUnits(1_000);
        archive.rewards_paid = TAPE(0);

        let accounts = vec![
            sol(signer, 1_000_000_000),
            pda(system_address, system.pack(), tapedrive::ID),
            pda(archive_address, archive.pack(), tapedrive::ID),
            pda(epoch_address, epoch.pack(), tapedrive::ID),
            pda(pool_address, node.pack(), tapedrive::ID),
        ];

        // Expected state after instruction
        let e0 = epoch.id;

        let rewards_owed = calc_rewards(
            node.id,
            archive.recent_usage,
            &system.committee_prev,
            &system.seats_prev,
            archive.rewards_pool,
        );

        archive.rewards_paid = archive
            .rewards_paid
            .saturating_add(rewards_owed.into());

        node.pool
            .advance_epoch(e0, rewards_owed)
            .expect("advance epoch");

        let new_rate = node.pool.get_current_rate();
        node.history.push(e0, new_rate);
        node.metadata.bls_pubkey = node.metadata.next_bls_pubkey;

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
            ],
        );
    }
}
