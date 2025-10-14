use tape_api::prelude::*;
use steel::*;

pub fn process_advance_epoch(accounts: &[AccountInfo<'_>], data: &[u8]) -> ProgramResult {
    let _args = AdvanceEpoch::try_from_bytes(data)?;
    let [
        signer_info,
        epoch_info,
        committee_info,
        previous_committee_info,
    ] = accounts else {
        return Err(ProgramError::NotEnoughAccountKeys);
    };

    signer_info
        .is_signer()?;

    let mut epoch = epoch_info
        .is_writable()?
        .is_epoch()?
        .as_account_mut::<Epoch>(&tape_api::ID)?;

    let mut committee = committee_info
        .is_writable()?
        .is_current_committee()?
        .as_account_mut::<Committee>(&tape_api::ID)?;

    let mut previous_committee = previous_committee_info
        .is_writable()?
        .is_previous_committee()?
        .as_account::<Committee>(&tape_api::ID)?;

    // Advance to the next epoch
    epoch.id = next_epoch(epoch);

    solana_program::log::sol_log_compute_units();
    let stake_weights = &epoch.leaders.stakes;
    let seat_assignments = allocate_seats(stake_weights, 1000);
    solana_program::log::sol_log_compute_units();

    let mut fixed_assignments : [u16; 256] = [0u16; 256];
    for i in 0..256 {
        fixed_assignments[i] = seat_assignments.get(i).cloned().unwrap_or(0);
    }

    solana_program::log::sol_log_compute_units();
    let _res = move_seats2(
        &[0; 1000], 
        &fixed_assignments
    );
    solana_program::log::sol_log_compute_units();



    // Epoch phases: Syncing -> Active -> NextEpoch (this instruction)
    // - Syncing: nodes move recovery symbols based on seat assignments for the new committee
    // - Active: old committee stops serving reads for the previous epoch, new committee starts
    // serving reads for the current epoch. Rewards are distributed to the old committee. Voting
    // may start for features to be activated in E+2.
    // - NextEpoch: called once the epoch duration has elapsed (epoch duration starts at the Active
    // transition, not Syncing).
    
    // LeaderSet -> Next Committee
    // - Update seat assignments

    // Update future accounting
    // - pop a value off the ring buffer (storage and rewards)

    // Update archive
    // - Set total_capacity_size = max(next_capacity_size, used_capacity_size)
    // - Set storage_price_per_unit from features

    // Distribute rewards (during "Syncing" -> "Active" transition)
    // - Let total_rewards = old_epoch_balance
    // - For each node in old_epoch_leaders:
    //    - weight = seats(from previous committee)
    //    - stored = old_epoch_used_capacity - node.blacklist_size
    //    - node.score = weight * stored
    // - Split total epoch rewards proportionally to node scores
    // - Leftover rounding remainder is carried into the next epoch

    Ok(())
}


#[cfg(test)]
mod tests {
    use super::*;
    use tape_test::*;

    #[test]
    fn test_advance_epoch() {
        let signer = Pubkey::new_unique();

        let instruction = build_advance_epoch_ix(signer);

        let (epoch_address, _) = epoch_pda();
        let (committee_address, _) = current_committee_pda();
        let (previous_committee_address, _) = previous_committee_pda();

        // Setup existing accounts

        let mut epoch = Epoch {
            id: EpochNumber(42),
            state: EpochState::zeroed(),
            leaders: LeaderSet::zeroed(),
            last_epoch_ms: 0,
        };

        epoch.leaders = LeaderSet {
            member_count: COMMITTEE_SIZE as u64,
            //members: [CommitteeMember::zeroed(); COMMITTEE_SIZE],
            members: (0..COMMITTEE_SIZE as u64)
                .map(|i| CommitteeMember {
                    id: NodeId::new(i + 1),
                    key: BlsPubkey::zeroed(),
                })
                .collect::<Vec<_>>()
                .try_into()
                .unwrap(),
            stakes: (0..COMMITTEE_SIZE as u64)
                .map(|i| TAPE(1280 - i*10))
                .collect::<Vec<_>>()
                .try_into()
                .unwrap()
        };

        println!("stakes: {:?}", epoch.leaders.stakes);

        let previous_committee = Committee {
            id: CommitteeNumber::previous(),
            epoch: EpochNumber(41),
            inner: AppointedSet::zeroed(),
        };

        let committee = Committee {
            id: CommitteeNumber::current(),
            epoch: EpochNumber(42),
            inner: AppointedSet::zeroed(),
        };


        let accounts = vec![
            sol(signer, 1_000_000_000),
            pda(epoch_address, epoch.pack()),
            pda(committee_address, committee.pack()),
            pda(previous_committee_address, previous_committee.pack()),
        ];

        let env = test_env("tape".to_string());
        env.process_instruction(
            &instruction, 
            &accounts,
            &[
                Check::success(),
            ]
        );
    }
}
