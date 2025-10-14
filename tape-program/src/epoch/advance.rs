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
        .as_account_mut::<Committee>(&tape_api::ID)?;

    // Advance to the next epoch
    epoch.id = next_epoch(epoch);


    // Rotate committees

    let stake_weights = &epoch.leaders.stakes;
    let seat_allocations = allocate_seats(stake_weights, 1000);

    // TODO: the seat allocations are sorted, but the leader member list is not

    // unique_set: [CommitteeMember; 256] - mapping from array index to CommitteeMember (in either the current committee or the leader set)
    // seat_counts: [u16; 256]            - mapping from array index to number of seats assigned in the new committee
    let (unique_set, seat_counts) = {

        let mut unique_set = Vec::new();
        let mut seat_count = [0u16; 256];

        // First add all members from the current committee to the unique_set array
        for member in committee.inner.iter_members() {
            unique_set.push(member);
        }

        // Then add members from the leader set, skipping any that are already in the unique_set array.
        for index in 0..epoch.leaders.size() {
            let member = &epoch.leaders.members[index];
            let seats = seat_allocations[index]; // <- not correct (TODO)

            // Check if this member is already in the unique_set array
            let previous = unique_set
                .iter()
                .position(|&m| m.id == member.id);

            // If it is already in the unique_set array, update its seat assignment. 
            if let Some(index) = previous {
                seat_count[index] = seats;

                // Update to the latest CommitteeMember 
                // (in case the BlsPubkey changed)
                unique_set[index] = member; 

            // Otherwise, add it to the end of the unique_set array and set its seat assignment.
            } else {
                seat_count[unique_set.len()] = seats;
                unique_set.push(member);
            }
        }

        (unique_set, seat_count)
    };

    solana_program::log::sol_log_compute_units();
    let new_seats = move_seats2(
        &committee.inner.seats,
        &seat_counts,
    );
    solana_program::log::sol_log_compute_units();

    // New seats is a list of indexes into *unique_set*, where each index represents a seat in the
    // new committee.

    //solana_program::msg!("Node mappings: {:?}", node_mappings);
    //solana_program::msg!("Res: {:?}", new_seats);

    previous_committee.inner = committee.inner;

    //for seat in 0..new_seats.len() {
    //    let member_index = new_seats[seat] as usize;
    //    let member = node_mappings.get(member_index)
    //        .ok_or(ProgramError::InvalidAccountData)?;
    //
    //    committee.inner.members[seat] = *member;
    //    committee.inner.seats[seat] = seat_counts[member_index];
    //}



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
