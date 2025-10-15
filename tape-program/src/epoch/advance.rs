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

    //solana_program::msg!("before: \n{}", committee.inner);
    //solana_program::msg!("seats: {:?}", committee.inner.seats);

    // Advance to the next epoch
    epoch.id = next_epoch(epoch);


    // Rotate committees

    solana_program::msg!("1");
    solana_program::log::sol_log_compute_units();

    // Save previous committee
    previous_committee.inner = committee.inner;

    // Seat allocation for leaders (d’Hondt)
    let seats_total = committee.inner.seats.len();
    let leader_count = epoch.leaders.size();
    let counts = allocate_seats(&epoch.leaders.stakes[..leader_count], seats_total as u16);

    // Active slices
    let cur_count = committee.inner.size();
    let cur_members = &committee.inner.members[..cur_count];
    let lead_members = &epoch.leaders.members[..leader_count];
    let lead_counts = &counts[..leader_count];

    // Minimal-churn reassignment
    let new_seats = reassign_seats(
        &committee.inner.seats,
        cur_members,
        lead_members,
        lead_counts,
    )   
        .map_err(|_| TapeError::UnexpectedState)?
        .try_into()
        .map_err(|_| TapeError::UnexpectedState)?;

    // Install new seats and leaders
    committee.inner.seats = new_seats;
    committee.inner.members = epoch.leaders.members;
    committee.inner.member_count = epoch.leaders.member_count;

    //solana_program::msg!("after: \n{}", committee.inner);
    //solana_program::msg!("seats: {:?}", committee.inner.seats);

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

        // Current committee members
        let mut c = AppointedSet::zeroed();
        c.member_count = 3;
        c.members[0] = NodeId::new(1);
        c.members[1] = NodeId::new(2);
        c.members[1] = NodeId::new(3);
        c.seats[0..10].copy_from_slice(&[0;10]);
        c.seats[10..15].copy_from_slice(&[1;5]);
        c.seats[15..18].copy_from_slice(&[2;3]);

        // New leaders (some overlap with current committee)
        let mut l = LeaderSet::zeroed();
        l.member_count = 4;
        l.members[0] = NodeId::new(1);
        l.members[1] = NodeId::new(3);
        l.members[2] = NodeId::new(4);
        l.members[3] = NodeId::new(5);
        l.stakes[0..4].copy_from_slice(&[TAPE(900), TAPE(300), TAPE(200), TAPE(100)]);

        let mut epoch = Epoch {
            id: EpochNumber(42),
            state: EpochState::zeroed(),
            leaders: l,
            last_epoch_ms: 0,
            // leaders: LeaderSet {
            //     member_count: 5,
            //     members: (0..COMMITTEE_SIZE as u64)
            //         .map(|i| CommitteeMember {
            //             id: NodeId::new(i + 1),
            //             key: BlsPubkey::zeroed(),
            //         })
            //         .collect::<Vec<_>>()
            //         .try_into()
            //         .unwrap(),
            //     stakes: (0..COMMITTEE_SIZE as u64)
            //         .map(|i| TAPE(1280 - i*10))
            //         .collect::<Vec<_>>()
            //         .try_into()
            //         .unwrap()
            // },
        };

        // epoch.leaders.stakes[25..].copy_from_slice(&[TAPE(10); COMMITTEE_SIZE - 25]);
        // epoch.leaders.members[25..].copy_from_slice(&[CommitteeMember {
        //     id: NodeId::new(1),
        //     key: BlsPubkey::zeroed(),
        // }; COMMITTEE_SIZE - 25]);

        //println!("stakes: {:?}", epoch.leaders.stakes);

        let previous_committee = Committee {
            id: CommitteeNumber::previous(),
            epoch: EpochNumber(41),
            inner: AppointedSet::zeroed(),
        };

        let committee = Committee {
            id: CommitteeNumber::current(),
            epoch: EpochNumber(42),
            inner: c,
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
                // Check::account(&committee_address).data(
                //     Committee {
                //         inner: AppointedSet {
                //             member_count: COMMITTEE_SIZE as u64,
                //             members: leaders, // new leaders
                //             seats: committee.inner.seats, // reassigned seats
                //         },
                //
                //         ..committee
                //     }.pack().as_ref()
                // ).build(),

                // Check::account(&committee_address).data(
                //     Committee {
                //         //...
                //     }.pack().as_ref()
                // ).build(),
                // Check::account(&previous_committee_address).data(
                //     Committee {
                //         //...
                //     }.pack().as_ref()
                // ).build(),
                // Check::account(&epoch_address).data(
                //     Epoch {
                //         //...
                //     }.pack().as_ref()
                // ).build(),
            ]
        );
    }
}
