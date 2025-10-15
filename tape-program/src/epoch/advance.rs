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

    solana_program::msg!("1");
    solana_program::log::sol_log_compute_units();

    // 0) Save previous committee before mutating
    previous_committee.inner = committee.inner;

    // 1) Seat allocation for leaders (use full stakes, no slicing)
    let seats_total = committee.inner.seats.len();
    let leader_len = epoch.leaders.member_count as usize;
    solana_program::msg!("leader_len: {}", leader_len);
    solana_program::msg!("seats_len: {}", seats_total);
    //solana_program::msg!("seats: {:?}", epoch.leaders.stakes);

    let seat_count_per_leader = allocate_seats(&epoch.leaders.stakes[..leader_len], seats_total as u16);

    solana_program::msg!("seat_count_per_leader: {:?}", seat_count_per_leader);

    solana_program::msg!("2");
    solana_program::log::sol_log_compute_units();
    // 2) Build unique_set (current committee first) and per-unique seat counts
    let cur_len = committee.inner.size();

    let mut unique_set: Vec<&CommitteeMember> = Vec::with_capacity(cur_len + leader_len);
    let mut seat_counts: [u16; 256] = [0; 256];

    solana_program::msg!("3");
    solana_program::log::sol_log_compute_units();
    // Current committee members first: keeps indices aligned with current_seats
    for m in committee.inner.iter_members() {
        unique_set.push(m);
    }

    solana_program::msg!("4");
    solana_program::log::sol_log_compute_units();
    // Add/refresh leaders; set desired seat counts in unique_set index-space
    for li in 0..leader_len {
        let m = &epoch.leaders.members[li];
        let seats = seat_count_per_leader[li];

        if let Some(ui) = unique_set.iter().position(|&x| x.id == m.id) {
            seat_counts[ui] = seats;
            unique_set[ui] = m; // refresh pubkey, etc.
        } else {
            // Append new leader
            let ui = unique_set.len();
            debug_assert!(ui < 256, "union cannot exceed 256 entries");
            seat_counts[ui] = seats;
            unique_set.push(m);
        }
    }

    solana_program::msg!("5");
    solana_program::log::sol_log_compute_units();
    // 3) Reassign seats with minimal churn
    let new_seats_unique_idx = move_seats2(&committee.inner.seats, &seat_counts);

    solana_program::msg!("6");
    solana_program::log::sol_log_compute_units();
    // 4) Map unique_set index -> leaders index using a tiny fixed map
    let mut unique_to_leader: [u8; 256] = [u8::MAX; 256];
    for li in 0..leader_len {
        let id = epoch.leaders.members[li].id;
        if let Some(ui) = unique_set.iter().position(|&x| x.id == id) {
            unique_to_leader[ui] = li as u8;
        }
    }

    // Rewrite seats in place: unique_idx -> leader_idx
    for s in 0..committee.inner.seats.len() {
        let ui = new_seats_unique_idx[s] as usize;
        let li = unique_to_leader[ui];
        debug_assert!(li != u8::MAX, "Seat mapped to non-leader; check seat_counts");
        committee.inner.seats[s] = li;
    }

    solana_program::msg!("before: \n{}", committee.inner);

    // 5) Commit leaders as the new committee (no local clones)
    committee.inner.members = epoch.leaders.members;
    committee.inner.member_count = epoch.leaders.member_count;

    solana_program::msg!("after: \n{}", committee.inner);
    solana_program::msg!("seats: {:?}", committee.inner.seats);

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
        c.members[0] = CommitteeMember { id: NodeId::new(1), key: BlsPubkey::zeroed(), };
        c.members[1] = CommitteeMember { id: NodeId::new(2), key: BlsPubkey::zeroed(), };
        c.members[1] = CommitteeMember { id: NodeId::new(3), key: BlsPubkey::zeroed(), };
        c.seats[0..10].copy_from_slice(&[0;10]);
        c.seats[10..15].copy_from_slice(&[1;5]);
        c.seats[15..18].copy_from_slice(&[2;3]);

        // New leaders (some overlap with current committee)
        let mut l = LeaderSet::zeroed();
        l.member_count = 4;
        l.members[0] = CommitteeMember { id: NodeId::new(1), key: BlsPubkey::zeroed(), };
        l.members[1] = CommitteeMember { id: NodeId::new(3), key: BlsPubkey::zeroed(), };
        l.members[2] = CommitteeMember { id: NodeId::new(4), key: BlsPubkey::zeroed(), };
        l.members[3] = CommitteeMember { id: NodeId::new(5), key: BlsPubkey::zeroed(), };
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
