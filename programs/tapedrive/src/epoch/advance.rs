use steel::*;
use tape_api::prelude::*;

pub fn process_advance_epoch(accounts: &[AccountInfo<'_>], data: &[u8]) -> ProgramResult {
    let _args = AdvanceEpoch::try_from_bytes(data)?;
    let [
        signer_info,
        system_info,
        archive_info,
        epoch_info,
    ] = accounts else {
        return Err(ProgramError::NotEnoughAccountKeys);
    };

    signer_info
        .is_signer()?;

    let mut system = system_info
        .is_writable()?
        .is_system()?
        .as_account_mut::<System>(&tapedrive::ID)?;

    let mut archive = archive_info
        .is_writable()?
        .is_archive()?
        .as_account_mut::<Archive>(&tapedrive::ID)?;

    let mut epoch = epoch_info
        .is_writable()?
        .is_epoch()?
        .as_account_mut::<Epoch>(&tapedrive::ID)?;

    // Advance to the next epoch
    epoch.id = next_epoch(epoch);

    // Rotate committees

    solana_program::msg!("1");
    solana_program::log::sol_log_compute_units();

    //// Save previous committee
    //previous_committee.inner = committee.inner;
    //
    //// Seat allocation for leaders (d’Hondt)
    //let seats_total = committee.inner.seats.len();
    //let leader_count = epoch.leaders.size();
    //let counts = allocate_seats(&epoch.leaders.stakes[..leader_count], seats_total as u16);
    //
    //// Active slices
    //let cur_count = committee.inner.size();
    //let cur_members = &committee.inner.members[..cur_count];
    //let lead_members = &epoch.leaders.members[..leader_count];
    //let lead_counts = &counts[..leader_count];
    //
    //// Minimal-churn reassignment
    //let new_seats = reassign_seats(
    //    &committee.inner.seats,
    //    cur_members,
    //    lead_members,
    //    lead_counts,
    //)   
    //    .map_err(|_| TapeError::UnexpectedState)?
    //    .try_into()
    //    .map_err(|_| TapeError::UnexpectedState)?;
    //
    //// Install new seats and leaders
    //committee.inner.seats = new_seats;
    //committee.inner.members = epoch.leaders.members;
    //committee.inner.member_count = epoch.leaders.member_count;

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

        let (system_address, _) = system_pda();
        let (archive_address, _) = archive_pda();
        let (epoch_address, _) = epoch_pda();

        // Setup existing accounts

        let mut epoch = Epoch::zeroed();
        let mut system = System::zeroed();
        let mut archive = Archive::zeroed();

        let accounts = vec![
            sol(signer, 1_000_000_000),
            pda(system_address, system.pack(), tapedrive::ID),
            pda(archive_address, archive.pack(), tapedrive::ID),
            pda(epoch_address, epoch.pack(), tapedrive::ID),
        ];

        let env = test_env();
        env.process_instruction(
            &instruction, 
            &accounts,
            &[
                Check::success(),
            ]
        );
    }
}
