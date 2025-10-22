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

    let system = system_info
        .is_writable()?
        .is_system()?
        .as_account_mut::<System>(&tapedrive::ID)?;

    let archive = archive_info
        .is_writable()?
        .is_archive()?
        .as_account_mut::<Archive>(&tapedrive::ID)?;

    let epoch = epoch_info
        .is_writable()?
        .is_epoch()?
        .as_account_mut::<Epoch>(&tapedrive::ID)?;


    // Seat assignments
    system.seats.reassign(
        &system.committee,
        &system.committee_next,
    ).map_err(|_| TapeError::UnexpectedState)?;

    // solana_program::msg!("seats: {:?}", system.seats);

    // Rotate committees
    system.seats_prev = system.seats;
    system.committee_prev = system.committee;
    system.committee = system.committee_next;

    // Update future accounting

    assert!(archive.fees_collected.current_epoch() == epoch.id);
    assert!(archive.capacity_used.current_epoch() == epoch.id);

    // TODO: maybe this belongs somewhere else?
    let rewards = archive.fees_collected.advance_epoch();
    let usage = archive.capacity_used.advance_epoch();

    solana_program::msg!("Rewards for epoch {}: {}", epoch.id.0, rewards);
    solana_program::msg!("Usage for epoch {}: {}", epoch.id.0, usage);

    solana_program::msg!("Advancing epoch from {} to {}", epoch.id.0, next_epoch(epoch).0);

    // Advance to the next epoch
    epoch.id = next_epoch(epoch);

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

    fn member(id: u64, stake: u64) -> CommitteeMember {
        CommitteeMember {
            id: NodeId(id),
            stake: TAPE(stake),
            key: BlsPubkey::zeroed(),
        }
    }

    #[test]
    fn test_advance_epoch() {
        let signer = Pubkey::new_unique();

        let instruction = build_advance_epoch_ix(signer);

        let (system_address, _) = system_pda();
        let (archive_address, _) = archive_pda();
        let (epoch_address, _) = epoch_pda();

        // Setup existing accounts

        let mut epoch = Epoch::zeroed();
        let mut archive = Archive::zeroed();
        let mut system = System::zeroed();

        epoch.id = EpochNumber(42);

        system.version = VersionId::default();
        system.committee_prev = Committee::from_members(&[ member(2, 2_000), member(1, 1_000), ]);
        system.committee      = Committee::from_members(&[ member(3, 3_000), member(2, 2_000), member(1, 1_000), ]);
        system.committee_next = Committee::from_members(&[ member(3, 3_500), member(4, 2_100), member(2, 2_000), member(1, 1_000), ]);

        archive.fees_collected.fast_forward_to(epoch.id);
        archive.capacity_used.fast_forward_to(epoch.id);

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
