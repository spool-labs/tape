use steel::*;
use tape_api::prelude::*;

pub fn process_advance_epoch(accounts: &[AccountInfo<'_>], data: &[u8]) -> ProgramResult {
    let now = Clock::get()?.unix_timestamp;
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

    if !epoch.state.is_next_ready() {
        return Err(ProgramError::Custom(1));
        //return Err(TapeError::InvalidEpochState);
    }

    if epoch.last_epoch_ms + EPOCH_DURATION > now {
        return Err(ProgramError::Custom(3));
        //return Err(TapeError::EpochNotYetOver);
    }

    debug_assert!(archive.fees_collected.current_epoch() == epoch.id);
    debug_assert!(archive.capacity_used.current_epoch() == epoch.id);

    // Seat assignments
    system.seats_prev = system.seats;
    system.seats.reassign(
        &system.committee,
        &system.committee_next,
    ).map_err(|_| TapeError::UnexpectedState)?;

    // Rotate committees
    system.committee_prev = system.committee;
    system.committee = system.committee_next;

    // Update future accounting
    archive.recent_fees = archive.fees_collected.advance_epoch();
    archive.recent_usage = archive.capacity_used.advance_epoch();

    // Advance to the next epoch
    epoch.id = next_epoch(epoch);
    epoch.state.set_syncing();
    epoch.last_epoch_ms = now;

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
        let env = test_env();

        let signer = Pubkey::new_unique();

        let instruction = build_advance_epoch_ix(signer);

        let (system_address, _) = system_pda();
        let (archive_address, _) = archive_pda();
        let (epoch_address, _) = epoch_pda();

        // Setup existing accounts

        let mut epoch = Epoch::zeroed();
        let mut archive = Archive::zeroed();
        let mut system = System::zeroed();

        let last_epoch = env.now() - (EPOCH_DURATION + 100);

        epoch.id = EpochNumber(42);
        epoch.state = EpochState::next_ready();
        epoch.last_epoch_ms = last_epoch;

        system.committee_prev = Committee::from_members(&[ member(2, 2_000), member(1, 1_000), ]);
        system.committee      = Committee::from_members(&[ member(3, 3_000), member(2, 2_000), member(1, 1_000), ]);
        system.committee_next = Committee::from_members(&[ member(3, 3_500), member(4, 2_100), member(2, 2_000), member(1, 1_000), ]);

        // Pre-fill archive usage and fees
        archive.capacity_used = FutureUsage::new_at(epoch.id);
        archive.capacity_used.reserve_capacity(StorageUnits(500), epoch.id, EpochNumber(100)).unwrap();

        archive.fees_collected = FutureRewards::new_at(epoch.id);
        archive.fees_collected.checked_add(TAPE(1000), epoch.id, EpochNumber(100)).unwrap();

        let accounts = vec![
            sol(signer, 1_000_000_000),
            pda(system_address, system.pack(), tapedrive::ID),
            pda(archive_address, archive.pack(), tapedrive::ID),
            pda(epoch_address, epoch.pack(), tapedrive::ID),
        ];

        // Expected seat allocation

        let seat_count = dhondt_allocate(
            &system.committee_next.active_stakes(),
            SEAT_COUNT as u16,
        );

        let seats = reassign_seats(
            &system.seats.seats,
            &system.committee.active_members(),
            &system.committee_next.active_members(),
            &seat_count,
        ).expect("seat reassignment failed");

        let expected_seats = Seats::try_from_slice(seats.as_ref())
            .unwrap();

        env.process_instruction(
            &instruction, 
            &accounts,
            &[
                Check::success(),
                Check::account(&system_address).data(
                    System { 
                        seats: expected_seats,
                        seats_prev: system.seats,
                        committee_prev: system.committee,
                        committee: system.committee_next,
                        committee_next: system.committee_next,
                        ..system
                    }.pack().as_ref()
                ).build(),
                Check::account(&epoch_address).data(
                    Epoch {
                        id: EpochNumber(43),
                        state: EpochState::syncing(),
                        last_epoch_ms: env.now(),
                    }.pack().as_ref()
                ).build(),
                Check::account(&archive_address).data({
                    let mut fees_collected = FutureRewards::new_at(EpochNumber(43));
                    fees_collected.checked_add(TAPE(1000), EpochNumber(43), EpochNumber(100)).unwrap();

                    let mut capacity_used = FutureUsage::new_at(EpochNumber(43));
                    capacity_used.reserve_capacity(StorageUnits(500), EpochNumber(43), EpochNumber(100)).unwrap();

                    Archive {
                        fees_collected,
                        capacity_used,
                        recent_fees: TAPE(1000),
                        recent_usage: StorageUnits(500),
                        ..archive
                    }.pack().as_ref()
                }).build(),
            ]
        );
    }
}
