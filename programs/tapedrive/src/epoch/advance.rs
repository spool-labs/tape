use tape_solana::*;
use crate::error::*;
use tape_api::prelude::*;
use tape_api::event::EpochAdvanced;

pub fn process_advance_epoch(accounts: &[AccountInfo<'_>], data: &[u8]) -> ProgramResult {
    let now = Clock::get()?.unix_timestamp;
    let _args = AdvanceEpoch::try_from_bytes(data)?;
    let [
        fee_payer_info,
        authority_info,
        system_info,
        archive_info,
        epoch_info,
    ] = accounts else {
        return Err(ProgramError::NotEnoughAccountKeys);
    };

    fee_payer_info
        .is_signer()?
        .is_writable()?;
    authority_info
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

    // Check epoch state and timing
    if system.is_low_quorum() {
        // Low-quorum: relaxed state checks, but still enforce minimum timing
        if epoch.last_epoch + MIN_EPOCH_DURATION > now {
            return Err(TapeError::TooSoon.into());
        }
    } else {
        // Normal mode: strict requirements
        if !epoch.state.is_active() {
            return Err(TapeError::BadEpochState.into());
        }
        if epoch.last_epoch + EPOCH_DURATION > now {
            return Err(TapeError::TooSoon.into());
        }
    }

    // Ensure the archive schedule is aligned with the current epoch
    if archive.schedule.current_epoch() != epoch.id {
        return Err(TapeError::BadSchedule.into());
    }

    // Save old epoch for event logging
    let old_epoch = epoch.id;

    // Empty committee_next Handling
    if system.committee_next_empty() {
        if system.is_low_quorum() {
            // Low-quorum with no nodes: advance counters, stay ready
            let _ = archive.schedule.advance_epoch();
            epoch.id = next_epoch(epoch);
            epoch.last_epoch = now;
            epoch.state = EpochState::active();

            EpochAdvanced {
                old_epoch,
                new_epoch: epoch.id,
                timestamp: (now as u64).to_le_bytes(),
                committee_size: 0u64.to_le_bytes(),
                total_stake: 0u64.to_le_bytes(),
                storage_price: archive.storage_price.as_u64().to_le_bytes(),
                storage_capacity: archive.storage_capacity,
            }.log();

            return Ok(());
        } else {
            return Err(TapeError::UnexpectedState.into());
        }
    }

    let entering_low_quorum = system.will_be_low_quorum();

    // Save previous spools, then reassign for the next committee
    system.spools_prev = system.spools;
    system.spools.migrate_dhondt(
        &system.committee,
        &system.committee_next,
    ).map_err(|_| TapeError::UnexpectedState)?;

    // Rotate committees: prev <- current <- next <- empty
    system.rotate_committees();

    system.committee
        .apply_weights_from_spools(&system.spools);

    // Update future accounting
    let epoch_usage = archive.schedule
        .advance_epoch();

    // Carry-over dust from last epoch
    let leftover = archive.rewards_pool
        .saturating_sub(archive.rewards_paid);

    // Check if we're entering/staying in low-quorum mode
    if entering_low_quorum {
        // Low-quorum: keep only unclaimed rewards
        archive.rewards_paid = TAPE::zero();
        archive.rewards_pool = leftover;
    } else {
        // Normal: add new rewards from schedule
        archive.rewards_paid = TAPE::zero();
        archive.rewards_pool = epoch_usage.paid()
            .saturating_add(leftover);
        archive.recent_usage = epoch_usage.reserved();
    }

    // Advance epoch metadata
    epoch.id = next_epoch(epoch);
    epoch.last_epoch = now;
    epoch.state = if entering_low_quorum {
        EpochState::active()
    } else {
        EpochState::syncing()
    };

    // Update the archive storage price and capacity based on the new committee preferences
    // (only update if not entering low-quorum mode)
    if !entering_low_quorum {
        let mut storage_prices : Vec<ValueAndWeight> = vec![];
        let mut storage_capacities : Vec<ValueAndWeight> = vec![];
        let mut total_weight = 0u64;

        system.committee.iter().for_each(|member| {
            let weight = member.weight as u64;
            let preferences = &member.preferences;

            storage_prices
                .push((preferences.storage_price.into(), weight));
            storage_capacities
                .push((preferences.storage_capacity.into(), weight));

            total_weight = total_weight.saturating_add(weight);
        });

        // We select the lowest price that achieves quorum
        // and the highest capacity that achieves quorum
        archive.storage_capacity =
            quorum_above(&storage_capacities, total_weight).into();
        archive.storage_price =
            quorum_below(&storage_prices, total_weight).into();
    }

    // Calculate committee size and total stake for event
    let committee_size = system.committee.size() as u64;
    let total_stake: u64 = system.committee.iter()
        .map(|m| m.stake.as_u64())
        .sum();

    EpochAdvanced {
        old_epoch,
        new_epoch: epoch.id,
        timestamp: (now as u64).to_le_bytes(),
        committee_size: committee_size.to_le_bytes(),
        total_stake: total_stake.to_le_bytes(),
        storage_price: archive.storage_price.as_u64().to_le_bytes(),
        storage_capacity: archive.storage_capacity,
    }.log();

    solana_program::msg!(
        "Advanced to {}, capacity: {}, price: {}",
        epoch.id,
        archive.storage_capacity,
        archive.storage_price,
    );

    Ok(())
}


#[cfg(test)]
mod tests {
    use super::*;
    use tape_test::*;

    fn member(id: u64, stake: u64, size: u64, price: u64) -> CommitteeMember {
        let mut m = CommitteeMember::new(NodeId(id), TAPE(stake));
        m.preferences.storage_capacity = StorageUnits(size);
        m.preferences.storage_price = TAPE(price);
        m
    }

    #[test]
    fn test_advance_epoch() {
        let env = test_env();

        let fee_payer = Pubkey::new_unique();
        let authority = Pubkey::new_unique();

        let instruction = build_advance_epoch_ix(fee_payer, authority);

        let (system_address, _) = system_pda();
        let (archive_address, _) = archive_pda();
        let (epoch_address, _) = epoch_pda();

        // Setup existing accounts

        let mut epoch = Epoch::zeroed();
        let mut archive = Archive::zeroed();
        let mut system = System::zeroed();

        let last_epoch = env.now() - (EPOCH_DURATION + 100);

        let e0 = EpochNumber(42);
        let e1 = e0 + EpochNumber(1);
        let e100 = e0 + EpochNumber(100);

        epoch.id = e0;
        epoch.state = EpochState::active();
        epoch.last_epoch = last_epoch;

        // Need >= MIN_COMMITTEE_SIZE (24) members for normal mode
        let prev_members: Vec<CommitteeMember> = (1..=24)
            .map(|i| member(i, 1_000 + i * 100, 8_000_000, 950))
            .collect();
        system.committee_prev = Committee::from_members(&prev_members);

        let curr_members: Vec<CommitteeMember> = (1..=25)
            .map(|i| member(i, 1_000 + i * 100, 8_050_000, 1050))
            .collect();
        system.committee = Committee::from_members(&curr_members);

        let next_members: Vec<CommitteeMember> = (1..=26)
            .map(|i| member(i, 1_000 + i * 100, 1_500_000, 850))
            .collect();
        system.committee_next = Committee::from_members(&next_members);

        archive.schedule = EpochSchedule::new_at(epoch.id);
        archive.schedule.reserve_capacity(
            StorageUnits(500), TAPE(1000), e0, e100
        ).expect("reserve capacity");

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            sol(authority, 0),
            pda(system_address, system.pack(), tapedrive::ID),
            pda(archive_address, archive.pack(), tapedrive::ID),
            pda(epoch_address, epoch.pack(), tapedrive::ID),
        ];

        // Expected state after instruction

        let seat_count = dhondt_allocate(
            &system.committee_next.active_stakes(),
            SLICE_COUNT as u16,
        );

        let spools = migrate_spools(
            &system.spools.0,
            &system.committee.active_members(),
            &system.committee_next.active_members(),
            &seat_count,
        ).expect("seat reassignment failed");

        let expected_seats = SpoolAssignment::try_from(spools.as_ref()).unwrap();

        let mut expected_committee = system.committee_next.clone();
        expected_committee
            .apply_weights_from_spools(&expected_seats);

        let mut schedule = EpochSchedule::new_at(e1);
        schedule.reserve_capacity(
            StorageUnits(500), TAPE(1000), e1, e100
        ).expect("reserve capacity");

        let total_weight: u64 = expected_committee
            .iter()
            .map(|m| m.weight as u64)
            .sum();

        let price_pairs: Vec<(u64, u64)> = expected_committee
            .iter()
            .map(|m| (m.preferences.storage_price.as_u64(), m.weight as u64))
            .collect();

        let cap_pairs: Vec<(u64, u64)> = expected_committee
            .iter()
            .map(|m| (m.preferences.storage_capacity.as_u64(), m.weight as u64))
            .collect();

        let storage_capacity = quorum_above(&cap_pairs, total_weight).into();
        let storage_price    = quorum_below(&price_pairs, total_weight).into();

        env.process_instruction(
            &instruction,
            &accounts,
            &[
                Check::success(),
                Check::account(&system_address).data(
                    System {
                        spools: expected_seats,
                        spools_prev: system.spools,
                        committee_prev: system.committee,
                        committee: expected_committee,
                        committee_next: Committee::new(),  // Cleared after epoch advance
                        ..system
                    }.pack().as_ref()
                ).build(),
                Check::account(&epoch_address).data(
                    Epoch {
                        id: e1,
                        state: EpochState::syncing(),
                        last_epoch: env.now(),
                    }.pack().as_ref()
                ).build(),
                Check::account(&archive_address).data({
                    Archive {
                        schedule,

                        rewards_pool: TAPE(1000),      // fees_prev + leftover(=0)
                        rewards_paid: TAPE(0),         // reset
                        recent_usage: StorageUnits(500),

                        storage_capacity,
                        storage_price,

                        ..archive
                    }.pack().as_ref()
                }).build(),
            ]
        );
    }

    #[test]
    fn test_advance_too_soon() {
        // Test that advance fails if EPOCH_DURATION hasn't elapsed (in normal mode)
        let env = test_env();

        let fee_payer = Pubkey::new_unique();
        let authority = Pubkey::new_unique();

        let instruction = build_advance_epoch_ix(fee_payer, authority);

        let (system_address, _) = system_pda();
        let (archive_address, _) = archive_pda();
        let (epoch_address, _) = epoch_pda();

        let mut epoch = Epoch::zeroed();
        let mut archive = Archive::zeroed();
        let mut system = System::zeroed();

        // Recent last_epoch - not enough time has passed (EPOCH_DURATION is 60 seconds)
        let last_epoch = env.now() - 30; // Only 30 seconds ago, need 60

        epoch.id = EpochNumber(2);
        epoch.state = EpochState::active();
        epoch.last_epoch = last_epoch;

        // Need >= MIN_COMMITTEE_SIZE (24) members in current committee for normal mode
        let members: Vec<CommitteeMember> = (1..=25)
            .map(|i| member(i, 1_000, 1_000_000, 1000))
            .collect();
        system.committee = Committee::from_members(&members);
        system.committee_next = Committee::from_members(&[
            member(1, 3_000, 1_000_000, 1000),
        ]);

        archive.schedule = EpochSchedule::new_at(epoch.id);

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            sol(authority, 0),
            pda(system_address, system.pack(), tapedrive::ID),
            pda(archive_address, archive.pack(), tapedrive::ID),
            pda(epoch_address, epoch.pack(), tapedrive::ID),
        ];

        env.process_instruction(
            &instruction,
            &accounts,
            &[
                Check::err(TapeError::TooSoon.into()),
            ]
        );
    }

    #[test]
    fn test_advance_bad_state() {
        // Test that advance fails if not in Active state (in normal mode)
        let env = test_env();

        let fee_payer = Pubkey::new_unique();
        let authority = Pubkey::new_unique();

        let instruction = build_advance_epoch_ix(fee_payer, authority);

        let (system_address, _) = system_pda();
        let (archive_address, _) = archive_pda();
        let (epoch_address, _) = epoch_pda();

        let mut epoch = Epoch::zeroed();
        let mut archive = Archive::zeroed();
        let mut system = System::zeroed();

        let last_epoch = env.now() - (EPOCH_DURATION + 100);

        epoch.id = EpochNumber(2);
        epoch.state = EpochState::syncing(); // Wrong state - should be Active
        epoch.last_epoch = last_epoch;

        // Need >= MIN_COMMITTEE_SIZE (24) members in current committee for normal mode
        let members: Vec<CommitteeMember> = (1..=25)
            .map(|i| member(i, 1_000, 1_000_000, 1000))
            .collect();
        system.committee = Committee::from_members(&members);
        system.committee_next = Committee::from_members(&[
            member(1, 3_000, 1_000_000, 1000),
        ]);

        archive.schedule = EpochSchedule::new_at(epoch.id);

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            sol(authority, 0),
            pda(system_address, system.pack(), tapedrive::ID),
            pda(archive_address, archive.pack(), tapedrive::ID),
            pda(epoch_address, epoch.pack(), tapedrive::ID),
        ];

        env.process_instruction(
            &instruction,
            &accounts,
            &[
                Check::err(TapeError::BadEpochState.into()),
            ]
        );
    }

    #[test]
    fn test_low_quorum_advance() {
        // Test that in low-quorum mode, we can advance even without enough time
        let env = test_env();

        let fee_payer = Pubkey::new_unique();
        let authority = Pubkey::new_unique();

        let instruction = build_advance_epoch_ix(fee_payer, authority);

        let (system_address, _) = system_pda();
        let (archive_address, _) = archive_pda();
        let (epoch_address, _) = epoch_pda();

        let mut epoch = Epoch::zeroed();
        let mut archive = Archive::zeroed();
        let mut system = System::zeroed();

        // Recent last_epoch - in low-quorum mode this should be OK
        let last_epoch = env.now() - 100;

        let e0 = EpochNumber(2);

        epoch.id = e0;
        epoch.state = EpochState::active();
        epoch.last_epoch = last_epoch;

        // Current committee has only 1 node (< MIN_COMMITTEE_SIZE), so we're in low-quorum
        system.committee = Committee::from_members(&[
            member(1, 1_000, 1_000_000, 1000),
        ]);

        // Next committee has 2 nodes (still low-quorum)
        system.committee_next = Committee::from_members(&[
            member(1, 1_000, 1_000_000, 1000),
            member(2, 2_000, 1_000_000, 1000),
        ]);

        archive.schedule = EpochSchedule::new_at(epoch.id);

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            sol(authority, 0),
            pda(system_address, system.pack(), tapedrive::ID),
            pda(archive_address, archive.pack(), tapedrive::ID),
            pda(epoch_address, epoch.pack(), tapedrive::ID),
        ];

        // Expected state: epoch advances, state is active (low-quorum), committee rotates
        let mut expected_epoch = Epoch::zeroed();
        expected_epoch.id = e0 + EpochNumber(1);
        expected_epoch.state = EpochState::active();  // Stays in next_ready (low-quorum)
        expected_epoch.last_epoch = env.now();

        // Should succeed despite not enough time passing
        env.process_instruction(
            &instruction,
            &accounts,
            &[
                Check::success(),
                // Verify epoch state is active (not syncing) since entering low-quorum
                Check::account(&epoch_address).data(
                    expected_epoch.pack().as_ref()
                ).build(),
            ]
        );
    }

    #[test]
    fn test_low_quorum_too_soon() {
        // Test that MIN_EPOCH_DURATION is still enforced in low-quorum mode
        let env = test_env();

        let fee_payer = Pubkey::new_unique();
        let authority = Pubkey::new_unique();

        let instruction = build_advance_epoch_ix(fee_payer, authority);

        let (system_address, _) = system_pda();
        let (archive_address, _) = archive_pda();
        let (epoch_address, _) = epoch_pda();

        let mut epoch = Epoch::zeroed();
        let mut archive = Archive::zeroed();
        let mut system = System::zeroed();

        // Only 10 seconds ago - less than MIN_EPOCH_DURATION (30 seconds)
        let last_epoch = env.now() - 10;

        epoch.id = EpochNumber(2);
        epoch.state = EpochState::active();
        epoch.last_epoch = last_epoch;

        // Current committee has only 1 node (< MIN_COMMITTEE_SIZE), so we're in low-quorum
        system.committee = Committee::from_members(&[
            member(1, 1_000, 1_000_000, 1000),
        ]);

        // Next committee also has 1 node
        system.committee_next = Committee::from_members(&[
            member(1, 1_000, 1_000_000, 1000),
        ]);

        archive.schedule = EpochSchedule::new_at(epoch.id);

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            sol(authority, 0),
            pda(system_address, system.pack(), tapedrive::ID),
            pda(archive_address, archive.pack(), tapedrive::ID),
            pda(epoch_address, epoch.pack(), tapedrive::ID),
        ];

        // Should fail with TooSoon even in low-quorum mode
        env.process_instruction(
            &instruction,
            &accounts,
            &[
                Check::err(TapeError::TooSoon.into()),
            ]
        );
    }

    #[test]
    fn test_low_quorum_empty_committee_next() {
        // Test that in low-quorum mode with empty committee_next, we advance counters
        let env = test_env();

        let fee_payer = Pubkey::new_unique();
        let authority = Pubkey::new_unique();

        let instruction = build_advance_epoch_ix(fee_payer, authority);

        let (system_address, _) = system_pda();
        let (archive_address, _) = archive_pda();
        let (epoch_address, _) = epoch_pda();

        let mut epoch = Epoch::zeroed();
        let mut archive = Archive::zeroed();
        let mut system = System::zeroed();

        let last_epoch = env.now() - (EPOCH_DURATION + 100);

        let e0 = EpochNumber(5);

        epoch.id = e0;
        epoch.state = EpochState::active();
        epoch.last_epoch = last_epoch;

        // Current committee has < MIN_COMMITTEE_SIZE, so we're in low-quorum
        system.committee = Committee::from_members(&[
            member(1, 1_000, 1_000_000, 1000),
        ]);

        // Empty next committee
        system.committee_next = Committee::new();

        archive.schedule = EpochSchedule::new_at(epoch.id);

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            sol(authority, 0),
            pda(system_address, system.pack(), tapedrive::ID),
            pda(archive_address, archive.pack(), tapedrive::ID),
            pda(epoch_address, epoch.pack(), tapedrive::ID),
        ];

        // Expected state: epoch counter advances, state stays active
        let mut expected_epoch = Epoch::zeroed();
        expected_epoch.id = e0 + EpochNumber(1);
        expected_epoch.state = EpochState::active();
        expected_epoch.last_epoch = env.now();

        // Should succeed and advance epoch counter
        env.process_instruction(
            &instruction,
            &accounts,
            &[
                Check::success(),
                Check::account(&epoch_address).data(
                    expected_epoch.pack().as_ref()
                ).build(),
            ]
        );
    }

    #[test]
    fn test_transition_low_quorum_to_normal() {
        // Test transition from low-quorum to normal mode when committee_next is large enough
        let env = test_env();

        let fee_payer = Pubkey::new_unique();
        let authority = Pubkey::new_unique();

        let instruction = build_advance_epoch_ix(fee_payer, authority);

        let (system_address, _) = system_pda();
        let (archive_address, _) = archive_pda();
        let (epoch_address, _) = epoch_pda();

        let mut epoch = Epoch::zeroed();
        let mut archive = Archive::zeroed();
        let mut system = System::zeroed();

        let last_epoch = env.now() - (EPOCH_DURATION + 100);

        let e0 = EpochNumber(10);
        let e1 = e0 + EpochNumber(1);
        let e100 = e0 + EpochNumber(100);

        epoch.id = e0;
        epoch.state = EpochState::active();
        epoch.last_epoch = last_epoch;

        // Current committee is small (low-quorum)
        system.committee = Committee::from_members(&[
            member(1, 1_000, 1_000_000, 1000),
        ]);

        // Next committee has >= MIN_COMMITTEE_SIZE (24) nodes
        let members: Vec<CommitteeMember> = (1..=25)
            .map(|i| member(i, 1_000, 1_000_000, 1000))
            .collect();
        system.committee_next = Committee::from_members(&members);

        archive.schedule = EpochSchedule::new_at(epoch.id);
        archive.schedule.reserve_capacity(
            StorageUnits(500), TAPE(1000), e0, e100
        ).expect("reserve capacity");

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            sol(authority, 0),
            pda(system_address, system.pack(), tapedrive::ID),
            pda(archive_address, archive.pack(), tapedrive::ID),
            pda(epoch_address, epoch.pack(), tapedrive::ID),
        ];

        // Expected epoch state: syncing (normal mode)
        let expected_epoch = Epoch {
            id: e1,
            state: EpochState::syncing(),
            last_epoch: env.now(),
        };

        // Should transition to normal mode (syncing state)
        env.process_instruction(
            &instruction,
            &accounts,
            &[
                Check::success(),
                Check::account(&epoch_address).data(
                    expected_epoch.pack().as_ref()
                ).build(),
            ]
        );
    }
}
