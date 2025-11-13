use steel::*;
use crate::error::*;
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
    }

    if epoch.last_epoch_ms + EPOCH_DURATION > now {
        return Err(ProgramError::Custom(3));
    }

    // Ensure the archive schedule is aligned with the current epoch
    debug_assert!(archive.schedule.current_epoch() == epoch.id);

    // Save previous seats, then reassign for the next committee
    system.seats_prev = system.seats;
    system.seats.reassign(
        &system.committee,
        &system.committee_next,
    ).map_err(|_| TapeError::UnexpectedState)?;

    // Rotate committees
    system.committee_prev = system.committee.clone();
    system.committee = system.committee_next.clone();

    system.committee
        .apply_weights_from_seats(&system.seats);

    // The next committee should never have any weights assigned
    debug_assert!(system.committee_next.iter().all(|m| m.weight == 0));

    // Update future accounting
    let epoch_usage = archive.schedule
        .advance_epoch();

    // Carry-over dust from last epoch
    let leftover = archive.rewards_pool
        .saturating_sub(archive.rewards_paid);

    archive.rewards_paid = TAPE::zero();
    archive.rewards_pool = epoch_usage.paid()
        .saturating_add(leftover);
    archive.recent_usage = epoch_usage.reserved();

    // Advance epoch metadata
    epoch.id = next_epoch(epoch);
    epoch.state.set_syncing();
    epoch.last_epoch_ms = now;

    // Update the archive storage price and capacity based on the new committee preferences
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

        let e0 = EpochNumber(42);
        let e1 = e0 + EpochNumber(1);
        let e100 = e0 + EpochNumber(100);

        epoch.id = e0;
        epoch.state = EpochState::next_ready();
        epoch.last_epoch_ms = last_epoch;

        system.committee_prev = Committee::from_members(&[
            member(2, 2_000, 8_000_000, 950),
            member(1, 1_000, 9_000_000, 1150),
        ]);
        system.committee = Committee::from_members(&[
            member(3, 3_000, 8_050_000, 1050),
            member(2, 2_000, 9_050_000, 1250),
            member(1, 1_000, 9_000_000, 1150),
        ]);
        system.committee_next = Committee::from_members(&[
            member(3, 3_500, 1_500_000, 850),
            member(4, 2_100, 9_000_000, 1250),
            member(2, 2_000, 1_300_000, 1050),
            member(1, 1_000, 1_100_000, 1150),
        ]);

        archive.schedule = EpochSchedule::new_at(epoch.id);
        archive.schedule.reserve_capacity(
            StorageUnits(500), TAPE(1000), e0, e100
        ).expect("reserve capacity");

        let accounts = vec![
            sol(signer, 1_000_000_000),
            pda(system_address, system.pack(), tapedrive::ID),
            pda(archive_address, archive.pack(), tapedrive::ID),
            pda(epoch_address, epoch.pack(), tapedrive::ID),
        ];

        // Expected state after instruction

        let seat_count = dhondt_allocate(
            &system.committee_next.active_stakes(),
            SEAT_COUNT as u16,
        );

        let seats = reassign_seats(
            &system.seats.0,
            &system.committee.active_members(),
            &system.committee_next.active_members(),
            &seat_count,
        ).expect("seat reassignment failed");

        let expected_seats = Seats::try_from(seats.as_ref()).unwrap();

        let mut expected_committee = system.committee_next.clone();
        expected_committee
            .apply_weights_from_seats(&expected_seats);

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
                        seats: expected_seats,
                        seats_prev: system.seats,
                        committee_prev: system.committee,
                        committee: expected_committee,
                        ..system
                    }.pack().as_ref()
                ).build(),
                Check::account(&epoch_address).data(
                    Epoch {
                        id: e1,
                        state: EpochState::syncing(),
                        last_epoch_ms: env.now(),
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
}
