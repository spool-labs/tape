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

    debug_assert!(archive.schedule.current_epoch() == epoch.id);

    // Save previous seats, then reassign for the next committee
    system.seats_prev = system.seats;
    system.seats.reassign(
        &system.committee,
        &system.committee_next,
    ).map_err(|_| TapeError::UnexpectedState)?;

    // Rotate committees
    system.committee_prev = system.committee;
    system.committee = system.committee_next;

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
            blacklist: StorageUnits(0),
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

        let e0 = EpochNumber(42);
        let e1 = e0 + EpochNumber(1);
        let e100 = e0 + EpochNumber(100);

        epoch.id = e0;
        epoch.state = EpochState::next_ready();
        epoch.last_epoch_ms = last_epoch;

        system.committee_prev = Committee::from_members(&[ member(2, 2_000), member(1, 1_000) ]);
        system.committee      = Committee::from_members(&[ member(3, 3_000), member(2, 2_000), member(1, 1_000) ]);
        system.committee_next = Committee::from_members(&[ member(3, 3_500), member(4, 2_100), member(2, 2_000), member(1, 1_000) ]);

        // Pre-fill archive usage and fees
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
            &system.seats.seats,
            &system.committee.active_members(),
            &system.committee_next.active_members(),
            &seat_count,
        ).expect("seat reassignment failed");

        let expected_seats = Seats::try_from(seats.as_ref()).unwrap();

        let mut schedule = EpochSchedule::new_at(e1);
        schedule.reserve_capacity(
            StorageUnits(500), TAPE(1000), e1, e100
        ).expect("reserve capacity");


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

                        ..archive
                    }.pack().as_ref()
                }).build(),
            ]
        );
    }
}
