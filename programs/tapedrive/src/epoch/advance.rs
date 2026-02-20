use tape_solana::*;
use crate::error::*;
use tape_api::prelude::*;
use tape_api::event::EpochAdvanced;
use tape_crypto::hash::Hash;
use sysvar::slot_hashes::SlotHashes;

/* PHASE1:DISABLED — real advance_epoch logic
pub fn process_advance_epoch_real(accounts: &[AccountInfo<'_>], data: &[u8]) -> ProgramResult {
    let now = Clock::get()?.unix_timestamp;
    let _args = AdvanceEpoch::try_from_bytes(data)?;
    let [
        fee_payer_info,
        authority_info,
        system_info,
        archive_info,
        epoch_info,
        snapshot_state_info,
        slot_hashes_info,
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

    // Check epoch state and timing - always enforce normal requirements
    if !epoch.state.is_active() {
        return Err(TapeError::BadEpochState.into());
    }
    if epoch.last_epoch + EPOCH_DURATION > now {
        return Err(TapeError::TooSoon.into());
    }

    // Ensure the archive schedule is aligned with the current epoch
    if archive.schedule.current_epoch() != epoch.id {
        return Err(TapeError::BadSchedule.into());
    }

    // Snapshot gate
    let snapshot_state = snapshot_state_info
        .is_snapshot_state()?
        .as_account::<SnapshotState>(&tapedrive::ID)?;
    require_previous_snapshot(epoch, &snapshot_state)?;

    // Save old epoch for event logging
    let old_epoch = epoch.id;

    // Block epoch advancement if committee_next is below threshold
    // Exception: Allow bootstrap (first epoch where current committee is empty)
    // Note: We check committee_empty() not committee_prev_empty() because
    // the current committee will become committee_prev after rotation.
    if system.will_be_low_quorum() && !system.committee_empty() {
        return Err(TapeError::InsufficientCommittee.into());
    }

    // Empty committee_next is an error (unless in bootstrap phase handled above)
    if system.committee_next_empty() {
        return Err(TapeError::UnexpectedState.into());
    }

    // Extract seed from slot hashes
    let seed = slot_hash_seed(slot_hashes_info)?;

    // Save previous spools, then reassign for the next committee
    system.spools_prev = system.spools;
    tape_spooler::migrate_dhondt(
        &mut system.spools,
        &system.committee,
        &system.committee_next,
        &seed,
    )
    .map_err(|_| TapeError::UnexpectedState)?;

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

    // Update reward pool: add new rewards from schedule plus leftover
    archive.rewards_paid = TAPE::zero();
    archive.rewards_pool = epoch_usage.paid()
        .saturating_add(leftover);
    archive.recent_usage = epoch_usage.reserved();

    // Advance epoch metadata - always transition to Syncing
    epoch.id = next_epoch(epoch);
    epoch.last_epoch = now;
    epoch.state = EpochState::syncing();
    epoch.nonce = seed;

    // Update storage price/capacity from committee preferences
    update_storage_params(archive, &system.committee);

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
        nonce: seed,
    }.log();

    solana_program::msg!(
        "Advanced to {}, capacity: {}, price: {}",
        epoch.id,
        archive.storage_capacity,
        archive.storage_price,
    );

    Ok(())
}
*/

pub fn process_advance_epoch(accounts: &[AccountInfo<'_>], data: &[u8]) -> ProgramResult {
    let _args = AdvanceEpoch::try_from_bytes(data)?;
    let [
        _fee_payer_info,
        authority_info,
        system_info,
        _archive_info,
        epoch_info,
        _snapshot_state_info,
        slot_hashes_info,
    ] = accounts else {
        return Err(ProgramError::NotEnoughAccountKeys);
    };

    authority_info.is_signer()?;
    let system = system_info
        .is_writable()?
        .is_system()?
        .as_account_mut::<System>(&tapedrive::ID)?;
    let epoch = epoch_info
        .is_writable()?
        .is_epoch()?
        .as_account_mut::<Epoch>(&tapedrive::ID)?;

    // PHASE1: only enforce timing, plus committee rotation for activation
    let now = Clock::get()?.unix_timestamp;
    if epoch.last_epoch + EPOCH_DURATION > now {
        return Err(TapeError::TooSoon.into());
    }

    let old_epoch = epoch.id;
    let seed = slot_hash_seed(slot_hashes_info)?;

    // PHASE1: rotate committees so active nodes are discoverable after bootstrap
    system.rotate_committees();
    // PHASE1: self-sustain for phase1; no JoinNetwork means reuse committee as next committee
    system.committee_next = system.committee;
    let committee_size = system.committee.size() as u64;

    epoch.id = next_epoch(epoch);
    epoch.last_epoch = now;
    epoch.state = EpochState::active(); // PHASE1: skip Syncing→Settling→Active dance
    epoch.nonce = seed;

    EpochAdvanced {
        old_epoch,
        new_epoch: epoch.id,
        timestamp: (now as u64).to_le_bytes(),
        committee_size: committee_size.to_le_bytes(),
        total_stake: 0u64.to_le_bytes(),
        storage_price: 0u64.to_le_bytes(),
        storage_capacity: StorageUnits(0),
        nonce: seed,
    }
    .log();

    solana_program::msg!("AdvanceEpoch (phase1): {} -> {}", old_epoch, epoch.id);
    Ok(())
}

fn require_previous_snapshot(epoch: &Epoch, snapshot: &SnapshotState) -> ProgramResult {
    if epoch.id > EpochNumber(1) {
        let required = epoch.id - EpochNumber(1);
        if snapshot.latest_epoch < required {
            return Err(TapeError::SnapshotIncomplete.into());
        }
    }
    Ok(())
}

fn slot_hash_seed(slot_hashes_info: &AccountInfo<'_>) -> Result<Hash, ProgramError> {
    // SlotHashes binary layout: 8-byte count, then (8-byte slot + 32-byte hash) entries.
    // First entry's hash is at bytes 16..48.

    slot_hashes_info.is_sysvar(&sysvar::slot_hashes::ID)?;
    let slot_hashes_data = slot_hashes_info.try_borrow_data()?;
    let seed = Hash(
        slot_hashes_data[16..48]
            .try_into()
            .map_err(|_| TapeError::UnexpectedState)?
    );
    Ok(seed)
}

fn update_storage_params(archive: &mut Archive, committee: &Committee<MEMBER_COUNT>) {
    let mut total_weight = 0u64;
    let mut storage_prices = Vec::new();
    let mut storage_capacities = Vec::new();

    for member in committee.iter() {
        let weight = member.weight as u64;

        storage_prices.push((member.preferences.storage_price.into(), weight));
        storage_capacities.push((member.preferences.storage_capacity.into(), weight));

        total_weight = total_weight.saturating_add(weight);
    }

    archive.storage_capacity =
        quorum_above(&storage_capacities, total_weight).into();
    archive.storage_price =
        quorum_below(&storage_prices, total_weight).into();
}


#[cfg(test)]
mod tests {
    use super::*;
    use tape_test::*;
    use tape_spooler::{dhondt_allocate, migrate_spools};

    fn member(id: u64, stake: u64, size: u64, price: u64) -> CommitteeMember {
        let mut m = CommitteeMember::new(NodeId(id), TAPE(stake));
        m.preferences.storage_capacity = StorageUnits(size);
        m.preferences.storage_price = TAPE(price);
        m
    }

    /// Build a fake SlotHashes sysvar account with a single entry containing a zero hash.
    fn slot_hashes_account() -> (Pubkey, solana_sdk::account::Account) {
        // Layout: 8-byte count + (8-byte slot + 32-byte hash) per entry
        let mut data = vec![0u8; 48];
        data[0] = 1; // count = 1 (little-endian u64)
        // slot = 0, hash = [0; 32] (all zeros = Hash::default)
        (sysvar::slot_hashes::ID, solana_sdk::account::Account {
            lamports: 1,
            data,
            owner: sysvar::ID,
            executable: false,
            rent_epoch: 0,
        })
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
        let (snapshot_state_address, _) = snapshot_state_pda();

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

        // Need >= MIN_COMMITTEE_SIZE (20) members for normal mode
        let prev_members: Vec<CommitteeMember> = (1..=20)
            .map(|i| member(i, 1_000 + i * 100, 8_000_000, 950))
            .collect();
        system.committee_prev = Committee::from_members(&prev_members);

        let curr_members: Vec<CommitteeMember> = (1..=21)
            .map(|i| member(i, 1_000 + i * 100, 8_050_000, 1050))
            .collect();
        system.committee = Committee::from_members(&curr_members);

        let next_members: Vec<CommitteeMember> = (1..=22)
            .map(|i| member(i, 1_000 + i * 100, 1_500_000, 850))
            .collect();
        system.committee_next = Committee::from_members(&next_members);

        archive.schedule = EpochSchedule::new_at(epoch.id);
        archive.schedule.reserve_capacity(
            StorageUnits(500), TAPE(1000), e0, e100
        ).expect("reserve capacity");

        // Snapshot for epoch 41 must be complete to advance from epoch 42
        let snapshot_state = SnapshotState {
            latest_epoch: EpochNumber(41),
            ..SnapshotState::zeroed()
        };

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            sol(authority, 0),
            pda(system_address, system.pack(), tapedrive::ID),
            pda(archive_address, archive.pack(), tapedrive::ID),
            pda(epoch_address, epoch.pack(), tapedrive::ID),
            pda(snapshot_state_address, snapshot_state.pack(), tapedrive::ID),
            slot_hashes_account(),
        ];

        // Expected state after instruction

        let stakes = system.committee_next.active_stakes();
        let seat_count = dhondt_allocate(
            &stakes,
            SPOOL_COUNT as u16,
        ).unwrap();

        let seed = tape_crypto::hash::Hash::default();
        let spools = migrate_spools(
            &system.spools.0,
            &system.committee.active_members(),
            &system.committee_next.active_members(),
            &seat_count,
            &seed,
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
                        nonce: Hash::default(),
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
        let (snapshot_state_address, _) = snapshot_state_pda();

        let mut epoch = Epoch::zeroed();
        let mut archive = Archive::zeroed();
        let mut system = System::zeroed();

        // Recent last_epoch - not enough time has passed (EPOCH_DURATION is 5 seconds locally)
        let last_epoch = env.now() - 2; // Only 2 seconds ago, need 5

        epoch.id = EpochNumber(2);
        epoch.state = EpochState::active();
        epoch.last_epoch = last_epoch;

        // Need >= MIN_COMMITTEE_SIZE members in both committees to test timing check
        let members: Vec<CommitteeMember> = (1..=20)
            .map(|i| member(i, 1_000, 1_000_000, 1000))
            .collect();
        system.committee = Committee::from_members(&members);
        // committee_next must have >= 20 to pass the low-quorum check and reach TooSoon
        system.committee_next = Committee::from_members(&members);

        archive.schedule = EpochSchedule::new_at(epoch.id);

        let snapshot_state = SnapshotState {
            latest_epoch: EpochNumber(1),
            ..SnapshotState::zeroed()
        };

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            sol(authority, 0),
            pda(system_address, system.pack(), tapedrive::ID),
            pda(archive_address, archive.pack(), tapedrive::ID),
            pda(epoch_address, epoch.pack(), tapedrive::ID),
            pda(snapshot_state_address, snapshot_state.pack(), tapedrive::ID),
            slot_hashes_account(),
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
        let (snapshot_state_address, _) = snapshot_state_pda();

        let mut epoch = Epoch::zeroed();
        let mut archive = Archive::zeroed();
        let mut system = System::zeroed();

        let last_epoch = env.now() - (EPOCH_DURATION + 100);

        epoch.id = EpochNumber(2);
        epoch.state = EpochState::syncing(); // Wrong state - should be Active
        epoch.last_epoch = last_epoch;

        // Need >= MIN_COMMITTEE_SIZE (20) members in current committee for normal mode
        let members: Vec<CommitteeMember> = (1..=20)
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
            pda(snapshot_state_address, SnapshotState::zeroed().pack(), tapedrive::ID),
            slot_hashes_account(),
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
    fn test_advance_blocked_below_threshold() {
        // Test that AdvanceEpoch is blocked when committee_next < MIN_COMMITTEE_SIZE
        let env = test_env();

        let fee_payer = Pubkey::new_unique();
        let authority = Pubkey::new_unique();

        let instruction = build_advance_epoch_ix(fee_payer, authority);

        let (system_address, _) = system_pda();
        let (archive_address, _) = archive_pda();
        let (epoch_address, _) = epoch_pda();
        let (snapshot_state_address, _) = snapshot_state_pda();

        let mut epoch = Epoch::zeroed();
        let mut archive = Archive::zeroed();
        let mut system = System::zeroed();

        let last_epoch = env.now() - (EPOCH_DURATION + 100);

        let e0 = EpochNumber(2);

        epoch.id = e0;
        epoch.state = EpochState::active();
        epoch.last_epoch = last_epoch;

        // Current committee has enough nodes (20)
        let curr_members: Vec<CommitteeMember> = (1..=20)
            .map(|i| member(i, 1_000, 1_000_000, 1000))
            .collect();
        system.committee = Committee::from_members(&curr_members);

        // Previous committee exists (not bootstrap)
        let prev_members: Vec<CommitteeMember> = (1..=20)
            .map(|i| member(i, 1_000, 1_000_000, 1000))
            .collect();
        system.committee_prev = Committee::from_members(&prev_members);

        // Next committee has < MIN_COMMITTEE_SIZE (only 19 nodes)
        let next_members: Vec<CommitteeMember> = (1..=19)
            .map(|i| member(i, 1_000, 1_000_000, 1000))
            .collect();
        system.committee_next = Committee::from_members(&next_members);

        archive.schedule = EpochSchedule::new_at(epoch.id);

        let snapshot_state = SnapshotState {
            latest_epoch: EpochNumber(1),
            ..SnapshotState::zeroed()
        };

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            sol(authority, 0),
            pda(system_address, system.pack(), tapedrive::ID),
            pda(archive_address, archive.pack(), tapedrive::ID),
            pda(epoch_address, epoch.pack(), tapedrive::ID),
            pda(snapshot_state_address, snapshot_state.pack(), tapedrive::ID),
            slot_hashes_account(),
        ];

        // Should fail with InsufficientCommittee since committee_next < 20
        env.process_instruction(
            &instruction,
            &accounts,
            &[
                Check::err(TapeError::InsufficientCommittee.into()),
            ]
        );
    }

    #[test]
    fn test_advance_allowed_at_threshold() {
        // Test that AdvanceEpoch succeeds when committee_next == MIN_COMMITTEE_SIZE
        let env = test_env();

        let fee_payer = Pubkey::new_unique();
        let authority = Pubkey::new_unique();

        let instruction = build_advance_epoch_ix(fee_payer, authority);

        let (system_address, _) = system_pda();
        let (archive_address, _) = archive_pda();
        let (epoch_address, _) = epoch_pda();
        let (snapshot_state_address, _) = snapshot_state_pda();

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

        // Current committee has nodes
        let curr_members: Vec<CommitteeMember> = (1..=20)
            .map(|i| member(i, 1_000, 1_000_000, 1000))
            .collect();
        system.committee = Committee::from_members(&curr_members);

        // Previous committee exists
        let prev_members: Vec<CommitteeMember> = (1..=20)
            .map(|i| member(i, 1_000, 1_000_000, 1000))
            .collect();
        system.committee_prev = Committee::from_members(&prev_members);

        // Next committee has exactly MIN_COMMITTEE_SIZE (20) nodes
        let next_members: Vec<CommitteeMember> = (1..=20)
            .map(|i| member(i, 1_000, 1_000_000, 1000))
            .collect();
        system.committee_next = Committee::from_members(&next_members);

        archive.schedule = EpochSchedule::new_at(epoch.id);
        archive.schedule.reserve_capacity(
            StorageUnits(500), TAPE(1000), e0, e100
        ).expect("reserve capacity");

        let snapshot_state = SnapshotState {
            latest_epoch: EpochNumber(9),
            ..SnapshotState::zeroed()
        };

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            sol(authority, 0),
            pda(system_address, system.pack(), tapedrive::ID),
            pda(archive_address, archive.pack(), tapedrive::ID),
            pda(epoch_address, epoch.pack(), tapedrive::ID),
            pda(snapshot_state_address, snapshot_state.pack(), tapedrive::ID),
            slot_hashes_account(),
        ];

        // Expected epoch state: syncing
        let expected_epoch = Epoch {
            id: e1,
            state: EpochState::syncing(),
            last_epoch: env.now(),
            nonce: Hash::default(),
        };

        // Should succeed since committee_next == 20
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
    fn test_advance_bootstrap_exception() {
        // Test that first epoch (empty committee) can advance with < MIN_COMMITTEE_SIZE
        // Bootstrap = committee is empty, only committee_next has nodes
        let env = test_env();

        let fee_payer = Pubkey::new_unique();
        let authority = Pubkey::new_unique();

        let instruction = build_advance_epoch_ix(fee_payer, authority);

        let (system_address, _) = system_pda();
        let (archive_address, _) = archive_pda();
        let (epoch_address, _) = epoch_pda();
        let (snapshot_state_address, _) = snapshot_state_pda();

        let mut epoch = Epoch::zeroed();
        let mut archive = Archive::zeroed();
        let mut system = System::zeroed();

        let last_epoch = env.now() - (EPOCH_DURATION + 100);

        let e0 = EpochNumber(1);
        let e1 = e0 + EpochNumber(1);
        let e100 = e0 + EpochNumber(100);

        epoch.id = e0;
        epoch.state = EpochState::active();
        epoch.last_epoch = last_epoch;

        // Current committee is EMPTY (true bootstrap - first epoch)
        system.committee = Committee::new();

        // Previous committee is also EMPTY
        system.committee_prev = Committee::new();

        // Next committee has < MIN_COMMITTEE_SIZE (20 nodes joining)
        let next_members: Vec<CommitteeMember> = (1..=20)
            .map(|i| member(i, 1_000, 1_000_000, 1000))
            .collect();
        system.committee_next = Committee::from_members(&next_members);

        archive.schedule = EpochSchedule::new_at(epoch.id);
        archive.schedule.reserve_capacity(
            StorageUnits(500), TAPE(1000), e0, e100
        ).expect("reserve capacity");

        // Epoch 1 — snapshot gate is skipped (bootstrap)
        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            sol(authority, 0),
            pda(system_address, system.pack(), tapedrive::ID),
            pda(archive_address, archive.pack(), tapedrive::ID),
            pda(epoch_address, epoch.pack(), tapedrive::ID),
            pda(snapshot_state_address, SnapshotState::zeroed().pack(), tapedrive::ID),
            slot_hashes_account(),
        ];

        // Expected epoch state: syncing (bootstrap allowed)
        let expected_epoch = Epoch {
            id: e1,
            state: EpochState::syncing(),
            last_epoch: env.now(),
            nonce: Hash::default(),
        };

        // Should succeed due to bootstrap exception (empty committee)
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
    fn test_advance_blocked_after_bootstrap() {
        // Test that once we have a functioning committee, low quorum is blocked
        // even if committee_prev is empty (second epoch scenario)
        let env = test_env();

        let fee_payer = Pubkey::new_unique();
        let authority = Pubkey::new_unique();

        let instruction = build_advance_epoch_ix(fee_payer, authority);

        let (system_address, _) = system_pda();
        let (archive_address, _) = archive_pda();
        let (epoch_address, _) = epoch_pda();
        let (snapshot_state_address, _) = snapshot_state_pda();

        let mut epoch = Epoch::zeroed();
        let mut archive = Archive::zeroed();
        let mut system = System::zeroed();

        let last_epoch = env.now() - (EPOCH_DURATION + 100);

        let e0 = EpochNumber(2);

        epoch.id = e0;
        epoch.state = EpochState::active();
        epoch.last_epoch = last_epoch;

        // Current committee has 20 nodes (functioning committee)
        let curr_members: Vec<CommitteeMember> = (1..=20)
            .map(|i| member(i, 1_000, 1_000_000, 1000))
            .collect();
        system.committee = Committee::from_members(&curr_members);

        // Previous committee is EMPTY (this is epoch 2, first real epoch)
        system.committee_prev = Committee::new();

        // Next committee has < MIN_COMMITTEE_SIZE (only 10 rejoined)
        let next_members: Vec<CommitteeMember> = (1..=10)
            .map(|i| member(i, 1_000, 1_000_000, 1000))
            .collect();
        system.committee_next = Committee::from_members(&next_members);

        archive.schedule = EpochSchedule::new_at(epoch.id);

        let snapshot_state = SnapshotState {
            latest_epoch: EpochNumber(1),
            ..SnapshotState::zeroed()
        };

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            sol(authority, 0),
            pda(system_address, system.pack(), tapedrive::ID),
            pda(archive_address, archive.pack(), tapedrive::ID),
            pda(epoch_address, epoch.pack(), tapedrive::ID),
            pda(snapshot_state_address, snapshot_state.pack(), tapedrive::ID),
            slot_hashes_account(),
        ];

        // Should FAIL - committee is non-empty so bootstrap exception doesn't apply
        env.process_instruction(
            &instruction,
            &accounts,
            &[
                Check::err(TapeError::InsufficientCommittee.into()),
            ]
        );
    }

    #[test]
    fn test_advance_empty_committee_next_fails() {
        // Test that empty committee_next always fails (even during bootstrap)
        let env = test_env();

        let fee_payer = Pubkey::new_unique();
        let authority = Pubkey::new_unique();

        let instruction = build_advance_epoch_ix(fee_payer, authority);

        let (system_address, _) = system_pda();
        let (archive_address, _) = archive_pda();
        let (epoch_address, _) = epoch_pda();
        let (snapshot_state_address, _) = snapshot_state_pda();

        let mut epoch = Epoch::zeroed();
        let mut archive = Archive::zeroed();
        let mut system = System::zeroed();

        let last_epoch = env.now() - (EPOCH_DURATION + 100);

        let e0 = EpochNumber(5);

        epoch.id = e0;
        epoch.state = EpochState::active();
        epoch.last_epoch = last_epoch;

        // Current committee has nodes
        let curr_members: Vec<CommitteeMember> = (1..=20)
            .map(|i| member(i, 1_000, 1_000_000, 1000))
            .collect();
        system.committee = Committee::from_members(&curr_members);

        // Previous committee exists
        let prev_members: Vec<CommitteeMember> = (1..=20)
            .map(|i| member(i, 1_000, 1_000_000, 1000))
            .collect();
        system.committee_prev = Committee::from_members(&prev_members);

        // Empty next committee
        system.committee_next = Committee::new();

        archive.schedule = EpochSchedule::new_at(epoch.id);

        let snapshot_state = SnapshotState {
            latest_epoch: EpochNumber(4),
            ..SnapshotState::zeroed()
        };

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            sol(authority, 0),
            pda(system_address, system.pack(), tapedrive::ID),
            pda(archive_address, archive.pack(), tapedrive::ID),
            pda(epoch_address, epoch.pack(), tapedrive::ID),
            pda(snapshot_state_address, snapshot_state.pack(), tapedrive::ID),
            slot_hashes_account(),
        ];

        // Should fail with InsufficientCommittee (empty is < 20)
        env.process_instruction(
            &instruction,
            &accounts,
            &[
                Check::err(TapeError::InsufficientCommittee.into()),
            ]
        );
    }

    #[test]
    fn test_advance_blocked_snapshot_incomplete() {
        // Test that epoch advance is blocked when the previous epoch's snapshot is not done
        let env = test_env();

        let fee_payer = Pubkey::new_unique();
        let authority = Pubkey::new_unique();

        let instruction = build_advance_epoch_ix(fee_payer, authority);

        let (system_address, _) = system_pda();
        let (archive_address, _) = archive_pda();
        let (epoch_address, _) = epoch_pda();
        let (snapshot_state_address, _) = snapshot_state_pda();

        let mut epoch = Epoch::zeroed();
        let mut archive = Archive::zeroed();
        let mut system = System::zeroed();

        let last_epoch = env.now() - (EPOCH_DURATION + 100);

        let e0 = EpochNumber(5);

        epoch.id = e0;
        epoch.state = EpochState::active();
        epoch.last_epoch = last_epoch;

        let members: Vec<CommitteeMember> = (1..=20)
            .map(|i| member(i, 1_000, 1_000_000, 1000))
            .collect();
        system.committee = Committee::from_members(&members);
        system.committee_prev = Committee::from_members(&members);
        system.committee_next = Committee::from_members(&members);

        archive.schedule = EpochSchedule::new_at(epoch.id);

        // latest_epoch = 3 but we need >= 4 to advance from epoch 5
        let snapshot_state = SnapshotState {
            latest_epoch: EpochNumber(3),
            ..SnapshotState::zeroed()
        };

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            sol(authority, 0),
            pda(system_address, system.pack(), tapedrive::ID),
            pda(archive_address, archive.pack(), tapedrive::ID),
            pda(epoch_address, epoch.pack(), tapedrive::ID),
            pda(snapshot_state_address, snapshot_state.pack(), tapedrive::ID),
            slot_hashes_account(),
        ];

        env.process_instruction(
            &instruction,
            &accounts,
            &[
                Check::err(TapeError::SnapshotIncomplete.into()),
            ]
        );
    }
}
