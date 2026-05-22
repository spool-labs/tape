use tape_solana::*;
use tape_api::program::prelude::*;
use tape_api::event::EpochCommitted;

pub fn process_commit_epoch(accounts: &[AccountInfo<'_>], data: &[u8]) -> ProgramResult {
    let _args = CommitEpoch::try_from_bytes(data)?;
    let [
        fee_payer_info,
        system_info,
        curr_epoch_info,
        next_epoch_info,
        next_committee_info,
        snapshot_tape_info,
        slot_hashes_info,
    ] = accounts else {
        return Err(ProgramError::NotEnoughAccountKeys);
    };

    fee_payer_info
        .is_signer()?
        .is_writable()?;

    let system = system_info
        .is_system()?
        .as_account::<System>(&tapedrive::ID)?;

    let curr = system.current_epoch;
    let next = curr.saturating_add(EpochNumber(1));

    let curr_epoch = curr_epoch_info
        .is_writable()?
        .is_epoch(curr)?
        .as_account_mut::<Epoch>(&tapedrive::ID)?;

    if curr_epoch.state.phase != EpochPhase::Active as u64 {
        return Err(TapeError::BadEpochState.into());
    }

    let now = Clock::get()?.unix_timestamp;
    if now.saturating_sub(curr_epoch.start_time) < EPOCH_DURATION {
        return Err(TapeError::TooSoon.into());
    }

    let next_epoch = next_epoch_info
        .is_writable()?
        .is_epoch(next)?
        .as_account_mut::<Epoch>(&tapedrive::ID)?;

    if next_epoch.state.phase != EpochPhase::Unknown as u64 {
        return Err(TapeError::BadEpochState.into());
    }

    next_committee_info
        .is_committee(next)?;

    let (committee_header, _) = Committee::read(next_committee_info, &tapedrive::ID)?;

    if committee_header.epoch != next {
        return Err(TapeError::BadEpochId.into());
    }
    if committee_header.members.capacity != system.committee_size {
        return Err(TapeError::InsufficientCommittee.into());
    }
    if (committee_header.members.count as usize) < GROUP_SIZE {
        return Err(TapeError::InsufficientCommittee.into());
    }

    let prev = curr.saturating_sub(EpochNumber(1));
    snapshot_tape_info
        .is_snapshot_tape(prev)?;

    let next_nonce = slot_hash_seed(slot_hashes_info)?;
    next_epoch.nonce = next_nonce;

    curr_epoch.state.phase = EpochPhase::Closing as u64;

    EpochCommitted {
        epoch: curr,
        next_nonce,
    }.log();

    Ok(())
}

fn slot_hash_seed(slot_hashes_info: &AccountInfo<'_>) -> Result<Hash, ProgramError> {
    slot_hashes_info.is_sysvar(&sysvar::slot_hashes::ID)?;
    let data = slot_hashes_info.try_borrow_data()?;
    let seed = Hash(
        data
            .get(16..48)
            .ok_or(TapeError::UnexpectedState)?
            .try_into()
            .map_err(|_| TapeError::UnexpectedState)?,
    );
    Ok(seed)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tape_api::state::Committee;
    use tape_test::*;

    const COMMITTEE_SIZE: u64 = 128;
    const SPOOL_GROUPS: u64 = 50;

    fn slot_hashes_account() -> (Pubkey, solana_sdk::account::Account) {
        // 8-byte count = 1, then one entry: 8-byte slot + 32-byte hash.
        let mut data = vec![0u8; 48];
        data[0] = 1;
        data[16..48].copy_from_slice(&[0x42u8; 32]); // deterministic test hash
        (
            sysvar::slot_hashes::ID,
            solana_sdk::account::Account {
                lamports: 1,
                data,
                owner: sysvar::ID,
                executable: false,
                rent_epoch: 0,
            },
        )
    }

    fn populated_committee(epoch: EpochNumber) -> Vec<u8> {
        let members: Vec<Member> = (0..GROUP_SIZE)
            .map(|i| {
                let mut bytes = [0u8; 32];
                bytes[0] = (i as u8) + 1;
                Member {
                    node: Address::new(bytes),
                    stake: TAPE(1_000),
                    assigned: StorageUnits::zero(),
                    blacklisted: StorageUnits::zero(),
                    spools: 0,
                }
            })
            .collect();
        Committee { epoch, members: Tail::new(COMMITTEE_SIZE, members.len() as u64) }
            .pack_with(&members)
    }

    #[test]
    fn commit() {
        let fee_payer = Pubkey::new_unique();

        let curr = EpochNumber(10);
        let next = EpochNumber(11);

        let (system_address, _) = system_pda();
        let (archive_address, _) = archive_pda();
        let (curr_epoch_address, _) = epoch_pda(curr);
        let (next_epoch_address, _) = epoch_pda(next);
        let (next_committee_address, _) = committee_pda(next);
        let prev = curr.saturating_sub(EpochNumber(1));
        let (snapshot_tape_address, _) = snapshot_tape_pda(prev);

        let env = test_env();
        let now = env.now();

        let system = System {
            current_epoch: curr,
            target_group_count: SPOOL_GROUPS,
            live_group_count: SPOOL_GROUPS,
            committee_size: COMMITTEE_SIZE,
            ..System::zeroed()
        };

        let curr_epoch = Epoch {
            id: curr,
            start_time: now - EPOCH_DURATION,
            state: EpochState {
                phase: EpochPhase::Active as u64,
                ..EpochState::zeroed()
            },
            ..Epoch::zeroed()
        };

        let next_epoch = Epoch {
            id: next,
            total_groups: SPOOL_GROUPS,
            state: EpochState::zeroed(), // phase = Unknown
            ..Epoch::zeroed()
        };

        let snapshot_tape = Tape {
            id: TapeNumber(0),
            authority: SYSTEM_ADDRESS,
            capacity: StorageUnits(u64::MAX),
            used: StorageUnits::zero(),
            active_epoch: prev,
            expiry_epoch: EpochNumber(u64::MAX),
            ..Tape::zeroed()
        };

        let archive_data = Archive::zeroed().pack();

        let instruction = build_commit_epoch_ix(fee_payer.into(), curr);

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            pda(system_address, system.pack(), tapedrive::ID),
            pda(archive_address, archive_data, tapedrive::ID),
            pda(curr_epoch_address, curr_epoch.pack(), tapedrive::ID),
            pda(next_epoch_address, next_epoch.pack(), tapedrive::ID),
            pda(next_committee_address, populated_committee(next), tapedrive::ID),
            pda(snapshot_tape_address, snapshot_tape.pack(), tapedrive::ID),
            slot_hashes_account(),
        ];

        let expected_nonce = Hash([0x42u8; 32]);

        let mut expected_curr = curr_epoch;
        expected_curr.state.phase = EpochPhase::Closing as u64;

        let mut expected_next = next_epoch;
        expected_next.nonce = expected_nonce;

        env.process_instruction(
            &instruction,
            &accounts,
            &[
                Check::success(),
                Check::account(&Pubkey::from(curr_epoch_address))
                    .data(expected_curr.pack().as_ref())
                    .build(),
                Check::account(&Pubkey::from(next_epoch_address))
                    .data(expected_next.pack().as_ref())
                    .build(),
            ],
        );
    }

    #[test]
    fn rejects_malformed_previous_snapshot_tape() {
        let fee_payer = Pubkey::new_unique();

        let curr = EpochNumber(10);
        let next = EpochNumber(11);
        let prev = curr.saturating_sub(EpochNumber(1));

        let (system_address, _) = system_pda();
        let (archive_address, _) = archive_pda();
        let (curr_epoch_address, _) = epoch_pda(curr);
        let (next_epoch_address, _) = epoch_pda(next);
        let (next_committee_address, _) = committee_pda(next);
        let (snapshot_tape_address, _) = snapshot_tape_pda(prev);

        let env = test_env();
        let now = env.now();

        let system = System {
            current_epoch: curr,
            target_group_count: SPOOL_GROUPS,
            live_group_count: SPOOL_GROUPS,
            committee_size: COMMITTEE_SIZE,
            ..System::zeroed()
        };

        let curr_epoch = Epoch {
            id: curr,
            start_time: now - EPOCH_DURATION,
            state: EpochState {
                phase: EpochPhase::Active as u64,
                ..EpochState::zeroed()
            },
            ..Epoch::zeroed()
        };

        let next_epoch = Epoch {
            id: next,
            total_groups: SPOOL_GROUPS,
            state: EpochState::zeroed(),
            ..Epoch::zeroed()
        };

        let snapshot_tape = Tape {
            id: TapeNumber(1),
            authority: SYSTEM_ADDRESS,
            capacity: StorageUnits(u64::MAX),
            active_epoch: prev,
            expiry_epoch: EpochNumber(u64::MAX),
            ..Tape::zeroed()
        };

        let archive_data = Archive::zeroed().pack();
        let instruction = build_commit_epoch_ix(fee_payer.into(), curr);

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            pda(system_address, system.pack(), tapedrive::ID),
            pda(archive_address, archive_data, tapedrive::ID),
            pda(curr_epoch_address, curr_epoch.pack(), tapedrive::ID),
            pda(next_epoch_address, next_epoch.pack(), tapedrive::ID),
            pda(next_committee_address, populated_committee(next), tapedrive::ID),
            pda(snapshot_tape_address, snapshot_tape.pack(), tapedrive::ID),
            slot_hashes_account(),
        ];

        env.process_instruction(
            &instruction,
            &accounts,
            &[Check::err(TapeError::UnexpectedState.into())],
        );
    }
}
