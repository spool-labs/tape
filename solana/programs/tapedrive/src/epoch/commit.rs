use tape_solana::*;
use tape_api::program::prelude::*;
use tape_api::event::EpochCommitted;
use tape_core::system::{
    aggregate_node_preferences, NodePreferenceAggregationError,
};

pub fn process_commit_epoch(accounts: &[AccountInfo<'_>], data: &[u8]) -> ProgramResult {
    let _args = CommitEpoch::try_from_bytes(data)?;
    let [
        fee_payer_info,
        system_info,
        curr_epoch_info,
        next_epoch_info,
        curr_committee_info,
        next_committee_info,
        peer_set_info,
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
    let next = curr.next();
    let prev = curr.prev();

    let curr_epoch = curr_epoch_info
        .is_writable()?
        .is_epoch(curr)?
        .as_account_mut::<Epoch>(&tapedrive::ID)?;

    if curr_epoch.id != curr {
        return Err(TapeError::BadEpochId.into());
    }

    if curr_epoch.state.phase != EpochPhase::Active as u64 {
        return Err(TapeError::BadEpochState.into());
    }

    let now = Clock::get()?.unix_timestamp;
    let elapsed = now.saturating_sub(curr_epoch.start_time);
    if elapsed < curr_epoch.preferences.epoch_duration.0 as i64 {
        return Err(TapeError::TooSoon.into());
    }

    let next_epoch = next_epoch_info
        .is_writable()?
        .is_epoch(next)?
        .as_account_mut::<Epoch>(&tapedrive::ID)?;

    if next_epoch.id != next {
        return Err(TapeError::BadEpochId.into());
    }
    if next_epoch.state.phase != EpochPhase::Unknown as u64 {
        return Err(TapeError::BadEpochState.into());
    }

    curr_committee_info
        .is_committee(curr)?;

    next_committee_info
        .is_committee(next)?;


    let (curr_committee, curr_members) =
        Committee::read(curr_committee_info, &tapedrive::ID)?;

    if curr_committee.epoch != curr {
        return Err(TapeError::BadEpochId.into());
    }

    let (next_committee, _) = 
        Committee::read(next_committee_info, &tapedrive::ID)?;

    if next_committee.epoch != next {
        return Err(TapeError::BadEpochId.into());
    }
    if next_committee.members.count < next_committee.members.capacity {
        return Err(TapeError::InsufficientCommittee.into());
    }

    peer_set_info
        .is_peer_set()?;

    let (_, peers) = 
        PeerSet::read(peer_set_info, &tapedrive::ID)?;

    let preferences = 
        aggregate_preferences(system, curr_members, peers)?;

    snapshot_tape_info
        .is_snapshot_tape(prev)?;

    let next_nonce = slot_hash_seed(slot_hashes_info)?;

    next_epoch.nonce = next_nonce;
    next_epoch.preferences = preferences;

    curr_epoch.state.phase = EpochPhase::Closing as u64;

    EpochCommitted {
        epoch: curr,
        next_nonce,
        preferences,
    }.log();

    Ok(())
}

fn aggregate_preferences(
    system: &System,
    members: &[Member],
    peers: &[Peer],
) -> Result<NodePreferences, ProgramError> {

    let bounds = NodePreferences {
        storage_capacity: StorageUnits(MIN_STORAGE_CAPACITY as u64),
        storage_price: TAPE(MIN_STORAGE_PRICE as u64),
        committee_size: MIN_COMMITTEE_SIZE as u64,
        spool_groups: system.target_group_count,
        burn_fee_bps: BasisPoints(0),
        subsidy_decay_bps: BasisPoints(0),
        access_threshold: TAPE(0),
        epoch_duration: system.min_epoch_duration,
    };

    aggregate_node_preferences(members, peers, bounds).map_err(|error| match error {
        NodePreferenceAggregationError::MissingPeer { .. } => TapeError::BadMember.into(),
        NodePreferenceAggregationError::ZeroWeight => TapeError::UnexpectedState.into(),
    })
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
    use tape_api::state::{Committee, PeerSet};
    use tape_test::*;

    const COMMITTEE_SIZE: u64 = GROUP_SIZE as u64;
    const SPOOL_GROUPS: u64 = 50;

    fn slot_hashes_account() -> (Pubkey, solana_account::Account) {
        // 8-byte count = 1, then one entry: 8-byte slot + 32-byte hash.
        let mut data = vec![0u8; 48];
        data[0] = 1;
        data[16..48].copy_from_slice(&[0x42u8; 32]); // deterministic test hash
        (
            sysvar::slot_hashes::ID,
            solana_account::Account {
                lamports: 1,
                data,
                owner: sysvar::ID,
                executable: false,
                rent_epoch: 0,
            },
        )
    }

    fn test_preferences() -> NodePreferences {
        NodePreferences {
            storage_capacity: StorageUnits::mb(2_048),
            storage_price: TAPE(950),
            committee_size: 256,
            spool_groups: 75,
            burn_fee_bps: BasisPoints(1_000),
            subsidy_decay_bps: DEFAULT_SUBSIDY_DECAY_BPS,
            access_threshold: TAPE(0),
            epoch_duration: TEST_EPOCH_DURATION,
        }
    }

    fn members() -> Vec<Member> {
        (0..GROUP_SIZE)
            .map(|i| {
                let mut bytes = [0u8; 32];
                bytes[0] = (i as u8) + 1;
                Member {
                    node: Address::new(bytes),
                    stake: TAPE(1_000),
                    assigned: StorageUnits::zero(),
                    blacklisted: StorageUnits::zero(),
                    spools: 1,
                }
            })
            .collect()
    }

    fn populated_committee(epoch: EpochNumber) -> Vec<u8> {
        let members = members();
        Committee { epoch, members: Tail::new(COMMITTEE_SIZE, members.len() as u64) }
            .pack_with(&members)
    }

    fn peer_set() -> Vec<u8> {
        let prefs = test_preferences();
        let peers: Vec<Peer> = members()
            .iter()
            .map(|m| Peer {
                node: m.node,
                preferences: prefs,
                ..Peer::zeroed()
            })
            .collect();

        PeerSet { peers: Tail::new(COMMITTEE_SIZE, peers.len() as u64) }
            .pack_with(&peers)
    }

    #[test]
    fn commit() {
        let fee_payer = Pubkey::new_unique();

        let curr = EpochNumber(10);
        let next = EpochNumber(11);

        let (system_address, _) = system_pda();
        let (curr_epoch_address, _) = epoch_pda(curr);
        let (next_epoch_address, _) = epoch_pda(next);
        let (curr_committee_address, _) = committee_pda(curr);
        let (next_committee_address, _) = committee_pda(next);
        let (peer_set_address, _) = peer_set_pda();
        let prev = curr.prev();
        let (snapshot_tape_address, _) = snapshot_tape_pda(prev);

        let env = test_env();
        let now = env.now();

        let system = System {
            current_epoch: curr,
            target_group_count: SPOOL_GROUPS,
            live_group_count: SPOOL_GROUPS,
            committee_size: COMMITTEE_SIZE,
            min_epoch_duration: TEST_MIN_EPOCH_DURATION,
            max_epoch_duration: TEST_MAX_EPOCH_DURATION,
            ..System::zeroed()
        };

        let curr_epoch = Epoch {
            id: curr,
            start_time: now - TEST_EPOCH_DURATION.0 as i64,
            preferences: test_preferences(),
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

        let snapshot_tape = Tape::snapshot(prev);

        let instruction = build_commit_epoch_ix(fee_payer.into(), curr);

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            pda(system_address, system.pack(), tapedrive::ID),
            pda(curr_epoch_address, curr_epoch.pack(), tapedrive::ID),
            pda(next_epoch_address, next_epoch.pack(), tapedrive::ID),
            pda(curr_committee_address, populated_committee(curr), tapedrive::ID),
            pda(next_committee_address, populated_committee(next), tapedrive::ID),
            pda(peer_set_address, peer_set(), tapedrive::ID),
            pda(snapshot_tape_address, snapshot_tape.pack(), tapedrive::ID),
            slot_hashes_account(),
        ];

        let expected_nonce = Hash([0x42u8; 32]);

        let mut expected_curr = curr_epoch;
        expected_curr.state.phase = EpochPhase::Closing as u64;

        let mut expected_next = next_epoch;
        expected_next.nonce = expected_nonce;
        expected_next.preferences = test_preferences();

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
        let mutators: &[fn(&mut Tape, EpochNumber)] = &[
            |t, prev| t.id = snapshot_tape_number(prev.next()),
            |t, _| t.flags = 0,
            |t, _| t.authority = Address::new([0xAA; 32]),
            |t, _| t.capacity = StorageUnits(1),
            |t, prev| t.active_epoch = prev.next(),
            |t, _| t.expiry_epoch = EpochNumber(0),
        ];

        for mutate in mutators {
            let mut snapshot_tape = Tape::snapshot(EpochNumber(9));
            mutate(&mut snapshot_tape, EpochNumber(9));
            assert_malformed_snapshot_rejected(snapshot_tape);
        }
    }

    fn assert_malformed_snapshot_rejected(snapshot_tape: Tape) {
        let fee_payer = Pubkey::new_unique();

        let curr = EpochNumber(10);
        let next = EpochNumber(11);
        let prev = curr.prev();

        let (system_address, _) = system_pda();
        let (curr_epoch_address, _) = epoch_pda(curr);
        let (next_epoch_address, _) = epoch_pda(next);
        let (curr_committee_address, _) = committee_pda(curr);
        let (next_committee_address, _) = committee_pda(next);
        let (peer_set_address, _) = peer_set_pda();
        let (snapshot_tape_address, _) = snapshot_tape_pda(prev);

        let env = test_env();
        let now = env.now();

        let system = System {
            current_epoch: curr,
            target_group_count: SPOOL_GROUPS,
            live_group_count: SPOOL_GROUPS,
            committee_size: COMMITTEE_SIZE,
            min_epoch_duration: TEST_MIN_EPOCH_DURATION,
            max_epoch_duration: TEST_MAX_EPOCH_DURATION,
            ..System::zeroed()
        };

        let curr_epoch = Epoch {
            id: curr,
            start_time: now - TEST_EPOCH_DURATION.0 as i64,
            preferences: test_preferences(),
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

        let instruction = build_commit_epoch_ix(fee_payer.into(), curr);

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            pda(system_address, system.pack(), tapedrive::ID),
            pda(curr_epoch_address, curr_epoch.pack(), tapedrive::ID),
            pda(next_epoch_address, next_epoch.pack(), tapedrive::ID),
            pda(curr_committee_address, populated_committee(curr), tapedrive::ID),
            pda(next_committee_address, populated_committee(next), tapedrive::ID),
            pda(peer_set_address, peer_set(), tapedrive::ID),
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
