use tape_solana::*;
use tape_api::program::prelude::*;
use tape_api::event::EpochAdvanced;

pub fn process_advance_epoch(accounts: &[AccountInfo<'_>], data: &[u8]) -> ProgramResult {
    let _args = AdvanceEpoch::try_from_bytes(data)?;
    let [
        fee_payer_info,
        system_info,
        archive_info,
        archive_ata_info,

        curr_epoch_info,

        next_epoch_info,
        next_committee_info,

        candidate_epoch_info,
        candidate_committee_info,

        peer_set_info,
        subsidy_info,
        subsidy_ata_info,
        mint_info,
        token_program_info,
    ] = accounts else {
        return Err(ProgramError::NotEnoughAccountKeys);
    };

    fee_payer_info
        .is_signer()?
        .is_writable()?;

    let system = system_info
        .is_writable()?
        .is_system()?
        .as_account_mut::<System>(&tapedrive::ID)?;

    let curr = system.current_epoch;
    let next = curr.next();
    let candidate = curr.saturating_add(EpochNumber(2));

    archive_info
        .is_writable()?
        .is_archive()?;

    let archive = archive_info.as_account_mut::<Archive>(&tapedrive::ID)?;

    archive_ata_info
        .is_writable()?
        .is_archive_ata()?
        .as_token_account()?
        .assert(|t| t.owner() == *archive_info.key)?
        .assert(|t| t.mint() == MINT_ADDRESS.into())?;

    subsidy_info
        .is_subsidy()?;

    let subsidy_account = subsidy_ata_info
        .is_writable()?
        .is_subsidy_ata()?
        .as_token_account()?;
    subsidy_account
        .assert(|t| t.owner() == *subsidy_info.key)?
        .assert(|t| t.mint() == MINT_ADDRESS.into())?;

    mint_info
        .is_mint()?;
    token_program_info
        .is_program(&spl_token::ID)?;

    let curr_epoch = curr_epoch_info
        .is_writable()?
        .is_epoch(curr)?
        .as_account_mut::<Epoch>(&tapedrive::ID)?;

    let next_epoch = next_epoch_info
        .is_writable()?
        .is_epoch(next)?
        .as_account_mut::<Epoch>(&tapedrive::ID)?;

    if curr_epoch.id != curr {
        return Err(TapeError::BadEpochId.into());
    }
    if curr_epoch.state.phase != EpochPhase::Closing as u64 {
        return Err(TapeError::BadEpochState.into());
    }

    if next_epoch.id != next {
        return Err(TapeError::BadEpochId.into());
    }
    if !next_epoch.has_assignment_hash() {
        return Err(TapeError::AssignmentIncomplete.into());
    }
    if next_epoch.total_groups != system.target_group_count {
        return Err(TapeError::AssignmentIncomplete.into());
    }

    let preferences = next_epoch.preferences;
    validate_preferences(system, preferences)?;

    if archive.schedule.current_epoch() != curr {
        return Err(TapeError::BadSchedule.into());
    }

    let target_epoch = candidate_epoch_info
        .is_epoch(candidate)?
        .as_account::<Epoch>(&tapedrive::ID)?;

    if target_epoch.id != candidate {
        return Err(TapeError::BadEpochId.into());
    }
    if target_epoch.state.phase != EpochPhase::Unknown as u64 {
        return Err(TapeError::BadEpochState.into());
    }

    next_committee_info
        .is_committee(next)?;

    candidate_committee_info
        .is_committee(candidate)?;

    let (next_committee, next_members) =
        Committee::read(next_committee_info, &tapedrive::ID)?;

    if next_committee.epoch != next {
        return Err(TapeError::BadEpochId.into());
    }
    if next_committee.members.capacity != system.committee_size {
        return Err(TapeError::InsufficientCommittee.into());
    }
    if (next_committee.members.count as usize) < GROUP_SIZE {
        return Err(TapeError::InsufficientCommittee.into());
    }

    let (candidate_committee, _) =
        Committee::read(candidate_committee_info, &tapedrive::ID)?;

    if candidate_committee.epoch != candidate {
        return Err(TapeError::BadEpochId.into());
    }
    if candidate_committee.members.capacity != preferences.committee_size {
        return Err(TapeError::InsufficientCommittee.into());
    }
    if candidate_committee.members.count != 0 {
        return Err(TapeError::UnexpectedState.into());
    }

    let committee_count = next_committee.members.count;
    let total_stake = next_members
        .iter()
        .fold(0u64, |total, member| total.saturating_add(member.stake.0));

    peer_set_info.is_peer_set()?;
    let peer_set = PeerSet::header(peer_set_info, &tapedrive::ID)?;
    let required_peer_capacity = system
        .committee_size
        .max(preferences.committee_size)
        .saturating_mul(3);

    if peer_set.peers.capacity < required_peer_capacity {
        return Err(TapeError::ListFull.into());
    }

    // Archive schedule and rewards rollover. Unspent reward dust from the
    // closing epoch carries into next epoch's pool.
    let epoch_usage = archive.schedule.advance_epoch();
    let leftover = archive.rewards_pool.saturating_sub(archive.rewards_paid);
    let subsidy_release = subsidy_release(subsidy_account.amount(), archive.subsidy_decay_bps)?;

    if !subsidy_release.is_zero() {
        transfer_signed_with_bump(
            subsidy_info,
            subsidy_ata_info,
            archive_ata_info,
            token_program_info,
            subsidy_release.as_u64(),
            &[SUBSIDY],
            SUBSIDY_BUMP,
        )?;
    }

    archive.rewards_pool = epoch_usage
        .paid()
        .saturating_add(subsidy_release)
        .saturating_add(leftover);
    archive.rewards_paid = TAPE::zero();
    archive.recent_usage = epoch_usage.reserved();
    archive.storage_capacity = preferences.storage_capacity;
    archive.storage_price = preferences.storage_price;
    archive.burn_fee_bps = preferences.burn_fee_bps;
    archive.subsidy_decay_bps = preferences.subsidy_decay_bps;

    // Light up the next epoch.
    let clock = Clock::get()?;
    next_epoch.start_slot = SlotNumber(clock.slot);
    next_epoch.start_time = clock.unix_timestamp;
    next_epoch.state.phase = EpochPhase::Sync as u64;

    // Mark the closing epoch completed.
    curr_epoch.state.phase = EpochPhase::Completed as u64;

    // Update system with new epoch and preferences.
    system.current_epoch = next;
    system.committee_size = preferences.committee_size;
    system.target_group_count = preferences.spool_groups;
    system.live_group_count = next_epoch.total_groups;
    system.min_version = preferences.min_version;

    EpochAdvanced {
        old_epoch: curr,
        new_epoch: next,
        timestamp: (clock.unix_timestamp as u64).to_le_bytes(),
        total_stake: total_stake.to_le_bytes(),
        committee_count: committee_count.to_le_bytes(),
        preferences,
        nonce: next_epoch.nonce,
        subsidy: subsidy_release.as_u64().to_le_bytes(),
    }.log();

    Ok(())
}

fn validate_preferences(system: &System, preferences: NodePreferences) -> ProgramResult {
    if preferences.storage_capacity.0 < MIN_STORAGE_CAPACITY as u64 {
        return Err(TapeError::UnexpectedState.into());
    }
    if preferences.storage_price.0 < MIN_STORAGE_PRICE as u64 {
        return Err(TapeError::UnexpectedState.into());
    }
    if preferences.committee_size < MIN_COMMITTEE_SIZE as u64 {
        return Err(TapeError::InsufficientCommittee.into());
    }
    if preferences.spool_groups < system.target_group_count {
        return Err(TapeError::UnexpectedState.into());
    }
    if preferences.min_version.0 < system.min_version.0 {
        return Err(TapeError::UnexpectedState.into());
    }
    if !preferences.burn_fee_bps.is_valid() || !preferences.subsidy_decay_bps.is_valid() {
        return Err(TapeError::UnexpectedState.into());
    }

    Ok(())
}

fn subsidy_release(balance: u64, decay: BasisPoints) -> Result<Coin<TAPE>, ProgramError> {
    let raw = (balance as u128)
        .checked_mul(decay.as_u128())
        .ok_or(ProgramError::ArithmeticOverflow)?
        / BasisPoints::MAX as u128;

    if raw > u64::MAX as u128 {
        return Err(ProgramError::ArithmeticOverflow);
    }

    Ok(TAPE(raw as u64))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tape_api::state::{Committee, PeerSet};
    use tape_core::system::{NodePreferences, Peer};
    use tape_test::*;

    const COMMITTEE_SIZE: u64 = 128;

    fn pref(capacity_mb: u64, price: u64, committee: u64, spool_groups: u64, version: u64) -> NodePreferences {
        NodePreferences {
            storage_capacity: StorageUnits::mb(capacity_mb),
            storage_price: TAPE(price),
            committee_size: committee,
            spool_groups,
            min_version: VersionId(version),
            burn_fee_bps: BasisPoints(1_000),
            subsidy_decay_bps: DEFAULT_SUBSIDY_DECAY_BPS,
        }
    }

    #[test]
    fn advance() {
        let fee_payer = Pubkey::new_unique();

        let curr = EpochNumber(10);
        let next = EpochNumber(11);
        let target = EpochNumber(12);

        let (system_address, _) = system_pda();
        let (archive_address, _) = archive_pda();
        let (archive_ata_address, _) = archive_ata();
        let (curr_epoch_address, _) = epoch_pda(curr);
        let (next_epoch_address, _) = epoch_pda(next);
        let (next_committee_address, _) = committee_pda(next);
        let (target_epoch_address, _) = epoch_pda(target);
        let (target_committee_address, _) = committee_pda(target);
        let (peer_set_address, _) = peer_set_pda();
        let (subsidy_address, _) = subsidy_pda();
        let (subsidy_ata_address, _) = subsidy_ata();

        let env = test_env();
        let now = env.now();
        let slot = env.slot();

        let prefs = pref(2_048, 950, 256, 75, 3);
        let next_members: Vec<Member> = (0..20)
            .map(|i| {
                let mut bytes = [0u8; 32];
                bytes[0] = (i as u8) + 1;
                Member {
                    node: Address::new(bytes),
                    stake: TAPE(1_000),
                    assigned: StorageUnits::zero(),
                    blacklisted: StorageUnits::zero(),
                    spools: 50,
                }
            })
            .collect();
        let next_peers: Vec<Peer> = next_members
            .iter()
            .map(|m| Peer { node: m.node, preferences: prefs, ..Peer::zeroed() })
            .collect();

        let nonce_value = Hash([0x77u8; 32]);

        let system = System {
            current_epoch: curr,
            committee_size: COMMITTEE_SIZE,
            target_group_count: 50,
            live_group_count: 50,
            ..System::zeroed()
        };

        let mut archive = Archive::zeroed();
        archive.schedule = EpochSchedule::new_at(curr);
        archive.rewards_pool = TAPE(500);
        archive.rewards_paid = TAPE(300);

        let curr_epoch = Epoch {
            id: curr,
            state: EpochState {
                phase: EpochPhase::Closing as u64,
                ..EpochState::zeroed()
            },
            ..Epoch::zeroed()
        };

        let next_epoch_data = Epoch {
            id: next,
            total_groups: 50,
            nonce: nonce_value,
            assignment_hash: Hash::from([0x88; 32]),
            preferences: prefs,
            state: EpochState::zeroed(),
            ..Epoch::zeroed()
        };
        let target_epoch_data = Epoch {
            id: target,
            state: EpochState::zeroed(),
            ..Epoch::zeroed()
        };

        let next_committee_data =
            Committee { epoch: next, members: Tail::new(COMMITTEE_SIZE, next_members.len() as u64) }
                .pack_with(&next_members);
        let target_committee_data =
            Committee { epoch: target, members: Tail::empty(prefs.committee_size) }
                .pack_with(&[]);
        let peer_set_data = PeerSet {
            peers: Tail::new(prefs.committee_size.saturating_mul(3), next_peers.len() as u64),
        }
            .pack_with(&next_peers);

        let instruction = build_advance_epoch_ix(fee_payer.into(), curr);

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            pda(system_address, system.pack(), tapedrive::ID),
            pda(archive_address, archive.pack(), tapedrive::ID),
            token(archive_ata_address, Pubkey::from(archive_address), 0),
            pda(curr_epoch_address, curr_epoch.pack(), tapedrive::ID),
            pda(next_epoch_address, next_epoch_data.pack(), tapedrive::ID),
            pda(next_committee_address, next_committee_data, tapedrive::ID),
            pda(target_epoch_address, target_epoch_data.pack(), tapedrive::ID),
            pda(target_committee_address, target_committee_data, tapedrive::ID),
            pda(peer_set_address, peer_set_data, tapedrive::ID),
            empty(subsidy_address),
            token(subsidy_ata_address, Pubkey::from(subsidy_address), 0),
            mint(0),
            token_program(),
        ];

        let expected_system = System {
            current_epoch: next,
            committee_size: 256,
            target_group_count: 75,
            live_group_count: 50,
            min_version: VersionId(3),
            ..system
        };

        let expected_curr_epoch = Epoch {
            state: EpochState {
                phase: EpochPhase::Completed as u64,
                ..curr_epoch.state
            },
            ..curr_epoch
        };

        let expected_next_epoch = Epoch {
            start_slot: SlotNumber(slot),
            start_time: now,
            state: EpochState {
                phase: EpochPhase::Sync as u64,
                ..EpochState::zeroed()
            },
            ..next_epoch_data
        };

        // Rewards rollover: (500 - 300) leftover + 0 from a fresh schedule = 200.
        let expected_archive = Archive {
            rewards_pool: TAPE(200),
            rewards_paid: TAPE::zero(),
            recent_usage: StorageUnits::zero(),
            storage_capacity: StorageUnits::mb(2_048),
            storage_price: TAPE(950),
            burn_fee_bps: prefs.burn_fee_bps,
            subsidy_decay_bps: prefs.subsidy_decay_bps,
            schedule: {
                let mut s = archive.schedule;
                let _ = s.advance_epoch();
                s
            },
            ..archive
        };

        env.process_instruction(
            &instruction,
            &accounts,
            &[
                Check::success(),
                Check::account(&Pubkey::from(system_address))
                    .data(expected_system.pack().as_ref())
                    .build(),
                Check::account(&Pubkey::from(curr_epoch_address))
                    .data(expected_curr_epoch.pack().as_ref())
                    .build(),
                Check::account(&Pubkey::from(next_epoch_address))
                    .data(expected_next_epoch.pack().as_ref())
                    .build(),
                Check::account(&Pubkey::from(archive_address))
                    .data(expected_archive.pack().as_ref())
                    .build(),
            ],
        );
    }

    #[test]
    fn holds_against_shrinking_votes() {
        let fee_payer = Pubkey::new_unique();

        let curr = EpochNumber(10);
        let next = EpochNumber(11);
        let target = EpochNumber(12);

        let (system_address, _) = system_pda();
        let (archive_address, _) = archive_pda();
        let (archive_ata_address, _) = archive_ata();
        let (curr_epoch_address, _) = epoch_pda(curr);
        let (next_epoch_address, _) = epoch_pda(next);
        let (next_committee_address, _) = committee_pda(next);
        let (target_epoch_address, _) = epoch_pda(target);
        let (target_committee_address, _) = committee_pda(target);
        let (peer_set_address, _) = peer_set_pda();
        let (subsidy_address, _) = subsidy_pda();
        let (subsidy_ata_address, _) = subsidy_ata();

        let prefs = pref(2_048, 950, /*committee*/ 64, /*spool_groups*/ 75, /*min_version*/ 5);
        let next_members: Vec<Member> = (0..20)
            .map(|i| {
                let mut bytes = [0u8; 32];
                bytes[0] = (i as u8) + 1;
                Member {
                    node: Address::new(bytes),
                    stake: TAPE(1_000),
                    assigned: StorageUnits::zero(),
                    blacklisted: StorageUnits::zero(),
                    spools: 50,
                }
            })
            .collect();
        let next_peers: Vec<Peer> = next_members
            .iter()
            .map(|m| Peer { node: m.node, preferences: prefs, ..Peer::zeroed() })
            .collect();

        // System sits at values HIGHER than the votes.
        let system = System {
            current_epoch: curr,
            committee_size: 256,
            target_group_count: 75,
            live_group_count: 75,
            min_version: VersionId(5),
            ..System::zeroed()
        };

        let archive = Archive {
            schedule: EpochSchedule::new_at(curr),
            ..Archive::zeroed()
        };

        let curr_epoch = Epoch {
            id: curr,
            state: EpochState {
                phase: EpochPhase::Closing as u64,
                ..EpochState::zeroed()
            },
            ..Epoch::zeroed()
        };

        let next_epoch_data = Epoch {
            id: next,
            total_groups: 75,
            assignment_hash: Hash::from([0x88; 32]),
            preferences: prefs,
            state: EpochState::zeroed(),
            ..Epoch::zeroed()
        };
        let target_epoch_data = Epoch {
            id: target,
            state: EpochState::zeroed(),
            ..Epoch::zeroed()
        };

        let next_committee_data = Committee {
            epoch: next,
            members: Tail::new(system.committee_size, next_members.len() as u64),
        }
        .pack_with(&next_members);
        let target_committee_data = Committee {
            epoch: target,
            members: Tail::empty(prefs.committee_size),
        }
        .pack_with(&[]);
        let peer_set_data = PeerSet {
            peers: Tail::new(system.committee_size.saturating_mul(3), next_peers.len() as u64),
        }
            .pack_with(&next_peers);

        let instruction = build_advance_epoch_ix(fee_payer.into(), curr);

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            pda(system_address, system.pack(), tapedrive::ID),
            pda(archive_address, archive.pack(), tapedrive::ID),
            token(archive_ata_address, Pubkey::from(archive_address), 0),
            pda(curr_epoch_address, curr_epoch.pack(), tapedrive::ID),
            pda(next_epoch_address, next_epoch_data.pack(), tapedrive::ID),
            pda(next_committee_address, next_committee_data, tapedrive::ID),
            pda(target_epoch_address, target_epoch_data.pack(), tapedrive::ID),
            pda(target_committee_address, target_committee_data, tapedrive::ID),
            pda(peer_set_address, peer_set_data, tapedrive::ID),
            empty(subsidy_address),
            token(subsidy_ata_address, Pubkey::from(subsidy_address), 0),
            mint(0),
            token_program(),
        ];

        let expected_system = System {
            current_epoch: next,
            committee_size: 64,
            target_group_count: 75,
            live_group_count: next_epoch_data.total_groups,
            min_version: VersionId(5),
            ..system
        };

        let env = test_env();
        env.process_instruction(
            &instruction,
            &accounts,
            &[
                Check::success(),
                Check::account(&Pubkey::from(system_address))
                    .data(expected_system.pack().as_ref())
                    .build(),
            ],
        );
    }
}
