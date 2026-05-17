use tape_solana::*;
use tape_api::program::prelude::*;
use tape_api::event::EpochAdvanced;

use tape_core::bft::{quorum_above, quorum_below};

pub fn process_advance_epoch(accounts: &[AccountInfo<'_>], data: &[u8]) -> ProgramResult {
    let _args = AdvanceEpoch::try_from_bytes(data)?;
    let [
        fee_payer_info,
        system_info,
        archive_info,
        curr_epoch_info,
        next_epoch_info,
        next_committee_info,
        peer_set_info,
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
    let next = curr.saturating_add(EpochNumber(1));

    archive_info
        .is_writable()?
        .is_archive()?;
    let archive = archive_info.as_account_mut::<Archive>(&tapedrive::ID)?;

    let curr_epoch = curr_epoch_info
        .is_writable()?
        .is_epoch(curr)?
        .as_account_mut::<Epoch>(&tapedrive::ID)?;

    if curr_epoch.state.phase != EpochPhase::Closing as u64 {
        return Err(TapeError::BadEpochState.into());
    }

    let next_epoch = next_epoch_info
        .is_writable()?
        .is_epoch(next)?
        .as_account_mut::<Epoch>(&tapedrive::ID)?;

    if !next_epoch.has_assignment_hash() {
        return Err(TapeError::SpoolsNotSettled.into());
    }

    if next_epoch.total_groups != system.target_group_count {
        return Err(TapeError::SpoolsNotSettled.into());
    }

    if archive.schedule.current_epoch() != curr {
        return Err(TapeError::BadSchedule.into());
    }

    // Aggregate preferences across the next-epoch committee, weighted by each
    // member's spool count.
    next_committee_info.is_committee(next)?;
    let (next_committee, next_members) = Committee::read(next_committee_info, &tapedrive::ID)?;
    let committee_count = next_committee.members.count;

    peer_set_info.is_peer_set()?;
    let (_, peers) = PeerSet::read(peer_set_info, &tapedrive::ID)?;

    let mut total_weight: u64 = 0;
    let mut total_stake: u64 = 0;
    let mut storage_capacities: Vec<(u64, u64)> = Vec::new();
    let mut storage_prices: Vec<(u64, u64)> = Vec::new();
    let mut committee_sizes: Vec<(u64, u64)> = Vec::new();
    let mut spool_group_counts: Vec<(u64, u64)> = Vec::new();
    let mut min_versions: Vec<(u64, u64)> = Vec::new();

    for member in next_members.iter() {
        if member.node == Address::default() {
            continue;
        }
        let peer = peers
            .iter()
            .find(|p| p.node == member.node)
            .ok_or(TapeError::BadMember)?;
        let weight = member.spools;

        storage_capacities.push((peer.preferences.storage_capacity.0, weight));
        storage_prices.push((peer.preferences.storage_price.0, weight));
        committee_sizes.push((peer.preferences.committee_size, weight));
        spool_group_counts.push((peer.preferences.spool_groups, weight));
        min_versions.push((peer.preferences.min_version.0, weight));

        total_weight = total_weight.saturating_add(weight);
        total_stake = total_stake.saturating_add(member.stake.0);
    }

    let new_min_version : VersionId = VersionId(
        quorum_above(&min_versions, total_weight)
        .max(system.min_version.0)
    );
    let new_storage_capacity = StorageUnits(
        quorum_above(&storage_capacities, total_weight)
        .max(MIN_STORAGE_CAPACITY as u64)
    );
    let new_storage_price = TAPE(
        quorum_below(&storage_prices, total_weight)
        .max(MIN_STORAGE_PRICE as u64)
    );

    let new_committee_size = quorum_above(&committee_sizes, total_weight)
        .max(MIN_COMMITTEE_SIZE as u64);
    let new_spool_groups = quorum_above(&spool_group_counts, total_weight) 
        .max(system.target_group_count);

    // Archive schedule and rewards rollover. Unspent reward dust from the
    // closing epoch carries into next epoch's pool.
    let epoch_usage = archive.schedule.advance_epoch();
    let leftover = archive.rewards_pool.saturating_sub(archive.rewards_paid);

    archive.rewards_pool = epoch_usage.paid().saturating_add(leftover);
    archive.rewards_paid = TAPE::zero();
    archive.recent_usage = epoch_usage.reserved();
    archive.storage_capacity = new_storage_capacity;
    archive.storage_price = new_storage_price;

    // Light up the next epoch.
    let clock = Clock::get()?;
    next_epoch.start_slot = SlotNumber(clock.slot);
    next_epoch.start_time = clock.unix_timestamp;
    next_epoch.state.phase = EpochPhase::Sync as u64;

    // Mark the closing epoch completed.
    curr_epoch.state.phase = EpochPhase::Completed as u64;

    // Update system with new epoch and preferences.
    system.current_epoch = next;
    system.committee_size = new_committee_size;
    system.target_group_count = new_spool_groups;
    system.live_group_count = next_epoch.total_groups;
    system.min_version = new_min_version;

    EpochAdvanced {
        old_epoch: curr,
        new_epoch: next,
        timestamp: (clock.unix_timestamp as u64).to_le_bytes(),
        total_stake: total_stake.to_le_bytes(),
        committee_count: committee_count.to_le_bytes(),
        preferences: NodePreferences {
            storage_capacity: new_storage_capacity,
            storage_price: new_storage_price,
            committee_size: new_committee_size,
            spool_groups: new_spool_groups,
            min_version: new_min_version,
        },
        nonce: next_epoch.nonce,
    }.log();

    Ok(())
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
        }
    }

    #[test]
    fn advance() {
        let fee_payer = Pubkey::new_unique();

        let curr = EpochNumber(10);
        let next = EpochNumber(11);

        let (system_address, _) = system_pda();
        let (archive_address, _) = archive_pda();
        let (curr_epoch_address, _) = epoch_pda(curr);
        let (next_epoch_address, _) = epoch_pda(next);
        let (next_committee_address, _) = committee_pda(next);
        let (peer_set_address, _) = peer_set_pda();

        let env = test_env();
        let now = env.now();
        let slot = env.slot();

        // 20 next-committee members, each owning 50 spools (full 1000 spools
        // distributed evenly). All agree on the same preferences so the
        // quorum aggregation returns the unanimous value. Storage capacity
        // is set above MIN_STORAGE_CAPACITY (1 GiB) so the floor doesn't
        // override the aggregation.
        let prefs = pref(2_048, 950, 256, 75, 3);
        let next_members: Vec<Member> = (0..20)
            .map(|i| {
                let mut bytes = [0u8; 32];
                bytes[0] = (i as u8) + 1;
                Member {
                    node: Address::new(bytes),
                    stake: TAPE(1_000),
                    blacklist: StorageUnits::zero(),
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
            state: EpochState::zeroed(),
            ..Epoch::zeroed()
        };

        let next_committee_data =
            Committee { epoch: next, members: Tail::new(COMMITTEE_SIZE, next_members.len() as u64) }
                .pack_with(&next_members);
        let peer_set_data = PeerSet { peers: Tail::new(20, next_peers.len() as u64) }
            .pack_with(&next_peers);

        let instruction = build_advance_epoch_ix(fee_payer.into(), curr);

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            pda(system_address, system.pack(), tapedrive::ID),
            pda(archive_address, archive.pack(), tapedrive::ID),
            pda(curr_epoch_address, curr_epoch.pack(), tapedrive::ID),
            pda(next_epoch_address, next_epoch_data.pack(), tapedrive::ID),
            pda(next_committee_address, next_committee_data, tapedrive::ID),
            pda(peer_set_address, peer_set_data, tapedrive::ID),
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

        let (system_address, _) = system_pda();
        let (archive_address, _) = archive_pda();
        let (curr_epoch_address, _) = epoch_pda(curr);
        let (next_epoch_address, _) = epoch_pda(next);
        let (next_committee_address, _) = committee_pda(next);
        let (peer_set_address, _) = peer_set_pda();

        // Members vote for values BELOW the current system parameters.
        let prefs = pref(2_048, 950, /*committee*/ 64, /*spool_groups*/ 30, /*min_version*/ 1);
        let next_members: Vec<Member> = (0..20)
            .map(|i| {
                let mut bytes = [0u8; 32];
                bytes[0] = (i as u8) + 1;
                Member {
                    node: Address::new(bytes),
                    stake: TAPE(1_000),
                    blacklist: StorageUnits::zero(),
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
            state: EpochState::zeroed(),
            ..Epoch::zeroed()
        };

        let next_committee_data =
            Committee { epoch: next, members: Tail::new(COMMITTEE_SIZE, next_members.len() as u64) }
                .pack_with(&next_members);
        let peer_set_data = PeerSet { peers: Tail::new(20, next_peers.len() as u64) }
            .pack_with(&next_peers);

        let instruction = build_advance_epoch_ix(fee_payer.into(), curr);

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            pda(system_address, system.pack(), tapedrive::ID),
            pda(archive_address, archive.pack(), tapedrive::ID),
            pda(curr_epoch_address, curr_epoch.pack(), tapedrive::ID),
            pda(next_epoch_address, next_epoch_data.pack(), tapedrive::ID),
            pda(next_committee_address, next_committee_data, tapedrive::ID),
            pda(peer_set_address, peer_set_data, tapedrive::ID),
        ];

        // spool_groups: ratchet holds at 75 (vote of 30 ignored).
        // min_version: ratchet holds at 5 (vote of 1 ignored).
        // committee_size: shrinks to 64 (no ratchet; only floored at 20).
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
