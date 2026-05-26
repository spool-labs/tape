use tape_solana::*;
use tape_api::program::prelude::*;


pub fn process_start_network(accounts: &[AccountInfo<'_>], data: &[u8]) -> ProgramResult {
    let args = StartNetwork::try_from_bytes(data)?;
    let [
        fee_payer_info,
        subsidy_authority_info,
        subsidy_authority_ata_info,
        system_info,
        archive_info,
        epoch_info,
        committee_info,
        candidate_epoch_info,
        candidate_committee_info,
        peer_set_info,
        group_info,
        snapshot_tape_info,
        subsidy_info,
        subsidy_ata_info,
        token_program_info,
        system_program_info,
        rent_sysvar_info,
    ] = accounts else {
        return Err(ProgramError::NotEnoughAccountKeys);
    };

    fee_payer_info
        .is_signer()?
        .is_writable()?;
    subsidy_authority_info
        .is_signer()?;

    let system = system_info
        .is_writable()?
        .is_system()?
        .as_account_mut::<System>(&tapedrive::ID)?;

    if system.current_epoch != EpochNumber(0) {
        return Err(TapeError::BadEpochState.into());
    }

    system_program_info
        .is_program(&system_program::ID)?;
    rent_sysvar_info
        .is_sysvar(&sysvar::rent::ID)?;
    token_program_info
        .is_program(&spl_token::ID)?;

    let committee_size = u64::from_le_bytes(args.committee_size);
    if committee_size != GROUP_SIZE as u64 {
        return Err(TapeError::InsufficientCommittee.into());
    }

    let spool_groups = u64::from_le_bytes(args.spool_groups);
    if spool_groups == 0 {
        return Err(TapeError::UnexpectedState.into());
    }

    let subsidy_amount = TAPE::unpack(args.subsidy_amount);
    let burn_fee_bps = BasisPoints::unpack(args.burn_fee_bps);
    let subsidy_decay_bps = BasisPoints::unpack(args.subsidy_decay_bps);
    if !burn_fee_bps.is_valid() || !subsidy_decay_bps.is_valid() {
        return Err(ProgramError::InvalidArgument);
    }

    archive_info
        .is_writable()?
        .is_archive()?;

    let target = EpochNumber(1);
    let candidate = EpochNumber(2);
    let group_id = GroupIndex(0);
    let archive = archive_info.as_account_mut::<Archive>(&tapedrive::ID)?;
    archive.schedule = EpochSchedule::new_at(target);
    archive.burn_fee_bps = burn_fee_bps;
    archive.subsidy_decay_bps = subsidy_decay_bps;

    subsidy_info
        .is_subsidy()?;

    subsidy_authority_ata_info
        .is_writable()?
        .as_token_account()?
        .assert(|t| t.owner() == *subsidy_authority_info.key)?
        .assert(|t| t.mint() == MINT_ADDRESS.into())?;

    subsidy_ata_info
        .is_writable()?
        .is_subsidy_ata()?
        .as_token_account()?
        .assert(|t| t.owner() == *subsidy_info.key)?
        .assert(|t| t.mint() == MINT_ADDRESS.into())?;

    if !subsidy_amount.is_zero() {
        transfer(
            subsidy_authority_info,
            subsidy_authority_ata_info,
            subsidy_ata_info,
            token_program_info,
            subsidy_amount.as_u64(),
        )?;
    }

    let epoch = epoch_info
        .is_writable()?
        .is_epoch(target)?
        .as_account_mut::<Epoch>(&tapedrive::ID)?;

    if epoch.state.phase != EpochPhase::Unknown as u64 {
        return Err(TapeError::BadEpochState.into());
    }
    if epoch.id != target {
        return Err(TapeError::BadEpochId.into());
    }

    if epoch.total_groups != 0 {
        return Err(TapeError::UnexpectedState.into());
    }

    committee_info
        .is_writable()?
        .is_committee(target)?;

    let (committee_header, members) =
        Committee::read_full_mut(committee_info, &tapedrive::ID)?;

    if committee_header.epoch != target {
        return Err(TapeError::BadEpochId.into());
    }

    if committee_header.members.capacity != committee_size {
        return Err(TapeError::InsufficientCommittee.into());
    }

    if committee_header.members.count != committee_size {
        return Err(TapeError::InsufficientCommittee.into());
    }

    let candidate_epoch = candidate_epoch_info
        .is_writable()?
        .is_epoch(candidate)?
        .as_account::<Epoch>(&tapedrive::ID)?;

    if candidate_epoch.id != candidate {
        return Err(TapeError::BadEpochId.into());
    }
    if candidate_epoch.state.phase != EpochPhase::Unknown as u64 {
        return Err(TapeError::BadEpochState.into());
    }
    if candidate_epoch.total_groups != 0 {
        return Err(TapeError::UnexpectedState.into());
    }

    candidate_committee_info
        .is_writable()?
        .is_committee(candidate)?;

    ensure_committee_capacity(
        candidate_committee_info,
        system_program_info,
        fee_payer_info,
        candidate,
        committee_size,
    )?;

    let candidate_committee =
        Committee::header(candidate_committee_info, &tapedrive::ID)?;
    if candidate_committee.epoch != candidate {
        return Err(TapeError::BadEpochId.into());
    }
    if candidate_committee.members.capacity != committee_size {
        return Err(TapeError::InsufficientCommittee.into());
    }
    if candidate_committee.members.count != 0 {
        return Err(TapeError::UnexpectedState.into());
    }

    peer_set_info
        .is_peer_set()?;

    let (peer_header, peers) = PeerSet::read(peer_set_info, &tapedrive::ID)?;

    if peer_header.peers.capacity < committee_size {
        return Err(TapeError::ListFull.into());
    }
    if peer_header.peers.count as usize > peers.len() {
        return Err(TapeError::UnexpectedState.into());
    }

    let (group_address, bump) = group_pda(target, group_id);
    group_info
        .is_empty()?
        .is_writable()?
        .has_address(&group_address.into())?;

    create_program_account_with_bump::<Group>(
        group_info,
        system_program_info,
        fee_payer_info,
        &tapedrive::ID,
        &[GROUP, &target.pack(), &group_id.pack()],
        bump,
    )?;

    let group = group_info.as_account_mut::<Group>(&tapedrive::ID)?;
    group.id = group_id;
    group.epoch = target;

    let bootstrap_snapshot_epoch = EpochNumber(0);
    let (snapshot_tape_address, snapshot_tape_bump) =
        snapshot_tape_pda(bootstrap_snapshot_epoch);
    snapshot_tape_info
        .is_empty()?
        .is_writable()?
        .has_address(&snapshot_tape_address.into())?;

    create_program_account_with_bump::<Tape>(
        snapshot_tape_info,
        system_program_info,
        fee_payer_info,
        &tapedrive::ID,
        &[SNAPSHOT_TAPE, &bootstrap_snapshot_epoch.pack()],
        snapshot_tape_bump,
    )?;

    let snapshot_tape = snapshot_tape_info.as_account_mut::<Tape>(&tapedrive::ID)?;
    *snapshot_tape = Tape::snapshot(bootstrap_snapshot_epoch);

    let active_peers = &peers[..peer_header.peers.count as usize];
    for (i, member) in members[..committee_size as usize].iter_mut().enumerate() {
        if member.stake.is_zero() {
            return Err(TapeError::NotStaked.into());
        }
        let peer = active_peers
            .iter()
            .find(|peer| peer.node == member.node)
            .ok_or(TapeError::BadMember)?;

        member.spools = 1;
        group.spools[i] = Spool {
            node: member.node,
            bls_pubkey: peer.bls_pubkey,
        };
    }

    epoch.total_groups = 1;
    epoch.total_assigned = StorageUnits::zero();
    epoch.preferences = NodePreferences {
        storage_capacity: archive.storage_capacity,
        storage_price: archive.storage_price,
        committee_size,
        spool_groups,
        min_version: system.min_version,
        burn_fee_bps,
        subsidy_decay_bps,
    };

    let clock = Clock::get()?;
    epoch.start_slot = SlotNumber(clock.slot);
    epoch.start_time = clock.unix_timestamp;

    // Epoch 1 has no prev-epoch groups to sync, settle, or snapshot. Skip
    // straight to Active; the natural Sync->Settle->Snapshot->Active cycle
    // begins with Epoch 2 against Epoch 1's groups.
    epoch.state.phase = EpochPhase::Active as u64;

    system.committee_size = committee_size;
    system.target_group_count = spool_groups;
    system.live_group_count = 1;
    system.current_epoch = target;

    Ok(())
}

pub(super) fn ensure_committee_capacity<'info>(
    committee_info: &AccountInfo<'info>,
    system_program_info: &AccountInfo<'info>,
    fee_payer_info: &AccountInfo<'info>,
    epoch: EpochNumber,
    capacity: u64,
) -> ProgramResult {
    let header = Committee::header(committee_info, &tapedrive::ID)?;
    if header.epoch != epoch {
        return Err(TapeError::BadEpochId.into());
    }
    if header.members.capacity >= capacity {
        return Ok(());
    }

    resize_account(
        committee_info,
        system_program_info,
        fee_payer_info,
        Committee::size_for_capacity(capacity),
    )?;

    let header = Committee::header_mut(committee_info, &tapedrive::ID)?;
    header.epoch = epoch;
    header.members.capacity = capacity;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tape_api::state::{Committee, PeerSet};
    use tape_core::system::{sort_members_for_committee, Peer};
    use tape_test::*;

    fn genesis_committee() -> (Vec<Member>, Vec<Member>, Vec<Peer>) {
        let mut staged_members: Vec<Member> = Vec::with_capacity(GROUP_SIZE);
        let mut expected_peers: Vec<Peer> = Vec::with_capacity(GROUP_SIZE);
        for i in 0..GROUP_SIZE {
            let bls_sk = BlsPrivateKey::from_random();
            let bls_pk = bls_sk.public_key().expect("bls pk");
            let mut bytes = [0u8; 32];
            bytes[0] = (i as u8) + 1;
            let addr = Address::new(bytes);

            staged_members.push(Member {
                node: addr,
                stake: TAPE(i as u64 + 1),
                assigned: StorageUnits::zero(),
                blacklisted: StorageUnits::zero(),
                spools: 0,
            });
            expected_peers.push(Peer {
                node: addr,
                bls_pubkey: bls_pk,
                ..Peer::zeroed()
            });
        }
        sort_members_for_committee(&mut staged_members);

        let expected_members = staged_members
            .iter()
            .map(|member| Member { spools: 1, ..*member })
            .collect();

        (staged_members, expected_members, expected_peers)
    }

    // happy-path
    #[test]
    fn start() {
        let fee_payer = Pubkey::new_unique();
        let subsidy_authority = Pubkey::new_unique();

        let target = EpochNumber(1);
        let candidate = EpochNumber(2);
        let group_id = GroupIndex(0);

        let (system_address, _) = system_pda();
        let (archive_address, _) = archive_pda();
        let (subsidy_address, _) = subsidy_pda();
        let (subsidy_ata_address, _) = subsidy_ata();
        let (epoch_address, _) = epoch_pda(target);
        let (committee_address, _) = committee_pda(target);
        let (candidate_epoch_address, _) = epoch_pda(candidate);
        let (candidate_committee_address, _) = committee_pda(candidate);
        let (peer_set_address, _) = peer_set_pda();
        let (group_address, _) = group_pda(target, group_id);
        let (snapshot_tape_address, _) = snapshot_tape_pda(EpochNumber(0));

        let (staged_members, expected_members, expected_peers) = genesis_committee();

        let system = System {
            current_epoch: EpochNumber(0),
            ..System::zeroed()
        };

        let epoch = Epoch {
            id: target,
            state: EpochState::zeroed(),
            ..Epoch::zeroed()
        };
        let candidate_epoch = Epoch {
            id: candidate,
            state: EpochState::zeroed(),
            ..Epoch::zeroed()
        };

        let committee_size = GROUP_SIZE as u64;
        let peer_capacity = committee_size;
        let committee_data = Committee {
            epoch: target,
            members: Tail::new(committee_size, staged_members.len() as u64),
        }
        .pack_with(&staged_members);
        let candidate_committee_data =
            Committee { epoch: candidate, members: Tail::empty(0) }
                .pack_with(&[]);
        let peer_set_data = PeerSet {
            peers: Tail::new(peer_capacity, expected_peers.len() as u64),
        }
        .pack_with(&expected_peers);

        let spool_groups: u64 = 50;
        let subsidy_amount = TAPE(50);
        let burn_fee_bps = DEFAULT_BURN_FEE_BPS;
        let subsidy_decay_bps = DEFAULT_SUBSIDY_DECAY_BPS;
        let instruction = build_start_network_ix(
            fee_payer.into(),
            subsidy_authority.into(),
            committee_size,
            spool_groups,
            subsidy_amount,
            burn_fee_bps,
            subsidy_decay_bps,
        );

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            sol(subsidy_authority, 0),
            tape_test::ata(subsidy_authority, subsidy_amount.as_u64()),
            pda(system_address, system.pack(), tapedrive::ID),
            pda(archive_address, Archive::zeroed().pack(), tapedrive::ID),
            pda(epoch_address, epoch.pack(), tapedrive::ID),
            pda(committee_address, committee_data, tapedrive::ID),
            pda(candidate_epoch_address, candidate_epoch.pack(), tapedrive::ID),
            pda(candidate_committee_address, candidate_committee_data, tapedrive::ID),
            pda(peer_set_address, peer_set_data, tapedrive::ID),
            empty(group_address),
            empty(snapshot_tape_address),
            empty(subsidy_address),
            token(subsidy_ata_address, Pubkey::from(subsidy_address), 0),
            token_program(),
            system_program(),
            rent_sysvar(),
        ];

        let mut expected_group = Group {
            id: group_id,
            epoch: target,
            ..Group::zeroed()
        };
        for i in 0..GROUP_SIZE {
            let peer = expected_peers
                .iter()
                .find(|peer| peer.node == expected_members[i].node)
                .expect("expected peer");
            expected_group.spools[i] = Spool {
                node: expected_members[i].node,
                bls_pubkey: peer.bls_pubkey,
            };
        }

        let env = test_env();
        let now = env.now();
        let slot = env.slot();
        env.process_instruction(
            &instruction,
            &accounts,
            &[
                Check::success(),
                Check::account(&Pubkey::from(system_address)).data(
                    System {
                        current_epoch: target,
                        committee_size,
                        target_group_count: spool_groups,
                        live_group_count: 1,
                        ..system
                    }.pack().as_ref()
                ).build(),
                Check::account(&Pubkey::from(epoch_address)).data(
                    Epoch {
                        total_groups: 1,
                        total_assigned: StorageUnits::zero(),
                        preferences: NodePreferences {
                            storage_capacity: StorageUnits::zero(),
                            storage_price: TAPE::zero(),
                            committee_size,
                            spool_groups,
                            min_version: VersionId(0),
                            burn_fee_bps,
                            subsidy_decay_bps,
                        },
                        start_slot: SlotNumber(slot),
                        start_time: now,
                        state: EpochState {
                            phase: EpochPhase::Active as u64,
                            ..EpochState::zeroed()
                        },
                        ..epoch
                    }.pack().as_ref()
                ).build(),
                Check::account(&Pubkey::from(archive_address)).data(
                    Archive {
                        schedule: EpochSchedule::new_at(target),
                        burn_fee_bps,
                        subsidy_decay_bps,
                        ..Archive::zeroed()
                    }.pack().as_ref()
                ).build(),
                Check::account(&Pubkey::from(subsidy_ata_address))
                    .data(token(subsidy_ata_address, Pubkey::from(subsidy_address), subsidy_amount.as_u64()).1.data.as_ref())
                    .build(),
                Check::account(&Pubkey::from(group_address))
                    .data(expected_group.pack().as_ref())
                    .build(),
                Check::account(&Pubkey::from(snapshot_tape_address))
                    .data(Tape::snapshot(EpochNumber(0)).pack().as_ref())
                    .build(),
                Check::account(&Pubkey::from(committee_address))
                    .data(Committee {
                        epoch: target,
                        members: Tail::new(committee_size, expected_members.len() as u64),
                    }.pack_with(&expected_members).as_ref())
                    .build(),
                Check::account(&Pubkey::from(candidate_committee_address))
                    .data(Committee {
                        epoch: candidate,
                        members: Tail::empty(committee_size),
                    }.pack_with(&[]).as_ref())
                    .build(),
                Check::account(&Pubkey::from(peer_set_address))
                    .data(PeerSet {
                        peers: Tail::new(peer_capacity, expected_peers.len() as u64),
                    }.pack_with(&expected_peers).as_ref())
                    .build(),
            ],
        );
    }

    #[test]
    fn rejects_zero_stake_staged_member() {
        let fee_payer = Pubkey::new_unique();
        let subsidy_authority = Pubkey::new_unique();

        let target = EpochNumber(1);
        let candidate = EpochNumber(2);
        let group_id = GroupIndex(0);

        let (system_address, _) = system_pda();
        let (archive_address, _) = archive_pda();
        let (subsidy_address, _) = subsidy_pda();
        let (subsidy_ata_address, _) = subsidy_ata();
        let (epoch_address, _) = epoch_pda(target);
        let (committee_address, _) = committee_pda(target);
        let (candidate_epoch_address, _) = epoch_pda(candidate);
        let (candidate_committee_address, _) = committee_pda(candidate);
        let (peer_set_address, _) = peer_set_pda();
        let (group_address, _) = group_pda(target, group_id);
        let (snapshot_tape_address, _) = snapshot_tape_pda(EpochNumber(0));

        let (mut staged_members, _, expected_peers) = genesis_committee();
        staged_members[0].stake = TAPE::zero();

        let system = System {
            current_epoch: EpochNumber(0),
            ..System::zeroed()
        };

        let epoch = Epoch {
            id: target,
            state: EpochState::zeroed(),
            ..Epoch::zeroed()
        };
        let candidate_epoch = Epoch {
            id: candidate,
            state: EpochState::zeroed(),
            ..Epoch::zeroed()
        };

        let committee_size = GROUP_SIZE as u64;
        let peer_capacity = committee_size;
        let committee_data = Committee {
            epoch: target,
            members: Tail::new(committee_size, staged_members.len() as u64),
        }
        .pack_with(&staged_members);
        let candidate_committee_data =
            Committee { epoch: candidate, members: Tail::empty(GROUP_SIZE as u64) }
                .pack_with(&[]);
        let peer_set_data = PeerSet {
            peers: Tail::new(peer_capacity, expected_peers.len() as u64),
        }
        .pack_with(&expected_peers);

        let instruction = build_start_network_ix(
            fee_payer.into(),
            subsidy_authority.into(),
            committee_size,
            50,
            TAPE::zero(),
            DEFAULT_BURN_FEE_BPS,
            DEFAULT_SUBSIDY_DECAY_BPS,
        );

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            sol(subsidy_authority, 0),
            tape_test::ata(subsidy_authority, 0),
            pda(system_address, system.pack(), tapedrive::ID),
            pda(archive_address, Archive::zeroed().pack(), tapedrive::ID),
            pda(epoch_address, epoch.pack(), tapedrive::ID),
            pda(committee_address, committee_data, tapedrive::ID),
            pda(candidate_epoch_address, candidate_epoch.pack(), tapedrive::ID),
            pda(candidate_committee_address, candidate_committee_data, tapedrive::ID),
            pda(peer_set_address, peer_set_data, tapedrive::ID),
            empty(group_address),
            empty(snapshot_tape_address),
            empty(subsidy_address),
            token(subsidy_ata_address, Pubkey::from(subsidy_address), 0),
            token_program(),
            system_program(),
            rent_sysvar(),
        ];

        let env = test_env();
        env.process_instruction(
            &instruction,
            &accounts,
            &[Check::err(TapeError::NotStaked.into())],
        );
    }

    #[test]
    fn rejects_incomplete_genesis_committee() {
        let fee_payer = Pubkey::new_unique();
        let subsidy_authority = Pubkey::new_unique();

        let target = EpochNumber(1);
        let candidate = EpochNumber(2);
        let group_id = GroupIndex(0);

        let (system_address, _) = system_pda();
        let (archive_address, _) = archive_pda();
        let (subsidy_address, _) = subsidy_pda();
        let (subsidy_ata_address, _) = subsidy_ata();
        let (epoch_address, _) = epoch_pda(target);
        let (committee_address, _) = committee_pda(target);
        let (candidate_epoch_address, _) = epoch_pda(candidate);
        let (candidate_committee_address, _) = committee_pda(candidate);
        let (peer_set_address, _) = peer_set_pda();
        let (group_address, _) = group_pda(target, group_id);
        let (snapshot_tape_address, _) = snapshot_tape_pda(EpochNumber(0));

        let (staged_members, _, expected_peers) = genesis_committee();
        let staged_members = &staged_members[..GROUP_SIZE - 1];

        let system = System {
            current_epoch: EpochNumber(0),
            ..System::zeroed()
        };
        let epoch = Epoch {
            id: target,
            state: EpochState::zeroed(),
            ..Epoch::zeroed()
        };
        let candidate_epoch = Epoch {
            id: candidate,
            state: EpochState::zeroed(),
            ..Epoch::zeroed()
        };

        let committee_size = GROUP_SIZE as u64;
        let peer_capacity = committee_size;
        let committee_data = Committee {
            epoch: target,
            members: Tail::new(committee_size, staged_members.len() as u64),
        }
        .pack_with(staged_members);
        let candidate_committee_data =
            Committee { epoch: candidate, members: Tail::empty(0) }.pack_with(&[]);
        let peer_set_data = PeerSet {
            peers: Tail::new(peer_capacity, expected_peers.len() as u64),
        }
        .pack_with(&expected_peers);

        let instruction = build_start_network_ix(
            fee_payer.into(),
            subsidy_authority.into(),
            committee_size,
            50,
            TAPE::zero(),
            DEFAULT_BURN_FEE_BPS,
            DEFAULT_SUBSIDY_DECAY_BPS,
        );

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            sol(subsidy_authority, 0),
            tape_test::ata(subsidy_authority, 0),
            pda(system_address, system.pack(), tapedrive::ID),
            pda(archive_address, Archive::zeroed().pack(), tapedrive::ID),
            pda(epoch_address, epoch.pack(), tapedrive::ID),
            pda(committee_address, committee_data, tapedrive::ID),
            pda(candidate_epoch_address, candidate_epoch.pack(), tapedrive::ID),
            pda(candidate_committee_address, candidate_committee_data, tapedrive::ID),
            pda(peer_set_address, peer_set_data, tapedrive::ID),
            empty(group_address),
            empty(snapshot_tape_address),
            empty(subsidy_address),
            token(subsidy_ata_address, Pubkey::from(subsidy_address), 0),
            token_program(),
            system_program(),
            rent_sysvar(),
        ];

        let env = test_env();
        env.process_instruction(
            &instruction,
            &accounts,
            &[Check::err(TapeError::InsufficientCommittee.into())],
        );
    }

    #[test]
    fn rejects_missing_genesis_peer() {
        let fee_payer = Pubkey::new_unique();
        let subsidy_authority = Pubkey::new_unique();

        let target = EpochNumber(1);
        let candidate = EpochNumber(2);
        let group_id = GroupIndex(0);

        let (system_address, _) = system_pda();
        let (archive_address, _) = archive_pda();
        let (subsidy_address, _) = subsidy_pda();
        let (subsidy_ata_address, _) = subsidy_ata();
        let (epoch_address, _) = epoch_pda(target);
        let (committee_address, _) = committee_pda(target);
        let (candidate_epoch_address, _) = epoch_pda(candidate);
        let (candidate_committee_address, _) = committee_pda(candidate);
        let (peer_set_address, _) = peer_set_pda();
        let (group_address, _) = group_pda(target, group_id);
        let (snapshot_tape_address, _) = snapshot_tape_pda(EpochNumber(0));

        let (staged_members, _, expected_peers) = genesis_committee();
        let expected_peers = &expected_peers[..GROUP_SIZE - 1];

        let system = System {
            current_epoch: EpochNumber(0),
            ..System::zeroed()
        };
        let epoch = Epoch {
            id: target,
            state: EpochState::zeroed(),
            ..Epoch::zeroed()
        };
        let candidate_epoch = Epoch {
            id: candidate,
            state: EpochState::zeroed(),
            ..Epoch::zeroed()
        };

        let committee_size = GROUP_SIZE as u64;
        let peer_capacity = committee_size;
        let committee_data = Committee {
            epoch: target,
            members: Tail::new(committee_size, staged_members.len() as u64),
        }
        .pack_with(&staged_members);
        let candidate_committee_data =
            Committee { epoch: candidate, members: Tail::empty(0) }.pack_with(&[]);
        let peer_set_data = PeerSet {
            peers: Tail::new(peer_capacity, expected_peers.len() as u64),
        }
        .pack_with(expected_peers);

        let instruction = build_start_network_ix(
            fee_payer.into(),
            subsidy_authority.into(),
            committee_size,
            50,
            TAPE::zero(),
            DEFAULT_BURN_FEE_BPS,
            DEFAULT_SUBSIDY_DECAY_BPS,
        );

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            sol(subsidy_authority, 0),
            tape_test::ata(subsidy_authority, 0),
            pda(system_address, system.pack(), tapedrive::ID),
            pda(archive_address, Archive::zeroed().pack(), tapedrive::ID),
            pda(epoch_address, epoch.pack(), tapedrive::ID),
            pda(committee_address, committee_data, tapedrive::ID),
            pda(candidate_epoch_address, candidate_epoch.pack(), tapedrive::ID),
            pda(candidate_committee_address, candidate_committee_data, tapedrive::ID),
            pda(peer_set_address, peer_set_data, tapedrive::ID),
            empty(group_address),
            empty(snapshot_tape_address),
            empty(subsidy_address),
            token(subsidy_ata_address, Pubkey::from(subsidy_address), 0),
            token_program(),
            system_program(),
            rent_sysvar(),
        ];

        let env = test_env();
        env.process_instruction(
            &instruction,
            &accounts,
            &[Check::err(TapeError::BadMember.into())],
        );
    }
}
