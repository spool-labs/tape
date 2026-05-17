use tape_solana::*;
use tape_api::program::prelude::*;


pub fn process_start_network(accounts: &[AccountInfo<'_>], data: &[u8]) -> ProgramResult {
    let args = StartNetwork::try_from_bytes(data)?;
    let [
        fee_payer_info,
        system_info,
        archive_info,
        epoch_info,
        committee_info,
        peer_set_info,
        group_info,
        snapshot_tape_info,
        system_program_info,
        rent_sysvar_info,
        genesis_node_infos @ ..,
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

    if system.current_epoch != EpochNumber(0) {
        return Err(TapeError::BadEpochState.into());
    }

    system_program_info
        .is_program(&system_program::ID)?;
    rent_sysvar_info
        .is_sysvar(&sysvar::rent::ID)?;

    let committee_size = u64::from_le_bytes(args.committee_size);
    if committee_size < GROUP_SIZE as u64 {
        return Err(TapeError::InsufficientCommittee.into());
    }

    let spool_groups = u64::from_le_bytes(args.spool_groups);
    if spool_groups == 0 {
        return Err(TapeError::UnexpectedState.into());
    }

    if genesis_node_infos.len() != GROUP_SIZE {
        return Err(ProgramError::NotEnoughAccountKeys);
    }

    archive_info
        .is_writable()?
        .is_archive()?;

    let target = EpochNumber(1);
    let group_id = GroupIndex(0);

    let epoch = epoch_info
        .is_writable()?
        .is_epoch(target)?
        .as_account_mut::<Epoch>(&tapedrive::ID)?;

    if epoch.state.phase != EpochPhase::Unknown as u64 {
        return Err(TapeError::BadEpochState.into());
    }

    if epoch.total_groups != 0 {
        return Err(TapeError::UnexpectedState.into());
    }

    committee_info
        .is_writable()?
        .is_committee(target)?;

    let (committee_header, members) = Committee::read_mut(committee_info, &tapedrive::ID)?;

    if committee_header.members.capacity < GROUP_SIZE as u64 {
        return Err(TapeError::InsufficientCommittee.into());
    }

    if committee_header.members.count != 0 {
        return Err(TapeError::UnexpectedState.into());
    }

    peer_set_info
        .is_writable()?
        .is_peer_set()?;

    let (peer_header, peers) = PeerSet::read_mut(peer_set_info, &tapedrive::ID)?;

    if peer_header.peers.capacity < GROUP_SIZE as u64 {
        return Err(TapeError::ListFull.into());
    }

    if peer_header.peers.count != 0 {
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
    snapshot_tape.id = TapeNumber(0);
    snapshot_tape.authority = SYSTEM_ADDRESS;
    snapshot_tape.capacity = StorageUnits(u64::MAX);
    snapshot_tape.active_epoch = bootstrap_snapshot_epoch;
    snapshot_tape.expiry_epoch = EpochNumber(u64::MAX);

    for (i, node_info) in genesis_node_infos.iter().enumerate() {
        let node = node_info.as_account::<Node>(&tapedrive::ID)?;
        let node_address: Address = (*node_info.key).into();

        if genesis_node_infos[..i]
            .iter()
            .any(|prior| prior.key == node_info.key)
        {
            return Err(TapeError::BadMember.into());
        }

        let bls_pubkey = node.metadata.bls_pubkey;
        let stake = node.pool.stake;
        if stake.is_zero() {
            return Err(TapeError::NotStaked.into());
        }

        members[i] = Member {
            node: node_address,
            stake,
            blacklist: node.blacklist.total_size(),
            spools: 1,
        };
        peers[i] = Peer {
            node: node_address,
            bls_pubkey,
            network_address: node.metadata.network_address,
            network_tls: node.metadata.network_tls,
            preferences: node.preferences,
        };
        group.spools[i] = Spool { node: node_address, bls_pubkey };
    }

    committee_header.members.count = GROUP_SIZE as u64;
    peer_header.peers.count = GROUP_SIZE as u64;

    epoch.total_groups = 1;
    epoch.total_assigned = StorageUnits::zero();

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

#[cfg(test)]
mod tests {
    use super::*;
    use tape_api::state::{Committee, PeerSet};
    use tape_core::system::Peer;
    use tape_test::*;

    fn genesis_nodes() -> (Vec<Address>, Vec<Node>, Vec<Member>, Vec<Peer>) {
        let mut genesis_nodes: Vec<Address> = Vec::with_capacity(GROUP_SIZE);
        let mut node_accounts: Vec<Node> = Vec::with_capacity(GROUP_SIZE);
        let mut expected_members: Vec<Member> = Vec::with_capacity(GROUP_SIZE);
        let mut expected_peers: Vec<Peer> = Vec::with_capacity(GROUP_SIZE);
        for i in 0..GROUP_SIZE {
            let bls_sk = BlsPrivateKey::from_random();
            let bls_pk = bls_sk.public_key().expect("bls pk");
            let mut bytes = [0u8; 32];
            bytes[0] = (i as u8) + 1;
            let addr = Address::new(bytes);
            let mut node = Node::zeroed();
            node.pool.stake = TAPE(i as u64 + 1);
            node.metadata.bls_pubkey = bls_pk;

            genesis_nodes.push(addr);
            node_accounts.push(node);
            expected_members.push(Member {
                node: addr,
                stake: node.pool.stake,
                blacklist: StorageUnits::zero(),
                spools: 1,
            });
            expected_peers.push(Peer {
                node: addr,
                bls_pubkey: bls_pk,
                network_address: node.metadata.network_address,
                network_tls: node.metadata.network_tls,
                preferences: node.preferences,
            });
        }

        (genesis_nodes, node_accounts, expected_members, expected_peers)
    }

    // happy-path
    #[test]
    fn start() {
        let fee_payer = Pubkey::new_unique();

        let target = EpochNumber(1);
        let group_id = GroupIndex(0);

        let (system_address, _) = system_pda();
        let (archive_address, _) = archive_pda();
        let (epoch_address, _) = epoch_pda(target);
        let (committee_address, _) = committee_pda(target);
        let (peer_set_address, _) = peer_set_pda();
        let (group_address, _) = group_pda(target, group_id);
        let (snapshot_tape_address, _) = snapshot_tape_pda(EpochNumber(0));

        let (genesis_nodes, node_accounts, expected_members, expected_peers) = genesis_nodes();

        let system = System {
            current_epoch: EpochNumber(0),
            ..System::zeroed()
        };

        let epoch = Epoch {
            id: target,
            state: EpochState::zeroed(),
            ..Epoch::zeroed()
        };

        let committee_data =
            Committee { epoch: target, members: Tail::empty(GROUP_SIZE as u64) }
                .pack_with(&[]);
        let peer_set_data =
            PeerSet { peers: Tail::empty(GROUP_SIZE as u64) }
                .pack_with(&[]);

        let committee_size: u64 = 128;
        let spool_groups: u64 = 50;
        let instruction = build_start_network_ix(
            fee_payer.into(),
            committee_size,
            spool_groups,
            &genesis_nodes,
        );

        let mut accounts = vec![
            sol(fee_payer, 1_000_000_000),
            pda(system_address, system.pack(), tapedrive::ID),
            pda(archive_address, Archive::zeroed().pack(), tapedrive::ID),
            pda(epoch_address, epoch.pack(), tapedrive::ID),
            pda(committee_address, committee_data, tapedrive::ID),
            pda(peer_set_address, peer_set_data, tapedrive::ID),
            empty(group_address),
            empty(snapshot_tape_address),
            system_program(),
            rent_sysvar(),
        ];
        accounts.extend(
            genesis_nodes
                .iter()
                .zip(node_accounts.iter())
                .map(|(address, node)| pda(*address, node.pack(), tapedrive::ID)),
        );

        let mut expected_group = Group {
            id: group_id,
            epoch: target,
            ..Group::zeroed()
        };
        for i in 0..GROUP_SIZE {
            expected_group.spools[i] = Spool {
                node: expected_members[i].node,
                bls_pubkey: expected_peers[i].bls_pubkey,
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
                        start_slot: SlotNumber(slot),
                        start_time: now,
                        state: EpochState {
                            phase: EpochPhase::Active as u64,
                            ..EpochState::zeroed()
                        },
                        ..epoch
                    }.pack().as_ref()
                ).build(),
                Check::account(&Pubkey::from(group_address))
                    .data(expected_group.pack().as_ref())
                    .build(),
                Check::account(&Pubkey::from(snapshot_tape_address))
                    .data(Tape {
                        id: TapeNumber(0),
                        authority: SYSTEM_ADDRESS,
                        capacity: StorageUnits(u64::MAX),
                        active_epoch: EpochNumber(0),
                        expiry_epoch: EpochNumber(u64::MAX),
                        ..Tape::zeroed()
                    }.pack().as_ref())
                    .build(),
                Check::account(&Pubkey::from(committee_address))
                    .data(Committee {
                        epoch: target,
                        members: Tail::new(GROUP_SIZE as u64, expected_members.len() as u64),
                    }.pack_with(&expected_members).as_ref())
                    .build(),
                Check::account(&Pubkey::from(peer_set_address))
                    .data(PeerSet {
                        peers: Tail::new(GROUP_SIZE as u64, expected_peers.len() as u64),
                    }.pack_with(&expected_peers).as_ref())
                    .build(),
            ],
        );
    }

    #[test]
    fn rejects_zero_stake_genesis_node() {
        let fee_payer = Pubkey::new_unique();

        let target = EpochNumber(1);
        let group_id = GroupIndex(0);

        let (system_address, _) = system_pda();
        let (archive_address, _) = archive_pda();
        let (epoch_address, _) = epoch_pda(target);
        let (committee_address, _) = committee_pda(target);
        let (peer_set_address, _) = peer_set_pda();
        let (group_address, _) = group_pda(target, group_id);
        let (snapshot_tape_address, _) = snapshot_tape_pda(EpochNumber(0));

        let (genesis_nodes, mut node_accounts, _, _) = genesis_nodes();
        node_accounts[0].pool.stake = TAPE::zero();

        let system = System {
            current_epoch: EpochNumber(0),
            ..System::zeroed()
        };

        let epoch = Epoch {
            id: target,
            state: EpochState::zeroed(),
            ..Epoch::zeroed()
        };

        let committee_data =
            Committee { epoch: target, members: Tail::empty(GROUP_SIZE as u64) }
                .pack_with(&[]);
        let peer_set_data =
            PeerSet { peers: Tail::empty(GROUP_SIZE as u64) }
                .pack_with(&[]);

        let instruction = build_start_network_ix(
            fee_payer.into(),
            128,
            50,
            &genesis_nodes,
        );

        let mut accounts = vec![
            sol(fee_payer, 1_000_000_000),
            pda(system_address, system.pack(), tapedrive::ID),
            pda(archive_address, Archive::zeroed().pack(), tapedrive::ID),
            pda(epoch_address, epoch.pack(), tapedrive::ID),
            pda(committee_address, committee_data, tapedrive::ID),
            pda(peer_set_address, peer_set_data, tapedrive::ID),
            empty(group_address),
            empty(snapshot_tape_address),
            system_program(),
            rent_sysvar(),
        ];
        accounts.extend(
            genesis_nodes
                .iter()
                .zip(node_accounts.iter())
                .map(|(address, node)| pda(*address, node.pack(), tapedrive::ID)),
        );

        let env = test_env();
        env.process_instruction(
            &instruction,
            &accounts,
            &[Check::err(TapeError::NotStaked.into())],
        );
    }
}
