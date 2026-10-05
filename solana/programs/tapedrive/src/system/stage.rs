use tape_solana::*;
use tape_api::event::NodeJoinedCommittee;
use tape_api::program::prelude::*;
use tape_api::state::{Committee, PeerSet};
use tape_core::system::{apply_peer_join_slice, sort_members_for_committee, Peer};

use super::start::ensure_committee_capacity;

pub fn process_stage_genesis_node(accounts: &[AccountInfo<'_>], data: &[u8]) -> ProgramResult {
    let _args = StageGenesisNode::try_from_bytes(data)?;
    let [
        fee_payer_info,
        authority_info,
        system_info,
        epoch_info,
        committee_info,
        peer_set_info,
        node_info,
        system_program_info,
    ] = accounts else {
        return Err(ProgramError::NotEnoughAccountKeys);
    };

    fee_payer_info
        .is_signer()?
        .is_writable()?;
    authority_info
        .is_signer()?;

    system_program_info
        .is_program(&system_program::ID)?;

    let system = system_info
        .is_system()?
        .as_account::<System>(&tapedrive::ID)?;

    if system.current_epoch != EpochNumber(0) {
        return Err(TapeError::BadEpochState.into());
    }

    let target = EpochNumber(1);
    let epoch = epoch_info
        .is_epoch(target)?
        .as_account::<Epoch>(&tapedrive::ID)?;

    if epoch.id != target {
        return Err(TapeError::BadEpochId.into());
    }
    if epoch.state.phase != EpochPhase::Unknown as u64 {
        return Err(TapeError::BadEpochState.into());
    }
    if epoch.total_groups != 0 {
        return Err(TapeError::BadEpochState.into());
    }

    committee_info
        .is_writable()?
        .is_committee(target)?;

    ensure_committee_capacity(
        committee_info,
        system_program_info,
        fee_payer_info,
        target,
        GROUP_SIZE as u64,
    )?;

    peer_set_info
        .is_writable()?
        .is_peer_set()?;

    let authority: Address = (*authority_info.key).into();
    let (expected_node, _) = node_pda(authority);
    node_info
        .has_address(&expected_node.into())?;

    let node = node_info
        .as_account::<Node>(&tapedrive::ID)?;

    if node.authority != authority {
        return Err(ProgramError::InvalidAccountData);
    }

    let stake = node.pool.stake;
    if stake.is_zero() {
        return Err(TapeError::NotStaked.into());
    }

    let node_address: Address = (*node_info.key).into();

    let member = Member {
        node: node_address,
        stake,
        assigned: StorageUnits::zero(),
        blacklisted: StorageUnits::zero(),
        spools: 0,
    };

    let peer = Peer {
        node: node_address,
        bls_pubkey: node.metadata.bls_pubkey,
        network_address: node.metadata.network_address,
        network_tls: node.metadata.network_tls,
        preferences: node.preferences,
    };

    let (committee_header, members) =
        Committee::read_full_mut(committee_info, &tapedrive::ID)?;

    if committee_header.epoch != target {
        return Err(TapeError::BadEpochId.into());
    }
    if committee_header.members.capacity != GROUP_SIZE as u64 {
        return Err(TapeError::InsufficientCommittee.into());
    }

    stage_genesis_member(
        members,
        &mut committee_header.members.count,
        committee_header.members.capacity,
        member,
    )?;

    let (peer_header, peers) =
        PeerSet::read_full_mut(peer_set_info, &tapedrive::ID)?;
    if peer_header.peers.capacity < GROUP_SIZE as u64 {
        return Err(TapeError::ListFull.into());
    }

    apply_peer_join_slice(
        peers,
        &mut peer_header.peers.count,
        peer_header.peers.capacity,
        peer,
    )
    .map_err(|_| TapeError::ListFull)?;

    NodeJoinedCommittee {
        node: node_address,
        stake,
        key: node.metadata.bls_pubkey,
        preferences: node.preferences,
        activation_epoch: target,
    }
    .log();

    Ok(())
}

fn stage_genesis_member(
    members: &mut [Member],
    count: &mut u64,
    capacity: u64,
    member: Member,
) -> ProgramResult {
    let count_usize = *count as usize;
    let capacity_usize = capacity as usize;
    if count_usize > members.len() || capacity_usize > members.len() {
        return Err(TapeError::ListFull.into());
    }

    if let Some(index) = members[..count_usize]
        .iter()
        .position(|m| m.node == member.node)
    {
        members[index] = member;
        sort_members_for_committee(&mut members[..count_usize]);
        return Ok(());
    }

    if count_usize >= capacity_usize {
        return Err(TapeError::ListFull.into());
    }

    members[count_usize] = member;
    *count = (*count).saturating_add(1);
    sort_members_for_committee(&mut members[..(*count as usize)]);

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tape_test::*;

    fn authority_node(stake: TAPE) -> (Pubkey, Address, Node, Member, Peer) {
        let authority = Pubkey::new_unique();
        let node_address = node_pda(authority.into()).0;
        let bls_sk = BlsPrivateKey::from_random();
        let bls_pubkey = bls_sk.public_key().expect("bls pk");
        let preferences = NodePreferences {
            storage_price: TAPE(10),
            burn_fee_bps: BasisPoints(1_000),
            subsidy_decay_bps: DEFAULT_SUBSIDY_DECAY_BPS,
            ..NodePreferences::zeroed()
        };
        let node = Node {
            authority: authority.into(),
            metadata: NodeMetadata {
                bls_pubkey,
                ..NodeMetadata::zeroed()
            },
            preferences,
            pool: StakingPool {
                stake,
                ..StakingPool::zeroed()
            },
            ..Node::zeroed()
        };
        let member = Member {
            node: node_address,
            stake,
            assigned: StorageUnits::zero(),
            blacklisted: StorageUnits::zero(),
            spools: 0,
        };
        let peer = Peer {
            node: node_address,
            bls_pubkey,
            network_address: node.metadata.network_address,
            network_tls: node.metadata.network_tls,
            preferences,
        };

        (authority, node_address, node, member, peer)
    }

    fn stage_accounts(
        fee_payer: Pubkey,
        authority: Pubkey,
        node_address: Address,
        node: Node,
        committee_data: Vec<u8>,
        peer_set_data: Vec<u8>,
    ) -> Vec<(Pubkey, solana_account::Account)> {
        let target = EpochNumber(1);
        let (system_address, _) = system_pda();
        let (epoch_address, _) = epoch_pda(target);
        let (committee_address, _) = committee_pda(target);
        let (peer_set_address, _) = peer_set_pda();

        let system = System {
            current_epoch: EpochNumber(0),
            ..System::zeroed()
        };
        let epoch = Epoch {
            id: target,
            state: EpochState::zeroed(),
            ..Epoch::zeroed()
        };

        vec![
            sol(fee_payer, 1_000_000_000),
            sol(authority, 0),
            pda(system_address, system.pack(), tapedrive::ID),
            pda(epoch_address, epoch.pack(), tapedrive::ID),
            pda(committee_address, committee_data, tapedrive::ID),
            pda(peer_set_address, peer_set_data, tapedrive::ID),
            pda(node_address, node.pack(), tapedrive::ID),
            system_program(),
        ]
    }

    #[test]
    fn stage_genesis_node() {
        let fee_payer = Pubkey::new_unique();
        let target = EpochNumber(1);
        let (committee_address, _) = committee_pda(target);
        let (peer_set_address, _) = peer_set_pda();
        let (authority, node_address, node, expected_member, expected_peer) =
            authority_node(TAPE(1_000));

        let instruction =
            build_stage_genesis_node_ix(fee_payer.into(), authority.into(), node_address);
        let committee_data = Committee {
            epoch: target,
            members: Tail::empty(0),
        }
        .pack_with(&[]);
        let peer_set_data = PeerSet {
            peers: Tail::empty(GROUP_SIZE as u64),
        }
        .pack_with(&[]);
        let accounts = stage_accounts(
            fee_payer,
            authority,
            node_address,
            node,
            committee_data,
            peer_set_data,
        );

        let env = test_env();
        env.process_instruction(
            &instruction,
            &accounts,
            &[
                Check::success(),
                Check::account(&Pubkey::from(committee_address))
                    .data(
                        Committee {
                            epoch: target,
                            members: Tail::new(GROUP_SIZE as u64, 1),
                        }
                        .pack_with(&[expected_member])
                        .as_ref(),
                    )
                    .build(),
                Check::account(&Pubkey::from(peer_set_address))
                    .data(
                        PeerSet {
                            peers: Tail::new(GROUP_SIZE as u64, 1),
                        }
                        .pack_with(&[expected_peer])
                        .as_ref(),
                    )
                    .build(),
            ],
        );
    }

    #[test]
    fn refreshes_existing_genesis_node() {
        let fee_payer = Pubkey::new_unique();
        let target = EpochNumber(1);
        let (committee_address, _) = committee_pda(target);
        let (peer_set_address, _) = peer_set_pda();
        let (authority, node_address, node, expected_member, expected_peer) =
            authority_node(TAPE(2_000));

        let old_member = Member {
            node: node_address,
            stake: TAPE(1),
            assigned: StorageUnits::mb(10),
            blacklisted: StorageUnits::mb(1),
            spools: 0,
        };
        let old_peer = Peer {
            node: node_address,
            ..Peer::zeroed()
        };

        let instruction =
            build_stage_genesis_node_ix(fee_payer.into(), authority.into(), node_address);
        let committee_data = Committee {
            epoch: target,
            members: Tail::new(GROUP_SIZE as u64, 1),
        }
        .pack_with(&[old_member]);
        let peer_set_data = PeerSet {
            peers: Tail::new(GROUP_SIZE as u64, 1),
        }
        .pack_with(&[old_peer]);
        let accounts = stage_accounts(
            fee_payer,
            authority,
            node_address,
            node,
            committee_data,
            peer_set_data,
        );

        let env = test_env();
        env.process_instruction(
            &instruction,
            &accounts,
            &[
                Check::success(),
                Check::account(&Pubkey::from(committee_address))
                    .data(
                        Committee {
                            epoch: target,
                            members: Tail::new(GROUP_SIZE as u64, 1),
                        }
                        .pack_with(&[expected_member])
                        .as_ref(),
                    )
                    .build(),
                Check::account(&Pubkey::from(peer_set_address))
                    .data(
                        PeerSet {
                            peers: Tail::new(GROUP_SIZE as u64, 1),
                        }
                        .pack_with(&[expected_peer])
                        .as_ref(),
                    )
                    .build(),
            ],
        );
    }

    #[test]
    fn rejects_zero_stake() {
        let fee_payer = Pubkey::new_unique();
        let target = EpochNumber(1);
        let (authority, node_address, node, _, _) = authority_node(TAPE::zero());

        let instruction =
            build_stage_genesis_node_ix(fee_payer.into(), authority.into(), node_address);
        let committee_data = Committee {
            epoch: target,
            members: Tail::empty(0),
        }
        .pack_with(&[]);
        let peer_set_data = PeerSet {
            peers: Tail::empty(GROUP_SIZE as u64),
        }
        .pack_with(&[]);
        let accounts = stage_accounts(
            fee_payer,
            authority,
            node_address,
            node,
            committee_data,
            peer_set_data,
        );

        let env = test_env();
        env.process_instruction(
            &instruction,
            &accounts,
            &[Check::err(TapeError::NotStaked.into())],
        );
    }

    #[test]
    fn rejects_wrong_authority() {
        let fee_payer = Pubkey::new_unique();
        let target = EpochNumber(1);
        let (authority, node_address, mut node, _, _) = authority_node(TAPE(1_000));
        node.authority = Pubkey::new_unique().into();

        let instruction =
            build_stage_genesis_node_ix(fee_payer.into(), authority.into(), node_address);
        let committee_data = Committee {
            epoch: target,
            members: Tail::empty(0),
        }
        .pack_with(&[]);
        let peer_set_data = PeerSet {
            peers: Tail::empty(GROUP_SIZE as u64),
        }
        .pack_with(&[]);
        let accounts = stage_accounts(
            fee_payer,
            authority,
            node_address,
            node,
            committee_data,
            peer_set_data,
        );

        let env = test_env();
        env.process_instruction(
            &instruction,
            &accounts,
            &[Check::err(ProgramError::InvalidAccountData)],
        );
    }

    #[test]
    fn rejects_21st_distinct_node() {
        let fee_payer = Pubkey::new_unique();
        let target = EpochNumber(1);
        let (authority, node_address, node, _, _) = authority_node(TAPE(10_000));

        let instruction =
            build_stage_genesis_node_ix(fee_payer.into(), authority.into(), node_address);

        let mut members = Vec::with_capacity(GROUP_SIZE);
        for i in 0..GROUP_SIZE {
            let mut bytes = [0u8; 32];
            bytes[0] = (i as u8) + 1;
            members.push(Member::new(Address::new(bytes), TAPE(i as u64 + 1)));
        }
        sort_members_for_committee(&mut members);

        let committee_data = Committee {
            epoch: target,
            members: Tail::new(GROUP_SIZE as u64, members.len() as u64),
        }
        .pack_with(&members);
        let peer_set_data = PeerSet {
            peers: Tail::empty(GROUP_SIZE as u64),
        }
        .pack_with(&[]);
        let accounts = stage_accounts(
            fee_payer,
            authority,
            node_address,
            node,
            committee_data,
            peer_set_data,
        );

        let env = test_env();
        env.process_instruction(
            &instruction,
            &accounts,
            &[Check::err(TapeError::ListFull.into())],
        );
    }
}
