use tape_solana::*;
use tape_api::program::prelude::*;
use tape_api::state::{Committee, PeerSet};
use tape_api::event::NodeJoinedCommittee;
use tape_core::system::{EpochPhase, Peer};

pub fn process_join_committee(accounts: &[AccountInfo<'_>], data: &[u8]) -> ProgramResult {
    let _args = JoinCommittee::try_from_bytes(data)?;
    let [
        fee_payer_info,
        authority_info,
        system_info,
        curr_epoch_info,
        curr_committee_info,
        next_committee_info,
        peer_set_info,
        node_info,
    ] = accounts else {
        return Err(ProgramError::NotEnoughAccountKeys);
    };

    fee_payer_info
        .is_signer()?
        .is_writable()?;
    authority_info
        .is_signer()?;

    let system = system_info
        .is_system()?
        .as_account::<System>(&tapedrive::ID)?;

    let curr = system.current_epoch;
    let next = curr.saturating_add(EpochNumber(1));

    let curr_epoch = curr_epoch_info
        .is_epoch(curr)?
        .as_account::<Epoch>(&tapedrive::ID)?;

    if curr_epoch.state.phase >= EpochPhase::Closing as u64 {
        return Err(TapeError::BadEpochState.into());
    }

    curr_committee_info.is_committee(curr)?;
    let (_, curr_members) = Committee::read(curr_committee_info, &tapedrive::ID)?;

    next_committee_info
        .is_writable()?
        .is_committee(next)?;

    let (next_committee, next_members) =
        Committee::read_mut(next_committee_info, &tapedrive::ID)?;

    if next_committee.members.capacity < system.committee_size {
        return Err(TapeError::InsufficientCommittee.into());
    }

    peer_set_info
        .is_writable()?
        .is_peer_set()?;

    let (peer_set, peers) = PeerSet::read_mut(peer_set_info, &tapedrive::ID)?;

    let node = node_info
        .is_writable()?
        .as_account_mut::<Node>(&tapedrive::ID)?;

    if node.authority != (*authority_info.key).into() {
        return Err(ProgramError::InvalidAccountData);
    }

    let stake = node.pool.stake;
    if stake.is_zero() {
        return Err(TapeError::NotStaked.into());
    }

    let node_address: Address = (*node_info.key).into();

    // A seated node must have advanced its pool through the previous epoch
    // before re-joining. Without this gate, the bump below would skip past
    // any pending rewards still waiting to be drained.
    let in_current_committee = curr_members.iter().any(|m| m.node == node_address);
    if in_current_committee
        && node.latest_advance_epoch < curr.saturating_sub(EpochNumber(1))
    {
        return Err(TapeError::NodeStale.into());
    }

    let member = Member {
        node: node_address,
        stake,
        blacklist: node.blacklist.total_size(),
        spools: 0,
    };

    try_join_committee(next_committee, next_members, &member)
        .map_err(|_| TapeError::UnexpectedState)?;

    let count = peer_set.peers.count as usize;
    let existing = peers[..count].iter().position(|p| p.node == node_address);

    let peer_index = if let Some(idx) = existing {
        // Refresh the peer's routing/identity from the (possibly-rotated)
        // node state so cert verification picks up the new key next epoch.
        peers[idx].bls_pubkey = node.metadata.bls_pubkey;
        peers[idx].network_address = node.metadata.network_address;
        peers[idx].network_tls = node.metadata.network_tls;
        peers[idx].preferences = node.preferences;
        idx
    } else {
        if peer_set.peers.is_full() {
            return Err(TapeError::ListFull.into());
        }
        peers[count] = Peer {
            node: node_address,
            bls_pubkey: node.metadata.bls_pubkey,
            network_address: node.metadata.network_address,
            network_tls: node.metadata.network_tls,
            preferences: node.preferences,
        };
        peer_set.peers.count = count as u64 + 1;
        count
    };

    bubble_up(peer_set, peers, peer_index);

    if node.latest_advance_epoch < curr {
        node.latest_advance_epoch = curr;
    }

    NodeJoinedCommittee {
        node: node_address,
        stake: stake.as_u64().to_le_bytes(),
        key: node.metadata.bls_pubkey,
        blacklist: member.blacklist,
        preferences: node.preferences,
        activation_epoch: next,
    }.log();

    Ok(())
}

fn try_join_committee(
    committee: &mut Committee,
    members: &mut [Member],
    member: &Member,
) -> Result<(), TapeError> {
    if member.stake == TAPE::zero() {
        return Err(TapeError::UnexpectedState);
    }

    let count = committee.members.count as usize;
    let capacity = committee.members.capacity as usize;

    if members[..count].iter().any(|m| m.node == member.node) {
        return Ok(());
    }

    if count < capacity {
        members[count] = *member;
        committee.members.count = (count as u64) + 1;
    } else {
        let (min_idx, min_stake) = members[..count]
            .iter()
            .enumerate()
            .min_by_key(|(_, m)| m.stake)
            .map(|(i, m)| (i, m.stake))
            .ok_or(TapeError::UnexpectedState)?;

        if member.stake <= min_stake {
            return Err(TapeError::UnexpectedState);
        }

        members[min_idx] = *member;
    }

    sort_committee_desc(committee, members);

    Ok(())
}

fn sort_committee_desc(committee: &Committee, members: &mut [Member]) {
    let n = committee.members.count as usize;
    if n <= 1 {
        return;
    }
    members[..n].sort_by(|a, b| {
        b.stake.cmp(&a.stake).then(a.node.as_bytes().cmp(b.node.as_bytes()))
    });
}

fn bubble_up(peer_set: &PeerSet, peers: &mut [Peer], i: usize) {
    let count = peer_set.peers.count;
    let bottom_threshold = (count * 2) / 3;
    if (i as u64) >= bottom_threshold && i != 0 {
        peers.swap(0, i);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tape_test::*;

    fn member(address_byte: u8, stake: u64) -> Member {
        let mut bytes = [0u8; 32];
        bytes[0] = address_byte;
        Member::new(Address::new(bytes), TAPE(stake))
    }

    fn epoch_in_phase(epoch: EpochNumber, phase: EpochPhase) -> Epoch {
        Epoch {
            id: epoch,
            state: EpochState {
                phase: phase as u64,
                ..EpochState::zeroed()
            },
            ..Epoch::zeroed()
        }
    }

    // happy-path enrolment — adds a Member and a Peer
    #[test]
    fn join() {
        let fee_payer = Pubkey::new_unique();
        let authority = Pubkey::new_unique();
        let committee_size: u64 = 128;
        let peer_capacity: u64 = committee_size * 3;

        let (node_address, _) = node_pda(authority.into());
        let (system_address, _) = system_pda();
        let (peer_set_address, _) = peer_set_pda();
        let curr = EpochNumber(42);
        let next = EpochNumber(43);
        let (curr_epoch_addr, _) = epoch_pda(curr);
        let (curr_committee_addr, _) = committee_pda(curr);
        let (next_committee_addr, _) = committee_pda(next);

        let instruction =
            build_join_committee_ix(fee_payer.into(), authority.into(), node_address, curr);

        let system = System {
            current_epoch: curr,
            committee_size,
            ..System::zeroed()
        };

        let curr_epoch = epoch_in_phase(curr, EpochPhase::Active);
        let curr_committee = Committee { epoch: curr, members: Tail::empty(committee_size) }
            .pack_with(&[]);
        let next_members = [member(3, 3_500), member(4, 2_100)];
        let next_committee =
            Committee { epoch: next, members: Tail::new(committee_size, next_members.len() as u64) }
                .pack_with(&next_members);
        let peer_set = PeerSet { peers: Tail::empty(peer_capacity) }
            .pack_with(&[]);

        let preferences = NodePreferences {
            storage_price: TAPE(10),
            storage_capacity: StorageUnits::mb(1_000_000),
            committee_size: system.committee_size,
            spool_groups: system.target_group_count,
            min_version: system.min_version,
        };
        let node = Node {
            authority: authority.into(),
            pool: StakingPool {
                stake: TAPE(1_000),
                shares: ShareAmount(1_000),
                ..StakingPool::zeroed()
            },
            preferences,
            ..Node::zeroed()
        };

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            sol(authority, 0),
            pda(system_address, system.pack(), tapedrive::ID),
            pda(curr_epoch_addr, curr_epoch.pack(), tapedrive::ID),
            pda(curr_committee_addr, curr_committee, tapedrive::ID),
            pda(next_committee_addr, next_committee, tapedrive::ID),
            pda(peer_set_address, peer_set, tapedrive::ID),
            pda(node_address, node.pack(), tapedrive::ID),
        ];

        let env = test_env();
        env.process_instruction(
            &instruction,
            &accounts,
            &[Check::success()],
        );
    }

    // zero-stake nodes cannot enrol
    #[test]
    fn zero_stake() {
        let fee_payer = Pubkey::new_unique();
        let authority = Pubkey::new_unique();
        let committee_size: u64 = 128;
        let peer_capacity: u64 = committee_size * 3;

        let (node_address, _) = node_pda(authority.into());
        let (system_address, _) = system_pda();
        let (peer_set_address, _) = peer_set_pda();
        let curr = EpochNumber(42);
        let next = EpochNumber(43);
        let (curr_epoch_addr, _) = epoch_pda(curr);
        let (curr_committee_addr, _) = committee_pda(curr);
        let (next_committee_addr, _) = committee_pda(next);

        let instruction =
            build_join_committee_ix(fee_payer.into(), authority.into(), node_address, curr);

        let system = System {
            current_epoch: curr,
            committee_size,
            ..System::zeroed()
        };

        let curr_epoch = epoch_in_phase(curr, EpochPhase::Active);
        let curr_committee = Committee { epoch: curr, members: Tail::empty(committee_size) }
            .pack_with(&[]);
        let next_committee = Committee { epoch: next, members: Tail::empty(committee_size) }
            .pack_with(&[]);
        let peer_set = PeerSet { peers: Tail::empty(peer_capacity) }
            .pack_with(&[]);

        let node = Node {
            authority: authority.into(),
            pool: StakingPool::zeroed(),
            ..Node::zeroed()
        };

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            sol(authority, 0),
            pda(system_address, system.pack(), tapedrive::ID),
            pda(curr_epoch_addr, curr_epoch.pack(), tapedrive::ID),
            pda(curr_committee_addr, curr_committee, tapedrive::ID),
            pda(next_committee_addr, next_committee, tapedrive::ID),
            pda(peer_set_address, peer_set, tapedrive::ID),
            pda(node_address, node.pack(), tapedrive::ID),
        ];

        let env = test_env();
        env.process_instruction(
            &instruction,
            &accounts,
            &[Check::err(TapeError::NotStaked.into())],
        );
    }

    // Closing phase blocks new joiners so next-epoch membership stays stable
    #[test]
    fn closing_blocks() {
        let fee_payer = Pubkey::new_unique();
        let authority = Pubkey::new_unique();
        let committee_size: u64 = 128;
        let peer_capacity: u64 = committee_size * 3;

        let (node_address, _) = node_pda(authority.into());
        let (system_address, _) = system_pda();
        let (peer_set_address, _) = peer_set_pda();
        let curr = EpochNumber(42);
        let next = EpochNumber(43);
        let (curr_epoch_addr, _) = epoch_pda(curr);
        let (curr_committee_addr, _) = committee_pda(curr);
        let (next_committee_addr, _) = committee_pda(next);

        let instruction =
            build_join_committee_ix(fee_payer.into(), authority.into(), node_address, curr);

        let system = System {
            current_epoch: curr,
            committee_size,
            ..System::zeroed()
        };

        let curr_epoch = epoch_in_phase(curr, EpochPhase::Closing);
        let curr_committee = Committee { epoch: curr, members: Tail::empty(committee_size) }
            .pack_with(&[]);
        let next_committee = Committee { epoch: next, members: Tail::empty(committee_size) }
            .pack_with(&[]);
        let peer_set = PeerSet { peers: Tail::empty(peer_capacity) }
            .pack_with(&[]);

        let node = Node {
            authority: authority.into(),
            pool: StakingPool {
                stake: TAPE(1_000),
                shares: ShareAmount(1_000),
                ..StakingPool::zeroed()
            },
            ..Node::zeroed()
        };

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            sol(authority, 0),
            pda(system_address, system.pack(), tapedrive::ID),
            pda(curr_epoch_addr, curr_epoch.pack(), tapedrive::ID),
            pda(curr_committee_addr, curr_committee, tapedrive::ID),
            pda(next_committee_addr, next_committee, tapedrive::ID),
            pda(peer_set_address, peer_set, tapedrive::ID),
            pda(node_address, node.pack(), tapedrive::ID),
        ];

        let env = test_env();
        env.process_instruction(
            &instruction,
            &accounts,
            &[Check::err(TapeError::BadEpochState.into())],
        );
    }

    // a seated node with a stale pool must advance before re-joining
    #[test]
    fn stale_seated() {
        let fee_payer = Pubkey::new_unique();
        let authority = Pubkey::new_unique();
        let committee_size: u64 = 128;
        let peer_capacity: u64 = committee_size * 3;

        let (node_address, _) = node_pda(authority.into());
        let (system_address, _) = system_pda();
        let (peer_set_address, _) = peer_set_pda();
        let curr = EpochNumber(10);
        let next = EpochNumber(11);
        let (curr_epoch_addr, _) = epoch_pda(curr);
        let (curr_committee_addr, _) = committee_pda(curr);
        let (next_committee_addr, _) = committee_pda(next);

        let instruction =
            build_join_committee_ix(fee_payer.into(), authority.into(), node_address, curr);

        let system = System {
            current_epoch: curr,
            committee_size,
            ..System::zeroed()
        };

        let curr_epoch = epoch_in_phase(curr, EpochPhase::Active);
        let curr_members = [
            Member {
                node: node_address,
                stake: TAPE(3_000),
                blacklist: StorageUnits::zero(),
                spools: 0,
            },
            member(6, 2_000),
        ];
        let curr_committee =
            Committee { epoch: curr, members: Tail::new(committee_size, curr_members.len() as u64) }
                .pack_with(&curr_members);
        let next_committee = Committee { epoch: next, members: Tail::empty(committee_size) }
            .pack_with(&[]);
        let peer_set = PeerSet { peers: Tail::empty(peer_capacity) }
            .pack_with(&[]);

        let node = Node {
            authority: authority.into(),
            pool: StakingPool {
                stake: TAPE(3_000),
                shares: ShareAmount(3_000),
                ..StakingPool::zeroed()
            },
            latest_advance_epoch: EpochNumber(7),
            ..Node::zeroed()
        };

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            sol(authority, 0),
            pda(system_address, system.pack(), tapedrive::ID),
            pda(curr_epoch_addr, curr_epoch.pack(), tapedrive::ID),
            pda(curr_committee_addr, curr_committee, tapedrive::ID),
            pda(next_committee_addr, next_committee, tapedrive::ID),
            pda(peer_set_address, peer_set, tapedrive::ID),
            pda(node_address, node.pack(), tapedrive::ID),
        ];

        let env = test_env();
        env.process_instruction(
            &instruction,
            &accounts,
            &[Check::err(TapeError::NodeStale.into())],
        );
    }
}
