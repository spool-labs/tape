use tape_api::event::AssignmentGroupFinalized;
use tape_api::program::prelude::*;
use tape_core::cert::verify_assignment_group_payload;

pub fn process_finalize_group(accounts: &[AccountInfo<'_>], data: &[u8]) -> ProgramResult {
    let args = FinalizeGroup::try_from_bytes(data)?;
    let [
        fee_payer_info,
        system_info,
        target_epoch_info,
        group_info,
        committee_info,
        peer_set_info,
        system_program_info,
    ] = accounts else {
        return Err(ProgramError::NotEnoughAccountKeys);
    };

    fee_payer_info
        .is_signer()?
        .is_writable()?;

    system_program_info
        .is_program(&system_program::ID)?;

    let system = system_info
        .is_system()?
        .as_account::<System>(&tapedrive::ID)?;

    let target_epoch_id = EpochNumber::unpack(args.epoch);
    let expected_target = system.current_epoch.saturating_add(EpochNumber(1));

    if target_epoch_id != expected_target {
        return Err(TapeError::BadEpochId.into());
    }

    let target_epoch = target_epoch_info
        .is_writable()?
        .is_epoch(target_epoch_id)?
        .as_account_mut::<Epoch>(&tapedrive::ID)?;

    if !target_epoch.has_assignment_hash() {
        return Err(TapeError::UnexpectedState.into());
    }

    let group_id = args.payload.group();
    if group_id.0 >= system.target_group_count {
        return Err(TapeError::BadMember.into());
    }

    if target_epoch.total_groups >= system.target_group_count {
        return Err(TapeError::UnexpectedState.into());
    }

    if !verify_assignment_group_payload(
        &target_epoch.assignment_hash,
        &args.payload,
        &args.proof,
    ) {
        return Err(TapeError::BadProof.into());
    }

    require_unique(&args.payload.peer_indices)?;

    peer_set_info.is_peer_set()?;
    let (peer_set, peers) = PeerSet::read(peer_set_info, &tapedrive::ID)?;

    committee_info
        .is_writable()?
        .is_committee(target_epoch_id)?;

    let (committee, members) = Committee::read_mut(committee_info, &tapedrive::ID)?;
    if committee.epoch != target_epoch_id {
        return Err(TapeError::BadEpochId.into());
    }

    let (group_address, bump) = group_pda(target_epoch_id, group_id);

    group_info
        .is_empty()?
        .is_writable()?
        .has_address(&group_address.into())?;

    create_program_account_with_bump::<Group>(
        group_info,
        system_program_info,
        fee_payer_info,
        &tapedrive::ID,
        &[GROUP, &target_epoch_id.pack(), &group_id.pack()],
        bump,
    )?;

    let group = group_info
        .as_account_mut::<Group>(&tapedrive::ID)?;

    group.id = group_id;
    group.epoch = target_epoch_id;
    group.size = args.payload.size;

    for i in 0..GROUP_SIZE {
        let peer_index = usize::try_from(args.payload.peer_indices[i])
            .map_err(|_| TapeError::BadMember)?;

        if peer_index >= peer_set.peers.count as usize {
            return Err(TapeError::BadMember.into());
        }

        let peer = peers.get(peer_index).ok_or(TapeError::BadMember)?;
        group.spools[i] = Spool {
            node: peer.node,
            bls_pubkey: peer.bls_pubkey,
        };

        let member = members
            .iter_mut()
            .find(|m| m.node == peer.node)
            .ok_or(TapeError::BadMember)?;

        member.spools = member.spools.saturating_add(1);
        member.assigned = member
            .assigned
            .checked_add(args.payload.size)
            .ok_or(TapeError::RewardsOverflow)?;
    }

    target_epoch.total_groups = target_epoch.total_groups
        .saturating_add(1);

    let group_total = args.payload.size.0.saturating_mul(GROUP_SIZE as u64);

    target_epoch.total_assigned =
        StorageUnits(target_epoch.total_assigned.0.saturating_add(group_total));

    AssignmentGroupFinalized {
        epoch: target_epoch_id,
        hash: target_epoch.assignment_hash,
        group: group_id,
        group_account: group_address,
        size: args.payload.size,
        total_groups: target_epoch.total_groups.to_le_bytes(),
        total_assigned: target_epoch.total_assigned,
    }
    .log();

    Ok(())
}

fn require_unique(peer_indices: &[u64; GROUP_SIZE]) -> ProgramResult {
    for i in 0..GROUP_SIZE {
        for j in (i + 1)..GROUP_SIZE {
            if peer_indices[i] == peer_indices[j] {
                return Err(TapeError::BadMember.into());
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tape_core::system::Peer;
    use tape_crypto::merkle::{
        create_proof_from_leaf_hashes, root_from_leaf_hashes,
    };
    use tape_test::*;

    #[test]
    fn finalize_group() {
        let fee_payer = Pubkey::new_unique();
        let current_epoch_id = EpochNumber(12);
        let target_epoch_id = EpochNumber(13);
        let group_id = GroupIndex(0);
        let size = StorageUnits::gb(2);
        let peer_indices = core::array::from_fn(|i| i as u64);
        let payload =
            AssignmentGroupPayload::new(group_id, peer_indices, size);
        let leaf_hash = payload.hash();
        let assignment_hash =
            root_from_leaf_hashes::<ASSIGNMENT_TREE_HEIGHT>(&[leaf_hash]);
        let proof: [Hash; ASSIGNMENT_TREE_HEIGHT] =
            create_proof_from_leaf_hashes::<ASSIGNMENT_TREE_HEIGHT>(
                &[leaf_hash],
                group_id.0 as usize,
            )
            .expect("assignment proof")
            .try_into()
            .expect("proof length");

        let (system_address, _) = system_pda();
        let (target_epoch_address, _) = epoch_pda(target_epoch_id);
        let (group_address, _) = group_pda(target_epoch_id, group_id);
        let (committee_address, _) = committee_pda(target_epoch_id);
        let (peer_set_address, _) = peer_set_pda();

        let system = System {
            current_epoch: current_epoch_id,
            target_group_count: 1,
            ..System::zeroed()
        };

        let target_epoch = Epoch {
            id: target_epoch_id,
            assignment_hash,
            ..Epoch::zeroed()
        };

        let mut members = Vec::with_capacity(GROUP_SIZE);
        let mut peers = Vec::with_capacity(GROUP_SIZE);
        for i in 0..GROUP_SIZE {
            let sk = BlsPrivateKey::from_random();
            let pk = sk.public_key().expect("pubkey");
            let mut bytes = [0u8; 32];
            bytes[0] = (i as u8) + 1;
            let addr = Address::new(bytes);

            members.push(Member {
                node: addr,
                stake: TAPE(0),
                assigned: StorageUnits::zero(),
                refused: StorageUnits::zero(),
                spools: 0,
            });
            peers.push(Peer {
                node: addr,
                bls_pubkey: pk,
                ..Peer::zeroed()
            });
        }

        let committee = Committee {
            epoch: target_epoch_id,
            members: Tail::new(GROUP_SIZE as u64, members.len() as u64),
        };
        let peer_set = PeerSet {
            peers: Tail::new(GROUP_SIZE as u64, peers.len() as u64),
        };

        let instruction =
            build_finalize_group_ix(fee_payer.into(), target_epoch_id, payload, proof);

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            pda(system_address, system.pack(), tapedrive::ID),
            pda(target_epoch_address, target_epoch.pack(), tapedrive::ID),
            empty(group_address),
            pda(committee_address, committee.pack_with(&members), tapedrive::ID),
            pda(peer_set_address, peer_set.pack_with(&peers), tapedrive::ID),
            system_program(),
        ];

        let mut expected_group = Group {
            id: group_id,
            epoch: target_epoch_id,
            size,
            ..Group::zeroed()
        };
        for i in 0..GROUP_SIZE {
            expected_group.spools[i] = Spool {
                node: peers[i].node,
                bls_pubkey: peers[i].bls_pubkey,
            };
        }

        let mut expected_members = members.clone();
        for member in &mut expected_members {
            member.spools = 1;
            member.assigned = size;
        }

        let expected_target_epoch = Epoch {
            total_groups: 1,
            total_assigned: StorageUnits(size.0 * GROUP_SIZE as u64),
            ..target_epoch
        };

        let env = test_env();
        env.process_instruction(
            &instruction,
            &accounts,
            &[
                Check::success(),
                Check::account(&Pubkey::from(target_epoch_address))
                    .data(expected_target_epoch.pack().as_ref())
                    .build(),
                Check::account(&Pubkey::from(group_address))
                    .owner(&tapedrive::ID)
                    .data(expected_group.pack().as_ref())
                    .build(),
                Check::account(&Pubkey::from(committee_address))
                    .data(
                        Committee {
                            epoch: target_epoch_id,
                            members: Tail::new(
                                GROUP_SIZE as u64,
                                expected_members.len() as u64,
                            ),
                        }
                        .pack_with(&expected_members)
                        .as_ref(),
                    )
                    .build(),
            ],
        );
    }
}
