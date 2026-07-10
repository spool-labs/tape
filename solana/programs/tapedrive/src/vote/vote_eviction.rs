use tape_api::event::{NodeEvicted, VoteRecorded};
use tape_api::program::prelude::*;
use tape_core::cert::NodeEvictMessage;
use tape_core::system::apply_member_remove_slice;
use tape_crypto::bls12254::min_sig::*;
use tape_crypto::Hash;

pub fn process_vote_eviction(accounts: &[AccountInfo<'_>], data: &[u8]) -> ProgramResult {
    let args = VoteEviction::try_from_bytes(data)?;
    let [
        fee_payer_info,
        system_info,
        voting_epoch_info,
        target_epoch_info,
        curr_group_info,
        vote_info,
        node_info,
        committee_info,
    ] = accounts else {
        return Err(ProgramError::NotEnoughAccountKeys);
    };

    fee_payer_info
        .is_signer()?
        .is_writable()?;

    let system = system_info
        .is_system()?
        .as_account::<System>(&tapedrive::ID)?;

    let voting_epoch_id = system.current_epoch;
    let target_epoch_id = voting_epoch_id.next();

    let voting_epoch = voting_epoch_info
        .is_epoch(voting_epoch_id)?
        .as_account::<Epoch>(&tapedrive::ID)?;

    // Same window as join: voting closes at Closing so the landing can still
    // mutate the next committee before assignment computation freezes it.
    if voting_epoch.state.phase >= EpochPhase::Closing as u64 {
        return Err(TapeError::BadEpochState.into());
    }

    let target_epoch = target_epoch_info
        .is_epoch(target_epoch_id)?
        .as_account::<Epoch>(&tapedrive::ID)?;

    let group_id = args.group;
    if group_id.0 >= voting_epoch.total_groups {
        return Err(TapeError::BadMember.into());
    }

    let curr_group = curr_group_info
        .is_group(voting_epoch_id, group_id)?
        .as_account::<Group>(&tapedrive::ID)?;

    let weight = args.bitmap.count_ones() as u64;
    if !is_supermajority(weight, GROUP_SIZE as u64) {
        return Err(TapeError::NoQuorum.into());
    }

    let indices = args.bitmap.indices();
    if indices.is_empty() {
        return Err(TapeError::NoSigners.into());
    }

    let mut pubkeys = Vec::with_capacity(indices.len());
    for spool_index in &indices {
        let spool = curr_group.spools.get(*spool_index).ok_or(TapeError::BadMember)?;
        pubkeys.push(spool.bls_pubkey.0);
    }

    let decompressed_sig = G1Point::try_from(&args.signature.0)
        .map_err(|_| TapeError::BadSignature)?;

    let message = NodeEvictMessage::new(target_epoch_id, target_epoch.nonce, args.node)
        .to_bytes();

    verify_aggregate(&message, &pubkeys, &decompressed_sig)
        .map_err(|_| TapeError::BadSignature)?;

    let node_hash = Hash(args.node.to_bytes());
    let (vote_address, _) = eviction_vote_pda(voting_epoch_id, target_epoch_id, args.node);
    vote_info
        .is_writable()?
        .has_address(&vote_address.into())?
        .is_type::<Vote>(&tapedrive::ID)?;

    let (vote, bitmap) = Vote::read_mut(vote_info, &tapedrive::ID)?;
    if vote.kind != VoteKind::Eviction as u64
        || vote.hash != node_hash
        || vote.voting_epoch != voting_epoch_id
        || vote.target_epoch != target_epoch_id
    {
        return Err(TapeError::UnexpectedState.into());
    }

    let bits = usize::try_from(voting_epoch.total_groups)
        .map_err(|_| ProgramError::InvalidInstructionData)?;

    if bitmap.len() < bytes_for_members(bits) {
        return Err(TapeError::UnexpectedState.into());
    }

    let group_index = usize::try_from(group_id.0)
        .map_err(|_| ProgramError::InvalidInstructionData)?;

    let mut bitmap = BitmapMut::new(bitmap, bits);
    if bitmap.is_set(group_index) {
        return Err(TapeError::AlreadySigned.into());
    }

    bitmap.set(group_index);
    let signed_groups = bitmap.count_ones() as u64;
    let landed = signed_groups == voting_epoch.total_groups;

    if landed {
        // Suspend the node from the target committee for the epoch. This is set
        // even when the node is not yet seated, so a pre-emptive eviction still
        // blocks a later join.
        let node = node_info
            .is_writable()?
            .has_address(&args.node.into())?
            .as_account_mut::<Node>(&tapedrive::ID)?;
        node.suspended_until = target_epoch_id;

        // Remove the member from the next committee if it is seated. A missing
        // member is not an error (pre-emptive eviction). The peer set is left
        // untouched so next-epoch owners can still sync the node's spools.
        committee_info
            .is_writable()?
            .is_committee(target_epoch_id)?;
        let (committee, members) = Committee::read_full_mut(committee_info, &tapedrive::ID)?;
        apply_member_remove_slice(members, &mut committee.members.count, args.node)
            .map_err(|_| TapeError::UnexpectedState)?;

        NodeEvicted {
            node: args.node,
            target_epoch: target_epoch_id,
        }
        .log();
    }

    VoteRecorded {
        kind: VoteKind::Eviction as u64,
        vote: vote_address,
        voting_epoch: voting_epoch_id,
        target_epoch: target_epoch_id,
        hash: node_hash,
        group: group_id,
        signer_count: weight,
        signed_groups,
        total_groups: voting_epoch.total_groups,
        bitmap: args.bitmap,
    }
    .log();

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tape_core::system::Spool;
    use tape_test::*;

    fn make_group(epoch: EpochNumber, group_id: GroupIndex) -> (Vec<BlsPrivateKey>, Group) {
        let mut group = Group::zeroed();
        group.epoch = epoch;
        group.id = group_id;
        group.size = StorageUnits::mb(50);

        let mut sks = Vec::with_capacity(GROUP_SIZE);
        for i in 0..GROUP_SIZE {
            let sk = BlsPrivateKey::from_random();
            let pk = sk.public_key().expect("pubkey");
            let mut bytes = [0u8; 32];
            bytes[0] = (i as u8) + 1;
            let addr = Address::new(bytes);

            group.spools[i] = Spool {
                node: addr,
                bls_pubkey: pk,
            };
            sks.push(sk);
        }

        (sks, group)
    }

    #[test]
    fn vote_eviction_lands() {
        let fee_payer = Pubkey::new_unique();
        let voting_epoch_id = EpochNumber(12);
        let target_epoch_id = EpochNumber(13);
        let nonce = Hash::new_unique();
        let group_id = GroupIndex(0);
        let total_groups = 1;
        let bitmap_len = bytes_for_members(total_groups as usize);

        // Target node seated in the next committee.
        let node = Address::new([0xEE; 32]);
        let node_hash = Hash(node.to_bytes());

        let (system_address, _) = system_pda();
        let (voting_epoch_address, _) = epoch_pda(voting_epoch_id);
        let (target_epoch_address, _) = epoch_pda(target_epoch_id);
        let (group_address, _) = group_pda(voting_epoch_id, group_id);
        let (vote_address, _) = eviction_vote_pda(voting_epoch_id, target_epoch_id, node);
        let (committee_address, _) = committee_pda(target_epoch_id);

        let (sks, group) = make_group(voting_epoch_id, group_id);

        let system = System {
            current_epoch: voting_epoch_id,
            ..System::zeroed()
        };

        // Phase defaults to 0 (< Closing), the eviction voting window.
        let voting_epoch = Epoch {
            id: voting_epoch_id,
            total_groups,
            ..Epoch::zeroed()
        };

        let target_epoch = Epoch {
            id: target_epoch_id,
            nonce,
            ..Epoch::zeroed()
        };

        let vote = Vote {
            kind: VoteKind::Eviction as u64,
            hash: node_hash,
            voting_epoch: voting_epoch_id,
            target_epoch: target_epoch_id,
            registered_by: fee_payer,
            bitmap: Tail::new(bitmap_len as u64, bitmap_len as u64),
        };

        let member = Member {
            node,
            stake: TAPE(0),
            assigned: StorageUnits::zero(),
            blacklisted: StorageUnits::zero(),
            spools: 0,
        };
        let members = vec![member];
        let committee = Committee {
            epoch: target_epoch_id,
            members: Tail::new(1, members.len() as u64),
        };

        let node_account = Node {
            authority: node,
            ..Node::zeroed()
        };

        const SIGNERS: usize = 14;
        let signed_indices: Vec<usize> = (0..SIGNERS).collect();
        let bitmap = SpoolBitmap::from_indices(&signed_indices);
        let message = NodeEvictMessage::new(target_epoch_id, nonce, node).to_bytes();
        let partials: Vec<BlsSignature> = signed_indices
            .iter()
            .map(|&i| sks[i].sign(&message).unwrap())
            .collect();
        let agg_sig = BlsSignature::aggregate(&partials).unwrap();

        let instruction = build_vote_eviction_ix(
            fee_payer.into(),
            voting_epoch_id,
            node,
            group_id,
            bitmap,
            agg_sig,
        );

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            pda(system_address, system.pack(), tapedrive::ID),
            pda(voting_epoch_address, voting_epoch.pack(), tapedrive::ID),
            pda(target_epoch_address, target_epoch.pack(), tapedrive::ID),
            pda(group_address, group.pack(), tapedrive::ID),
            pda(vote_address, vote.pack_with(&vec![0u8; bitmap_len]), tapedrive::ID),
            pda(node, node_account.pack(), tapedrive::ID),
            pda(committee_address, committee.pack_with(&members), tapedrive::ID),
        ];

        // Landing: group bit set, node suspended, member removed (count 1 -> 0).
        let expected_vote = vote.pack_with(&[1u8]);
        let expected_node = Node {
            authority: node,
            suspended_until: target_epoch_id,
            ..Node::zeroed()
        };
        let expected_committee = Committee {
            epoch: target_epoch_id,
            members: Tail::new(1, 0),
        };

        let env = test_env();
        env.process_instruction(
            &instruction,
            &accounts,
            &[
                Check::success(),
                Check::account(&Pubkey::from(node))
                    .data(expected_node.pack().as_ref())
                    .build(),
                Check::account(&Pubkey::from(vote_address))
                    .data(expected_vote.as_ref())
                    .build(),
                Check::account(&Pubkey::from(committee_address))
                    .data(expected_committee.pack_with(&members).as_ref())
                    .build(),
            ],
        );
    }

    #[test]
    fn vote_eviction_preemptive() {
        // Evict a node that is not seated in the next committee: the removal is a
        // no-op but the suspension still lands, blocking a later join.
        let fee_payer = Pubkey::new_unique();
        let voting_epoch_id = EpochNumber(12);
        let target_epoch_id = EpochNumber(13);
        let nonce = Hash::new_unique();
        let group_id = GroupIndex(0);
        let total_groups = 1;
        let bitmap_len = bytes_for_members(total_groups as usize);

        let node = Address::new([0xEE; 32]);
        let node_hash = Hash(node.to_bytes());
        let other = Address::new([0x11; 32]);

        let (system_address, _) = system_pda();
        let (voting_epoch_address, _) = epoch_pda(voting_epoch_id);
        let (target_epoch_address, _) = epoch_pda(target_epoch_id);
        let (group_address, _) = group_pda(voting_epoch_id, group_id);
        let (vote_address, _) = eviction_vote_pda(voting_epoch_id, target_epoch_id, node);
        let (committee_address, _) = committee_pda(target_epoch_id);

        let (sks, group) = make_group(voting_epoch_id, group_id);

        let system = System {
            current_epoch: voting_epoch_id,
            ..System::zeroed()
        };
        let voting_epoch = Epoch {
            id: voting_epoch_id,
            total_groups,
            ..Epoch::zeroed()
        };
        let target_epoch = Epoch {
            id: target_epoch_id,
            nonce,
            ..Epoch::zeroed()
        };
        let vote = Vote {
            kind: VoteKind::Eviction as u64,
            hash: node_hash,
            voting_epoch: voting_epoch_id,
            target_epoch: target_epoch_id,
            registered_by: fee_payer,
            bitmap: Tail::new(bitmap_len as u64, bitmap_len as u64),
        };

        // Committee seats a different node, so the target is not present.
        let seated = Member {
            node: other,
            stake: TAPE(0),
            assigned: StorageUnits::zero(),
            blacklisted: StorageUnits::zero(),
            spools: 0,
        };
        let members = vec![seated];
        let committee = Committee {
            epoch: target_epoch_id,
            members: Tail::new(1, members.len() as u64),
        };
        let node_account = Node {
            authority: node,
            ..Node::zeroed()
        };

        const SIGNERS: usize = 14;
        let signed_indices: Vec<usize> = (0..SIGNERS).collect();
        let bitmap = SpoolBitmap::from_indices(&signed_indices);
        let message = NodeEvictMessage::new(target_epoch_id, nonce, node).to_bytes();
        let partials: Vec<BlsSignature> = signed_indices
            .iter()
            .map(|&i| sks[i].sign(&message).unwrap())
            .collect();
        let agg_sig = BlsSignature::aggregate(&partials).unwrap();

        let instruction = build_vote_eviction_ix(
            fee_payer.into(),
            voting_epoch_id,
            node,
            group_id,
            bitmap,
            agg_sig,
        );

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            pda(system_address, system.pack(), tapedrive::ID),
            pda(voting_epoch_address, voting_epoch.pack(), tapedrive::ID),
            pda(target_epoch_address, target_epoch.pack(), tapedrive::ID),
            pda(group_address, group.pack(), tapedrive::ID),
            pda(vote_address, vote.pack_with(&vec![0u8; bitmap_len]), tapedrive::ID),
            pda(node, node_account.pack(), tapedrive::ID),
            pda(committee_address, committee.pack_with(&members), tapedrive::ID),
        ];

        // Suspension lands; the committee is untouched (target was never seated).
        let expected_node = Node {
            authority: node,
            suspended_until: target_epoch_id,
            ..Node::zeroed()
        };

        let env = test_env();
        env.process_instruction(
            &instruction,
            &accounts,
            &[
                Check::success(),
                Check::account(&Pubkey::from(node))
                    .data(expected_node.pack().as_ref())
                    .build(),
                Check::account(&Pubkey::from(committee_address))
                    .data(committee.pack_with(&members).as_ref())
                    .build(),
            ],
        );
    }

    // Voting is closed once the epoch reaches Closing, mirroring the join window.
    #[test]
    fn vote_eviction_wrong_phase() {
        let fee_payer = Pubkey::new_unique();
        let voting_epoch_id = EpochNumber(12);
        let target_epoch_id = EpochNumber(13);
        let nonce = Hash::new_unique();
        let group_id = GroupIndex(0);
        let total_groups = 1;
        let bitmap_len = bytes_for_members(total_groups as usize);

        let node = Address::new([0xEE; 32]);
        let node_hash = Hash(node.to_bytes());

        let (system_address, _) = system_pda();
        let (voting_epoch_address, _) = epoch_pda(voting_epoch_id);
        let (target_epoch_address, _) = epoch_pda(target_epoch_id);
        let (group_address, _) = group_pda(voting_epoch_id, group_id);
        let (vote_address, _) = eviction_vote_pda(voting_epoch_id, target_epoch_id, node);
        let (committee_address, _) = committee_pda(target_epoch_id);

        let (sks, group) = make_group(voting_epoch_id, group_id);

        let system = System {
            current_epoch: voting_epoch_id,
            ..System::zeroed()
        };

        // Closing (>= Closing) is past the eviction voting window.
        let voting_epoch = Epoch {
            id: voting_epoch_id,
            total_groups,
            state: EpochState {
                phase: EpochPhase::Closing as u64,
                ..EpochState::zeroed()
            },
            ..Epoch::zeroed()
        };

        let target_epoch = Epoch {
            id: target_epoch_id,
            nonce,
            ..Epoch::zeroed()
        };

        let vote = Vote {
            kind: VoteKind::Eviction as u64,
            hash: node_hash,
            voting_epoch: voting_epoch_id,
            target_epoch: target_epoch_id,
            registered_by: fee_payer,
            bitmap: Tail::new(bitmap_len as u64, bitmap_len as u64),
        };

        let member = Member {
            node,
            stake: TAPE(0),
            assigned: StorageUnits::zero(),
            blacklisted: StorageUnits::zero(),
            spools: 0,
        };
        let members = vec![member];
        let committee = Committee {
            epoch: target_epoch_id,
            members: Tail::new(1, members.len() as u64),
        };

        let node_account = Node {
            authority: node,
            ..Node::zeroed()
        };

        const SIGNERS: usize = 14;
        let signed_indices: Vec<usize> = (0..SIGNERS).collect();
        let bitmap = SpoolBitmap::from_indices(&signed_indices);
        let message = NodeEvictMessage::new(target_epoch_id, nonce, node).to_bytes();
        let partials: Vec<BlsSignature> = signed_indices
            .iter()
            .map(|&i| sks[i].sign(&message).unwrap())
            .collect();
        let agg_sig = BlsSignature::aggregate(&partials).unwrap();

        let instruction = build_vote_eviction_ix(
            fee_payer.into(),
            voting_epoch_id,
            node,
            group_id,
            bitmap,
            agg_sig,
        );

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            pda(system_address, system.pack(), tapedrive::ID),
            pda(voting_epoch_address, voting_epoch.pack(), tapedrive::ID),
            pda(target_epoch_address, target_epoch.pack(), tapedrive::ID),
            pda(group_address, group.pack(), tapedrive::ID),
            pda(vote_address, vote.pack_with(&vec![0u8; bitmap_len]), tapedrive::ID),
            pda(node, node_account.pack(), tapedrive::ID),
            pda(committee_address, committee.pack_with(&members), tapedrive::ID),
        ];

        let env = test_env();
        env.process_instruction(
            &instruction,
            &accounts,
            &[Check::err(TapeError::BadEpochState.into())],
        );
    }

    // A valid aggregate over the wrong message must not land the eviction.
    #[test]
    fn vote_eviction_bad_signature() {
        let fee_payer = Pubkey::new_unique();
        let voting_epoch_id = EpochNumber(12);
        let target_epoch_id = EpochNumber(13);
        let nonce = Hash::new_unique();
        let group_id = GroupIndex(0);
        let total_groups = 1;
        let bitmap_len = bytes_for_members(total_groups as usize);

        let node = Address::new([0xEE; 32]);
        let node_hash = Hash(node.to_bytes());

        let (system_address, _) = system_pda();
        let (voting_epoch_address, _) = epoch_pda(voting_epoch_id);
        let (target_epoch_address, _) = epoch_pda(target_epoch_id);
        let (group_address, _) = group_pda(voting_epoch_id, group_id);
        let (vote_address, _) = eviction_vote_pda(voting_epoch_id, target_epoch_id, node);
        let (committee_address, _) = committee_pda(target_epoch_id);

        let (sks, group) = make_group(voting_epoch_id, group_id);

        let system = System {
            current_epoch: voting_epoch_id,
            ..System::zeroed()
        };
        let voting_epoch = Epoch {
            id: voting_epoch_id,
            total_groups,
            ..Epoch::zeroed()
        };
        let target_epoch = Epoch {
            id: target_epoch_id,
            nonce,
            ..Epoch::zeroed()
        };
        let vote = Vote {
            kind: VoteKind::Eviction as u64,
            hash: node_hash,
            voting_epoch: voting_epoch_id,
            target_epoch: target_epoch_id,
            registered_by: fee_payer,
            bitmap: Tail::new(bitmap_len as u64, bitmap_len as u64),
        };

        let member = Member {
            node,
            stake: TAPE(0),
            assigned: StorageUnits::zero(),
            blacklisted: StorageUnits::zero(),
            spools: 0,
        };
        let members = vec![member];
        let committee = Committee {
            epoch: target_epoch_id,
            members: Tail::new(1, members.len() as u64),
        };
        let node_account = Node {
            authority: node,
            ..Node::zeroed()
        };

        // Sign over a different nonce so the aggregate is well-formed but fails
        // verification against the message the program reconstructs.
        let wrong_nonce = Hash::new_unique();
        const SIGNERS: usize = 14;
        let signed_indices: Vec<usize> = (0..SIGNERS).collect();
        let bitmap = SpoolBitmap::from_indices(&signed_indices);
        let wrong_message = NodeEvictMessage::new(target_epoch_id, wrong_nonce, node).to_bytes();
        let partials: Vec<BlsSignature> = signed_indices
            .iter()
            .map(|&i| sks[i].sign(&wrong_message).unwrap())
            .collect();
        let agg_sig = BlsSignature::aggregate(&partials).unwrap();

        let instruction = build_vote_eviction_ix(
            fee_payer.into(),
            voting_epoch_id,
            node,
            group_id,
            bitmap,
            agg_sig,
        );

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            pda(system_address, system.pack(), tapedrive::ID),
            pda(voting_epoch_address, voting_epoch.pack(), tapedrive::ID),
            pda(target_epoch_address, target_epoch.pack(), tapedrive::ID),
            pda(group_address, group.pack(), tapedrive::ID),
            pda(vote_address, vote.pack_with(&vec![0u8; bitmap_len]), tapedrive::ID),
            pda(node, node_account.pack(), tapedrive::ID),
            pda(committee_address, committee.pack_with(&members), tapedrive::ID),
        ];

        let env = test_env();
        env.process_instruction(
            &instruction,
            &accounts,
            &[Check::err(TapeError::BadSignature.into())],
        );
    }

    // A group that already signed cannot sign the same eviction again.
    #[test]
    fn vote_eviction_replay() {
        let fee_payer = Pubkey::new_unique();
        let voting_epoch_id = EpochNumber(12);
        let target_epoch_id = EpochNumber(13);
        let nonce = Hash::new_unique();
        let group_id = GroupIndex(0);
        let total_groups = 1;
        let bitmap_len = bytes_for_members(total_groups as usize);

        let node = Address::new([0xEE; 32]);
        let node_hash = Hash(node.to_bytes());

        let (system_address, _) = system_pda();
        let (voting_epoch_address, _) = epoch_pda(voting_epoch_id);
        let (target_epoch_address, _) = epoch_pda(target_epoch_id);
        let (group_address, _) = group_pda(voting_epoch_id, group_id);
        let (vote_address, _) = eviction_vote_pda(voting_epoch_id, target_epoch_id, node);
        let (committee_address, _) = committee_pda(target_epoch_id);

        let (sks, group) = make_group(voting_epoch_id, group_id);

        let system = System {
            current_epoch: voting_epoch_id,
            ..System::zeroed()
        };
        let voting_epoch = Epoch {
            id: voting_epoch_id,
            total_groups,
            ..Epoch::zeroed()
        };
        let target_epoch = Epoch {
            id: target_epoch_id,
            nonce,
            ..Epoch::zeroed()
        };
        let vote = Vote {
            kind: VoteKind::Eviction as u64,
            hash: node_hash,
            voting_epoch: voting_epoch_id,
            target_epoch: target_epoch_id,
            registered_by: fee_payer,
            bitmap: Tail::new(bitmap_len as u64, bitmap_len as u64),
        };

        let member = Member {
            node,
            stake: TAPE(0),
            assigned: StorageUnits::zero(),
            blacklisted: StorageUnits::zero(),
            spools: 0,
        };
        let members = vec![member];
        let committee = Committee {
            epoch: target_epoch_id,
            members: Tail::new(1, members.len() as u64),
        };
        let node_account = Node {
            authority: node,
            ..Node::zeroed()
        };

        const SIGNERS: usize = 14;
        let signed_indices: Vec<usize> = (0..SIGNERS).collect();
        let bitmap = SpoolBitmap::from_indices(&signed_indices);
        let message = NodeEvictMessage::new(target_epoch_id, nonce, node).to_bytes();
        let partials: Vec<BlsSignature> = signed_indices
            .iter()
            .map(|&i| sks[i].sign(&message).unwrap())
            .collect();
        let agg_sig = BlsSignature::aggregate(&partials).unwrap();

        let instruction = build_vote_eviction_ix(
            fee_payer.into(),
            voting_epoch_id,
            node,
            group_id,
            bitmap,
            agg_sig,
        );

        // Group 0's bit is already set in the vote bitmap: a replay.
        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            pda(system_address, system.pack(), tapedrive::ID),
            pda(voting_epoch_address, voting_epoch.pack(), tapedrive::ID),
            pda(target_epoch_address, target_epoch.pack(), tapedrive::ID),
            pda(group_address, group.pack(), tapedrive::ID),
            pda(vote_address, vote.pack_with(&[1u8]), tapedrive::ID),
            pda(node, node_account.pack(), tapedrive::ID),
            pda(committee_address, committee.pack_with(&members), tapedrive::ID),
        ];

        let env = test_env();
        env.process_instruction(
            &instruction,
            &accounts,
            &[Check::err(TapeError::AlreadySigned.into())],
        );
    }

    // 13/20 falls short of the supermajority and must be rejected.
    #[test]
    fn vote_eviction_below_quorum() {
        let fee_payer = Pubkey::new_unique();
        let voting_epoch_id = EpochNumber(12);
        let target_epoch_id = EpochNumber(13);
        let nonce = Hash::new_unique();
        let group_id = GroupIndex(0);
        let total_groups = 1;
        let bitmap_len = bytes_for_members(total_groups as usize);

        let node = Address::new([0xEE; 32]);
        let node_hash = Hash(node.to_bytes());

        let (system_address, _) = system_pda();
        let (voting_epoch_address, _) = epoch_pda(voting_epoch_id);
        let (target_epoch_address, _) = epoch_pda(target_epoch_id);
        let (group_address, _) = group_pda(voting_epoch_id, group_id);
        let (vote_address, _) = eviction_vote_pda(voting_epoch_id, target_epoch_id, node);
        let (committee_address, _) = committee_pda(target_epoch_id);

        let (sks, group) = make_group(voting_epoch_id, group_id);

        let system = System {
            current_epoch: voting_epoch_id,
            ..System::zeroed()
        };
        let voting_epoch = Epoch {
            id: voting_epoch_id,
            total_groups,
            ..Epoch::zeroed()
        };
        let target_epoch = Epoch {
            id: target_epoch_id,
            nonce,
            ..Epoch::zeroed()
        };
        let vote = Vote {
            kind: VoteKind::Eviction as u64,
            hash: node_hash,
            voting_epoch: voting_epoch_id,
            target_epoch: target_epoch_id,
            registered_by: fee_payer,
            bitmap: Tail::new(bitmap_len as u64, bitmap_len as u64),
        };

        let member = Member {
            node,
            stake: TAPE(0),
            assigned: StorageUnits::zero(),
            blacklisted: StorageUnits::zero(),
            spools: 0,
        };
        let members = vec![member];
        let committee = Committee {
            epoch: target_epoch_id,
            members: Tail::new(1, members.len() as u64),
        };
        let node_account = Node {
            authority: node,
            ..Node::zeroed()
        };

        // 13 signers: is_supermajority(13, 20) is false.
        const SIGNERS: usize = 13;
        let signed_indices: Vec<usize> = (0..SIGNERS).collect();
        let bitmap = SpoolBitmap::from_indices(&signed_indices);
        let message = NodeEvictMessage::new(target_epoch_id, nonce, node).to_bytes();
        let partials: Vec<BlsSignature> = signed_indices
            .iter()
            .map(|&i| sks[i].sign(&message).unwrap())
            .collect();
        let agg_sig = BlsSignature::aggregate(&partials).unwrap();

        let instruction = build_vote_eviction_ix(
            fee_payer.into(),
            voting_epoch_id,
            node,
            group_id,
            bitmap,
            agg_sig,
        );

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            pda(system_address, system.pack(), tapedrive::ID),
            pda(voting_epoch_address, voting_epoch.pack(), tapedrive::ID),
            pda(target_epoch_address, target_epoch.pack(), tapedrive::ID),
            pda(group_address, group.pack(), tapedrive::ID),
            pda(vote_address, vote.pack_with(&vec![0u8; bitmap_len]), tapedrive::ID),
            pda(node, node_account.pack(), tapedrive::ID),
            pda(committee_address, committee.pack_with(&members), tapedrive::ID),
        ];

        let env = test_env();
        env.process_instruction(
            &instruction,
            &accounts,
            &[Check::err(TapeError::NoQuorum.into())],
        );
    }
}

