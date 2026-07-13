use tape_api::event::{NodeEvicted, VoteRecorded};
use tape_api::program::prelude::*;
use tape_core::cert::{NodeEvictMessage, eviction_vote_hash};
use tape_core::system::apply_member_remove_slice;
use tape_crypto::bls12254::min_sig::*;

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

    let node_hash = eviction_vote_hash(args.node);
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

    // Land on a supermajority of groups rather than all of them, so a second
    // unhealthy node cannot stall the vote by breaking quorum in its own
    // group. The landing effects run only on the vote that crosses the
    // threshold; later votes just accumulate.
    let landed = is_supermajority(signed_groups, voting_epoch.total_groups)
        && !is_supermajority(signed_groups - 1, voting_epoch.total_groups);

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
    use tape_crypto::Hash;
    use tape_test::*;

    const VOTING_EPOCH: EpochNumber = EpochNumber(12);
    const TARGET_EPOCH: EpochNumber = EpochNumber(13);
    const SUPERMAJORITY: usize = 14;

    // Single-group eviction vote scenario. The defaults land the vote with the
    // target seated in the next committee; each knob isolates one rejection.
    struct Fixture {
        fee_payer: Pubkey,
        node: Address,
        nonce: Hash,
        sks: Vec<BlsPrivateKey>,
        group: Group,
        group_id: GroupIndex,
        total_groups: u64,
        phase: u64,
        signers: usize,
        sign_nonce: Hash,
        vote_bitmap: Vec<u8>,
        seated: Address,
    }

    impl Fixture {
        fn new() -> Self {
            Self::with_groups(GroupIndex(0), 1)
        }

        // The signing group casts its vote among total_groups groups.
        fn with_groups(group_id: GroupIndex, total_groups: u64) -> Self {
            let nonce = Hash::new_unique();
            let node = Address::new([0xEE; 32]);
            let (sks, group) = make_group(VOTING_EPOCH, group_id);

            Self {
                fee_payer: Pubkey::new_unique(),
                node,
                nonce,
                sks,
                group,
                group_id,
                total_groups,
                phase: 0,
                signers: SUPERMAJORITY,
                sign_nonce: nonce,
                vote_bitmap: vec![0u8; bytes_for_members(total_groups as usize)],
                seated: node,
            }
        }

        fn vote_address(&self) -> Address {
            eviction_vote_pda(VOTING_EPOCH, TARGET_EPOCH, self.node).0
        }

        fn committee_address(&self) -> Address {
            committee_pda(TARGET_EPOCH).0
        }

        fn vote(&self) -> Vote {
            Vote {
                kind: VoteKind::Eviction as u64,
                hash: eviction_vote_hash(self.node),
                voting_epoch: VOTING_EPOCH,
                target_epoch: TARGET_EPOCH,
                registered_by: self.fee_payer,
                bitmap: Tail::new(self.vote_bitmap.len() as u64, self.vote_bitmap.len() as u64),
            }
        }

        fn members(&self) -> Vec<Member> {
            vec![Member {
                node: self.seated,
                stake: TAPE(0),
                assigned: StorageUnits::zero(),
                blacklisted: StorageUnits::zero(),
                spools: 0,
            }]
        }

        fn committee(&self) -> Committee {
            Committee {
                epoch: TARGET_EPOCH,
                members: Tail::new(1, 1),
            }
        }

        fn run(&self, checks: &[Check]) {
            let system = System {
                current_epoch: VOTING_EPOCH,
                ..System::zeroed()
            };
            let voting_epoch = Epoch {
                id: VOTING_EPOCH,
                total_groups: self.total_groups,
                state: EpochState {
                    phase: self.phase,
                    ..EpochState::zeroed()
                },
                ..Epoch::zeroed()
            };
            let target_epoch = Epoch {
                id: TARGET_EPOCH,
                nonce: self.nonce,
                ..Epoch::zeroed()
            };
            let node_account = Node {
                authority: self.node,
                ..Node::zeroed()
            };

            let signed_indices: Vec<usize> = (0..self.signers).collect();
            let bitmap = SpoolBitmap::from_indices(&signed_indices);
            let message = NodeEvictMessage::new(TARGET_EPOCH, self.sign_nonce, self.node).to_bytes();
            let partials: Vec<BlsSignature> = signed_indices
                .iter()
                .map(|&i| self.sks[i].sign(&message).unwrap())
                .collect();
            let agg_sig = BlsSignature::aggregate(&partials).unwrap();

            let instruction = build_vote_eviction_ix(
                self.fee_payer.into(),
                VOTING_EPOCH,
                self.node,
                self.group_id,
                bitmap,
                agg_sig,
            );

            let accounts = vec![
                sol(self.fee_payer, 1_000_000_000),
                pda(system_pda().0, system.pack(), tapedrive::ID),
                pda(epoch_pda(VOTING_EPOCH).0, voting_epoch.pack(), tapedrive::ID),
                pda(epoch_pda(TARGET_EPOCH).0, target_epoch.pack(), tapedrive::ID),
                pda(group_pda(VOTING_EPOCH, self.group_id).0, self.group.pack(), tapedrive::ID),
                pda(self.vote_address(), self.vote().pack_with(&self.vote_bitmap), tapedrive::ID),
                pda(self.node, node_account.pack(), tapedrive::ID),
                pda(
                    self.committee_address(),
                    self.committee().pack_with(&self.members()),
                    tapedrive::ID,
                ),
            ];

            test_env().process_instruction(&instruction, &accounts, checks);
        }
    }

    #[test]
    fn vote_eviction_lands() {
        let fixture = Fixture::new();

        // Landing: group bit set, node suspended, member removed (count 1 -> 0).
        let expected_vote = fixture.vote().pack_with(&[1u8]);
        let expected_node = Node {
            authority: fixture.node,
            suspended_until: TARGET_EPOCH,
            ..Node::zeroed()
        };
        let expected_committee = Committee {
            epoch: TARGET_EPOCH,
            members: Tail::new(1, 0),
        };

        fixture.run(&[
            Check::success(),
            Check::account(&Pubkey::from(fixture.node))
                .data(expected_node.pack().as_ref())
                .build(),
            Check::account(&Pubkey::from(fixture.vote_address()))
                .data(expected_vote.as_ref())
                .build(),
            Check::account(&Pubkey::from(fixture.committee_address()))
                .data(expected_committee.pack_with(&fixture.members()).as_ref())
                .build(),
        ]);
    }

    // Evict a node that is not seated in the next committee: the removal is a
    // no-op but the suspension still lands, blocking a later join.
    #[test]
    fn vote_eviction_preemptive() {
        let mut fixture = Fixture::new();
        fixture.seated = Address::new([0x11; 32]);

        let expected_node = Node {
            authority: fixture.node,
            suspended_until: TARGET_EPOCH,
            ..Node::zeroed()
        };

        fixture.run(&[
            Check::success(),
            Check::account(&Pubkey::from(fixture.node))
                .data(expected_node.pack().as_ref())
                .build(),
            Check::account(&Pubkey::from(fixture.committee_address()))
                .data(fixture.committee().pack_with(&fixture.members()).as_ref())
                .build(),
        ]);
    }

    // Voting is closed once the epoch reaches Closing, mirroring the join window.
    #[test]
    fn vote_eviction_wrong_phase() {
        let mut fixture = Fixture::new();
        fixture.phase = EpochPhase::Closing as u64;

        fixture.run(&[Check::err(TapeError::BadEpochState.into())]);
    }

    // A valid aggregate over the wrong message must not land the eviction.
    #[test]
    fn vote_eviction_bad_signature() {
        let mut fixture = Fixture::new();
        fixture.sign_nonce = Hash::new_unique();

        fixture.run(&[Check::err(TapeError::BadSignature.into())]);
    }

    // A group that already signed cannot sign the same eviction again.
    #[test]
    fn vote_eviction_replay() {
        let mut fixture = Fixture::new();
        fixture.vote_bitmap = vec![1u8];

        fixture.run(&[Check::err(TapeError::AlreadySigned.into())]);
    }

    // 13/20 falls short of the supermajority and must be rejected.
    #[test]
    fn vote_eviction_below_quorum() {
        let mut fixture = Fixture::new();
        fixture.signers = SUPERMAJORITY - 1;

        fixture.run(&[Check::err(TapeError::NoQuorum.into())]);
    }

    // With four groups, the third signing group crosses the 2/3 threshold and
    // lands the eviction without waiting for every group.
    #[test]
    fn vote_eviction_lands_at_group_supermajority() {
        let mut fixture = Fixture::with_groups(GroupIndex(2), 4);
        fixture.vote_bitmap = vec![0b0000_0011];

        let expected_vote = fixture.vote().pack_with(&[0b0000_0111]);
        let expected_node = Node {
            authority: fixture.node,
            suspended_until: TARGET_EPOCH,
            ..Node::zeroed()
        };
        let expected_committee = Committee {
            epoch: TARGET_EPOCH,
            members: Tail::new(1, 0),
        };

        fixture.run(&[
            Check::success(),
            Check::account(&Pubkey::from(fixture.node))
                .data(expected_node.pack().as_ref())
                .build(),
            Check::account(&Pubkey::from(fixture.vote_address()))
                .data(expected_vote.as_ref())
                .build(),
            Check::account(&Pubkey::from(fixture.committee_address()))
                .data(expected_committee.pack_with(&fixture.members()).as_ref())
                .build(),
        ]);
    }

    // A group signing after the threshold has landed only accumulates; the
    // landing effects do not run again.
    #[test]
    fn vote_eviction_after_landing_accumulates_only() {
        let mut fixture = Fixture::with_groups(GroupIndex(3), 4);
        fixture.vote_bitmap = vec![0b0000_0111];

        let expected_vote = fixture.vote().pack_with(&[0b0000_1111]);
        let untouched_node = Node {
            authority: fixture.node,
            ..Node::zeroed()
        };

        fixture.run(&[
            Check::success(),
            Check::account(&Pubkey::from(fixture.node))
                .data(untouched_node.pack().as_ref())
                .build(),
            Check::account(&Pubkey::from(fixture.vote_address()))
                .data(expected_vote.as_ref())
                .build(),
            Check::account(&Pubkey::from(fixture.committee_address()))
                .data(fixture.committee().pack_with(&fixture.members()).as_ref())
                .build(),
        ]);
    }
}
