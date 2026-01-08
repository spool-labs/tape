use tape_solana::*;
use tape_api::prelude::*;
use crate::error::*;

pub fn process_sync_epoch(accounts: &[AccountInfo<'_>], data: &[u8]) -> ProgramResult {
    let args = SyncEpoch::try_from_bytes(data)?;
    let [
        fee_payer_info,
        authority_info,
        system_info,
        epoch_info,
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

    let epoch = epoch_info
        .is_writable()?
        .is_epoch()?
        .as_account_mut::<Epoch>(&tapedrive::ID)?;

    let node = node_info
        .is_writable()?
        .as_account_mut::<Node>(&tapedrive::ID)?;

    if node.authority != *authority_info.key {
        return Err(ProgramError::InvalidAccountData);
    }

    if !epoch.state.is_syncing() {
        return Err(TapeError::BadEpochState.into());
    }

    if node.latest_epoch >= epoch.id {
        return Err(TapeError::AlreadySynced.into());
    }

    // Find our member index in the committee
    let member_index = system
        .committee
        .index_of(&node.id)
        .ok_or(TapeError::NotInCommittee)?;

    if member_index >= MEMBER_COUNT {
        return Err(TapeError::NotInCommittee.into());
    }

    // Find the spools this member is assigned
    let spools = system.spools
        .spools_for_member(member_index);

    // Verify the seat hash matches
    let seat_hash = get_spool_hash(&spools);
    if seat_hash != args.spools {
        return Err(TapeError::BadSeatHash.into());
    }

    // Verify the epoch ID matches
    let epoch_number = EpochNumber::unpack(args.epoch);
    if epoch.id != epoch_number {
        return Err(TapeError::BadEpochId.into());
    }

    let weight = spools.len() as u64;
    let total = SLICE_COUNT as u64;

    // Attest our weight for this epoch sync
    let transitioned_to_active = epoch.state
        .add_sync_weight(weight, total);

    // If we just transitioned to Active and there's no committee_prev (first epoch),
    // immediately transition to NextReady since no one needs to advance
    if transitioned_to_active && system.committee_prev_empty() {
        epoch.state.set_next_ready();
    }

    node.latest_epoch = current_epoch(epoch);

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tape_test::*;

    fn member(id: u64, stake: u64) -> CommitteeMember {
        CommitteeMember::new(NodeId(id), TAPE(stake))
    }

    #[test]
    fn test_epoch_sync() {
        let fee_payer = Pubkey::new_unique();
        let authority = Pubkey::new_unique();

        let (system_address, _) = system_pda();
        let (epoch_address, _) = epoch_pda();
        let (node_address, _) = node_pda(authority);

        // Setup existing accounts

        let mut system = System::zeroed();
        let mut epoch = Epoch::zeroed();
        let mut node = Node::zeroed();

        node.id = NodeId(9000);
        node.authority = authority;
        node.latest_epoch = EpochNumber(7);

        system.committee = Committee::from_members(&[
            member(3, 3_000),
            member(node.id.into(), 2_000),  // index 1
            member(1, 1_000)
        ]);
        system.spools = SpoolAssignment::try_from_counts(
            &[700, 250, 74]
        ).expect("spools");

        epoch.id = EpochNumber(42);
        epoch.state = EpochState::syncing();

        let instruction = build_epoch_sync_ix(
            fee_payer,
            authority,
            node_address,
            epoch.id,
            &system.spools.spools_for_member(1) // index 1
        );

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            sol(authority, 0),
            pda(system_address, system.pack(), tapedrive::ID),
            pda(epoch_address, epoch.pack(), tapedrive::ID),
            pda(node_address, node.pack(), tapedrive::ID),
        ];

        let env = test_env();

        // Test: happy path

        env.process_instruction(
            &instruction,
            &accounts,
            &[
                Check::success(),
                Check::account(&epoch_address).data(
                    Epoch {
                        state: EpochState {
                            phase: EpochPhase::Syncing.into(),
                            weight: 250,
                        },
                        ..epoch
                    }.pack().as_ref()
                ).build(),
                Check::account(&node_address).data(
                    Node {
                        latest_epoch: EpochNumber(42),
                        ..node
                    }.pack().as_ref()
                ).build(),
            ]
        );

        // Test: fail to sync again

        node.latest_epoch = EpochNumber(42);

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            sol(authority, 0),
            pda(system_address, system.pack(), tapedrive::ID),
            pda(epoch_address, epoch.pack(), tapedrive::ID),
            pda(node_address, node.pack(), tapedrive::ID),
        ];

        env.process_instruction(
            &instruction,
            &accounts,
            &[
                Check::err(TapeError::AlreadySynced.into()),
            ]
        );

        // Test: above threshold sync (should go to active)
        // Add non-empty committee_prev so we transition to Active (not NextReady)
        system.committee_prev = Committee::from_members(&[member(99, 1_000)]);

        node.latest_epoch = EpochNumber(7);
        system.spools = SpoolAssignment::try_from_counts(
            &[250, 700, 74]
        ).expect("spools");

        epoch.state = EpochState::syncing();

        let instruction = build_epoch_sync_ix(
            fee_payer,
            authority,
            node_address,
            epoch.id,
            &system.spools.spools_for_member(1)
        );

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            sol(authority, 0),
            pda(system_address, system.pack(), tapedrive::ID),
            pda(epoch_address, epoch.pack(), tapedrive::ID),
            pda(node_address, node.pack(), tapedrive::ID),
        ];

        env.process_instruction(
            &instruction,
            &accounts,
            &[
                Check::success(),
                Check::account(&epoch_address).data(
                    Epoch {
                        state: EpochState {
                            phase: EpochPhase::Active.into(),
                            weight: 0,
                        },
                        ..epoch
                    }.pack().as_ref()
                ).build(),
                Check::account(&node_address).data(
                    Node {
                        latest_epoch: EpochNumber(42),
                        ..node
                    }.pack().as_ref()
                ).build(),
            ]
        );
    }

    #[test]
    fn test_first_epoch_sync_skips_active() {
        // Test that in the first epoch (empty committee_prev), when sync reaches
        // supermajority, we skip Active and go directly to NextReady
        let fee_payer = Pubkey::new_unique();
        let authority = Pubkey::new_unique();

        let (system_address, _) = system_pda();
        let (epoch_address, _) = epoch_pda();
        let (node_address, _) = node_pda(authority);

        let mut system = System::zeroed();
        let mut epoch = Epoch::zeroed();
        let mut node = Node::zeroed();

        node.id = NodeId(1);
        node.authority = authority;
        node.latest_epoch = EpochNumber(1);

        // First epoch - committee_prev is empty
        system.committee_prev = Committee::new();
        system.committee = Committee::from_members(&[
            member(1, 3_000),
        ]);
        // Node gets all spools (supermajority threshold = 683)
        system.spools = SpoolAssignment::try_from_counts(
            &[SLICE_COUNT as u16]
        ).expect("spools");

        epoch.id = EpochNumber(2);
        epoch.state = EpochState::syncing();

        let instruction = build_epoch_sync_ix(
            fee_payer,
            authority,
            node_address,
            epoch.id,
            &system.spools.spools_for_member(0)
        );

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            sol(authority, 0),
            pda(system_address, system.pack(), tapedrive::ID),
            pda(epoch_address, epoch.pack(), tapedrive::ID),
            pda(node_address, node.pack(), tapedrive::ID),
        ];

        let env = test_env();

        // When committee_prev is empty and we reach supermajority,
        // state should skip Active and go directly to NextReady
        env.process_instruction(
            &instruction,
            &accounts,
            &[
                Check::success(),
                Check::account(&epoch_address).data(
                    Epoch {
                        state: EpochState {
                            phase: EpochPhase::NextEpochReady.into(),
                            weight: 0,
                        },
                        ..epoch
                    }.pack().as_ref()
                ).build(),
                Check::account(&node_address).data(
                    Node {
                        latest_epoch: EpochNumber(2),
                        ..node
                    }.pack().as_ref()
                ).build(),
            ]
        );
    }

    #[test]
    fn test_bad_epoch_state_active() {
        // Sync should fail when epoch state is Active
        let fee_payer = Pubkey::new_unique();
        let authority = Pubkey::new_unique();

        let (system_address, _) = system_pda();
        let (epoch_address, _) = epoch_pda();
        let (node_address, _) = node_pda(authority);

        let mut system = System::zeroed();
        let mut epoch = Epoch::zeroed();
        let mut node = Node::zeroed();

        node.id = NodeId(1);
        node.authority = authority;
        node.latest_epoch = EpochNumber(1);

        system.committee = Committee::from_members(&[member(1, 1_000)]);
        system.spools = SpoolAssignment::try_from_counts(&[SLICE_COUNT as u16]).unwrap();

        epoch.id = EpochNumber(42);
        epoch.state = EpochState::active(); // Already active

        let instruction = build_epoch_sync_ix(
            fee_payer,
            authority,
            node_address,
            epoch.id,
            &system.spools.spools_for_member(0),
        );

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            sol(authority, 0),
            pda(system_address, system.pack(), tapedrive::ID),
            pda(epoch_address, epoch.pack(), tapedrive::ID),
            pda(node_address, node.pack(), tapedrive::ID),
        ];

        let env = test_env();
        env.process_instruction(
            &instruction,
            &accounts,
            &[Check::err(TapeError::BadEpochState.into())],
        );
    }

    #[test]
    fn test_bad_epoch_state_next_ready() {
        // Sync should fail when epoch state is NextReady
        let fee_payer = Pubkey::new_unique();
        let authority = Pubkey::new_unique();

        let (system_address, _) = system_pda();
        let (epoch_address, _) = epoch_pda();
        let (node_address, _) = node_pda(authority);

        let mut system = System::zeroed();
        let mut epoch = Epoch::zeroed();
        let mut node = Node::zeroed();

        node.id = NodeId(1);
        node.authority = authority;
        node.latest_epoch = EpochNumber(1);

        system.committee = Committee::from_members(&[member(1, 1_000)]);
        system.spools = SpoolAssignment::try_from_counts(&[SLICE_COUNT as u16]).unwrap();

        epoch.id = EpochNumber(42);
        epoch.state = EpochState::next_ready(); // NextReady

        let instruction = build_epoch_sync_ix(
            fee_payer,
            authority,
            node_address,
            epoch.id,
            &system.spools.spools_for_member(0),
        );

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            sol(authority, 0),
            pda(system_address, system.pack(), tapedrive::ID),
            pda(epoch_address, epoch.pack(), tapedrive::ID),
            pda(node_address, node.pack(), tapedrive::ID),
        ];

        let env = test_env();
        env.process_instruction(
            &instruction,
            &accounts,
            &[Check::err(TapeError::BadEpochState.into())],
        );
    }

    #[test]
    fn test_bad_epoch_state_unknown() {
        // Sync should fail when epoch state is Unknown (zeroed)
        let fee_payer = Pubkey::new_unique();
        let authority = Pubkey::new_unique();

        let (system_address, _) = system_pda();
        let (epoch_address, _) = epoch_pda();
        let (node_address, _) = node_pda(authority);

        let mut system = System::zeroed();
        let mut epoch = Epoch::zeroed();
        let mut node = Node::zeroed();

        node.id = NodeId(1);
        node.authority = authority;
        node.latest_epoch = EpochNumber(1);

        system.committee = Committee::from_members(&[member(1, 1_000)]);
        system.spools = SpoolAssignment::try_from_counts(&[SLICE_COUNT as u16]).unwrap();

        epoch.id = EpochNumber(42);
        // epoch.state is zeroed/Unknown by default

        let instruction = build_epoch_sync_ix(
            fee_payer,
            authority,
            node_address,
            epoch.id,
            &system.spools.spools_for_member(0),
        );

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            sol(authority, 0),
            pda(system_address, system.pack(), tapedrive::ID),
            pda(epoch_address, epoch.pack(), tapedrive::ID),
            pda(node_address, node.pack(), tapedrive::ID),
        ];

        let env = test_env();
        env.process_instruction(
            &instruction,
            &accounts,
            &[Check::err(TapeError::BadEpochState.into())],
        );
    }

    #[test]
    fn test_not_in_committee() {
        // Node not in committee should fail
        let fee_payer = Pubkey::new_unique();
        let authority = Pubkey::new_unique();

        let (system_address, _) = system_pda();
        let (epoch_address, _) = epoch_pda();
        let (node_address, _) = node_pda(authority);

        let mut system = System::zeroed();
        let mut epoch = Epoch::zeroed();
        let mut node = Node::zeroed();

        node.id = NodeId(999); // Not in committee
        node.authority = authority;
        node.latest_epoch = EpochNumber(1);

        // Committee has different nodes
        system.committee = Committee::from_members(&[
            member(1, 1_000),
            member(2, 1_000),
        ]);
        system.spools = SpoolAssignment::try_from_counts(&[512, 512]).unwrap();

        epoch.id = EpochNumber(42);
        epoch.state = EpochState::syncing();

        // Build instruction with some valid-looking spools
        let instruction = build_epoch_sync_ix(
            fee_payer,
            authority,
            node_address,
            epoch.id,
            &system.spools.spools_for_member(0),
        );

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            sol(authority, 0),
            pda(system_address, system.pack(), tapedrive::ID),
            pda(epoch_address, epoch.pack(), tapedrive::ID),
            pda(node_address, node.pack(), tapedrive::ID),
        ];

        let env = test_env();
        env.process_instruction(
            &instruction,
            &accounts,
            &[Check::err(TapeError::NotInCommittee.into())],
        );
    }

    #[test]
    fn test_bad_spool_hash() {
        // Wrong spool hash should fail
        let fee_payer = Pubkey::new_unique();
        let authority = Pubkey::new_unique();

        let (system_address, _) = system_pda();
        let (epoch_address, _) = epoch_pda();
        let (node_address, _) = node_pda(authority);

        let mut system = System::zeroed();
        let mut epoch = Epoch::zeroed();
        let mut node = Node::zeroed();

        node.id = NodeId(1);
        node.authority = authority;
        node.latest_epoch = EpochNumber(1);

        system.committee = Committee::from_members(&[
            member(1, 2_000),
            member(2, 1_000),
        ]);
        system.spools = SpoolAssignment::try_from_counts(&[683, 341]).unwrap();

        epoch.id = EpochNumber(42);
        epoch.state = EpochState::syncing();

        // Pass wrong spools (member 1's spools instead of member 0's)
        let instruction = build_epoch_sync_ix(
            fee_payer,
            authority,
            node_address,
            epoch.id,
            &system.spools.spools_for_member(1), // Wrong member's spools
        );

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            sol(authority, 0),
            pda(system_address, system.pack(), tapedrive::ID),
            pda(epoch_address, epoch.pack(), tapedrive::ID),
            pda(node_address, node.pack(), tapedrive::ID),
        ];

        let env = test_env();
        env.process_instruction(
            &instruction,
            &accounts,
            &[Check::err(TapeError::BadSeatHash.into())],
        );
    }

    #[test]
    fn test_bad_epoch_id() {
        // Wrong epoch ID in args should fail
        let fee_payer = Pubkey::new_unique();
        let authority = Pubkey::new_unique();

        let (system_address, _) = system_pda();
        let (epoch_address, _) = epoch_pda();
        let (node_address, _) = node_pda(authority);

        let mut system = System::zeroed();
        let mut epoch = Epoch::zeroed();
        let mut node = Node::zeroed();

        node.id = NodeId(1);
        node.authority = authority;
        node.latest_epoch = EpochNumber(1);

        system.committee = Committee::from_members(&[member(1, 1_000)]);
        system.spools = SpoolAssignment::try_from_counts(&[SLICE_COUNT as u16]).unwrap();

        epoch.id = EpochNumber(42);
        epoch.state = EpochState::syncing();

        // Build instruction with wrong epoch ID
        let instruction = build_epoch_sync_ix(
            fee_payer,
            authority,
            node_address,
            EpochNumber(99), // Wrong epoch
            &system.spools.spools_for_member(0),
        );

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            sol(authority, 0),
            pda(system_address, system.pack(), tapedrive::ID),
            pda(epoch_address, epoch.pack(), tapedrive::ID),
            pda(node_address, node.pack(), tapedrive::ID),
        ];

        let env = test_env();
        env.process_instruction(
            &instruction,
            &accounts,
            &[Check::err(TapeError::BadEpochId.into())],
        );
    }

    #[test]
    fn test_wrong_authority() {
        // Wrong authority signer should fail
        let fee_payer = Pubkey::new_unique();
        let authority = Pubkey::new_unique();
        let wrong_authority = Pubkey::new_unique();

        let (system_address, _) = system_pda();
        let (epoch_address, _) = epoch_pda();
        let (node_address, _) = node_pda(authority);

        let mut system = System::zeroed();
        let mut epoch = Epoch::zeroed();
        let mut node = Node::zeroed();

        node.id = NodeId(1);
        node.authority = authority; // Node expects this authority
        node.latest_epoch = EpochNumber(1);

        system.committee = Committee::from_members(&[member(1, 1_000)]);
        system.spools = SpoolAssignment::try_from_counts(&[SLICE_COUNT as u16]).unwrap();

        epoch.id = EpochNumber(42);
        epoch.state = EpochState::syncing();

        // Build instruction with wrong authority
        let instruction = build_epoch_sync_ix(
            fee_payer,
            wrong_authority, // Wrong signer
            node_address,
            epoch.id,
            &system.spools.spools_for_member(0),
        );

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            sol(wrong_authority, 0),
            pda(system_address, system.pack(), tapedrive::ID),
            pda(epoch_address, epoch.pack(), tapedrive::ID),
            pda(node_address, node.pack(), tapedrive::ID),
        ];

        let env = test_env();
        env.process_instruction(
            &instruction,
            &accounts,
            &[Check::err(ProgramError::InvalidAccountData)],
        );
    }

    #[test]
    fn test_weight_accumulation() {
        // Test that multiple syncs accumulate weight correctly
        let fee_payer = Pubkey::new_unique();
        let authority1 = Pubkey::new_unique();
        let authority2 = Pubkey::new_unique();

        let (system_address, _) = system_pda();
        let (epoch_address, _) = epoch_pda();
        let (node_address1, _) = node_pda(authority1);
        let (node_address2, _) = node_pda(authority2);

        let mut system = System::zeroed();
        let mut epoch = Epoch::zeroed();
        let mut node1 = Node::zeroed();
        let mut node2 = Node::zeroed();

        node1.id = NodeId(1);
        node1.authority = authority1;
        node1.latest_epoch = EpochNumber(1);

        node2.id = NodeId(2);
        node2.authority = authority2;
        node2.latest_epoch = EpochNumber(1);

        // Stakes in descending order so indices match: node2(400) at 0, node1(300) at 1
        // Spools: member 0 gets 400, member 1 gets 300, member 2 gets 324
        system.committee = Committee::from_members(&[
            member(2, 4_000),  // index 0 after sort
            member(1, 3_000),  // index 1 after sort
            member(3, 2_000),  // index 2 after sort
        ]);
        system.spools = SpoolAssignment::try_from_counts(&[400, 300, 324]).unwrap();

        // Add non-empty committee_prev so we test Active transition (not first epoch)
        system.committee_prev = Committee::from_members(&[member(99, 1_000)]);

        epoch.id = EpochNumber(42);
        epoch.state = EpochState::syncing();

        let env = test_env();

        // node1 (NodeId(1)) is at index 1, gets 300 spools
        let idx1 = system.committee.index_of(&node1.id).unwrap();
        let instruction1 = build_epoch_sync_ix(
            fee_payer,
            authority1,
            node_address1,
            epoch.id,
            &system.spools.spools_for_member(idx1),
        );

        let accounts1 = vec![
            sol(fee_payer, 1_000_000_000),
            sol(authority1, 0),
            pda(system_address, system.pack(), tapedrive::ID),
            pda(epoch_address, epoch.pack(), tapedrive::ID),
            pda(node_address1, node1.pack(), tapedrive::ID),
        ];

        env.process_instruction(
            &instruction1,
            &accounts1,
            &[
                Check::success(),
                Check::account(&epoch_address).data(
                    Epoch {
                        state: EpochState {
                            phase: EpochPhase::Syncing.into(),
                            weight: 300,
                        },
                        ..epoch
                    }.pack().as_ref()
                ).build(),
            ],
        );

        // node2 (NodeId(2)) is at index 0, gets 400 spools
        // Total: 300 + 400 = 700, exceeds 683 threshold
        epoch.state.weight = 300;
        node1.latest_epoch = EpochNumber(42);

        let idx2 = system.committee.index_of(&node2.id).unwrap();
        let instruction2 = build_epoch_sync_ix(
            fee_payer,
            authority2,
            node_address2,
            epoch.id,
            &system.spools.spools_for_member(idx2),
        );

        let accounts2 = vec![
            sol(fee_payer, 1_000_000_000),
            sol(authority2, 0),
            pda(system_address, system.pack(), tapedrive::ID),
            pda(epoch_address, epoch.pack(), tapedrive::ID),
            pda(node_address2, node2.pack(), tapedrive::ID),
        ];

        env.process_instruction(
            &instruction2,
            &accounts2,
            &[
                Check::success(),
                Check::account(&epoch_address).data(
                    Epoch {
                        state: EpochState {
                            phase: EpochPhase::Active.into(),
                            weight: 0,
                        },
                        ..epoch
                    }.pack().as_ref()
                ).build(),
            ],
        );
    }

    #[test]
    fn test_exact_supermajority_683() {
        // Test exact supermajority threshold (683 out of 1024)
        let fee_payer = Pubkey::new_unique();
        let authority = Pubkey::new_unique();

        let (system_address, _) = system_pda();
        let (epoch_address, _) = epoch_pda();
        let (node_address, _) = node_pda(authority);

        let mut system = System::zeroed();
        let mut epoch = Epoch::zeroed();
        let mut node = Node::zeroed();

        node.id = NodeId(1);
        node.authority = authority;
        node.latest_epoch = EpochNumber(1);

        // Node gets exactly 683 spools (stake 683 > 341, so NodeId(1) is at index 0)
        system.committee = Committee::from_members(&[
            member(1, 683),
            member(2, 341),
        ]);
        system.spools = SpoolAssignment::try_from_counts(&[683, 341]).unwrap();

        // Add non-empty committee_prev so we transition to Active (not NextReady)
        system.committee_prev = Committee::from_members(&[member(99, 1_000)]);

        epoch.id = EpochNumber(42);
        epoch.state = EpochState::syncing();

        let idx = system.committee.index_of(&node.id).unwrap();
        let instruction = build_epoch_sync_ix(
            fee_payer,
            authority,
            node_address,
            epoch.id,
            &system.spools.spools_for_member(idx),
        );

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            sol(authority, 0),
            pda(system_address, system.pack(), tapedrive::ID),
            pda(epoch_address, epoch.pack(), tapedrive::ID),
            pda(node_address, node.pack(), tapedrive::ID),
        ];

        let env = test_env();
        // 683 should exactly meet supermajority (3*683 >= 2*1024+1 = 2049 >= 2049)
        env.process_instruction(
            &instruction,
            &accounts,
            &[
                Check::success(),
                Check::account(&epoch_address).data(
                    Epoch {
                        state: EpochState {
                            phase: EpochPhase::Active.into(),
                            weight: 0,
                        },
                        ..epoch
                    }.pack().as_ref()
                ).build(),
            ],
        );
    }

    #[test]
    fn test_below_supermajority_682() {
        // Test just below supermajority (682 out of 1024) - no transition
        let fee_payer = Pubkey::new_unique();
        let authority = Pubkey::new_unique();

        let (system_address, _) = system_pda();
        let (epoch_address, _) = epoch_pda();
        let (node_address, _) = node_pda(authority);

        let mut system = System::zeroed();
        let mut epoch = Epoch::zeroed();
        let mut node = Node::zeroed();

        node.id = NodeId(1);
        node.authority = authority;
        node.latest_epoch = EpochNumber(1);

        // Node gets exactly 682 spools (just below threshold)
        system.committee = Committee::from_members(&[
            member(1, 682),
            member(2, 342),
        ]);
        system.spools = SpoolAssignment::try_from_counts(&[682, 342]).unwrap();

        epoch.id = EpochNumber(42);
        epoch.state = EpochState::syncing();

        let instruction = build_epoch_sync_ix(
            fee_payer,
            authority,
            node_address,
            epoch.id,
            &system.spools.spools_for_member(0),
        );

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            sol(authority, 0),
            pda(system_address, system.pack(), tapedrive::ID),
            pda(epoch_address, epoch.pack(), tapedrive::ID),
            pda(node_address, node.pack(), tapedrive::ID),
        ];

        let env = test_env();
        // 682 should NOT meet supermajority (3*682 = 2046 < 2049)
        env.process_instruction(
            &instruction,
            &accounts,
            &[
                Check::success(),
                Check::account(&epoch_address).data(
                    Epoch {
                        state: EpochState {
                            phase: EpochPhase::Syncing.into(),
                            weight: 682, // Still syncing
                        },
                        ..epoch
                    }.pack().as_ref()
                ).build(),
            ],
        );
    }

    #[test]
    fn test_first_epoch_partial_sync() {
        // First epoch, partial sync (not supermajority) - stay in Syncing
        let fee_payer = Pubkey::new_unique();
        let authority = Pubkey::new_unique();

        let (system_address, _) = system_pda();
        let (epoch_address, _) = epoch_pda();
        let (node_address, _) = node_pda(authority);

        let mut system = System::zeroed();
        let mut epoch = Epoch::zeroed();
        let mut node = Node::zeroed();

        node.id = NodeId(1);
        node.authority = authority;
        node.latest_epoch = EpochNumber(1);

        // First epoch - committee_prev is empty
        // Stakes: NodeId(2)=524 at index 0, NodeId(1)=500 at index 1 after sort
        system.committee_prev = Committee::new();
        system.committee = Committee::from_members(&[
            member(2, 524),  // index 0 after sort (higher stake)
            member(1, 500),  // index 1 after sort
        ]);
        system.spools = SpoolAssignment::try_from_counts(&[524, 500]).unwrap();

        epoch.id = EpochNumber(2);
        epoch.state = EpochState::syncing();

        // Node 1 is at index 1, gets 500 spools
        let idx = system.committee.index_of(&node.id).unwrap();
        let instruction = build_epoch_sync_ix(
            fee_payer,
            authority,
            node_address,
            epoch.id,
            &system.spools.spools_for_member(idx),
        );

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            sol(authority, 0),
            pda(system_address, system.pack(), tapedrive::ID),
            pda(epoch_address, epoch.pack(), tapedrive::ID),
            pda(node_address, node.pack(), tapedrive::ID),
        ];

        let env = test_env();
        // 500 < 683, should stay in Syncing
        env.process_instruction(
            &instruction,
            &accounts,
            &[
                Check::success(),
                Check::account(&epoch_address).data(
                    Epoch {
                        state: EpochState {
                            phase: EpochPhase::Syncing.into(),
                            weight: 500,
                        },
                        ..epoch
                    }.pack().as_ref()
                ).build(),
            ],
        );
    }

    #[test]
    fn test_single_node_all_spools() {
        // Single node with all 1024 spools (first epoch - goes to NextReady)
        let fee_payer = Pubkey::new_unique();
        let authority = Pubkey::new_unique();

        let (system_address, _) = system_pda();
        let (epoch_address, _) = epoch_pda();
        let (node_address, _) = node_pda(authority);

        let mut system = System::zeroed();
        let mut epoch = Epoch::zeroed();
        let mut node = Node::zeroed();

        node.id = NodeId(1);
        node.authority = authority;
        node.latest_epoch = EpochNumber(1);

        // Single node gets all spools, committee_prev empty (first epoch)
        system.committee = Committee::from_members(&[member(1, 10_000)]);
        system.spools = SpoolAssignment::try_from_counts(&[SLICE_COUNT as u16]).unwrap();
        // committee_prev is empty (zeroed) - first epoch scenario

        epoch.id = EpochNumber(42);
        epoch.state = EpochState::syncing();

        let spools = system.spools.spools_for_member(0);
        assert_eq!(spools.len(), SLICE_COUNT);

        let instruction = build_epoch_sync_ix(
            fee_payer,
            authority,
            node_address,
            epoch.id,
            &spools,
        );

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            sol(authority, 0),
            pda(system_address, system.pack(), tapedrive::ID),
            pda(epoch_address, epoch.pack(), tapedrive::ID),
            pda(node_address, node.pack(), tapedrive::ID),
        ];

        let env = test_env();
        // First epoch with empty committee_prev: skips Active, goes directly to NextReady
        env.process_instruction(
            &instruction,
            &accounts,
            &[
                Check::success(),
                Check::account(&epoch_address).data(
                    Epoch {
                        state: EpochState {
                            phase: EpochPhase::NextEpochReady.into(),
                            weight: 0,
                        },
                        ..epoch
                    }.pack().as_ref()
                ).build(),
            ],
        );
    }

    #[test]
    fn test_latest_epoch_greater_than_current() {
        // Node's latest_epoch > epoch.id should fail with AlreadySynced
        let fee_payer = Pubkey::new_unique();
        let authority = Pubkey::new_unique();

        let (system_address, _) = system_pda();
        let (epoch_address, _) = epoch_pda();
        let (node_address, _) = node_pda(authority);

        let mut system = System::zeroed();
        let mut epoch = Epoch::zeroed();
        let mut node = Node::zeroed();

        node.id = NodeId(1);
        node.authority = authority;
        node.latest_epoch = EpochNumber(100); // Greater than epoch.id

        system.committee = Committee::from_members(&[member(1, 1_000)]);
        system.spools = SpoolAssignment::try_from_counts(&[SLICE_COUNT as u16]).unwrap();

        epoch.id = EpochNumber(42);
        epoch.state = EpochState::syncing();

        let instruction = build_epoch_sync_ix(
            fee_payer,
            authority,
            node_address,
            epoch.id,
            &system.spools.spools_for_member(0),
        );

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            sol(authority, 0),
            pda(system_address, system.pack(), tapedrive::ID),
            pda(epoch_address, epoch.pack(), tapedrive::ID),
            pda(node_address, node.pack(), tapedrive::ID),
        ];

        let env = test_env();
        env.process_instruction(
            &instruction,
            &accounts,
            &[Check::err(TapeError::AlreadySynced.into())],
        );
    }

    #[test]
    fn test_weight_with_prior_accumulation() {
        // Test sync when there's already accumulated weight
        let fee_payer = Pubkey::new_unique();
        let authority = Pubkey::new_unique();

        let (system_address, _) = system_pda();
        let (epoch_address, _) = epoch_pda();
        let (node_address, _) = node_pda(authority);

        let mut system = System::zeroed();
        let mut epoch = Epoch::zeroed();
        let mut node = Node::zeroed();

        node.id = NodeId(2);
        node.authority = authority;
        node.latest_epoch = EpochNumber(1);

        // Stakes sorted descending: member(1,400) idx 0, member(3,324) idx 1, member(2,300) idx 2
        system.committee = Committee::from_members(&[
            member(1, 400),
            member(2, 300),
            member(3, 324),
        ]);
        system.spools = SpoolAssignment::try_from_counts(&[400, 324, 300]).unwrap();

        // Add non-empty committee_prev so we transition to Active (not NextReady)
        system.committee_prev = Committee::from_members(&[member(99, 1_000)]);

        epoch.id = EpochNumber(42);
        // Start with 400 weight already accumulated (node 1 synced)
        epoch.state = EpochState {
            phase: EpochPhase::Syncing.into(),
            weight: 400,
        };

        // NodeId(2) is at index 2 after sorting, gets 300 spools
        let idx = system.committee.index_of(&node.id).unwrap();
        let instruction = build_epoch_sync_ix(
            fee_payer,
            authority,
            node_address,
            epoch.id,
            &system.spools.spools_for_member(idx),
        );

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            sol(authority, 0),
            pda(system_address, system.pack(), tapedrive::ID),
            pda(epoch_address, epoch.pack(), tapedrive::ID),
            pda(node_address, node.pack(), tapedrive::ID),
        ];

        let env = test_env();
        // 400 + 300 = 700 >= 683, should transition to Active
        env.process_instruction(
            &instruction,
            &accounts,
            &[
                Check::success(),
                Check::account(&epoch_address).data(
                    Epoch {
                        state: EpochState {
                            phase: EpochPhase::Active.into(),
                            weight: 0,
                        },
                        ..epoch
                    }.pack().as_ref()
                ).build(),
            ],
        );
    }

    #[test]
    fn test_committee_prev_not_empty() {
        // When committee_prev is NOT empty, reaching supermajority
        // should transition to Active (not NextReady)
        let fee_payer = Pubkey::new_unique();
        let authority = Pubkey::new_unique();

        let (system_address, _) = system_pda();
        let (epoch_address, _) = epoch_pda();
        let (node_address, _) = node_pda(authority);

        let mut system = System::zeroed();
        let mut epoch = Epoch::zeroed();
        let mut node = Node::zeroed();

        node.id = NodeId(1);
        node.authority = authority;
        node.latest_epoch = EpochNumber(1);

        // committee_prev is NOT empty
        system.committee_prev = Committee::from_members(&[member(99, 1_000)]);
        system.committee = Committee::from_members(&[member(1, 3_000)]);
        system.spools = SpoolAssignment::try_from_counts(&[SLICE_COUNT as u16]).unwrap();

        epoch.id = EpochNumber(42);
        epoch.state = EpochState::syncing();

        let instruction = build_epoch_sync_ix(
            fee_payer,
            authority,
            node_address,
            epoch.id,
            &system.spools.spools_for_member(0),
        );

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            sol(authority, 0),
            pda(system_address, system.pack(), tapedrive::ID),
            pda(epoch_address, epoch.pack(), tapedrive::ID),
            pda(node_address, node.pack(), tapedrive::ID),
        ];

        let env = test_env();
        // Should go to Active, NOT NextReady
        env.process_instruction(
            &instruction,
            &accounts,
            &[
                Check::success(),
                Check::account(&epoch_address).data(
                    Epoch {
                        state: EpochState {
                            phase: EpochPhase::Active.into(),
                            weight: 0,
                        },
                        ..epoch
                    }.pack().as_ref()
                ).build(),
            ],
        );
    }

    #[test]
    fn test_many_small_spools() {
        // Test with many members having small spool allocations
        let fee_payer = Pubkey::new_unique();
        let authority = Pubkey::new_unique();

        let (system_address, _) = system_pda();
        let (epoch_address, _) = epoch_pda();
        let (node_address, _) = node_pda(authority);

        let mut system = System::zeroed();
        let mut epoch = Epoch::zeroed();
        let mut node = Node::zeroed();

        node.id = NodeId(1);
        node.authority = authority;
        node.latest_epoch = EpochNumber(1);

        // 4 members with roughly equal spools
        system.committee = Committee::from_members(&[
            member(1, 1_000),
            member(2, 1_000),
            member(3, 1_000),
            member(4, 1_000),
        ]);
        system.spools = SpoolAssignment::try_from_counts(&[256, 256, 256, 256]).unwrap();

        epoch.id = EpochNumber(42);
        epoch.state = EpochState::syncing();

        let instruction = build_epoch_sync_ix(
            fee_payer,
            authority,
            node_address,
            epoch.id,
            &system.spools.spools_for_member(0),
        );

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            sol(authority, 0),
            pda(system_address, system.pack(), tapedrive::ID),
            pda(epoch_address, epoch.pack(), tapedrive::ID),
            pda(node_address, node.pack(), tapedrive::ID),
        ];

        let env = test_env();
        // 256 < 683, should stay in Syncing
        env.process_instruction(
            &instruction,
            &accounts,
            &[
                Check::success(),
                Check::account(&epoch_address).data(
                    Epoch {
                        state: EpochState {
                            phase: EpochPhase::Syncing.into(),
                            weight: 256,
                        },
                        ..epoch
                    }.pack().as_ref()
                ).build(),
            ],
        );
    }

    #[test]
    fn test_empty_spools_hash() {
        // Test that an empty spool list produces a different hash
        // and causes BadSeatHash if node has spools assigned
        let fee_payer = Pubkey::new_unique();
        let authority = Pubkey::new_unique();

        let (system_address, _) = system_pda();
        let (epoch_address, _) = epoch_pda();
        let (node_address, _) = node_pda(authority);

        let mut system = System::zeroed();
        let mut epoch = Epoch::zeroed();
        let mut node = Node::zeroed();

        node.id = NodeId(1);
        node.authority = authority;
        node.latest_epoch = EpochNumber(1);

        system.committee = Committee::from_members(&[member(1, 1_000)]);
        system.spools = SpoolAssignment::try_from_counts(&[SLICE_COUNT as u16]).unwrap();

        epoch.id = EpochNumber(42);
        epoch.state = EpochState::syncing();

        // Pass empty spools array
        let empty_spools: &[SpoolIndex] = &[];
        let instruction = build_epoch_sync_ix(
            fee_payer,
            authority,
            node_address,
            epoch.id,
            empty_spools,
        );

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            sol(authority, 0),
            pda(system_address, system.pack(), tapedrive::ID),
            pda(epoch_address, epoch.pack(), tapedrive::ID),
            pda(node_address, node.pack(), tapedrive::ID),
        ];

        let env = test_env();
        env.process_instruction(
            &instruction,
            &accounts,
            &[Check::err(TapeError::BadSeatHash.into())],
        );
    }
}
