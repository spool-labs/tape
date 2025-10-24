use tape_api::prelude::*;

use steel::*;

pub fn process_sync_epoch(accounts: &[AccountInfo<'_>], data: &[u8]) -> ProgramResult {
    let args = SyncEpoch::try_from_bytes(data)?;
    let [
        signer_info,
        system_info,
        epoch_info,
        node_info,
    ] = accounts else {
        return Err(ProgramError::NotEnoughAccountKeys);
    };

    signer_info.is_signer()?;

    let system = system_info
        .is_system()?
        .as_account::<System>(&tapedrive::ID)?;

    let epoch = epoch_info
        .is_writable()?
        .is_epoch()?
        .as_account_mut::<Epoch>(&tapedrive::ID)?;

    let node = node_info
        .is_writable()?
        .as_account_mut::<Node>(&tapedrive::ID)?
        .assert_mut(|n| n.authority.eq(signer_info.key))?;

    if !epoch.state.is_syncing() {
        return Err(ProgramError::Custom(1));
        //return Err(TapeError::InvalidEpochState);
    }

    if node.latest_epoch >= epoch.id {
        return Err(ProgramError::Custom(5));
        //return Err(TapeError::SyncAlreadyPerformed);
    }

    // Find our member index in the committee
    let member_index = system
        .committee
        .index_of(&node.id)
        .ok_or(ProgramError::Custom(2))?;
    //  .ok_or(TapeError::NodeNotInCommittee)?;

    assert!(member_index < MEMBER_COUNT);

    // Find the seats this member holds
    let seats = system.seats
        .seats_for_member(member_index);

    // Verify the seat hash matches
    let seat_hash = get_seat_hash(&seats);
    if seat_hash != args.seats {
        return Err(ProgramError::Custom(3));
        //return Err(TapeError::InvalidSeatHash);
    }

    // Verify the epoch ID matches
    let epoch_number = EpochNumber::unpack(args.epoch);
    if epoch.id != epoch_number {
        return Err(ProgramError::Custom(4));
        //return Err(TapeError::InvalidEpochId);
    }

    let weight = seats.len() as u64;
    let total = SEAT_COUNT as u64;

    epoch.state.add_weight(weight, total);
    node.latest_epoch = current_epoch(epoch);

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tape_test::*;

    fn member(id: u64, stake: u64) -> CommitteeMember {
        CommitteeMember {
            id: NodeId(id),
            stake: TAPE(stake),
            key: BlsPubkey::zeroed(),
        }
    }

    #[test]
    fn test_epoch_sync() {
        let signer = Pubkey::new_unique();

        let (system_address, _) = system_pda();
        let (epoch_address, _) = epoch_pda();
        let (node_address, _) = node_pda(signer);

        // Setup existing accounts

        let mut system = System::zeroed();
        let mut epoch = Epoch::zeroed();
        let mut node = Node::zeroed();

        node.id = NodeId(9000);
        node.authority = signer;
        node.latest_epoch = EpochNumber(7);

        system.committee = Committee::from_members(&[ 
            member(3, 3_000), 
            member(node.id.into(), 2_000),  // index 1
            member(1, 1_000)
        ]);
        system.seats = Seats::try_from_counts(
            &[700, 250, 50]
        ).expect("seats");

        epoch.id = EpochNumber(42);
        epoch.state = EpochState::syncing();

        let instruction = build_epoch_sync_ix(
            signer,
            epoch.id,
            &system.seats.seats_for_member(1) // index 1
        );

        let accounts = vec![
            sol(signer, 1_000_000_000),
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
            sol(signer, 1_000_000_000),
            pda(system_address, system.pack(), tapedrive::ID),
            pda(epoch_address, epoch.pack(), tapedrive::ID),
            pda(node_address, node.pack(), tapedrive::ID),
        ];

        env.process_instruction(
            &instruction,
            &accounts,
            &[
                Check::err(ProgramError::Custom(5)), // SyncAlreadyPerformed
            ]
        );

        // Test: above threshold sync (should go to active)

        node.latest_epoch = EpochNumber(7);
        system.seats = Seats::try_from_counts(
            &[250, 700, 50]
        ).expect("seats");
        epoch.state = EpochState::syncing();

        let instruction = build_epoch_sync_ix(
            signer,
            epoch.id,
            &system.seats.seats_for_member(1)
        );

        let accounts = vec![
            sol(signer, 1_000_000_000),
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
}
