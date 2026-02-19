use tape_solana::*;
use tape_api::prelude::*;
use tape_api::event::NodeSynced;
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

    // Check if already synced this epoch
    if node.latest_sync_epoch >= epoch.id {
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

    // Verify the spool hash matches
    let spool_hash = get_spool_hash(&spools);
    if spool_hash != args.spools {
        return Err(TapeError::BadSpoolHash.into());
    }

    // Verify the epoch ID matches
    let epoch_number = EpochNumber::unpack(args.epoch);
    if epoch.id != epoch_number {
        return Err(TapeError::BadEpochId.into());
    }

    let weight = spools.len() as u64;
    let total = SPOOL_COUNT as u64;

    // Attest our weight for this epoch sync
    let transitioned_to_settling = epoch.state
        .add_sync_weight(weight, total);

    // If we just transitioned to Settling and there's no committee_prev (first epoch),
    // immediately transition to Active since no one needs to advance
    if transitioned_to_settling && system.committee_prev_empty() {
        epoch.state.set_active();
    }

    node.latest_sync_epoch = current_epoch(epoch);

    NodeSynced {
        node: *node_info.key,
        id: node.id,
        epoch: current_epoch(epoch),
        spools_hash: args.spools,
    }.log();

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
        node.latest_sync_epoch = EpochNumber(7);

        system.committee = Committee::from_members(&[
            member(3, 3_000),
            member(node.id.into(), 2_000),  // index 1
            member(1, 1_000)
        ]);
        system.spools = SpoolAssignment::try_from_counts(
            &[700, 250, 50]
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
                        nonce: Hash::default(),
                        ..epoch
                    }.pack().as_ref()
                ).build(),
                Check::account(&node_address).data(
                    Node {
                        latest_sync_epoch: EpochNumber(42),
                        ..node
                    }.pack().as_ref()
                ).build(),
            ]
        );

        // Test: fail to sync again

        node.latest_sync_epoch = EpochNumber(42);

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

        // Test: above threshold sync (should go to settling)
        // Add non-empty committee_prev so we transition to Settling (not Active)
        system.committee_prev = Committee::from_members(&[member(99, 1_000)]);

        node.latest_sync_epoch = EpochNumber(7);
        system.spools = SpoolAssignment::try_from_counts(
            &[250, 700, 50]
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
                            phase: EpochPhase::Settling.into(),
                            weight: 0,
                        },
                        nonce: Hash::default(),
                        ..epoch
                    }.pack().as_ref()
                ).build(),
                Check::account(&node_address).data(
                    Node {
                        latest_sync_epoch: EpochNumber(42),
                        ..node
                    }.pack().as_ref()
                ).build(),
            ]
        );
    }
}
