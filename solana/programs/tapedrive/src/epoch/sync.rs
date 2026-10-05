use tape_api::program::prelude::*;
use tape_api::event::SpoolSynced;

pub fn process_sync_spool(accounts: &[AccountInfo<'_>], data: &[u8]) -> ProgramResult {
    let args = SyncSpool::try_from_bytes(data)?;
    let [
        fee_payer_info,
        authority_info,
        system_info,
        epoch_info,
        group_info,
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

    let epoch = epoch_info
        .is_writable()?
        .is_epoch(curr)?
        .as_account_mut::<Epoch>(&tapedrive::ID)?;

    if epoch.state.phase != EpochPhase::Sync as u64 {
        return Err(TapeError::BadEpochState.into());
    }

    let spool = args.spool;
    let group_id = GroupIndex::containing(spool);

    let group = group_info
        .is_writable()?
        .is_group(curr, group_id)?
        .as_account_mut::<Group>(&tapedrive::ID)?;

    let slice = group_id
        .position_of(spool)
        .ok_or(TapeError::BadSpoolHash)?;
    let slice_idx = slice;

    let node = node_info
        .is_writable()?
        .as_account_mut::<Node>(&tapedrive::ID)?;

    if node.authority != (*authority_info.key).into() {
        return Err(ProgramError::InvalidAccountData);
    }

    let node_address: Address = (*node_info.key).into();
    if group.spools[slice_idx].node != node_address {
        return Err(TapeError::NotInCommittee.into());
    }

    if group.synced.is_set(slice_idx) {
        return Err(TapeError::AlreadySynced.into());
    }

    let required = system.synced_threshold()?;
    let was_recoverable = group.synced.count_ones() as u64 >= required;
    group.synced.set(slice_idx);
    let is_recoverable = group.synced.count_ones() as u64 >= required;

    if !was_recoverable && is_recoverable {
        epoch.state.synced_count = epoch.state.synced_count.saturating_add(1);
        if epoch.state.synced_count == system.live_group_count {
            epoch.state.phase = EpochPhase::Snapshot as u64;
        }
    }

    node.latest_sync_epoch = curr;

    SpoolSynced {
        node: node_address,
        epoch: curr,
        group: group_id,
        spool: args.spool,
        phase: epoch.state.phase,
    }.log();

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tape_core::encoding::RSParams;
    use tape_test::*;

    fn group_with_owner(
        epoch: EpochNumber,
        group_id: GroupIndex,
        owner_slot: usize,
        owner: Address,
    ) -> Group {
        let mut g = Group::zeroed();
        g.id = group_id;
        g.epoch = epoch;
        g.spools[owner_slot].node = owner;
        g
    }

    #[test]
    fn sync() {
        let fee_payer = Pubkey::new_unique();
        let authority = Pubkey::new_unique();

        let curr = EpochNumber(42);
        let group_id = GroupIndex(3);
        let slice_in_group = 7usize;
        let spool = group_id.spool_at(slice_in_group);

        let (system_address, _) = system_pda();
        let (epoch_address, _) = epoch_pda(curr);
        let (group_address, _) = group_pda(curr, group_id);
        let (node_address, _) = node_pda(authority.into());

        let system = System {
            current_epoch: curr,
            target_group_count: 50,
            live_group_count: 50,
            ..permissionless_system()
        };

        let epoch = Epoch {
            id: curr,
            state: EpochState {
                phase: EpochPhase::Sync as u64,
                ..EpochState::zeroed()
            },
            total_groups: 50,
            ..Epoch::zeroed()
        };

        let group = group_with_owner(curr, group_id, slice_in_group, node_address);

        let node = Node {
            authority: authority.into(),
            ..Node::zeroed()
        };

        let instruction = build_sync_spool_ix(
            fee_payer.into(),
            authority.into(),
            node_address,
            curr,
            group_id,
            spool,
        );

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            sol(authority, 0),
            pda(system_address, system.pack(), tapedrive::ID),
            pda(epoch_address, epoch.pack(), tapedrive::ID),
            pda(group_address, group.pack(), tapedrive::ID),
            pda(node_address, node.pack(), tapedrive::ID),
        ];

        let mut expected_group = group;
        expected_group.synced.set(slice_in_group);

        let env = test_env();
        env.process_instruction(
            &instruction,
            &accounts,
            &[
                Check::success(),
                Check::account(&Pubkey::from(group_address))
                    .data(expected_group.pack().as_ref())
                    .build(),
                Check::account(&Pubkey::from(node_address))
                    .data(Node {
                        latest_sync_epoch: curr,
                        ..node
                    }.pack().as_ref())
                    .build(),
            ],
        );
    }

    // tapenet readies a group at f + 1 synced spools and leaves it waiting at f
    #[test]
    fn sync_transition() {
        transition_case(permissionless_system(), 7, 6, true);
        transition_case(permissionless_system(), 7, 5, false);
    }

    // after the group count drops, tapenet readies a group at the certify count
    #[test]
    fn shrink_transition() {
        let shrunk = System { previous_group_count: 2, ..permissionless_system() };

        transition_case(shrunk, 7, 6, false);
        transition_case(shrunk, 13, 12, false);
        transition_case(shrunk, 14, 13, true);
    }

    // a 3-of-4 cluster readies a group on its third synced spool
    #[test]
    fn trusted_transition() {
        let narrow = trusted_system(ErasureCode::ReedSolomon(RSParams::new(3, 1)));

        transition_case(narrow, 3, 2, true);
        transition_case(narrow, 3, 1, false);
    }

    // a spool past the network's width has no owner to sync it
    #[test]
    fn past_width() {
        let fee_payer = Pubkey::new_unique();
        let authority = Pubkey::new_unique();
        let curr = EpochNumber(42);
        let group_id = GroupIndex(0);
        let (node_address, _) = node_pda(authority.into());

        let system = System {
            current_epoch: curr,
            target_group_count: 1,
            live_group_count: 1,
            ..trusted_system(ErasureCode::ReedSolomon(RSParams::new(3, 1)))
        };
        let epoch = Epoch {
            id: curr,
            state: EpochState {
                phase: EpochPhase::Sync as u64,
                ..EpochState::zeroed()
            },
            total_groups: 1,
            ..Epoch::zeroed()
        };
        let group = group_with_owner(curr, group_id, 0, node_address);
        let node = Node {
            authority: authority.into(),
            ..Node::zeroed()
        };

        let instruction = build_sync_spool_ix(
            fee_payer.into(),
            authority.into(),
            node_address,
            curr,
            group_id,
            group_id.spool_at(4),
        );
        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            sol(authority, 0),
            pda(system_pda().0, system.pack(), tapedrive::ID),
            pda(epoch_pda(curr).0, epoch.pack(), tapedrive::ID),
            pda(group_pda(curr, group_id).0, group.pack(), tapedrive::ID),
            pda(node_address, node.pack(), tapedrive::ID),
        ];

        test_env().process_instruction(
            &instruction,
            &accounts,
            &[Check::err(TapeError::NotInCommittee.into())],
        );
    }

    fn transition_case(base: System, slice_in_group: usize, synced_before: usize, is_ready: bool) {
        let fee_payer = Pubkey::new_unique();
        let authority = Pubkey::new_unique();

        let curr = EpochNumber(42);
        let group_id = GroupIndex(0);
        let spool = group_id.spool_at(slice_in_group);

        let (system_address, _) = system_pda();
        let (epoch_address, _) = epoch_pda(curr);
        let (group_address, _) = group_pda(curr, group_id);
        let (node_address, _) = node_pda(authority.into());

        let system = System {
            current_epoch: curr,
            target_group_count: 1,
            live_group_count: 1,
            ..base
        };

        let epoch = Epoch {
            id: curr,
            state: EpochState {
                phase: EpochPhase::Sync as u64,
                ..EpochState::zeroed()
            },
            total_groups: 1,
            ..Epoch::zeroed()
        };

        let mut group = group_with_owner(curr, group_id, slice_in_group, node_address);
        for slot in 0..synced_before {
            group.synced.set(slot);
        }

        let node = Node {
            authority: authority.into(),
            ..Node::zeroed()
        };

        let instruction = build_sync_spool_ix(
            fee_payer.into(),
            authority.into(),
            node_address,
            curr,
            group_id,
            spool,
        );

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            sol(authority, 0),
            pda(system_address, system.pack(), tapedrive::ID),
            pda(epoch_address, epoch.pack(), tapedrive::ID),
            pda(group_address, group.pack(), tapedrive::ID),
            pda(node_address, node.pack(), tapedrive::ID),
        ];

        let mut expected_group = group;
        expected_group.synced.set(slice_in_group);

        let mut expected_epoch = epoch;
        if is_ready {
            expected_epoch.state.phase = EpochPhase::Snapshot as u64;
            expected_epoch.state.synced_count = 1;
        }

        let env = test_env();
        env.process_instruction(
            &instruction,
            &accounts,
            &[
                Check::success(),
                Check::account(&Pubkey::from(epoch_address))
                    .data(expected_epoch.pack().as_ref())
                    .build(),
                Check::account(&Pubkey::from(group_address))
                    .data(expected_group.pack().as_ref())
                    .build(),
            ],
        );
    }
}
