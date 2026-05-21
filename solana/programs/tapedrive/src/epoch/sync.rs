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

    let spool = SpoolIndex::unpack(args.spool);
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

    let was_supermajority = is_supermajority(
        group.synced.count_ones() as u64,
        GROUP_SIZE as u64,
    );
    group.synced.set(slice_idx);
    let now_supermajority = is_supermajority(
        group.synced.count_ones() as u64,
        GROUP_SIZE as u64,
    );

    if !was_supermajority && now_supermajority {
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
            ..System::zeroed()
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
}
