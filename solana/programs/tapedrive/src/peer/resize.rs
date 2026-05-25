use solana_program::entrypoint::MAX_PERMITTED_DATA_INCREASE;
use tape_api::dynamic::DynamicState;
use tape_api::event::PeerSetResized;
use tape_api::program::prelude::*;
use tape_api::state::PeerSet;

pub fn process_resize_peer_set(accounts: &[AccountInfo<'_>], data: &[u8]) -> ProgramResult {
    let _args = ResizePeerSet::try_from_bytes(data)?;
    let [
        fee_payer_info,
        system_info,
        curr_epoch_info,
        next_epoch_info,
        peer_set_info,
        system_program_info,
        rent_sysvar_info,
    ] = accounts else {
        return Err(ProgramError::NotEnoughAccountKeys);
    };

    fee_payer_info
        .is_signer()?
        .is_writable()?;
    system_program_info
        .is_program(&system_program::ID)?;
    rent_sysvar_info
        .is_sysvar(&sysvar::rent::ID)?;

    let system = system_info
        .is_system()?
        .as_account::<System>(&tapedrive::ID)?;

    let curr = system.current_epoch;
    let next = curr
        .checked_next()
        .ok_or(ProgramError::ArithmeticOverflow)?;

    let curr_epoch = curr_epoch_info
        .is_epoch(curr)?
        .as_account::<Epoch>(&tapedrive::ID)?;
    let next_epoch = next_epoch_info
        .is_epoch(next)?
        .as_account::<Epoch>(&tapedrive::ID)?;

    if curr_epoch.id != curr || next_epoch.id != next {
        return Err(TapeError::BadEpochId.into());
    }

    peer_set_info
        .is_writable()?
        .is_peer_set()?;

    if curr_epoch.state.phase != EpochPhase::Closing as u64 {
        return Err(TapeError::BadEpochState.into());
    }

    if next_epoch.preferences.committee_size < MIN_COMMITTEE_SIZE as u64 {
        return Err(TapeError::InsufficientCommittee.into());
    }

    let target_committee_capacity = system
        .committee_size
        .max(next_epoch.preferences.committee_size);

    let target_capacity = target_committee_capacity.saturating_mul(3);
    let target_size = PeerSet::size_for_capacity(target_capacity);

    let current_size = peer_set_info.data_len();

    let current = {
        let (header, _) = PeerSet::read_mut(peer_set_info, &tapedrive::ID)?;
        header.peers
    };

    if current.would_orphan(target_capacity) {
        return Err(TapeError::ResizeWouldOrphan.into());
    }

    if target_size > current_size {
        let next_size = (current_size + MAX_PERMITTED_DATA_INCREASE).min(target_size);
        resize_account(peer_set_info, system_program_info, fee_payer_info, next_size)?;
    }

    let capacity = if peer_set_info.data_len() >= target_size {
        let header = PeerSet::header_mut(peer_set_info, &tapedrive::ID)?;
        header.peers.capacity = target_capacity;
        target_capacity
    } else {
        PeerSet::header(peer_set_info, &tapedrive::ID)?.peers.capacity
    };

    PeerSetResized {
        capacity: capacity.to_le_bytes(),
    }
    .log();

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tape_test::*;

    #[test]
    fn resize_peer_set_noop_when_full() {
        let fee_payer = Pubkey::new_unique();
        let committee_size: u64 = 128;
        let target_capacity = committee_size * 3;

        let (system_address, _) = system_pda();
        let (peer_set_address, _) = peer_set_pda();

        let curr = EpochNumber(3);
        let next = curr.next();
        let system = System {
            committee_size,
            current_epoch: curr,
            ..System::zeroed()
        };
        let curr_epoch = Epoch {
            id: curr,
            state: EpochState {
                phase: EpochPhase::Closing as u64,
                ..EpochState::zeroed()
            },
            ..Epoch::zeroed()
        };
        let next_epoch = Epoch {
            id: next,
            preferences: NodePreferences {
                committee_size,
                ..NodePreferences::zeroed()
            },
            ..Epoch::zeroed()
        };
        let peer_set = PeerSet { peers: Tail::empty(target_capacity) }
            .pack_with(&[]);

        let instruction = build_resize_peer_set_ix(fee_payer.into(), curr);

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            pda(system_address, system.pack(), tapedrive::ID),
            pda(epoch_pda(curr).0, curr_epoch.pack(), tapedrive::ID),
            pda(epoch_pda(next).0, next_epoch.pack(), tapedrive::ID),
            pda(peer_set_address, peer_set, tapedrive::ID),
            system_program(),
            rent_sysvar(),
        ];

        let env = test_env();
        env.process_instruction(
            &instruction,
            &accounts,
            &[Check::success()],
        );
    }
}
