use solana_program::entrypoint::MAX_PERMITTED_DATA_INCREASE;
use tape_api::dynamic::DynamicState;
use tape_api::event::PeerSetResized;
use tape_api::program::prelude::*;
use tape_api::state::PeerSet;

pub fn process_resize_peer_set(accounts: &[AccountInfo<'_>], _data: &[u8]) -> ProgramResult {
    let [
        fee_payer_info,
        system_info,
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
    system_info
        .is_system()?;
    peer_set_info
        .is_writable()?
        .is_peer_set()?;

    let target_capacity = system_info
        .as_account::<System>(&tapedrive::ID)?
        .committee_size
        .saturating_mul(3);

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

    if peer_set_info.data_len() >= target_size {
        PeerSet::header_mut(peer_set_info, &tapedrive::ID)?.peers.capacity = target_capacity;
    }

    PeerSetResized {
        capacity: target_capacity.to_le_bytes(),
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

        let system = System {
            committee_size,
            ..System::zeroed()
        };
        let peer_set = PeerSet { peers: Tail::empty(target_capacity) }
            .pack_with(&[]);

        let instruction = build_resize_peer_set_ix(fee_payer.into());

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            pda(system_address, system.pack(), tapedrive::ID),
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
