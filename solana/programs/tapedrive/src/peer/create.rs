use solana_program::entrypoint::MAX_PERMITTED_DATA_INCREASE;
use tape_solana::*;
use tape_api::dynamic::DynamicState;
use tape_api::program::prelude::*;
use tape_api::state::PeerSet;

pub fn process_create_peer_set(accounts: &[AccountInfo<'_>], _data: &[u8]) -> ProgramResult {
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
    let system = system_info
        .is_system()?
        .as_account::<System>(&tapedrive::ID)?;

    let (peer_set_address, _) = peer_set_pda();
    peer_set_info
        .is_empty()?
        .is_writable()?
        .has_address(&peer_set_address.into())?;

    let genesis_capacity = if system.current_epoch == EpochNumber(0) {
        GROUP_SIZE as u64
    } else {
        0
    };

    let initial_size = if genesis_capacity > 0 {
        PeerSet::size_for_capacity(genesis_capacity)
    } else {
        MAX_PERMITTED_DATA_INCREASE.min(PeerSet::get_size())
    };

    create_account_with_size::<PeerSet>(
        peer_set_info,
        system_program_info,
        fee_payer_info,
        initial_size,
        &tapedrive::ID,
        &[PEER_SET],
        PEER_SET_BUMP,
    )?;

    if genesis_capacity > 0 {
        let peer_set = PeerSet::header_mut(peer_set_info, &tapedrive::ID)?;
        peer_set.peers = Tail::empty(genesis_capacity);
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tape_test::*;

    #[test]
    fn create_peer_set() {
        let fee_payer = Pubkey::new_unique();

        let (system_address, _) = system_pda();
        let (peer_set_address, _) = peer_set_pda();

        let system = System::zeroed();

        let instruction = build_create_peer_set_ix(fee_payer.into());

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            pda(system_address, system.pack(), tapedrive::ID),
            empty(peer_set_address),
            system_program(),
            rent_sysvar(),
        ];

        let initial_size = PeerSet::size_for_capacity(GROUP_SIZE as u64);

        let env = test_env();
        env.process_instruction(
            &instruction,
            &accounts,
            &[
                Check::success(),
                Check::account(&Pubkey::from(peer_set_address))
                    .space(initial_size)
                    .owner(&tapedrive::ID)
                    .data(PeerSet {
                        peers: Tail::empty(GROUP_SIZE as u64),
                    }.pack_with(&[]).as_ref())
                    .build(),
            ],
        );
    }
}
