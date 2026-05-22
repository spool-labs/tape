use tape_api::dynamic::DynamicState;
use tape_api::program::prelude::*;
use tape_api::state::PeerSet;

const BOOTSTRAP_PEER_CAPACITY: u64 = GROUP_SIZE as u64;

pub fn process_create_peer_set(accounts: &[AccountInfo<'_>], _data: &[u8]) -> ProgramResult {
    let [
        fee_payer_info,
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

    let (peer_set_address, _) = peer_set_pda();
    peer_set_info
        .is_empty()?
        .is_writable()?
        .has_address(&peer_set_address.into())?;

    create_account_with_size::<PeerSet>(
        peer_set_info,
        system_program_info,
        fee_payer_info,
        PeerSet::size_for_capacity(BOOTSTRAP_PEER_CAPACITY),
        &tapedrive::ID,
        &[PEER_SET],
        PEER_SET_BUMP,
    )?;

    PeerSet::header_mut(peer_set_info, &tapedrive::ID)?.peers =
        Tail::empty(BOOTSTRAP_PEER_CAPACITY);

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tape_test::*;

    #[test]
    fn create_peer_set() {
        let fee_payer = Pubkey::new_unique();

        let (peer_set_address, _) = peer_set_pda();

        let instruction = build_create_peer_set_ix(fee_payer.into());

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            empty(peer_set_address),
            system_program(),
            rent_sysvar(),
        ];

        let env = test_env();
        env.process_instruction(
            &instruction,
            &accounts,
            &[
                Check::success(),
                Check::account(&Pubkey::from(peer_set_address))
                    .space(PeerSet::size_for_capacity(BOOTSTRAP_PEER_CAPACITY))
                    .owner(&tapedrive::ID)
                    .data(PeerSet {
                        peers: Tail::empty(BOOTSTRAP_PEER_CAPACITY),
                    }.pack_with(&[]).as_ref())
                    .build(),
            ],
        );
    }
}
