use tape_api::event::ShareNonceAdvanced;
use tape_api::program::prelude::*;

use crate::tape::helpers::{
    authorize_tape_operator,
    verified_tape_address,
};
use crate::track::helpers::slot_hash_seed;

pub fn process_advance_share_nonce(accounts: &[AccountInfo<'_>], data: &[u8]) -> ProgramResult {
    let _args = AdvanceShareNonce::try_from_bytes(data)?;
    let [
        fee_payer_info,
        signer_info,
        tape_info,
        slot_hashes_info,
    ] = accounts else {
        return Err(ProgramError::NotEnoughAccountKeys);
    };

    fee_payer_info
        .is_signer()?
        .is_writable()?;

    signer_info
        .is_signer()?;

    let tape = tape_info
        .is_writable()?
        .as_account_mut::<Tape>(&tapedrive::ID)?;

    if tape.is_system() {
        return Err(TapeError::UnexpectedState.into());
    }

    verified_tape_address(tape_info, tape)?;
    authorize_tape_operator(tape, (*signer_info.key).into())?;

    tape.advance_share_nonce(slot_hash_seed(slot_hashes_info)?);

    ShareNonceAdvanced {
        tape: (*tape_info.key).into(),
        nonce: tape.share_nonce,
    }
    .log();

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use solana_account::Account;
    use tape_crypto::hash::hashv;
    use tape_crypto::Hash;
    use tape_test::*;

    const SLOT_HASH: Hash = Hash([0x5A; 32]);

    fn slot_hashes_account() -> (Pubkey, Account) {
        let mut data = vec![0u8; 48];
        data[0] = 1;
        data[16..48].copy_from_slice(&SLOT_HASH.to_bytes());
        (sysvar::slot_hashes::ID, Account {
            lamports: 1,
            data,
            owner: sysvar::ID,
            executable: false,
            rent_epoch: 0,
        })
    }

    // the delegate can advance the share nonce, chaining the latest slot hash
    #[test]
    fn advance_nonce() {
        let fee_payer = Pubkey::new_unique();
        let authority = Pubkey::new_unique();
        let delegate = Pubkey::new_unique();
        let (tape_address, _) = tape_pda(authority.into());

        let tape = Tape {
            authority: authority.into(),
            delegate: delegate.into(),
            capacity: StorageUnits::mb(1000),
            share_nonce: Hash([0x04; 32]),
            ..Tape::zeroed()
        };

        let instruction = build_advance_share_nonce_ix(
            fee_payer.into(),
            delegate.into(),
            tape_address,
        );

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            sol(delegate, 0),
            pda(tape_address, tape.pack(), tapedrive::ID),
            slot_hashes_account(),
        ];

        let env = test_env();
        env.process_instruction(
            &instruction,
            &accounts,
            &[
                Check::success(),
                Check::account(&Pubkey::from(tape_address))
                    .data(Tape {
                        share_nonce: hashv(&[&[0x04; 32], &SLOT_HASH.to_bytes()]),
                        ..tape
                    }.pack().as_ref())
                    .build(),
            ],
        );
    }

    // an unrelated signer cannot advance the share nonce
    #[test]
    fn rejects_stranger() {
        let fee_payer = Pubkey::new_unique();
        let authority = Pubkey::new_unique();
        let stranger = Pubkey::new_unique();
        let (tape_address, _) = tape_pda(authority.into());

        let tape = Tape {
            authority: authority.into(),
            capacity: StorageUnits::mb(1000),
            ..Tape::zeroed()
        };

        let instruction = build_advance_share_nonce_ix(
            fee_payer.into(),
            stranger.into(),
            tape_address,
        );

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            sol(stranger, 0),
            pda(tape_address, tape.pack(), tapedrive::ID),
            slot_hashes_account(),
        ];

        let env = test_env();
        env.process_instruction(
            &instruction,
            &accounts,
            &[Check::err(ProgramError::InvalidAccountData)],
        );
    }
}
