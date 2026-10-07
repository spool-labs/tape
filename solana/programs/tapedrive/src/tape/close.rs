use tape_api::event::TapeClosed;
use tape_api::program::prelude::*;

use crate::tape::helpers::{
    authorize_tape_authority,
    verified_tape_address,
};

pub fn process_close_tape(accounts: &[AccountInfo<'_>], data: &[u8]) -> ProgramResult {
    let _args = CloseTape::try_from_bytes(data)?;
    let [
        fee_payer_info,
        authority_info,
        tape_info,
        system_info,
    ] = accounts else {
        return Err(ProgramError::NotEnoughAccountKeys);
    };

    fee_payer_info
        .is_signer()?
        .is_writable()?;

    authority_info
        .is_signer()?
        .is_writable()?;

    let system = system_info
        .is_system()?
        .as_account::<System>(&tapedrive::ID)?;

    let tape = tape_info
        .is_writable()?
        .as_account::<Tape>(&tapedrive::ID)?;

    if tape.is_system() {
        return Err(TapeError::UnexpectedState.into());
    }

    let tape_address = verified_tape_address(tape_info, tape)?;
    authorize_tape_authority(tape, (*authority_info.key).into())?;

    // An immutable tape keeps its tracks for as long as it is reserved.
    let epoch = current_epoch(system);
    if tape.is_immutable() && !tape.is_expired(epoch) {
        return Err(TapeError::TapeImmutable.into());
    }

    TapeClosed {
        tape: tape_address,
        id: tape.id,
        epoch,
    }
    .log();

    // A tape does not record who paid its rent, so the rent goes to the authority.
    // The capacity the tape reserved stays scheduled through its expiry.
    close_account(tape_info, authority_info)
}

#[cfg(test)]
mod tests {
    use super::*;
    use solana_account::Account;
    use tape_core::spooler::VirtualGroup;
    use tape_core::tape::{user_tape_number, TapeFlags};
    use tape_core::track::types::{CompressedTrack, TrackFlags, TrackKind, TrackState};
    use tape_crypto::hash::hashv;
    use tape_test::*;

    use crate::tape::helpers::schedule_capacity;

    const CURRENT_EPOCH: EpochNumber = EpochNumber(60);
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

    fn system() -> System {
        System {
            current_epoch: CURRENT_EPOCH,
            ..System::zeroed()
        }
    }

    /// A live user tape that holds two tracks, one of them immutable
    fn live_tape(authority: Pubkey) -> Tape {
        let (tape_address, _) = tape_pda(authority.into());
        let mut tape = Tape {
            id: TapeNumber(7),
            authority: authority.into(),
            capacity: StorageUnits::mb(100),
            active_epoch: EpochNumber(40),
            expiry_epoch: EpochNumber(80),
            nonce: Hash([0x04; 32]),
            ..Tape::zeroed()
        };

        for (number, flags) in [(0, TrackFlags::NONE), (1, TrackFlags::IMMUTABLE)] {
            let track = CompressedTrack {
                tape: tape_address,
                tape_number: tape.id,
                track_number: TrackNumber(number),
                key: Hash([number as u8; 32]),
                kind: TrackKind::Coded as u64,
                state: TrackState::Certified as u64,
                flags,
                size: StorageUnits::mb(1),
                group: VirtualGroup(0),
                value_hash: Hash([0x22; 32]),
            };
            tape.write_track(&track).expect("write track");
        }

        tape
    }

    fn close_case(tape: Tape, signer: Pubkey, outcome: Result<(), ProgramError>) {
        let fee_payer = Pubkey::new_unique();
        let (tape_address, _) = tape_pda(tape.authority);
        let (system_address, _) = system_pda();

        let instruction = build_close_tape_ix(fee_payer.into(), signer.into(), tape_address);

        let tape_account = pda(tape_address, tape.pack(), tapedrive::ID);
        let tape_rent = tape_account.1.lamports;

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            sol(signer, 0),
            tape_account,
            pda(system_address, system().pack(), tapedrive::ID),
        ];

        let env = test_env();
        if let Err(error) = outcome {
            env.process_instruction(
                &instruction,
                &accounts,
                &[
                    Check::err(error),
                    Check::account(&Pubkey::from(tape_address))
                        .owner(&tapedrive::ID)
                        .data(tape.pack().as_ref())
                        .build(),
                ],
            );
            return;
        }

        env.process_instruction(
            &instruction,
            &accounts,
            &[
                Check::success(),
                Check::account(&Pubkey::from(tape_address)).closed().build(),
                Check::account(&signer).lamports(tape_rent).build(),
            ],
        );
    }

    // the authority closes a tape that still holds tracks, an immutable one
    // among them, and gets the rent
    #[test]
    fn close_tape() {
        let authority = Pubkey::new_unique();

        close_case(live_tape(authority), authority, Ok(()));
    }

    // an expired tape closes too
    #[test]
    fn close_expired() {
        let authority = Pubkey::new_unique();
        let tape = Tape {
            expiry_epoch: CURRENT_EPOCH,
            ..live_tape(authority)
        };

        close_case(tape, authority, Ok(()));
    }

    // an immutable tape stays open until it expires
    #[test]
    fn immutable_tape() {
        let authority = Pubkey::new_unique();
        let immutable = Tape {
            flags: TapeFlags::IMMUTABLE,
            ..live_tape(authority)
        };
        let expired = Tape {
            expiry_epoch: CURRENT_EPOCH,
            ..immutable
        };

        close_case(immutable, authority, Err(TapeError::TapeImmutable.into()));
        close_case(expired, authority, Ok(()));
    }

    // a delegate can delete single tracks and can't close the tape
    #[test]
    fn rejects_delegate() {
        let authority = Pubkey::new_unique();
        let delegate = Pubkey::new_unique();
        let tape = Tape {
            delegate: delegate.into(),
            ..live_tape(authority)
        };

        close_case(tape, delegate, Err(ProgramError::InvalidAccountData));
    }

    // a system tape is never closed
    #[test]
    fn rejects_system_tape() {
        let authority = Pubkey::new_unique();
        let tape = Tape {
            flags: TapeFlags::SYSTEM,
            ..live_tape(authority)
        };

        close_case(tape, authority, Err(TapeError::UnexpectedState.into()));
    }

    // an address closed and reserved again holds a tape with a new number, a
    // new nonce and no tracks, and the closed tape's capacity stays scheduled
    #[test]
    fn reserve_again() {
        let fee_payer = Pubkey::new_unique();
        let authority = Pubkey::new_unique();
        let tape = live_tape(authority);
        let storage_units = StorageUnits::mb(100);
        let expiry_epoch = EpochNumber(62);
        let tapes_reserved = 7;
        let tokens = 1_000_000_000_000;

        let (tape_address, _) = tape_pda(authority.into());
        let (system_address, _) = system_pda();
        let (archive_address, _) = archive_pda();
        let (archive_ata, _) = archive_ata();
        let authority_ata = ata_address(&authority);

        let mut archive = Archive {
            storage_capacity: StorageUnits::mb(1000),
            storage_price: TAPE::from("0.0001"),
            max_reservation_epochs: 87_600,
            tape_count: tapes_reserved,
            schedule: EpochSchedule::new_at(CURRENT_EPOCH),
            ..Archive::zeroed()
        };
        schedule_capacity(&system(), &mut archive, tape.capacity, CURRENT_EPOCH, tape.expiry_epoch)
            .expect("the closed tape's reservation");

        let close = build_close_tape_ix(fee_payer.into(), authority.into(), tape_address);
        let reserve = build_reserve_tape_ix(
            fee_payer.into(),
            authority.into(),
            storage_units,
            expiry_epoch,
            TapeFlags::NONE,
        );

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            sol(authority, 0),
            token(authority_ata, authority, tokens),

            pda(tape_address, tape.pack(), tapedrive::ID),
            pda(system_address, system().pack(), tapedrive::ID),
            pda(archive_address, archive.pack(), tapedrive::ID),
            token(archive_ata, archive_address, 0),
            mint(0),

            token_program(),
            system_program(),
            rent_sysvar(),
            slot_hashes_account(),
        ];

        let mut expected_archive = archive;
        schedule_capacity(&system(), &mut expected_archive, storage_units, CURRENT_EPOCH, expiry_epoch)
            .expect("the new tape's reservation");
        expected_archive.tape_count = tapes_reserved + 1;

        let new_id = user_tape_number(tapes_reserved + 1).expect("tape number");
        let expected_tape = Tape {
            id: new_id,
            authority: authority.into(),
            capacity: storage_units,
            active_epoch: CURRENT_EPOCH,
            expiry_epoch,
            nonce: hashv(&[&SLOT_HASH.to_bytes(), &new_id.pack()]),
            ..Tape::zeroed()
        };

        let env = test_env();
        env.process_chain(
            &[
                (
                    &close,
                    &[
                        Check::success(),
                        Check::account(&Pubkey::from(tape_address)).closed().build(),
                    ],
                ),
                (
                    &reserve,
                    &[
                        Check::success(),
                        Check::account(&Pubkey::from(tape_address))
                            .owner(&tapedrive::ID)
                            .data(expected_tape.pack().as_ref())
                            .build(),
                        Check::account(&Pubkey::from(archive_address))
                            .data(expected_archive.pack().as_ref())
                            .build(),
                    ],
                ),
            ],
            &accounts,
        );
    }
}
