use tape_api::event::TapeRenewed;
use tape_api::program::prelude::*;

use crate::tape::helpers::{
    TapeSpec,
    authorize_tape_authority,
    collect_payment,
    schedule_reservation,
    verified_tape_address,
};
use crate::track::helpers::slot_hash_seed;

pub fn process_renew_tape(accounts: &[AccountInfo<'_>], data: &[u8]) -> ProgramResult {
    let args = RenewTape::try_from_bytes(data)?;
    let [
        fee_payer_info,
        authority_info,
        authority_ata_info,

        tape_info,
        system_info,
        archive_info,
        archive_ata_info,
        mint_info,

        token_program_info,
        slot_hashes_info,
    ] = accounts else {
        return Err(ProgramError::NotEnoughAccountKeys);
    };

    fee_payer_info
        .is_signer()?
        .is_writable()?;

    authority_info
        .is_signer()?;

    authority_ata_info
        .is_writable()?
        .as_token_account()?
        .assert(|t| t.owner() == *authority_info.key)?
        .assert(|t| t.mint() == MINT_ADDRESS.into())?;

    token_program_info
        .is_program(&spl_token::ID)?;

    let system = system_info
        .is_system()?
        .as_account::<System>(&tapedrive::ID)?;

    archive_info
        .is_writable()?
        .is_archive()?;

    let archive = archive_info.as_account_mut::<Archive>(&tapedrive::ID)?;

    archive_ata_info
        .is_writable()?
        .is_archive_ata()?;

    mint_info
        .is_writable()?
        .is_mint()?;

    let tape = tape_info
        .is_writable()?
        .as_account_mut::<Tape>(&tapedrive::ID)?;

    if tape.is_system() {
        return Err(TapeError::UnexpectedState.into());
    }

    let tape_address = verified_tape_address(tape_info, tape)?;
    authorize_tape_authority(tape, (*authority_info.key).into())?;

    // A live tape is extended. Only an expired one is reserved again.
    let epoch = current_epoch(system);
    if !tape.is_expired(epoch) {
        return Err(TapeError::TapeNotExpired.into());
    }

    let spec = TapeSpec {
        address: tape_address,
        authority: tape.authority,
        flags: tape.flags,
        capacity: args.storage_units,
        expiry_epoch: args.expiry_epoch,
    };
    let (spec, payment) = schedule_reservation(system, archive, spec)?;

    collect_payment(
        authority_info,
        authority_ata_info,
        archive_ata_info,
        mint_info,
        token_program_info,
        payment,
    )?;

    // Nodes dropped the tape's data when it expired, so its tracks leave the tree.
    tape.rewind(slot_hash_seed(slot_hashes_info)?);
    tape.capacity = spec.capacity;
    tape.active_epoch = epoch;
    tape.expiry_epoch = spec.expiry_epoch;

    TapeRenewed {
        tape: tape_address,
        id: tape.id,
        capacity: tape.capacity,
        active_epoch: tape.active_epoch,
        expiry_epoch: tape.expiry_epoch,
        cost: payment.cost,
        burned: payment.burned,
        scheduled: payment.scheduled,
        tree_start: tape.tracks.tree_start(),
        nonce: tape.nonce,
    }
    .log();

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use solana_account::Account;
    use tape_core::spooler::VirtualGroup;
    use tape_core::tape::TapeFlags;
    use tape_core::track::archive::TrackArchive;
    use tape_core::track::types::{CompressedTrack, TrackFlags, TrackKind, TrackState};
    use tape_crypto::hash::hashv;
    use tape_crypto::merkle::MerkleTree;
    use tape_test::*;

    use crate::tape::helpers::schedule_capacity;

    const CURRENT_EPOCH: EpochNumber = EpochNumber(60);
    const NEW_EXPIRY: EpochNumber = EpochNumber(62);
    const SLOT_HASH: Hash = Hash([0x5A; 32]);
    const TAPES_RESERVED: u64 = 7;
    const TOKENS: u64 = 1_000_000_000_000;

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

    fn archive() -> Archive {
        Archive {
            storage_capacity: StorageUnits::mb(1000),
            storage_price: TAPE::from("0.0001"),
            max_reservation_epochs: 87_600,
            tape_count: TAPES_RESERVED,
            schedule: EpochSchedule::new_at(CURRENT_EPOCH),
            ..Archive::zeroed()
        }
    }

    /// A user tape that expired at epoch 50 and still has two tracks in its tree
    fn expired_tape(authority: Pubkey) -> Tape {
        let (tape_address, _) = tape_pda(authority.into());
        let mut tape = Tape {
            id: TapeNumber(3),
            authority: authority.into(),
            delegate: Pubkey::new_unique().into(),
            capacity: StorageUnits::mb(100),
            active_epoch: EpochNumber(40),
            expiry_epoch: EpochNumber(50),
            nonce: Hash([0x04; 32]),
            ..Tape::zeroed()
        };

        for _ in 0..2 {
            let track = CompressedTrack {
                tape: tape_address,
                tape_number: tape.id,
                track_number: tape.tracks.next_number(),
                key: Hash::new_unique(),
                kind: TrackKind::Coded as u64,
                state: TrackState::Certified as u64,
                flags: TrackFlags::NONE,
                size: StorageUnits::mb(1),
                group: VirtualGroup(0),
                value_hash: Hash([0x22; 32]),
            };
            tape.write_track(&track).expect("write track");
        }

        tape
    }

    fn renew_case(tape: Tape, signer: Pubkey, outcome: Result<(), ProgramError>) {
        let fee_payer = Pubkey::new_unique();
        let storage_units = StorageUnits::mb(200);

        let (tape_address, _) = tape_pda(tape.authority);
        let (system_address, _) = system_pda();
        let (archive_address, _) = archive_pda();
        let (archive_ata, _) = archive_ata();
        let signer_ata = ata_address(&signer);

        let instruction = build_renew_tape_ix(
            fee_payer.into(),
            signer.into(),
            tape_address,
            storage_units,
            NEW_EXPIRY,
        );

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            sol(signer, 0),
            token(signer_ata, signer, TOKENS),

            pda(tape_address, tape.pack(), tapedrive::ID),
            pda(system_address, system().pack(), tapedrive::ID),
            pda(archive_address, archive().pack(), tapedrive::ID),
            token(archive_ata, archive_address, 0),
            mint(0),

            token_program(),
            slot_hashes_account(),
        ];

        let env = test_env();
        if let Err(error) = outcome {
            env.process_instruction(
                &instruction,
                &accounts,
                &[
                    Check::err(error),
                    Check::account(&Pubkey::from(tape_address))
                        .data(tape.pack().as_ref())
                        .build(),
                ],
            );
            return;
        }

        // The tape keeps its number, so the archive's count of tapes stays.
        let mut expected_archive = archive();
        let payment = schedule_capacity(
            &system(),
            &mut expected_archive,
            storage_units,
            CURRENT_EPOCH,
            NEW_EXPIRY,
        )
        .expect("the renewed reservation");
        assert!(payment.burned.is_zero());

        let expected_tape = Tape {
            capacity: storage_units,
            used: StorageUnits::zero(),
            active_epoch: CURRENT_EPOCH,
            expiry_epoch: NEW_EXPIRY,
            nonce: hashv(&[&tape.nonce.to_bytes(), &SLOT_HASH.to_bytes()]),
            tracks: TrackArchive {
                tree: MerkleTree::zeroed(),
                next_number: tape.tracks.next_number(),
                num_tracks: 0,
            },
            ..tape
        };

        env.process_instruction(
            &instruction,
            &accounts,
            &[
                Check::success(),
                Check::account(&Pubkey::from(tape_address))
                    .data(expected_tape.pack().as_ref())
                    .build(),
                Check::account(&Pubkey::from(archive_address))
                    .data(expected_archive.pack().as_ref())
                    .build(),
                Check::account(&signer_ata)
                    .data(token(signer_ata, signer, TOKENS - payment.cost.as_u64()).1.data.as_ref())
                    .build(),
                Check::account(&Pubkey::from(archive_ata))
                    .data(token(archive_ata, archive_address, payment.scheduled.as_u64()).1.data.as_ref())
                    .build(),
            ],
        );
    }

    // an expired tape comes back under its number, delegate and track counter,
    // with an empty tree, a new reservation and the next nonce
    #[test]
    fn renew_tape() {
        let authority = Pubkey::new_unique();

        renew_case(expired_tape(authority), authority, Ok(()));
    }

    // an immutable tape is renewed once it has expired, and stays immutable
    #[test]
    fn immutable_tape() {
        let authority = Pubkey::new_unique();
        let tape = Tape {
            flags: TapeFlags::HIDDEN | TapeFlags::IMMUTABLE,
            ..expired_tape(authority)
        };

        renew_case(tape, authority, Ok(()));
    }

    // a tape that has not expired is extended, and a renew refuses it
    #[test]
    fn rejects_live_tape() {
        let authority = Pubkey::new_unique();
        let tape = Tape {
            expiry_epoch: CURRENT_EPOCH.next(),
            ..expired_tape(authority)
        };

        renew_case(tape, authority, Err(TapeError::TapeNotExpired.into()));
    }

    // a delegate can't renew the tape
    #[test]
    fn rejects_delegate() {
        let authority = Pubkey::new_unique();
        let delegate = Pubkey::new_unique();
        let tape = Tape {
            delegate: delegate.into(),
            ..expired_tape(authority)
        };

        renew_case(tape, delegate, Err(ProgramError::InvalidAccountData));
    }

    // a system tape is never renewed
    #[test]
    fn rejects_system_tape() {
        let authority = Pubkey::new_unique();
        let tape = Tape {
            flags: TapeFlags::SYSTEM,
            ..expired_tape(authority)
        };

        renew_case(tape, authority, Err(TapeError::UnexpectedState.into()));
    }
}
