use tape_solana::*;
use tape_api::program::prelude::*;
use tape_core::tape::TapeFlags;

use crate::tape::helpers::{TapeSpec, collect_payment, create_tape_account, reserve_archive};
use crate::track::helpers::slot_hash_seed;

pub fn process_reserve_tape(accounts: &[AccountInfo<'_>], data: &[u8]) -> ProgramResult {
    let args = ReserveTape::try_from_bytes(data)?;
    if !TapeFlags::is_reservable(args.flags) {
        return Err(ProgramError::InvalidInstructionData);
    }

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
        system_program_info,
        rent_info,
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
    system_program_info
        .is_program(&system_program::ID)?;
    rent_info
        .is_sysvar(&sysvar::rent::ID)?;

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

    let (tape_address, _) = tape_pda((*authority_info.key).into());

    tape_info
        .is_empty()?
        .is_writable()?
        .has_address(&tape_address.into())?;

    let spec = TapeSpec {
        address: tape_address,
        authority: (*authority_info.key).into(),
        flags: args.flags,
        capacity: args.storage_units,
        active_epoch: args.activation_epoch,
        expiry_epoch: args.expiry_epoch,
    };

    let reservation = reserve_archive(system, archive, spec)?;
    let seeds = [CASSETTE, authority_info.key.as_ref()];

    collect_payment(
        authority_info,
        authority_ata_info,
        archive_ata_info,
        mint_info,
        token_program_info,
        reservation.payment,
    )?;

    create_tape_account(
        tape_info,
        system_program_info,
        fee_payer_info,
        &seeds,
        reservation,
        slot_hash_seed(slot_hashes_info)?,
    )?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use solana_account::Account;
    use tape_crypto::hash::hashv;
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

    /// A user tape that expired at epoch 50
    fn expired_tape(authority: Pubkey) -> Tape {
        Tape {
            id: TapeNumber(1),
            authority: authority.into(),
            capacity: StorageUnits::mb(100),
            used: StorageUnits::zero(),
            active_epoch: EpochNumber(40),
            expiry_epoch: EpochNumber(50),
            ..Tape::zeroed()
        }
    }

    #[test]
    fn reserve_tape() {
        reserve_case(TapeFlags::NONE, None);
    }

    // a tape keeps the hidden and immutable flags it was reserved with
    #[test]
    fn reserve_flagged() {
        reserve_case(TapeFlags::HIDDEN | TapeFlags::IMMUTABLE, None);
    }

    // a user can't reserve a system tape or set a flag the program does not define
    #[test]
    fn reserve_bad_flags() {
        let refused = Some(ProgramError::InvalidInstructionData);

        reserve_case(TapeFlags::SYSTEM, refused.clone());
        reserve_case(1 << 40, refused);
    }

    fn reserve_case(flags: u64, error: Option<ProgramError>) {
        let fee_payer = Pubkey::new_unique();
        let authority = Pubkey::new_unique();

        let storage_units = StorageUnits::mb(100);   // 100 MB
        let start_epoch = EpochNumber(43);         // In the future
        let end_epoch = EpochNumber(45);           // Two epochs duration
        let price_per_unit = TAPE::from("0.0001"); // 0.0001 TAPE per MB

        let instruction = build_reserve_tape_ix(
            fee_payer.into(),
            authority.into(),
            storage_units,
            start_epoch,
            end_epoch,
            flags,
        );

        let (system_address, _) = system_pda();
        let (archive_address, _) = archive_pda();
        let (archive_ata, _) = archive_ata();
        let (tape_address, _) = tape_pda(authority.into());
        let authority_ata = ata_address(&authority);

        let system = System::zeroed();

        let archive = Archive {
            storage_capacity: StorageUnits::mb(1000), // 1000 MB capacity
            storage_price: price_per_unit,
            schedule: EpochSchedule::new(),
            ..Archive::zeroed()
        };

        // Calculate expected cost and state
        let num_epochs = (end_epoch - start_epoch).as_u64(); // 2 epochs
        let single_epoch_price = price_per_unit.as_u64() * storage_units.to_mb(); // 0.0001 * 100 = 0.01 TAPE
        let total_cost = single_epoch_price * num_epochs; // 0.01 * 2 = 0.02 TAPE
        let fee_per_epoch = TAPE(single_epoch_price);

        // Simulate reserve_capacity and add_rewards

        let mut expected_archive = archive;
        expected_archive
            .schedule
            .reserve_capacity(storage_units, fee_per_epoch, start_epoch, end_epoch)
            .unwrap();
        expected_archive.tape_count = 1; // New tape created

        let initial_token_balance: u64 = 100_000_000;

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            sol(authority, 0),
            token(authority_ata, authority, initial_token_balance),

            empty(tape_address),
            pda(system_address, system.pack(), tapedrive::ID),
            pda(archive_address, archive.pack(), tapedrive::ID),
            token(archive_ata, archive_address, 0),
            mint(0),

            token_program(),
            system_program(),
            rent_sysvar(),
            slot_hashes_account(),
        ];

        let env = test_env();
        if let Some(error) = error {
            env.process_instruction(&instruction, &accounts, &[Check::err(error)]);
            return;
        }

        env.process_instruction(
            &instruction,
            &accounts,
            &[
                Check::success(),
                Check::account(&Pubkey::from(tape_address)).data(
                    Tape {
                        id: TapeNumber(1),  // First tape
                        flags,
                        authority: authority.into(),
                        capacity: storage_units,
                        used: StorageUnits::zero(),
                        active_epoch: start_epoch,
                        expiry_epoch: end_epoch,
                        share_nonce: hashv(&[&SLOT_HASH.to_bytes(), &TapeNumber(1).pack()]),
                        ..Tape::zeroed()
                    }.pack().as_ref()
                ).build(),
                Check::account(&Pubkey::from(archive_address)).data(
                    expected_archive.pack().as_ref()
                ).build(),
                Check::account(&authority_ata).data(
                    token(authority_ata, authority, initial_token_balance - total_cost).1.data.as_ref()
                ).build(),
                Check::account(&Pubkey::from(archive_ata)).data(
                    token(archive_ata, archive_address, total_cost).1.data.as_ref()
                ).build(),
            ]
        );
    }

    // no tape instruction closes an expired tape, given the accounts closing one took
    #[test]
    fn expired_stays_open() {
        let fee_payer = Pubkey::new_unique();
        let authority = Pubkey::new_unique();
        let (tape_address, _) = tape_pda(authority.into());
        let (system_address, _) = system_pda();
        let tape = expired_tape(authority);
        let system = System {
            current_epoch: EpochNumber(60),
            ..System::zeroed()
        };

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            sol(authority, 0),

            pda(tape_address, tape.pack(), tapedrive::ID),
            pda(system_address, system.pack(), tapedrive::ID),

            system_program(),
        ];

        let env = test_env();
        for discriminator in TapeInstruction::ReserveTape as u8..TapeInstruction::TrackWrite as u8 {
            let instruction = Instruction {
                program_id: tapedrive::ID,
                accounts: vec![
                    AccountMeta::new(fee_payer, true),
                    AccountMeta::new_readonly(authority, true),

                    AccountMeta::new(tape_address.into(), false),
                    AccountMeta::new_readonly(system_address.into(), false),

                    AccountMeta::new_readonly(system_program::ID, false),
                ],
                data: vec![discriminator],
            };

            env.process_instruction(
                &instruction,
                &accounts,
                &[
                    Check::account(&Pubkey::from(tape_address))
                        .owner(&tapedrive::ID)
                        .data(tape.pack().as_ref())
                        .build(),
                ],
            );
        }
    }

    // reserving an expired tape's address fails, since that tape is still open
    #[test]
    fn reserve_expired() {
        let authority = Pubkey::new_unique();

        reserve_again(authority, expired_tape(authority));
    }

    // a second reserve can't take an immutable tape's flags off
    #[test]
    fn flags_stay() {
        let authority = Pubkey::new_unique();
        let tape = Tape {
            flags: TapeFlags::HIDDEN | TapeFlags::IMMUTABLE,
            ..expired_tape(authority)
        };

        reserve_again(authority, tape);
    }

    fn reserve_again(authority: Pubkey, tape: Tape) {
        let fee_payer = Pubkey::new_unique();

        let instruction = build_reserve_tape_ix(
            fee_payer.into(),
            authority.into(),
            StorageUnits::mb(100),
            EpochNumber(60),
            EpochNumber(62),
            TapeFlags::NONE,
        );

        let (system_address, _) = system_pda();
        let (archive_address, _) = archive_pda();
        let (archive_ata, _) = archive_ata();
        let (tape_address, _) = tape_pda(authority.into());
        let authority_ata = ata_address(&authority);

        let system = System {
            current_epoch: EpochNumber(60),
            ..System::zeroed()
        };

        let archive = Archive {
            storage_capacity: StorageUnits::mb(1000),
            storage_price: TAPE::from("0.0001"),
            schedule: EpochSchedule::new(),
            ..Archive::zeroed()
        };

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            sol(authority, 0),
            token(authority_ata, authority, 100_000_000),

            pda(tape_address, tape.pack(), tapedrive::ID),
            pda(system_address, system.pack(), tapedrive::ID),
            pda(archive_address, archive.pack(), tapedrive::ID),
            token(archive_ata, archive_address, 0),
            mint(0),

            token_program(),
            system_program(),
            rent_sysvar(),
            slot_hashes_account(),
        ];

        let env = test_env();
        env.process_instruction(
            &instruction,
            &accounts,
            &[
                Check::err(ProgramError::AccountAlreadyInitialized),
                Check::account(&Pubkey::from(tape_address))
                    .data(tape.pack().as_ref())
                    .build(),
            ],
        );
    }
}
