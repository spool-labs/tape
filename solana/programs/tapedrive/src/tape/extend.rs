use tape_solana::*;
use tape_api::event::TapeExtended;
use tape_api::program::prelude::*;

use crate::tape::helpers::{collect_payment, schedule_capacity, SchedulePayment};

pub fn process_extend_tape_capacity(accounts: &[AccountInfo<'_>], data: &[u8]) -> ProgramResult {
    let args = ExtendTapeCapacity::try_from_bytes(data)?;
    let [
        fee_payer_info,
        payer_info,
        payer_ata_info,

        tape_info,
        system_info,
        archive_info,
        archive_ata_info,
        mint_info,

        token_program_info,
    ] = accounts else {
        return Err(ProgramError::NotEnoughAccountKeys);
    };

    fee_payer_info
        .is_signer()?
        .is_writable()?;

    payer_info
        .is_signer()?;

    payer_ata_info
        .is_writable()?
        .as_token_account()?
        .assert(|t| t.owner() == *payer_info.key)?
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

    // Permissionless: any program-owned tape qualifies, including system
    // tapes at non-cassette addresses, so no PDA or authority check here.
    let tape = tape_info
        .is_writable()?
        .as_account_mut::<Tape>(&tapedrive::ID)?;

    let current = current_epoch(system);
    if current >= tape.expiry_epoch {
        return Err(TapeError::TapeExpired.into());
    }

    // Only the remaining window is charged; past epochs were already paid
    // for at the old capacity.
    let start = current.max(tape.active_epoch);
    let payment = schedule_capacity(system, archive, args.units, start, tape.expiry_epoch)?;

    tape.capacity = tape.capacity
        .checked_add(args.units)
        .ok_or(ProgramError::ArithmeticOverflow)?;

    settle_extension(
        payer_info,
        payer_ata_info,
        archive_ata_info,
        mint_info,
        token_program_info,
        tape_info,
        tape,
        payment,
    )
}

pub fn process_extend_tape_expiry(accounts: &[AccountInfo<'_>], data: &[u8]) -> ProgramResult {
    let args = ExtendTapeExpiry::try_from_bytes(data)?;
    let [
        fee_payer_info,
        payer_info,
        payer_ata_info,

        tape_info,
        system_info,
        archive_info,
        archive_ata_info,
        mint_info,

        token_program_info,
    ] = accounts else {
        return Err(ProgramError::NotEnoughAccountKeys);
    };

    fee_payer_info
        .is_signer()?
        .is_writable()?;

    payer_info
        .is_signer()?;

    payer_ata_info
        .is_writable()?
        .as_token_account()?
        .assert(|t| t.owner() == *payer_info.key)?
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

    // Permissionless: any program-owned tape qualifies, including system
    // tapes at non-cassette addresses, so no PDA or authority check here.
    let tape = tape_info
        .is_writable()?
        .as_account_mut::<Tape>(&tapedrive::ID)?;

    // An expired tape is destroyable and must not also be extendable.
    let current = current_epoch(system);
    if current >= tape.expiry_epoch {
        return Err(TapeError::TapeExpired.into());
    }

    let payment = schedule_capacity(
        system,
        archive,
        tape.capacity,
        tape.expiry_epoch,
        args.new_expiry_epoch,
    )?;

    tape.expiry_epoch = args.new_expiry_epoch;

    settle_extension(
        payer_info,
        payer_ata_info,
        archive_ata_info,
        mint_info,
        token_program_info,
        tape_info,
        tape,
        payment,
    )
}

fn settle_extension<'account_info>(
    payer_info: &AccountInfo<'account_info>,
    payer_ata_info: &AccountInfo<'account_info>,
    archive_ata_info: &AccountInfo<'account_info>,
    mint_info: &AccountInfo<'account_info>,
    token_program_info: &AccountInfo<'account_info>,
    tape_info: &AccountInfo<'account_info>,
    tape: &Tape,
    payment: SchedulePayment,
) -> ProgramResult {
    collect_payment(
        payer_info,
        payer_ata_info,
        archive_ata_info,
        mint_info,
        token_program_info,
        payment,
    )?;

    TapeExtended {
        tape: (*tape_info.key).into(),
        payer: (*payer_info.key).into(),
        capacity: tape.capacity,
        active_epoch: tape.active_epoch,
        expiry_epoch: tape.expiry_epoch,
        cost: payment.cost,
        burned: payment.burned,
        scheduled: payment.scheduled,
    }
    .log();

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use solana_account::Account;
    use tape_test::*;

    const PRICE: u64 = 100;

    fn base_archive(current: EpochNumber) -> Archive {
        Archive {
            storage_capacity: StorageUnits::mb(1000),
            storage_price: TAPE(PRICE),
            schedule: EpochSchedule::new_at(current),
            ..Archive::zeroed()
        }
    }

    fn base_system(current: EpochNumber) -> System {
        System {
            current_epoch: current,
            ..System::zeroed()
        }
    }

    fn user_tape(authority: Pubkey) -> Tape {
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

    fn expect_reject(
        fee_payer: Pubkey,
        payer: Pubkey,
        tape_address: Address,
        tape: &Tape,
        current: EpochNumber,
        instruction: Instruction,
        expected: ProgramError,
    ) {
        let system = base_system(current);
        let archive = base_archive(current);
        let accounts = extend_accounts(
            fee_payer, payer, tape_address, tape, &system, &archive, 1_000_000,
        );

        let env = test_env();
        env.process_instruction(&instruction, &accounts, &[Check::err(expected)]);
    }

    fn extend_accounts(
        fee_payer: Pubkey,
        payer: Pubkey,
        tape_address: Address,
        tape: &Tape,
        system: &System,
        archive: &Archive,
        payer_balance: u64,
    ) -> Vec<(Pubkey, Account)> {
        let (system_address, _) = system_pda();
        let (archive_address, _) = archive_pda();
        let (archive_ata_address, _) = archive_ata();
        let payer_ata = ata_address(&payer);

        vec![
            sol(fee_payer, 1_000_000_000),
            sol(payer, 0),
            token(payer_ata, payer, payer_balance),

            pda(tape_address, tape.pack(), tapedrive::ID),
            pda(system_address, system.pack(), tapedrive::ID),
            pda(archive_address, archive.pack(), tapedrive::ID),
            token(archive_ata_address, archive_address, 0),
            mint(0),

            token_program(),
        ]
    }

    // expiry extend pays for the added window and moves the expiry
    #[test]
    fn extend_expiry() {
        let fee_payer = Pubkey::new_unique();
        let payer = Pubkey::new_unique();
        let authority = Pubkey::new_unique();
        let (tape_address, _) = tape_pda(authority.into());

        let capacity = StorageUnits::mb(100);
        let tape = Tape {
            id: TapeNumber(1),
            authority: authority.into(),
            capacity,
            used: StorageUnits::zero(),
            active_epoch: EpochNumber(43),
            expiry_epoch: EpochNumber(45),
            ..Tape::zeroed()
        };

        let current = EpochNumber(0);
        let system = base_system(current);
        let archive = base_archive(current);

        let new_expiry = EpochNumber(47);
        let cost = PRICE * capacity.to_mb() * 2;
        let fee_per_epoch = TAPE(PRICE * capacity.to_mb());

        let mut expected_archive = archive.clone();
        expected_archive
            .schedule
            .reserve_capacity(capacity, fee_per_epoch, EpochNumber(45), new_expiry)
            .expect("schedule extension window");

        let expected_tape = Tape {
            expiry_epoch: new_expiry,
            ..tape
        };

        let instruction = build_extend_tape_expiry_ix(
            fee_payer.into(),
            payer.into(),
            tape_address,
            new_expiry,
        );
        let accounts = extend_accounts(
            fee_payer, payer, tape_address, &tape, &system, &archive, cost,
        );

        let (archive_address, _) = archive_pda();
        let (archive_ata_address, _) = archive_ata();
        let payer_ata = ata_address(&payer);

        let env = test_env();
        env.process_instruction(
            &instruction,
            &accounts,
            &[
                Check::success(),
                Check::account(&Pubkey::from(tape_address))
                    .data(expected_tape.pack().as_ref()).build(),
                Check::account(&Pubkey::from(archive_address))
                    .data(expected_archive.pack().as_ref()).build(),
                Check::account(&Pubkey::from(payer_ata))
                    .data(token(payer_ata, payer, 0).1.data.as_ref()).build(),
                Check::account(&Pubkey::from(archive_ata_address))
                    .data(token(archive_ata_address, archive_address, cost).1.data.as_ref()).build(),
            ],
        );
    }

    // capacity extend charges only the remaining window on an active tape
    #[test]
    fn extend_capacity() {
        let fee_payer = Pubkey::new_unique();
        let payer = Pubkey::new_unique();
        let authority = Pubkey::new_unique();
        let (tape_address, _) = tape_pda(authority.into());

        let tape = Tape {
            id: TapeNumber(1),
            authority: authority.into(),
            capacity: StorageUnits::mb(100),
            used: StorageUnits::mb(30),
            active_epoch: EpochNumber(40),
            expiry_epoch: EpochNumber(50),
            ..Tape::zeroed()
        };

        let current = EpochNumber(42);
        let system = base_system(current);
        let archive = base_archive(current);

        let units = StorageUnits::mb(50);
        let cost = PRICE * units.to_mb() * 8;
        let fee_per_epoch = TAPE(PRICE * units.to_mb());

        let mut expected_archive = archive.clone();
        expected_archive
            .schedule
            .reserve_capacity(units, fee_per_epoch, current, EpochNumber(50))
            .expect("schedule extension window");

        let expected_tape = Tape {
            capacity: StorageUnits::mb(150),
            ..tape
        };

        let instruction = build_extend_tape_capacity_ix(
            fee_payer.into(),
            payer.into(),
            tape_address,
            units,
        );
        let accounts = extend_accounts(
            fee_payer, payer, tape_address, &tape, &system, &archive, cost,
        );

        let (archive_address, _) = archive_pda();
        let (archive_ata_address, _) = archive_ata();
        let payer_ata = ata_address(&payer);

        let env = test_env();
        env.process_instruction(
            &instruction,
            &accounts,
            &[
                Check::success(),
                Check::account(&Pubkey::from(tape_address))
                    .data(expected_tape.pack().as_ref()).build(),
                Check::account(&Pubkey::from(archive_address))
                    .data(expected_archive.pack().as_ref()).build(),
                Check::account(&Pubkey::from(payer_ata))
                    .data(token(payer_ata, payer, 0).1.data.as_ref()).build(),
                Check::account(&Pubkey::from(archive_ata_address))
                    .data(token(archive_ata_address, archive_address, cost).1.data.as_ref()).build(),
            ],
        );
    }

    // capacity extend before activation charges the full window
    #[test]
    fn extend_before_activation() {
        let fee_payer = Pubkey::new_unique();
        let payer = Pubkey::new_unique();
        let authority = Pubkey::new_unique();
        let (tape_address, _) = tape_pda(authority.into());

        let tape = Tape {
            id: TapeNumber(1),
            authority: authority.into(),
            capacity: StorageUnits::mb(100),
            used: StorageUnits::zero(),
            active_epoch: EpochNumber(43),
            expiry_epoch: EpochNumber(45),
            ..Tape::zeroed()
        };

        let current = EpochNumber(0);
        let system = base_system(current);
        let archive = base_archive(current);

        let units = StorageUnits::mb(10);
        let cost = PRICE * units.to_mb() * 2;
        let fee_per_epoch = TAPE(PRICE * units.to_mb());

        let mut expected_archive = archive.clone();
        expected_archive
            .schedule
            .reserve_capacity(units, fee_per_epoch, EpochNumber(43), EpochNumber(45))
            .expect("schedule extension window");

        let instruction = build_extend_tape_capacity_ix(
            fee_payer.into(),
            payer.into(),
            tape_address,
            units,
        );
        let accounts = extend_accounts(
            fee_payer, payer, tape_address, &tape, &system, &archive, cost,
        );

        let (archive_address, _) = archive_pda();

        let env = test_env();
        env.process_instruction(
            &instruction,
            &accounts,
            &[
                Check::success(),
                Check::account(&Pubkey::from(archive_address))
                    .data(expected_archive.pack().as_ref()).build(),
            ],
        );
    }

    // a system-flagged tape with a finite window can be extended by anyone
    #[test]
    fn extend_system_tape() {
        let fee_payer = Pubkey::new_unique();
        let payer = Pubkey::new_unique();
        let tape_address = Address::from(Pubkey::new_unique());

        let capacity = StorageUnits::mb(10);
        let tape = Tape {
            id: TapeNumber(1),
            flags: TapeFlags::SYSTEM,
            capacity,
            used: StorageUnits::zero(),
            active_epoch: EpochNumber(0),
            expiry_epoch: EpochNumber(5),
            ..Tape::zeroed()
        };

        let current = EpochNumber(0);
        let system = base_system(current);
        let archive = base_archive(current);

        let new_expiry = EpochNumber(6);
        let cost = PRICE * capacity.to_mb();

        let expected_tape = Tape {
            expiry_epoch: new_expiry,
            ..tape
        };

        let instruction = build_extend_tape_expiry_ix(
            fee_payer.into(),
            payer.into(),
            tape_address,
            new_expiry,
        );
        let accounts = extend_accounts(
            fee_payer, payer, tape_address, &tape, &system, &archive, cost,
        );

        let env = test_env();
        env.process_instruction(
            &instruction,
            &accounts,
            &[
                Check::success(),
                Check::account(&Pubkey::from(tape_address))
                    .data(expected_tape.pack().as_ref()).build(),
            ],
        );
    }

    // extending an expired tape is rejected
    #[test]
    fn reject_expired() {
        let fee_payer = Pubkey::new_unique();
        let payer = Pubkey::new_unique();
        let authority = Pubkey::new_unique();
        let (tape_address, _) = tape_pda(authority.into());
        let tape = user_tape(authority);

        let instruction = build_extend_tape_expiry_ix(
            fee_payer.into(),
            payer.into(),
            tape_address,
            EpochNumber(60),
        );

        expect_reject(
            fee_payer,
            payer,
            tape_address,
            &tape,
            EpochNumber(50),
            instruction,
            TapeError::TapeExpired.into(),
        );
    }

    // moving the expiry backwards or nowhere is rejected
    #[test]
    fn reject_shrink() {
        let fee_payer = Pubkey::new_unique();
        let payer = Pubkey::new_unique();
        let authority = Pubkey::new_unique();
        let (tape_address, _) = tape_pda(authority.into());
        let tape = user_tape(authority);

        let instruction = build_extend_tape_expiry_ix(
            fee_payer.into(),
            payer.into(),
            tape_address,
            EpochNumber(50),
        );

        expect_reject(
            fee_payer,
            payer,
            tape_address,
            &tape,
            EpochNumber(42),
            instruction,
            ProgramError::InvalidArgument,
        );
    }

    // zero added capacity is rejected
    #[test]
    fn reject_zero_units() {
        let fee_payer = Pubkey::new_unique();
        let payer = Pubkey::new_unique();
        let authority = Pubkey::new_unique();
        let (tape_address, _) = tape_pda(authority.into());
        let tape = user_tape(authority);

        let instruction = build_extend_tape_capacity_ix(
            fee_payer.into(),
            payer.into(),
            tape_address,
            StorageUnits::zero(),
        );

        expect_reject(
            fee_payer,
            payer,
            tape_address,
            &tape,
            EpochNumber(42),
            instruction,
            ProgramError::InvalidArgument,
        );
    }

    // extending the expiry of a zero-capacity tape is rejected
    #[test]
    fn reject_zero_capacity() {
        let fee_payer = Pubkey::new_unique();
        let payer = Pubkey::new_unique();
        let authority = Pubkey::new_unique();
        let (tape_address, _) = tape_pda(authority.into());
        let tape = Tape {
            capacity: StorageUnits::zero(),
            ..user_tape(authority)
        };

        let instruction = build_extend_tape_expiry_ix(
            fee_payer.into(),
            payer.into(),
            tape_address,
            EpochNumber(60),
        );

        expect_reject(
            fee_payer,
            payer,
            tape_address,
            &tape,
            EpochNumber(42),
            instruction,
            ProgramError::InvalidArgument,
        );
    }

    // an extend that exceeds the global storage budget is rejected
    #[test]
    fn reject_over_budget() {
        let fee_payer = Pubkey::new_unique();
        let payer = Pubkey::new_unique();
        let authority = Pubkey::new_unique();
        let (tape_address, _) = tape_pda(authority.into());
        let tape = user_tape(authority);

        let instruction = build_extend_tape_capacity_ix(
            fee_payer.into(),
            payer.into(),
            tape_address,
            StorageUnits::mb(2000),
        );

        expect_reject(
            fee_payer,
            payer,
            tape_address,
            &tape,
            EpochNumber(42),
            instruction,
            TapeError::NoCapacity.into(),
        );
    }
}
