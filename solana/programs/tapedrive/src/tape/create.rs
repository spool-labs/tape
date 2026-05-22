use tape_solana::*;
use tape_api::program::prelude::*;

use crate::tape::helpers::{TapeSpec, create_tape_account, reserve_archive};

pub fn process_reserve_tape(accounts: &[AccountInfo<'_>], data: &[u8]) -> ProgramResult {
    let args = ReserveTape::try_from_bytes(data)?;
    let [
        fee_payer_info,
        authority_info,
        authority_ata_info,

        tape_info,
        system_info,
        archive_info,
        archive_ata_info,

        token_program_info,
        system_program_info,
        rent_info,
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

    let (tape_address, _) = tape_pda((*authority_info.key).into());

    tape_info
        .is_empty()?
        .is_writable()?
        .has_address(&tape_address.into())?;

    let spec = TapeSpec {
        address: tape_address,
        authority: (*authority_info.key).into(),
        capacity: StorageUnits::unpack(args.storage_units),
        active_epoch: EpochNumber::unpack(args.activation_epoch),
        expiry_epoch: EpochNumber::unpack(args.expiry_epoch),
    };

    let reservation = reserve_archive(system, archive, spec)?;
    let seeds = [CASSETTE, authority_info.key.as_ref()];

    transfer(
        authority_info,
        authority_ata_info,
        archive_ata_info,
        token_program_info,
        reservation.cost.as_u64(),
    )?;

    create_tape_account(
        tape_info,
        system_program_info,
        fee_payer_info,
        &seeds,
        reservation,
    )?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tape_test::*;

    #[test]
    fn reserve_tape() {
        let fee_payer = Pubkey::new_unique();
        let authority = Pubkey::new_unique();

        let storage_units = StorageUnits::mb(100);   // 100 MB
        let start_epoch = EpochNumber(43);         // In the future
        let end_epoch = EpochNumber(45);           // Two epochs duration
        let price_per_unit = TAPE::from("0.0001"); // 0.0001 TAPE per MB

        let instruction = build_reserve_tape_ix(fee_payer.into(), authority.into(), storage_units, start_epoch, end_epoch);

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

        let mut expected_archive = archive.clone();
        expected_archive
            .schedule
            .reserve_capacity(storage_units, fee_per_epoch, start_epoch, end_epoch)
            .unwrap();
        expected_archive.tape_count = 1; // New tape created

        let initial_token_balance: u64 = 1_000_000;

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            sol(authority, 0),
            token(authority_ata, authority, initial_token_balance),

            empty(tape_address),
            pda(system_address, system.pack(), tapedrive::ID),
            pda(archive_address, archive.pack(), tapedrive::ID),
            token(archive_ata, archive_address, 0),

            token_program(),
            system_program(),
            rent_sysvar(),
        ];

        let env = test_env();
        env.process_instruction(
            &instruction,
            &accounts,
            &[
                Check::success(),
                Check::account(&Pubkey::from(tape_address)).data(
                    Tape {
                        id: TapeNumber(1),  // First tape
                        authority: authority.into(),
                        capacity: storage_units,
                        used: StorageUnits::zero(),
                        active_epoch: start_epoch,
                        expiry_epoch: end_epoch,
                        ..Tape::zeroed()
                    }.pack().as_ref()
                ).build(),
                Check::account(&Pubkey::from(archive_address)).data(
                    expected_archive.pack().as_ref()
                ).build(),
                Check::account(&Pubkey::from(authority_ata)).data(
                    token(authority_ata, authority, initial_token_balance - total_cost).1.data.as_ref()
                ).build(),
                Check::account(&Pubkey::from(archive_ata)).data(
                    token(archive_ata, archive_address, total_cost).1.data.as_ref()
                ).build(),
            ]
        );
    }
}
