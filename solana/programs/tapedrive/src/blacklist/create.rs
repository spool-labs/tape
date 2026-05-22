use core::mem::size_of;

use tape_api::program::prelude::*;
use tape_core::system::BlacklistEntry;

use crate::tape::helpers::{TapeSpec, create_tape_account, reserve_archive};

pub fn process_create_blacklist(accounts: &[AccountInfo<'_>], data: &[u8]) -> ProgramResult {
    let args = CreateBlacklist::try_from_bytes(data)?;
    let [
        fee_payer_info,
        authority_info,
        authority_ata_info,
        node_info,
        blacklist_info,
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

    let node = node_info.as_account::<Node>(&tapedrive::ID)?;
    if node.authority != (*authority_info.key).into() {
        return Err(ProgramError::InvalidAccountData);
    }

    let node_address = (*node_info.key).into();
    let (blacklist_address, _) = blacklist_pda(node_address);

    blacklist_info
        .is_empty()?
        .is_writable()?
        .has_address(&blacklist_address.into())?;

    let system = system_info
        .is_system()?
        .as_account::<System>(&tapedrive::ID)?;

    archive_info
        .is_writable()?
        .is_archive()?;

    archive_ata_info
        .is_writable()?
        .is_archive_ata()?;

    let archive = archive_info.as_account_mut::<Archive>(&tapedrive::ID)?;

    let capacity = u64::from_le_bytes(args.capacity);
    let total_bytes = capacity
        .checked_mul(size_of::<BlacklistEntry>() as u64)
        .ok_or(ProgramError::ArithmeticOverflow)?;

    let spec = TapeSpec {
        address: blacklist_address,
        authority: node_address,
        capacity: StorageUnits::from_bytes(total_bytes),
        active_epoch: current_epoch(system),
        expiry_epoch: EpochNumber::unpack(args.expiry_epoch),
    };

    let reservation = reserve_archive(system, archive, spec)?;
    let seeds = [BLACKLIST, node_info.key.as_ref()];

    transfer(
        authority_info,
        authority_ata_info,
        archive_ata_info,
        token_program_info,
        reservation.cost.as_u64(),
    )?;

    create_tape_account(
        blacklist_info,
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
    use tape_core::tape::tape_reservation_cost;
    use tape_test::*;

    #[test]
    fn create_blacklist() {
        let fee_payer = Pubkey::new_unique();
        let authority = Pubkey::new_unique();
        let node_address = Pubkey::new_unique();

        let capacity = 3;
        let start_epoch = EpochNumber(0);
        let end_epoch = EpochNumber(4);
        let price_per_unit = TAPE::from("0.0001");

        let instruction = build_create_blacklist_ix(
            fee_payer.into(),
            authority.into(),
            node_address.into(),
            capacity,
            end_epoch,
        );

        let (system_address, _) = system_pda();
        let (archive_address, _) = archive_pda();
        let (archive_ata, _) = archive_ata();
        let (blacklist_address, _) = blacklist_pda(node_address.into());
        let authority_ata = ata_address(&authority);

        let system = System {
            current_epoch: start_epoch,
            ..System::zeroed()
        };
        let archive = Archive {
            storage_capacity: StorageUnits::mb(100),
            storage_price: price_per_unit,
            schedule: EpochSchedule::new_at(start_epoch),
            ..Archive::zeroed()
        };
        let node = Node {
            authority: authority.into(),
            ..Node::zeroed()
        };

        let entry_size = size_of::<BlacklistEntry>() as u64;
        let storage_units = StorageUnits::from_bytes(capacity * entry_size);
        let num_epochs = (end_epoch - start_epoch).as_u64();
        let fee_per_epoch = tape_reservation_cost(price_per_unit, storage_units, 1).unwrap();
        let total_cost =
            tape_reservation_cost(price_per_unit, storage_units, num_epochs).unwrap();

        let mut expected_archive = archive.clone();
        expected_archive
            .schedule
            .reserve_capacity(storage_units, fee_per_epoch, start_epoch, end_epoch)
            .unwrap();
        expected_archive.tape_count = 1;

        let initial_token_balance = total_cost.as_u64() + TAPE::from("1").as_u64();

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            sol(authority, 0),
            token(authority_ata, authority, initial_token_balance),
            pda(node_address, node.pack(), tapedrive::ID),
            empty(blacklist_address),
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
                Check::account(&Pubkey::from(blacklist_address))
                    .data(
                        Tape {
                            id: TapeNumber(1),
                            authority: node_address.into(),
                            capacity: storage_units,
                            used: StorageUnits::zero(),
                            active_epoch: start_epoch,
                            expiry_epoch: end_epoch,
                            ..Tape::zeroed()
                        }
                        .pack()
                        .as_ref(),
                    )
                    .build(),
                Check::account(&Pubkey::from(archive_address))
                    .data(expected_archive.pack().as_ref())
                    .build(),
                Check::account(&Pubkey::from(authority_ata))
                    .data(
                        token(
                            authority_ata,
                            authority,
                            initial_token_balance - total_cost.as_u64(),
                        )
                        .1
                        .data
                        .as_ref(),
                    )
                    .build(),
                Check::account(&Pubkey::from(archive_ata))
                    .data(token(archive_ata, archive_address, total_cost.as_u64()).1.data.as_ref())
                    .build(),
            ],
        );
    }
}
