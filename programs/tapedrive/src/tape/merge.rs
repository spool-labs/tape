use tape_solana::*;
use tape_api::prelude::*;
use crate::error::*;

pub fn process_merge_tape(accounts: &[AccountInfo<'_>], data: &[u8]) -> ProgramResult {
    let _args = MergeTape::try_from_bytes(data)?;
    let [
        fee_payer_info,
        source_authority_info,
        dest_authority_info,

        source_tape_info,
        dest_tape_info,
        archive_info,

        system_program_info,
    ] = accounts else {
        return Err(ProgramError::NotEnoughAccountKeys);
    };

    fee_payer_info
        .is_signer()?
        .is_writable()?;

    source_authority_info
        .is_signer()?;
    dest_authority_info
        .is_signer()?;

    system_program_info
        .is_program(&system_program::ID)?;

    let archive = archive_info
        .is_writable()?
        .is_archive()?
        .as_account_mut::<Archive>(&tapedrive::ID)?;

    archive.tape_count = archive.tape_count
        .checked_sub(1)
        .ok_or(ProgramError::ArithmeticOverflow)?;

    let (source_tape_address, _) = tape_pda(*source_authority_info.key);
    let (dest_tape_address, _)   = tape_pda(*dest_authority_info.key);

    let source_tape = source_tape_info
        .is_writable()?
        .has_address(&source_tape_address)?
        .as_account_mut::<Tape>(&tapedrive::ID)?;

    let dest_tape   = dest_tape_info
        .is_writable()?
        .has_address(&dest_tape_address)?
        .as_account_mut::<Tape>(&tapedrive::ID)?;

    // Require correct authorities
    if source_tape.authority != *source_authority_info.key ||
       dest_tape.authority != *dest_authority_info.key {
        return Err(ProgramError::InvalidAccountData);
    }

    // Merge tapes together if possible
    let Some((new_active_epoch, new_expiry_epoch, new_capacity, new_used)) =
        merge_tapes(
            source_tape.active_epoch,
            source_tape.expiry_epoch,
            source_tape.capacity,
            source_tape.used,
            dest_tape.active_epoch,
            dest_tape.expiry_epoch,
            dest_tape.capacity,
            dest_tape.used,
        )
    else {
        return Err(TapeError::CannotMerge.into());
    };

    dest_tape.active_epoch = new_active_epoch;
    dest_tape.expiry_epoch = new_expiry_epoch;
    dest_tape.capacity     = new_capacity;
    dest_tape.used         = new_used;

    close_account(source_tape_info, fee_payer_info)?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tape_test::*;

    #[test]
    fn test_merge_tape() {
        let fee_payer = Pubkey::new_unique();
        let source_authority = Pubkey::new_unique();
        let dest_authority = Pubkey::new_unique();

        let (source_tape_address, _) = tape_pda(source_authority);
        let (dest_tape_address, _)   = tape_pda(dest_authority);
        let (archive_address, _)     = archive_pda();

        // Two tapes with identical epochs
        let e0 = EpochNumber(100);
        let e1 = EpochNumber(110);

        let source_tape = Tape {
            authority: source_authority,
            capacity: StorageUnits(200),
            used: StorageUnits(30),
            active_epoch: e0,
            expiry_epoch: e1,
            ..Tape::zeroed()
        };
        let dest_tape = Tape {
            authority: dest_authority,
            capacity: StorageUnits(100),
            used: StorageUnits(20),
            active_epoch: e0,
            expiry_epoch: e1,
            ..Tape::zeroed()
        };

        let archive = Archive {
            tape_count: 100,
            ..Archive::zeroed()
        };

        let instruction = build_merge_tape_ix(fee_payer, source_authority, dest_authority);

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            sol(source_authority, 0),
            sol(dest_authority, 0),

            pda(source_tape_address, source_tape.pack(), tapedrive::ID),
            pda(dest_tape_address, dest_tape.pack(), tapedrive::ID),
            pda(archive_address, archive.pack(), tapedrive::ID),

            system_program(),
        ];

        let expected_tape = Tape {
            authority: dest_authority,
            capacity: StorageUnits(300),
            used: StorageUnits(50),
            active_epoch: e0,
            expiry_epoch: e1,
            ..Tape::zeroed()
        };

        let expected_archive = Archive {
            tape_count: 99,
            ..Archive::zeroed()
        };

        let env = test_env();
        env.process_instruction(
            &instruction,
            &accounts,
            &[
                Check::success(),
                Check::account(&dest_tape_address)
                    .data(expected_tape.pack().as_ref()).build(),
                Check::account(&source_tape_address)
                    .lamports(0).closed().build(),
                Check::account(&archive_address)
                    .data(expected_archive.pack().as_ref()).build(),
            ],
        );
    }
}
