use steel::*;
use tape_api::prelude::*;
use solana_program::entrypoint::MAX_PERMITTED_DATA_INCREASE;

pub fn process_expand_committee(accounts: &[AccountInfo<'_>], data: &[u8]) -> ProgramResult {
    let args = ExpandCommittee::try_from_bytes(data)?;
    let [
        signer_info, 
        committee_info,
        system_program_info, 
        rent_sysvar_info,
    ] = accounts else {
        return Err(ProgramError::NotEnoughAccountKeys);
    };

    system_program_info
        .is_program(&system_program::ID)?;
    rent_sysvar_info
        .is_sysvar(&sysvar::rent::ID)?;

    let epoch_number = EpochNumber::unpack(args.epoch);
    let (committee_address, _) = committee_pda(epoch_number);

    committee_info
        .is_type::<Committee>(&tape_api::ID)?
        .is_writable()?
        .has_address(&committee_address)?;

    let current_size = committee_info.data_len();
    let required_size = Committee::get_size();

    if current_size >= required_size {
        return Err(ProgramError::AccountAlreadyInitialized);
    }

    let new_size = current_size
        .saturating_add(MAX_PERMITTED_DATA_INCREASE)
        .min(required_size);

    resize_account(
        committee_info,
        system_program_info,
        signer_info,
        new_size,
    )?;

    if new_size == required_size {
        let committee = committee_info.as_account_mut::<Committee>(&tape_api::ID)?;
        committee.epoch = epoch_number;
    }

    Ok(())
}

// #[cfg(test)]
// mod tests {
//     use super::*;
//     use tape_test::*;
//
//     #[test]
//     fn test_create() {
//         let signer = Pubkey::new_unique();
//         let epoch_number = EpochNumber(0);
//
//         let instruction = build_create_committee(signer, epoch_number);
//         let (committee_address, _) = committee_pda(epoch_number);
//
//         let accounts = vec![
//             sol(signer, 1_000_000_000),
//             empty(committee_address),
//
//             system_program(),
//             rent_sysvar(),
//         ];
//
//         let env = test_env("tape".to_string());
//         env.process_instruction(
//             &instruction, 
//             &accounts,
//             &[
//                 Check::success(),
//                 //Check::account(&exchange_address).data(
//                 //    Exchange { 
//                 //        authority: signer,
//                 //        balance_tape: TAPE::zero(),
//                 //        balance_sol: SOL::zero(),
//                 //        rate: ExchangeRate::flat(),
//                 //    }.to_bytes()
//                 //).build(),
//             ]
//         );
//     }
// }
