use steel::*;
use solana_program::{
    system_instruction, 
    rent::Rent, 
};

pub fn create_account_with_size<'a, 'info, T: Discriminator + Pod>(
    target_account: &'a AccountInfo<'info>,
    system_program: &'a AccountInfo<'info>,
    payer: &'a AccountInfo<'info>,
    size: usize,
    owner: &Pubkey,
    seeds: &[&[u8]],
) -> ProgramResult {

    // Allocate space.
    allocate_account(
        target_account,
        system_program,
        payer,
        size,
        owner,
        seeds,
    )?;

    // Set discriminator.
    let mut data = target_account.data.borrow_mut();
    data[0] = T::discriminator();

    Ok(())
}

pub fn resize_account<'info>(
    target_account: &AccountInfo<'info>,
    system_program: &AccountInfo<'info>,
    payer: &AccountInfo<'info>,
    new_size: usize,
) -> ProgramResult {
    let rent = Rent::get()?;
    let rent_exempt_balance = rent
        .minimum_balance(new_size)
        .saturating_sub(target_account.lamports());

    if rent_exempt_balance.gt(&0) {
        solana_program::program::invoke(
            &system_instruction::transfer(
                payer.key, 
                target_account.key,
                rent_exempt_balance,
            ),
            &[
                payer.clone(),
                target_account.clone(),
                system_program.clone(),
            ],
        )?;
    }

    target_account.realloc(new_size, false)?;

    Ok(())
}
