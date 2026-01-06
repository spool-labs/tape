use tape_solana::*;
use solana_program::{
    program_pack::Pack,
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
    bump: u8,
) -> ProgramResult {

    // Note: we're using the "with_bump" variant as the address may be in a PDA space that is *not*
    // in the owner's space. The "allocate_account" function assumes the PDA will be in the same
    // space.

    allocate_account_with_bump(
        target_account,
        system_program,
        payer,
        size,
        owner,
        seeds,
        bump
    )?;

    // Set discriminator.
    let mut data = target_account.data.borrow_mut();
    data[0] = T::discriminator();
    data[1..8].fill(0); // zero out the rest of the discriminator bytes

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

    target_account.realloc(new_size, true)?;

    Ok(())
}

pub fn create_token_account<'info>(
    funder_info: &AccountInfo<'info>,
    target_info: &AccountInfo<'info>,
    mint_info: &AccountInfo<'info>,
    system_program: &AccountInfo<'info>,
    seeds: &[&[u8]],
    bump: u8,
) -> ProgramResult {

    allocate_account_with_bump(
        target_info,
        system_program,
        funder_info,
        spl_token::state::Account::LEN,
        &spl_token::id(),
        seeds,
        bump,
    )?;

    // Initialize the token account (requires no signers)
    solana_program::program::invoke(
        &spl_token::instruction::initialize_account3(
            &spl_token::id(),
            target_info.key,
            mint_info.key,
            target_info.key,
        ).unwrap(),
        &[
            target_info.clone(),
            mint_info.clone(),
        ],
    )
}
