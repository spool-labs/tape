//! SPL Token CPI helpers.
//!
//! Provides convenient wrappers for common SPL token operations.

use solana_program::{account_info::AccountInfo, entrypoint::ProgramResult, program_pack::Pack, pubkey::Pubkey};

use crate::account::{allocate_account_with_bump_signed_by, invoke_signed_with_bump};

/// Creates an associated token account.
#[inline(always)]
pub fn create_associated_token_account<'info>(
    funder_info: &AccountInfo<'info>,
    owner_info: &AccountInfo<'info>,
    token_account_info: &AccountInfo<'info>,
    mint_info: &AccountInfo<'info>,
    system_program: &AccountInfo<'info>,
    token_program: &AccountInfo<'info>,
    associated_token_program: &AccountInfo<'info>,
) -> ProgramResult {
    solana_program::program::invoke(
        &spl_associated_token_account::instruction::create_associated_token_account(
            funder_info.key,
            owner_info.key,
            mint_info.key,
            &token_program.key,
        ),
        &[
            funder_info.clone(),
            token_account_info.clone(),
            owner_info.clone(),
            mint_info.clone(),
            system_program.clone(),
            token_program.clone(),
            associated_token_program.clone(),
        ],
    )
}

/// Creates a PDA token account (not an ATA).
#[inline(always)]
pub fn create_token_account<'info>(
    funder_info: &AccountInfo<'info>,
    target_info: &AccountInfo<'info>,
    mint_info: &AccountInfo<'info>,
    system_program: &AccountInfo<'info>,
    signer_program_id: &Pubkey,
    seeds: &[&[u8]],
    bump: u8,
) -> ProgramResult {
    allocate_account_with_bump_signed_by(
        target_info,
        system_program,
        funder_info,
        spl_token::state::Account::LEN,
        &spl_token::id(),
        signer_program_id,
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
        )
        .unwrap(),
        &[target_info.clone(), mint_info.clone()],
    )
}

/// Closes a token account.
#[inline(always)]
pub fn close_token_account<'info>(
    account_info: &AccountInfo<'info>,
    destination_info: &AccountInfo<'info>,
    owner_info: &AccountInfo<'info>,
    token_program: &AccountInfo<'info>,
) -> ProgramResult {
    solana_program::program::invoke(
        &spl_token::instruction::close_account(
            &token_program.key,
            &account_info.key,
            &destination_info.key,
            &owner_info.key,
            &[&owner_info.key],
        )?,
        &[
            token_program.clone(),
            account_info.clone(),
            destination_info.clone(),
            owner_info.clone(),
        ],
    )
}

/// Closes a token account with PDA signer (auto-bump).
#[inline(always)]
pub fn close_token_account_signed<'info>(
    account_info: &AccountInfo<'info>,
    destination_info: &AccountInfo<'info>,
    owner_info: &AccountInfo<'info>,
    token_program: &AccountInfo<'info>,
    seeds: &[&[u8]],
) -> ProgramResult {
    let bump = Pubkey::find_program_address(seeds, owner_info.owner).1;
    close_token_account_signed_with_bump(
        account_info,
        destination_info,
        owner_info,
        token_program,
        seeds,
        bump,
    )
}

/// Closes a token account with PDA signer (explicit bump).
#[inline(always)]
pub fn close_token_account_signed_with_bump<'info>(
    account_info: &AccountInfo<'info>,
    destination_info: &AccountInfo<'info>,
    owner_info: &AccountInfo<'info>,
    token_program: &AccountInfo<'info>,
    seeds: &[&[u8]],
    bump: u8,
) -> ProgramResult {
    invoke_signed_with_bump(
        &spl_token::instruction::close_account(
            &token_program.key,
            &account_info.key,
            &destination_info.key,
            &owner_info.key,
            &[&owner_info.key],
        )?,
        &[
            token_program.clone(),
            account_info.clone(),
            destination_info.clone(),
            owner_info.clone(),
        ],
        seeds,
        bump,
    )
}

/// Transfers tokens.
#[inline(always)]
#[allow(deprecated)]
pub fn transfer<'info>(
    authority_info: &AccountInfo<'info>,
    from_info: &AccountInfo<'info>,
    to_info: &AccountInfo<'info>,
    token_program: &AccountInfo<'info>,
    amount: u64,
) -> ProgramResult {
    solana_program::program::invoke(
        &spl_token::instruction::transfer(
            &token_program.key,
            from_info.key,
            to_info.key,
            authority_info.key,
            &[authority_info.key],
            amount,
        )?,
        &[
            token_program.clone(),
            from_info.clone(),
            to_info.clone(),
            authority_info.clone(),
        ],
    )
}

/// Transfers tokens with PDA signer (auto-bump).
#[inline(always)]
pub fn transfer_signed<'info>(
    authority_info: &AccountInfo<'info>,
    from_info: &AccountInfo<'info>,
    to_info: &AccountInfo<'info>,
    token_program: &AccountInfo<'info>,
    amount: u64,
    seeds: &[&[u8]],
) -> ProgramResult {
    let bump = Pubkey::find_program_address(seeds, authority_info.owner).1;
    transfer_signed_with_bump(
        authority_info,
        from_info,
        to_info,
        token_program,
        amount,
        seeds,
        bump,
    )
}

/// Transfers tokens with PDA signer (explicit bump).
#[inline(always)]
#[allow(deprecated)]
pub fn transfer_signed_with_bump<'info>(
    authority_info: &AccountInfo<'info>,
    from_info: &AccountInfo<'info>,
    to_info: &AccountInfo<'info>,
    token_program: &AccountInfo<'info>,
    amount: u64,
    seeds: &[&[u8]],
    bump: u8,
) -> ProgramResult {
    invoke_signed_with_bump(
        &spl_token::instruction::transfer(
            &token_program.key,
            from_info.key,
            to_info.key,
            authority_info.key,
            &[authority_info.key],
            amount,
        )?,
        &[
            token_program.clone(),
            from_info.clone(),
            to_info.clone(),
            authority_info.clone(),
        ],
        seeds,
        bump,
    )
}

/// Transfers tokens with decimal check.
#[inline(always)]
pub fn transfer_checked<'info>(
    authority_info: &AccountInfo<'info>,
    from_info: &AccountInfo<'info>,
    mint_info: &AccountInfo<'info>,
    to_info: &AccountInfo<'info>,
    token_program: &AccountInfo<'info>,
    amount: u64,
    decimals: u8,
) -> ProgramResult {
    solana_program::program::invoke(
        &spl_token::instruction::transfer_checked(
            &token_program.key,
            from_info.key,
            mint_info.key,
            to_info.key,
            authority_info.key,
            &[authority_info.key],
            amount,
            decimals,
        )?,
        &[
            token_program.clone(),
            from_info.clone(),
            mint_info.clone(),
            to_info.clone(),
            authority_info.clone(),
        ],
    )
}

/// Transfers tokens with decimal check and PDA signer (auto-bump).
#[inline(always)]
pub fn transfer_checked_signed<'info>(
    authority_info: &AccountInfo<'info>,
    from_info: &AccountInfo<'info>,
    mint_info: &AccountInfo<'info>,
    to_info: &AccountInfo<'info>,
    token_program: &AccountInfo<'info>,
    amount: u64,
    decimals: u8,
    seeds: &[&[u8]],
) -> ProgramResult {
    let bump = Pubkey::find_program_address(seeds, authority_info.owner).1;
    transfer_checked_signed_with_bump(
        authority_info,
        from_info,
        mint_info,
        to_info,
        token_program,
        amount,
        decimals,
        seeds,
        bump,
    )
}

/// Transfers tokens with decimal check and PDA signer (explicit bump).
#[inline(always)]
pub fn transfer_checked_signed_with_bump<'info>(
    authority_info: &AccountInfo<'info>,
    from_info: &AccountInfo<'info>,
    mint_info: &AccountInfo<'info>,
    to_info: &AccountInfo<'info>,
    token_program: &AccountInfo<'info>,
    amount: u64,
    decimals: u8,
    seeds: &[&[u8]],
    bump: u8,
) -> ProgramResult {
    invoke_signed_with_bump(
        &spl_token::instruction::transfer_checked(
            &token_program.key,
            from_info.key,
            mint_info.key,
            to_info.key,
            authority_info.key,
            &[authority_info.key],
            amount,
            decimals,
        )?,
        &[
            token_program.clone(),
            from_info.clone(),
            mint_info.clone(),
            to_info.clone(),
            authority_info.clone(),
        ],
        seeds,
        bump,
    )
}

/// Mints tokens with PDA signer (auto-bump).
#[inline(always)]
pub fn mint_to_signed<'info>(
    mint_info: &AccountInfo<'info>,
    to_info: &AccountInfo<'info>,
    authority_info: &AccountInfo<'info>,
    token_program: &AccountInfo<'info>,
    amount: u64,
    seeds: &[&[u8]],
) -> ProgramResult {
    let bump = Pubkey::find_program_address(seeds, authority_info.owner).1;
    mint_to_signed_with_bump(
        mint_info,
        to_info,
        authority_info,
        token_program,
        amount,
        seeds,
        bump,
    )
}

/// Mints tokens with PDA signer (explicit bump).
#[inline(always)]
pub fn mint_to_signed_with_bump<'info>(
    mint_info: &AccountInfo<'info>,
    to_info: &AccountInfo<'info>,
    authority_info: &AccountInfo<'info>,
    token_program: &AccountInfo<'info>,
    amount: u64,
    seeds: &[&[u8]],
    bump: u8,
) -> ProgramResult {
    invoke_signed_with_bump(
        &spl_token::instruction::mint_to(
            &token_program.key,
            mint_info.key,
            to_info.key,
            authority_info.key,
            &[authority_info.key],
            amount,
        )?,
        &[
            token_program.clone(),
            mint_info.clone(),
            to_info.clone(),
            authority_info.clone(),
        ],
        seeds,
        bump,
    )
}

/// Mints tokens with decimal check and PDA signer (auto-bump).
#[inline(always)]
pub fn mint_to_checked_signed<'info>(
    mint_info: &AccountInfo<'info>,
    to_info: &AccountInfo<'info>,
    authority_info: &AccountInfo<'info>,
    token_program: &AccountInfo<'info>,
    amount: u64,
    decimals: u8,
    seeds: &[&[u8]],
) -> ProgramResult {
    let bump = Pubkey::find_program_address(seeds, authority_info.owner).1;
    mint_to_checked_signed_with_bump(
        mint_info,
        to_info,
        authority_info,
        token_program,
        amount,
        decimals,
        seeds,
        bump,
    )
}

/// Mints tokens with decimal check and PDA signer (explicit bump).
#[inline(always)]
pub fn mint_to_checked_signed_with_bump<'info>(
    mint_info: &AccountInfo<'info>,
    to_info: &AccountInfo<'info>,
    authority_info: &AccountInfo<'info>,
    token_program: &AccountInfo<'info>,
    amount: u64,
    decimals: u8,
    seeds: &[&[u8]],
    bump: u8,
) -> ProgramResult {
    invoke_signed_with_bump(
        &spl_token::instruction::mint_to_checked(
            &token_program.key,
            mint_info.key,
            to_info.key,
            authority_info.key,
            &[authority_info.key],
            amount,
            decimals,
        )?,
        &[
            token_program.clone(),
            mint_info.clone(),
            to_info.clone(),
            authority_info.clone(),
        ],
        seeds,
        bump,
    )
}

/// Burns tokens.
#[inline(always)]
pub fn burn<'info>(
    token_account_info: &AccountInfo<'info>,
    mint_info: &AccountInfo<'info>,
    authority_info: &AccountInfo<'info>,
    token_program: &AccountInfo<'info>,
    amount: u64,
) -> ProgramResult {
    solana_program::program::invoke(
        &spl_token::instruction::burn(
            &token_program.key,
            token_account_info.key,
            mint_info.key,
            authority_info.key,
            &[authority_info.key],
            amount,
        )?,
        &[
            token_program.clone(),
            token_account_info.clone(),
            mint_info.clone(),
            authority_info.clone(),
        ],
    )
}

/// Burns tokens with PDA signer (auto-bump).
#[inline(always)]
pub fn burn_signed<'info>(
    token_account_info: &AccountInfo<'info>,
    mint_info: &AccountInfo<'info>,
    authority_info: &AccountInfo<'info>,
    token_program: &AccountInfo<'info>,
    amount: u64,
    seeds: &[&[u8]],
) -> ProgramResult {
    let bump = Pubkey::find_program_address(seeds, authority_info.owner).1;
    burn_signed_with_bump(
        token_account_info,
        mint_info,
        authority_info,
        token_program,
        amount,
        seeds,
        bump,
    )
}

/// Burns tokens with PDA signer (explicit bump).
#[inline(always)]
pub fn burn_signed_with_bump<'info>(
    token_account_info: &AccountInfo<'info>,
    mint_info: &AccountInfo<'info>,
    authority_info: &AccountInfo<'info>,
    token_program: &AccountInfo<'info>,
    amount: u64,
    seeds: &[&[u8]],
    bump: u8,
) -> ProgramResult {
    invoke_signed_with_bump(
        &spl_token::instruction::burn(
            &token_program.key,
            token_account_info.key,
            mint_info.key,
            authority_info.key,
            &[authority_info.key],
            amount,
        )?,
        &[
            token_program.clone(),
            token_account_info.clone(),
            mint_info.clone(),
            authority_info.clone(),
        ],
        seeds,
        bump,
    )
}

/// Initializes a mint.
#[inline(always)]
pub fn initialize_mint<'info>(
    mint_info: &AccountInfo<'info>,
    mint_authority_info: &AccountInfo<'info>,
    freeze_authority_info: Option<&AccountInfo<'info>>,
    token_program: &AccountInfo<'info>,
    rent_sysvar: &AccountInfo<'info>,
    decimals: u8,
) -> ProgramResult {
    solana_program::program::invoke(
        &spl_token::instruction::initialize_mint(
            &token_program.key,
            mint_info.key,
            mint_authority_info.key,
            freeze_authority_info.map(|i| i.key),
            decimals,
        )?,
        &[
            token_program.clone(),
            mint_info.clone(),
            mint_authority_info.clone(),
            rent_sysvar.clone(),
        ],
    )
}

/// Initializes a mint with PDA signer (auto-bump).
#[inline(always)]
pub fn initialize_mint_signed<'info>(
    mint_info: &AccountInfo<'info>,
    mint_authority_info: &AccountInfo<'info>,
    freeze_authority_info: Option<&AccountInfo<'info>>,
    token_program: &AccountInfo<'info>,
    rent_sysvar: &AccountInfo<'info>,
    decimals: u8,
    seeds: &[&[u8]],
) -> ProgramResult {
    let bump = Pubkey::find_program_address(seeds, mint_info.owner).1;
    initialize_mint_signed_with_bump(
        mint_info,
        mint_authority_info,
        freeze_authority_info,
        token_program,
        rent_sysvar,
        decimals,
        seeds,
        bump,
    )
}

/// Initializes a mint with PDA signer (explicit bump).
#[inline(always)]
pub fn initialize_mint_signed_with_bump<'info>(
    mint_info: &AccountInfo<'info>,
    mint_authority_info: &AccountInfo<'info>,
    freeze_authority_info: Option<&AccountInfo<'info>>,
    token_program: &AccountInfo<'info>,
    rent_sysvar: &AccountInfo<'info>,
    decimals: u8,
    seeds: &[&[u8]],
    bump: u8,
) -> ProgramResult {
    invoke_signed_with_bump(
        &spl_token::instruction::initialize_mint(
            &token_program.key,
            mint_info.key,
            mint_authority_info.key,
            freeze_authority_info.map(|i| i.key),
            decimals,
        )?,
        &[
            token_program.clone(),
            mint_info.clone(),
            mint_authority_info.clone(),
            rent_sysvar.clone(),
        ],
        seeds,
        bump,
    )
}

/// Sets token authority.
#[inline(always)]
pub fn set_authority<'info>(
    account_or_mint: &AccountInfo<'info>,
    authority_info: &AccountInfo<'info>,
    new_authority_info: Option<&AccountInfo<'info>>,
    authority_type: spl_token::instruction::AuthorityType,
    token_program: &AccountInfo<'info>,
) -> ProgramResult {
    solana_program::program::invoke(
        &spl_token::instruction::set_authority(
            &token_program.key,
            account_or_mint.key,
            new_authority_info.map(|i| i.key),
            authority_type,
            authority_info.key,
            &[authority_info.key],
        )?,
        &[
            token_program.clone(),
            account_or_mint.clone(),
            authority_info.clone(),
        ],
    )
}

/// Sets token authority with PDA signer (auto-bump).
#[inline(always)]
pub fn set_authority_signed<'info>(
    account_or_mint: &AccountInfo<'info>,
    authority_info: &AccountInfo<'info>,
    new_authority_info: Option<&AccountInfo<'info>>,
    authority_type: spl_token::instruction::AuthorityType,
    token_program: &AccountInfo<'info>,
    seeds: &[&[u8]],
) -> ProgramResult {
    let bump = Pubkey::find_program_address(seeds, authority_info.owner).1;
    set_authority_signed_with_bump(
        account_or_mint,
        authority_info,
        new_authority_info,
        authority_type,
        token_program,
        seeds,
        bump,
    )
}

/// Sets token authority with PDA signer (explicit bump).
#[inline(always)]
pub fn set_authority_signed_with_bump<'info>(
    account_or_mint: &AccountInfo<'info>,
    authority_info: &AccountInfo<'info>,
    new_authority_info: Option<&AccountInfo<'info>>,
    authority_type: spl_token::instruction::AuthorityType,
    token_program: &AccountInfo<'info>,
    seeds: &[&[u8]],
    bump: u8,
) -> ProgramResult {
    invoke_signed_with_bump(
        &spl_token::instruction::set_authority(
            &token_program.key,
            account_or_mint.key,
            new_authority_info.map(|i| i.key),
            authority_type,
            authority_info.key,
            &[authority_info.key],
        )?,
        &[
            token_program.clone(),
            account_or_mint.clone(),
            authority_info.clone(),
        ],
        seeds,
        bump,
    )
}

/// Approves a delegate.
#[inline(always)]
pub fn approve<'info>(
    source_info: &AccountInfo<'info>,
    delegate_info: &AccountInfo<'info>,
    owner_info: &AccountInfo<'info>,
    signer_info: &AccountInfo<'info>,
    token_program: &AccountInfo<'info>,
    amount: u64,
) -> ProgramResult {
    solana_program::program::invoke(
        &spl_token::instruction::approve(
            &spl_token::ID,
            source_info.key,
            delegate_info.key,
            owner_info.key,
            &[signer_info.key],
            amount,
        )?,
        &[
            token_program.clone(),
            source_info.clone(),
            delegate_info.clone(),
            owner_info.clone(),
            signer_info.clone(),
        ],
    )
}

/// Revokes delegate authority.
#[inline(always)]
pub fn revoke<'info>(
    source_info: &AccountInfo<'info>,
    authority_info: &AccountInfo<'info>,
    token_program: &AccountInfo<'info>,
) -> ProgramResult {
    solana_program::program::invoke(
        &spl_token::instruction::revoke(
            &token_program.key,
            source_info.key,
            authority_info.key,
            &[authority_info.key],
        )?,
        &[
            token_program.clone(),
            source_info.clone(),
            authority_info.clone(),
        ],
    )
}

/// Revokes delegate authority with PDA signer (auto-bump).
#[inline(always)]
pub fn revoke_signed<'info>(
    source_info: &AccountInfo<'info>,
    authority_info: &AccountInfo<'info>,
    token_program: &AccountInfo<'info>,
    seeds: &[&[u8]],
) -> ProgramResult {
    let bump = Pubkey::find_program_address(seeds, authority_info.owner).1;
    revoke_signed_with_bump(source_info, authority_info, token_program, seeds, bump)
}

/// Revokes delegate authority with PDA signer (explicit bump).
#[inline(always)]
pub fn revoke_signed_with_bump<'info>(
    source_info: &AccountInfo<'info>,
    authority_info: &AccountInfo<'info>,
    token_program: &AccountInfo<'info>,
    seeds: &[&[u8]],
    bump: u8,
) -> ProgramResult {
    invoke_signed_with_bump(
        &spl_token::instruction::revoke(
            &token_program.key,
            source_info.key,
            authority_info.key,
            &[authority_info.key],
        )?,
        &[
            token_program.clone(),
            source_info.clone(),
            authority_info.clone(),
        ],
        seeds,
        bump,
    )
}

/// Freezes a token account.
#[inline(always)]
pub fn freeze<'info>(
    account_info: &AccountInfo<'info>,
    mint_info: &AccountInfo<'info>,
    owner_info: &AccountInfo<'info>,
    signer_info: &AccountInfo<'info>,
    token_program: &AccountInfo<'info>,
) -> ProgramResult {
    solana_program::program::invoke(
        &spl_token::instruction::freeze_account(
            &token_program.key,
            account_info.key,
            mint_info.key,
            owner_info.key,
            &[signer_info.key],
        )?,
        &[
            token_program.clone(),
            account_info.clone(),
            mint_info.clone(),
            owner_info.clone(),
            signer_info.clone(),
        ],
    )
}

/// Thaws a frozen token account.
#[inline(always)]
pub fn thaw_account<'info>(
    token_account_info: &AccountInfo<'info>,
    mint_info: &AccountInfo<'info>,
    authority_info: &AccountInfo<'info>,
    token_program: &AccountInfo<'info>,
) -> ProgramResult {
    solana_program::program::invoke(
        &spl_token::instruction::thaw_account(
            &token_program.key,
            token_account_info.key,
            mint_info.key,
            authority_info.key,
            &[authority_info.key],
        )?,
        &[
            token_program.clone(),
            token_account_info.clone(),
            mint_info.clone(),
            authority_info.clone(),
        ],
    )
}

/// Thaws a frozen token account with PDA signer (auto-bump).
#[inline(always)]
pub fn thaw_account_signed<'info>(
    token_account_info: &AccountInfo<'info>,
    mint_info: &AccountInfo<'info>,
    owner_info: &AccountInfo<'info>,
    authority_info: &AccountInfo<'info>,
    token_program: &AccountInfo<'info>,
    seeds: &[&[u8]],
) -> ProgramResult {
    let bump = Pubkey::find_program_address(seeds, authority_info.owner).1;
    thaw_account_signed_with_bump(
        token_account_info,
        mint_info,
        owner_info,
        authority_info,
        token_program,
        seeds,
        bump,
    )
}

/// Thaws a frozen token account with PDA signer (explicit bump).
#[inline(always)]
pub fn thaw_account_signed_with_bump<'info>(
    token_account_info: &AccountInfo<'info>,
    mint_info: &AccountInfo<'info>,
    owner_info: &AccountInfo<'info>,
    signer_info: &AccountInfo<'info>,
    token_program: &AccountInfo<'info>,
    seeds: &[&[u8]],
    bump: u8,
) -> ProgramResult {
    invoke_signed_with_bump(
        &spl_token::instruction::thaw_account(
            &token_program.key,
            token_account_info.key,
            mint_info.key,
            owner_info.key,
            &[signer_info.key],
        )?,
        &[
            token_program.clone(),
            token_account_info.clone(),
            mint_info.clone(),
            owner_info.clone(),
            signer_info.clone(),
        ],
        seeds,
        bump,
    )
}
