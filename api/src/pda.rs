#![allow(unexpected_cfgs)]

use std::mem::MaybeUninit;

use steel::*;
use crate::consts::*;


pub fn pda_derive_address<const N: usize>(
    seeds: &[&[u8]; N],
    bump: Option<u8>,
    program_id: &Pubkey,
) -> Pubkey {
    /// Maximum number of seeds.
    pub const MAX_SEEDS: usize = 16;

    /// The marker used to derive [program derived addresses][pda].
    ///
    /// [pda]: https://solana.com/docs/core/pda
    pub const PDA_MARKER: &[u8; 21] = b"ProgramDerivedAddress";

    const {
        assert!(N < MAX_SEEDS, "number of seeds must be less than MAX_SEEDS");
    }

    const UNINIT: MaybeUninit<&[u8]> = MaybeUninit::<&[u8]>::uninit();
    let mut data = [UNINIT; MAX_SEEDS + 2];
    let mut i = 0;

    while i < N {
        // SAFETY: `data` is guaranteed to have enough space for `N` seeds,
        // so `i` will always be within bounds.
        unsafe {
            data.get_unchecked_mut(i).write(seeds.get_unchecked(i));
        }
        i += 1;
    }

    let bump_seed = [bump.unwrap_or_default()];

    // SAFETY: `data` is guaranteed to have enough space for `MAX_SEEDS + 2`
    // elements, and `MAX_SEEDS` is as large as `N`.
    unsafe {
        if bump.is_some() {
            data.get_unchecked_mut(i).write(&bump_seed);
            i += 1;
        }
        data.get_unchecked_mut(i).write(program_id.as_ref());
        data.get_unchecked_mut(i + 1).write(PDA_MARKER.as_ref());
    }

    #[cfg(target_os = "solana")]
    {
        let mut pda = MaybeUninit::<[u8; 32]>::uninit();

        // SAFETY: `data` has `i + 2` elements initialized.
        unsafe {
            solana_sha256_hasher::sol_sha256(
                data.as_ptr() as *const u8,
                (i + 2) as u64,
                pda.as_mut_ptr() as *mut u8,
            );
        }

        // SAFETY: `pda` has been initialized by the syscall.
        let pubkey = unsafe { pda.assume_init() };
        Pubkey::new_from_array(pubkey)
    }

    #[cfg(not(target_os = "solana"))]
    unreachable!("deriving a pda is only available on target `solana`");
}

pub const fn archive_pda() -> (&'static Pubkey, u8) {
    (&ARCHIVE_ADDRESS, ARCHIVE_BUMP)
}

pub const fn epoch_pda() -> (&'static Pubkey, u8) {
    (&EPOCH_ADDRESS, EPOCH_BUMP)
}

pub const fn block_pda() -> (&'static Pubkey, u8) {
    (&BLOCK_ADDRESS, BLOCK_BUMP)
}

pub const fn treasury_pda() -> (&'static Pubkey, u8) {
    (&TREASURY_ADDRESS, TREASURY_BUMP)
}

pub fn treasury_find_ata() -> (Pubkey, u8) {
    let (treasury_pda,_bump) = treasury_pda();
    let (mint_pda, _bump) = mint_pda();
    Pubkey::find_program_address(
        &[
            treasury_pda.as_ref(),
            spl_token::ID.as_ref(),
            mint_pda.as_ref(),
        ],
        &spl_associated_token_account::ID,
    )
}

pub const fn mint_pda() -> (&'static Pubkey, u8) {
    (&MINT_ADDRESS, MINT_BUMP)
}

pub fn metadata_find_pda(mint: &Pubkey) -> (Pubkey, u8) {
    Pubkey::find_program_address(
        &[METADATA, mpl_token_metadata::ID.as_ref(), mint.as_ref() ],
        &mpl_token_metadata::ID,
    )
}

pub fn tape_find_pda(authority: &Pubkey, name: &[u8; NAME_LEN]) -> (Pubkey, u8) {
    Pubkey::find_program_address(&[TAPE, authority.as_ref(), name.as_ref()], &crate::id())
}

pub fn tape_derive_pda(authority: &Pubkey, name: &[u8; NAME_LEN], bump: u8) -> Pubkey {
    pda_derive_address(
        &[TAPE, authority.as_ref(), name.as_ref()],
        Some(bump),
        &crate::id(),
    )
}

pub fn writer_find_pda(tape: &Pubkey) -> (Pubkey, u8) {
    Pubkey::find_program_address(&[WRITER, tape.as_ref()], &crate::id())
}

pub fn writer_derive_pda(tape: &Pubkey, bump: u8) -> Pubkey {
    pda_derive_address(&[WRITER, tape.as_ref()], Some(bump), &crate::id())
}

pub fn miner_find_pda(authority: &Pubkey, name: [u8; NAME_LEN]) -> (Pubkey, u8) {
    Pubkey::find_program_address(&[MINER, authority.as_ref(), name.as_ref()], &crate::id())
}

pub fn miner_derive_pda(authority: &Pubkey, name: &[u8; NAME_LEN], bump: u8) -> Pubkey {
    pda_derive_address(
        &[MINER, authority.as_ref(), name.as_ref()],
        Some(bump),
        &crate::id(),
    )
}


pub fn spool_find_pda(miner: &Pubkey, number: u64) -> (Pubkey, u8) {
    Pubkey::find_program_address(
        &[SPOOL, miner.as_ref(), number.to_le_bytes().as_ref()],
        &crate::id(),
    )
}

pub fn spool_derive_pda(miner: &Pubkey, number: u64, bump: u8) -> Pubkey {
    pda_derive_address(
        &[SPOOL, miner.as_ref(), &number.to_le_bytes()],
        Some(bump),
        &crate::id(),
    )
}