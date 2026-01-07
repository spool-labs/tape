use tape_solana::*;
use tape_core::prelude::*;
use super::AccountType;

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Pod, Zeroable)]
pub struct Exchange {
    /// The authority that controls the exchange.
    pub authority: Pubkey,

    /// The total amount of TAPE in the exchange.
    pub balance_tape: Coin<TAPE>,

    /// The total amount of SOL in the exchange.
    pub balance_sol: Coin<SOL>,

    /// The rate of exchange between TAPE and SOL.
    pub rate: ExchangeRate,
}

tape_solana::state!(AccountType, Exchange);
