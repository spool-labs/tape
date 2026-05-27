use tape_solana::*;
use tape_crypto::address::Address;
use tape_core::types::coin::{Coin, SOL, TAPE};
use crate::program::exchange;
use crate::program::*;

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct RegisterExchange {}

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct SetExchangeRate {
    pub tape: u64,
    pub sol: u64,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct DepositTape {
    pub amount: Coin<TAPE>,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct DepositSol {
    pub amount: Coin<SOL>,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct WithdrawTape {
    pub amount: Coin<TAPE>,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct WithdrawSol {
    pub amount: Coin<SOL>,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct SwapForTape {
    pub amount_sol: Coin<SOL>,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct SwapForSol {
    pub amount_tape: Coin<TAPE>,
}


pub fn build_register_exchange_ix(
    fee_payer: Address,
    authority: Address,
) -> Instruction {

    let (mint_address, _) = mint_pda();
    let (exchange_address, _) = exchange_pda(authority);
    let (exchange_ata, _) = exchange_ata(exchange_address);

    Instruction {
        program_id: exchange::ID,
        accounts: vec![
            AccountMeta::new(fee_payer.into(), true),
            AccountMeta::new_readonly(authority.into(), true),
            AccountMeta::new(exchange_address.into(), false),
            AccountMeta::new(exchange_ata.into(), false),
            AccountMeta::new_readonly(mint_address.into(), false),
            AccountMeta::new_readonly(system_program::ID, false),
            AccountMeta::new_readonly(spl_token::ID, false),
            AccountMeta::new_readonly(spl_associated_token_account::ID, false),
            AccountMeta::new_readonly(sysvar::rent::ID, false),
        ],
        data: RegisterExchange {}.to_bytes(),
    }
}

pub fn build_deposit_sol_ix(
    fee_payer: Address,
    authority: Address,
    exchange: Address,
    amount: Coin<SOL>,
) -> Instruction {
    Instruction {
        program_id: exchange::ID,
        accounts: vec![
            AccountMeta::new(fee_payer.into(), true),
            AccountMeta::new(authority.into(), true),
            AccountMeta::new(exchange.into(), false),
            AccountMeta::new_readonly(system_program::ID, false),
        ],
        data: DepositSol {
            amount,
        }.to_bytes(),
    }
}

pub fn build_withdraw_sol_ix(
    fee_payer: Address,
    authority: Address,
    exchange: Address,
    amount: Coin<SOL>,
) -> Instruction {
    Instruction {
        program_id: exchange::ID,
        accounts: vec![
            AccountMeta::new(fee_payer.into(), true),
            AccountMeta::new(authority.into(), true),
            AccountMeta::new(exchange.into(), false),
            AccountMeta::new_readonly(sysvar::rent::ID, false),
        ],
        data: WithdrawSol {
            amount,
        }.to_bytes(),
    }
}

pub fn build_deposit_tape_ix(
    fee_payer: Address,
    authority: Address,
    authority_ata: Address,
    exchange: Address,
    amount: Coin<TAPE>,
) -> Instruction {
    let (exchange_ata, _) = exchange_ata(exchange);
    Instruction {
        program_id: exchange::ID,
        accounts: vec![
            AccountMeta::new(fee_payer.into(), true),
            AccountMeta::new_readonly(authority.into(), true),
            AccountMeta::new(authority_ata.into(), false),
            AccountMeta::new(exchange.into(), false),
            AccountMeta::new(exchange_ata.into(), false),
            AccountMeta::new_readonly(spl_token::ID, false),
        ],
        data: DepositTape {
            amount,
        }.to_bytes(),
    }
}

pub fn build_withdraw_tape_ix(
    fee_payer: Address,
    authority: Address,
    authority_ata: Address,
    exchange: Address,
    amount: Coin<TAPE>,
) -> Instruction {
    let (exchange_ata, _) = exchange_ata(exchange);
    Instruction {
        program_id: exchange::ID,
        accounts: vec![
            AccountMeta::new(fee_payer.into(), true),
            AccountMeta::new_readonly(authority.into(), true),
            AccountMeta::new(authority_ata.into(), false),
            AccountMeta::new(exchange.into(), false),
            AccountMeta::new(exchange_ata.into(), false),
            AccountMeta::new_readonly(spl_token::ID, false),
        ],
        data: WithdrawTape {
            amount,
        }.to_bytes(),
    }
}

pub fn build_set_exchange_rate_ix(
    fee_payer: Address,
    authority: Address,
    exchange: Address,
    tape: u64,
    sol: u64,
) -> Instruction {
    Instruction {
        program_id: exchange::ID,
        accounts: vec![
            AccountMeta::new(fee_payer.into(), true),
            AccountMeta::new_readonly(authority.into(), true),
            AccountMeta::new(exchange.into(), false),
        ],
        data: SetExchangeRate { tape, sol }.to_bytes(),
    }
}

pub fn build_swap_for_tape_ix(
    fee_payer: Address,
    authority: Address,
    authority_ata: Address,
    exchange: Address,
    amount_sol: Coin<SOL>,
) -> Instruction {
    let (exchange_ata, _) = exchange_ata(exchange);

    Instruction {
        program_id: exchange::ID,
        accounts: vec![
            AccountMeta::new(fee_payer.into(), true),
            AccountMeta::new(authority.into(), true),
            AccountMeta::new(authority_ata.into(), false),
            AccountMeta::new(exchange.into(), false),
            AccountMeta::new(exchange_ata.into(), false),
            AccountMeta::new_readonly(system_program::ID, false),
            AccountMeta::new_readonly(spl_token::ID, false),
        ],
        data: SwapForTape { amount_sol }.to_bytes(),
    }
}

pub fn build_swap_for_sol_ix(
    fee_payer: Address,
    authority: Address,
    authority_ata: Address,
    exchange: Address,
    amount_tape: Coin<TAPE>,
) -> Instruction {
    let (exchange_ata, _) = exchange_ata(exchange);

    Instruction {
        program_id: exchange::ID,
        accounts: vec![
            AccountMeta::new(fee_payer.into(), true),
            AccountMeta::new(authority.into(), true),
            AccountMeta::new(authority_ata.into(), false),
            AccountMeta::new(exchange.into(), false),
            AccountMeta::new(exchange_ata.into(), false),
            AccountMeta::new_readonly(spl_token::ID, false),
            AccountMeta::new_readonly(sysvar::rent::ID, false),
        ],
        data: SwapForSol { amount_tape }.to_bytes(),
    }
}
