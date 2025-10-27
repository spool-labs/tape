use steel::*;
use tape_core::prelude::*;
use crate::program::*;

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct RegisterExchange {}

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct SetExchangeRate {
    pub tape: [u8; 8],
    pub sol: [u8; 8],
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct DepositTape {
    pub amount: [u8; 8],
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct DepositSol {
    pub amount: [u8; 8],
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct WithdrawTape {
    pub amount: [u8; 8],
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct WithdrawSol {
    pub amount: [u8; 8],
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct SwapForTape {
    pub amount_sol: [u8; 8],
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct SwapForSol {
    pub amount_tape: [u8; 8],
}


pub fn build_register_exchange_ix(
    signer: Pubkey,
) -> Instruction {

    let (mint_address, _) = mint_pda();
    let (exchange_address, _) = exchange_pda(signer);
    let (exchange_ata, _) = exchange_ata(exchange_address);

    Instruction {
        program_id: crate::program::exchange::ID,
        accounts: vec![
            AccountMeta::new(signer, true),
            AccountMeta::new(exchange_address, false),
            AccountMeta::new(exchange_ata, false),
            AccountMeta::new_readonly(mint_address, false),
            AccountMeta::new_readonly(system_program::ID, false),
            AccountMeta::new_readonly(spl_token::ID, false),
            AccountMeta::new_readonly(spl_associated_token_account::ID, false),
            AccountMeta::new_readonly(sysvar::rent::ID, false),
        ],
        data: RegisterExchange {}.to_bytes(),
    }
}

pub fn build_deposit_sol_ix(
    signer: Pubkey,
    exchange: Pubkey,
    amount: Coin<SOL>,
) -> Instruction {
    let amount = amount.pack();

    Instruction {
        program_id: crate::program::exchange::ID,
        accounts: vec![
            AccountMeta::new(signer, true),
            AccountMeta::new(exchange, false),
            AccountMeta::new_readonly(system_program::ID, false),
        ],
        data: DepositSol {
            amount,
        }.to_bytes(),
    }
}

pub fn build_withdraw_sol_ix(
    signer: Pubkey,
    exchange: Pubkey,
    amount: Coin<SOL>,
) -> Instruction {
    let amount = amount.pack();

    Instruction {
        program_id: crate::program::exchange::ID,
        accounts: vec![
            AccountMeta::new(signer, true),
            AccountMeta::new(exchange, false),
        ],
        data: WithdrawSol {
            amount,
        }.to_bytes(),
    }
}

pub fn build_deposit_tape_ix(
    signer: Pubkey,
    signer_ata: Pubkey,
    exchange: Pubkey,
    amount: Coin<TAPE>,
) -> Instruction {
    let (exchange_ata, _) = exchange_ata(exchange);
    let amount = amount.pack();

    Instruction {
        program_id: crate::program::exchange::ID,
        accounts: vec![
            AccountMeta::new(signer, true),
            AccountMeta::new(signer_ata, false),
            AccountMeta::new(exchange, false),
            AccountMeta::new(exchange_ata, false),
            AccountMeta::new_readonly(spl_token::ID, false),
        ],
        data: DepositTape {
            amount,
        }.to_bytes(),
    }
}

pub fn build_withdraw_tape_ix(
    signer: Pubkey,
    signer_ata: Pubkey,
    exchange: Pubkey,
    amount: Coin<TAPE>,
) -> Instruction {
    let (exchange_ata, _) = exchange_ata(exchange);
    let amount = amount.pack();

    Instruction {
        program_id: crate::program::exchange::ID,
        accounts: vec![
            AccountMeta::new(signer, true),
            AccountMeta::new(signer_ata, false),
            AccountMeta::new(exchange, false),
            AccountMeta::new(exchange_ata, false),
            AccountMeta::new_readonly(spl_token::ID, false),
        ],
        data: WithdrawTape {
            amount,
        }.to_bytes(),
    }
}

pub fn build_set_exchange_rate_ix(
    signer: Pubkey,
    exchange: Pubkey,
    tape: u64,
    sol: u64,
) -> Instruction {
    Instruction {
        program_id: crate::program::exchange::ID,
        accounts: vec![
            AccountMeta::new(signer, true),
            AccountMeta::new(exchange, false),
        ],
        data: SetExchangeRate {
            tape: tape.to_le_bytes(),
            sol: sol.to_le_bytes(),
        }
        .to_bytes(),
    }
}

pub fn build_swap_for_tape_ix(
    signer: Pubkey,
    signer_ata: Pubkey,
    exchange: Pubkey,
    amount_sol: Coin<SOL>,
) -> Instruction {
    let (exchange_ata, _) = exchange_ata(exchange);
    let amount_sol = amount_sol.pack();

    Instruction {
        program_id: crate::program::exchange::ID,
        accounts: vec![
            AccountMeta::new(signer, true),
            AccountMeta::new(signer_ata, false),
            AccountMeta::new(exchange, false),
            AccountMeta::new(exchange_ata, false),
            AccountMeta::new_readonly(system_program::ID, false),
            AccountMeta::new_readonly(spl_token::ID, false),
        ],
        data: SwapForTape { amount_sol }.to_bytes(),
    }
}

pub fn build_swap_for_sol_ix(
    signer: Pubkey,
    signer_ata: Pubkey,
    exchange: Pubkey,
    amount_tape: Coin<TAPE>,
) -> Instruction {
    let (exchange_ata, _) = exchange_ata(exchange);
    let amount_tape = amount_tape.pack();

    Instruction {
        program_id: crate::program::exchange::ID,
        accounts: vec![
            AccountMeta::new(signer, true),
            AccountMeta::new(signer_ata, false),
            AccountMeta::new(exchange, false),
            AccountMeta::new(exchange_ata, false),
            AccountMeta::new_readonly(spl_token::ID, false),
        ],
        data: SwapForSol { amount_tape }.to_bytes(),
    }
}
