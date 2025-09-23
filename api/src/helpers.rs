use steel::*;
use tape_core::prelude::*;
use crate::pda::*;
use crate::consts::*;
use crate::instruction::*;
use spl_associated_token_account::get_associated_token_address;

pub fn build_initialize_ix(
    signer: Pubkey,
) -> Instruction {

    let (system_address, _) = system_pda();
    let (archive_address, _) = archive_pda();
    let (epoch_address, _) = epoch_pda();
    let (mint_address, _) = mint_pda();
    let (metadata_address, _) = metadata_pda();

    let treasury = get_associated_token_address(&signer, &mint_address);

    Instruction {
        program_id: crate::ID,
        accounts: vec![
            AccountMeta::new(signer, true),
            AccountMeta::new(treasury, false),
            AccountMeta::new(system_address, false),
            AccountMeta::new(archive_address, false),
            AccountMeta::new(epoch_address, false),
            AccountMeta::new(metadata_address, false),
            AccountMeta::new(mint_address, false),
            AccountMeta::new_readonly(system_program::ID, false),
            AccountMeta::new_readonly(spl_token::ID, false),
            AccountMeta::new_readonly(spl_associated_token_account::ID, false),
            AccountMeta::new_readonly(mpl_token_metadata::ID, false),
            AccountMeta::new_readonly(sysvar::rent::ID, false),
        ],
        data: Initialize {}.to_bytes(),
    }
}

pub fn build_register_exchange_ix(
    signer: Pubkey,
) -> Instruction {

    let (mint_address, _) = mint_pda();
    let (exchange_address, _) = exchange_pda(signer);
    let (exchange_ata, _) = exchange_ata(exchange_address);

    Instruction {
        program_id: crate::ID,
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
        program_id: crate::ID,
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
        program_id: crate::ID,
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
        program_id: crate::ID,
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
        program_id: crate::ID,
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
        program_id: crate::ID,
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
        program_id: crate::ID,
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
        program_id: crate::ID,
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

pub fn build_advance_epoch_ix(
    signer: Pubkey
) ->Instruction {
    let (epoch_address, _) = epoch_pda();

    Instruction {
        program_id: crate::ID,
        accounts: vec![
            AccountMeta::new(signer, true),
            AccountMeta::new(epoch_address, false),
        ],
        data: AdvanceEpoch {}.to_bytes(),
    }
}

pub fn build_register_node_ix(
    signer: Pubkey,
    name: [u8; NAME_LENGTH],
    commission_rate: BasisPoints,
    network_address: NetworkAddress,
    network_tls: Pubkey,
) -> Instruction {

    let (system_address, _) = system_pda();
    let (epoch_address, _) = epoch_pda();
    let (node_address, _) = storage_node_pda(signer);

    let commission_rate = commission_rate.pack();

    Instruction {
        program_id: crate::ID,
        accounts: vec![
            AccountMeta::new(signer, true),
            AccountMeta::new(system_address, false),
            AccountMeta::new(epoch_address, false),
            AccountMeta::new(node_address, false),
            AccountMeta::new_readonly(system_program::ID, false),
            AccountMeta::new_readonly(sysvar::rent::ID, false),
        ],
        data: RegisterNode {
            name,
            commission_rate,
            network_address,
            network_tls,
        }.to_bytes(),
    }
}

pub fn build_stake_ix(
    signer: Pubkey,
    node_address: Pubkey,
    amount: Coin<TAPE>,
) -> Instruction {

    let (system_address, _) = system_pda();
    let (epoch_address, _) = epoch_pda();
    let (mint_address, _) = mint_pda();
    let (stake_address, _) = staked_tape_pda(signer, node_address);
    let stake_ata = get_associated_token_address(&stake_address, &mint_address);
    let signer_ata = get_associated_token_address(&signer, &mint_address);

    let amount = amount.pack();

    Instruction {
        program_id: crate::ID,
        accounts: vec![
            AccountMeta::new(signer, true),
            AccountMeta::new(signer_ata, false),
            AccountMeta::new(stake_address, false),
            AccountMeta::new(stake_ata, false),

            AccountMeta::new(system_address, false),
            AccountMeta::new(epoch_address, false),
            AccountMeta::new(node_address, false),
            AccountMeta::new(mint_address, false),

            AccountMeta::new_readonly(spl_token::ID, false),
            AccountMeta::new_readonly(spl_associated_token_account::ID, false),
            AccountMeta::new_readonly(system_program::ID, false),
            AccountMeta::new_readonly(sysvar::rent::ID, false),
        ],
        data: Stake {
            amount
        }.to_bytes(),
    }
}


