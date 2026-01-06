#![allow(unexpected_cfgs)]

mod error;
mod deposit_sol;
mod deposit_tape;
mod register;
mod set_rate;
mod swap_sol;
mod swap_tape;
mod withdraw_sol;
mod withdraw_tape;

pub use deposit_sol::*;
pub use deposit_tape::*;
pub use register::*;
pub use set_rate::*;
pub use swap_sol::*;
pub use swap_tape::*;
pub use withdraw_sol::*;
pub use withdraw_tape::*;

use tape_api::prelude::*;
use tape_solana::*;

pub fn process_instruction(
    program_id: &Pubkey,
    accounts: &[AccountInfo],
    data: &[u8],
) -> ProgramResult {
    let (discriminator, data) = parse_instruction(&exchange::ID, program_id, data)?;

    solana_program::msg!("Exchange Program ID: {}", exchange::id());

    let ix_type = if let Ok(instruction) = ExchangeInstruction::try_from_primitive(discriminator) {
        format!("{:?}", instruction)
    } else {
        format!("Invalid (discriminator: {})", discriminator)
    };

    solana_program::msg!("Instruction: {}", ix_type);

    if let Ok(ix) = ExchangeInstruction::try_from_primitive(discriminator) {
        match ix {

            ExchangeInstruction::RegisterExchange => process_register_exchange(accounts, data)?,
            ExchangeInstruction::SetExchangeRate => process_set_exchange_rate(accounts, data)?,
            ExchangeInstruction::DepositSol => process_deposit_sol(accounts, data)?,
            ExchangeInstruction::DepositTape => process_deposit_tape(accounts, data)?,
            ExchangeInstruction::WithdrawSol => process_withdraw_sol(accounts, data)?,
            ExchangeInstruction::WithdrawTape => process_withdraw_tape(accounts, data)?,
            ExchangeInstruction::SwapForTape => process_swap_for_tape(accounts, data)?,
            ExchangeInstruction::SwapForSol => process_swap_for_sol(accounts, data)?,

            _ => return Err(ProgramError::InvalidInstructionData),
        }
    } else {
        return Err(ProgramError::InvalidInstructionData);
    }

    Ok(())
}

entrypoint!(process_instruction);


#[cfg(test)]
mod tests {
    use tape_api::prelude::*;
    use tape_test::*;
    use solana_sdk::account::Account;

    fn create_exchange(
        signer: Pubkey, 
        balance_tape: u64, 
        balance_sol: u64, 
        tape_rate: u64, 
        sol_rate: u64
    ) -> Exchange {

        Exchange {
            authority: signer,
            balance_tape: TAPE(balance_tape),
            balance_sol: SOL(balance_sol),
            rate: ExchangeRate {
                tape: tape_rate,
                other: sol_rate,
            },
        }
    }

    fn create_account(address: Pubkey, data: &Exchange) -> Account {
        let mut account = pda(address, data.pack(), exchange::ID).1;
        account.lamports += data.balance_sol.as_u64();
        account
    }

    #[test]
    fn test_register() {
        let fee_payer = Pubkey::new_unique();
        let authority = Pubkey::new_unique();
        let instruction = build_register_exchange_ix(fee_payer, authority);

        let (exchange_address, _) = exchange_pda(authority);
        let (exchange_ata, _) = exchange_ata(exchange_address);

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            sol(authority, 0),
            empty(exchange_address),
            empty(exchange_ata),
            mint(1_000),

            system_program(),
            token_program(),
            ata_program(),
            rent_sysvar(),
        ];

        let env = test_env();
        env.process_instruction(
            &instruction,
            &accounts,
            &[
                Check::success(),
                Check::account(&exchange_address).data(
                    Exchange {
                        authority: authority,
                        balance_tape: TAPE::zero(),
                        balance_sol: SOL::zero(),
                        rate: ExchangeRate::flat(),
                    }.pack().as_ref()
                ).build(),
            ]
        );
    }

    #[test]
    fn test_set_exchange_rate() {
        let fee_payer = Pubkey::new_unique();
        let authority = Pubkey::new_unique();
        let tape_rate = 100; // 100 TAPE
        let sol_rate = 1; // 1 SOL

        let (exchange_address, _) = exchange_pda(authority);
        let exchange = create_exchange(authority, 0, 0, 1, 1);
        let account = create_account(exchange_address, &exchange);

        let instruction = build_set_exchange_rate_ix(fee_payer, authority, exchange_address, tape_rate, sol_rate);

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            sol(authority, 0),
            (exchange_address, account),
        ];

        let expected_exchange = Exchange {
            rate: ExchangeRate {
                tape: tape_rate,
                other: sol_rate,
            },
            ..exchange
        };

        let env = test_env();
        env.process_instruction(
            &instruction,
            &accounts,
            &[
                Check::success(),
                Check::account(&exchange_address).data(
                    expected_exchange.pack().as_ref()
                ).build(),
            ]
        );
    }

    #[test]
    fn test_deposit_tape() {
        let fee_payer = Pubkey::new_unique();
        let authority = Pubkey::new_unique();
        let authority_ata = ata_address(&authority);

        let amount = TAPE(1000); // 0.001 TAPE

        let (exchange_address, _) = exchange_pda(authority);
        let (exchange_ata, _) = exchange_ata(exchange_address);

        let exchange = create_exchange(authority, 500, 0, 1, 1);
        let account = create_account(exchange_address, &exchange);
        let instruction = build_deposit_tape_ix(fee_payer, authority, authority_ata, exchange_address, amount);

        let initial_authority_balance = 2000; // Sufficient for 1000
        let initial_exchange_ata_balance = 500;

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            sol(authority, 0),
            token(authority_ata, authority, initial_authority_balance),
            (exchange_address, account),
            token(exchange_ata, exchange_address, initial_exchange_ata_balance),
            token_program(),
        ];

        let expected_exchange = Exchange {
            balance_tape: exchange.balance_tape + amount,
            ..exchange
        };

        let env = test_env();
        env.process_instruction(
            &instruction,
            &accounts,
            &[
                Check::success(),
                Check::account(&exchange_address).data(
                    expected_exchange.pack().as_ref()
                ).build(),
                Check::account(&authority_ata).data(
                    token(authority_ata, authority, initial_authority_balance - amount.as_u64()).1.data.as_ref()
                ).build(),
                Check::account(&exchange_ata).data(
                    token(exchange_ata, exchange_address, initial_exchange_ata_balance + amount.as_u64()).1.data.as_ref()
                ).build(),
            ]
        );
    }

    #[test]
    fn test_deposit_sol() {
        let fee_payer = Pubkey::new_unique();
        let authority = Pubkey::new_unique();
        let amount = SOL(1_000_000); // 0.001 SOL

        let (exchange_address, _) = exchange_pda(authority);

        let exchange = create_exchange(authority, 0, 500_000, 1, 1);
        let account = create_account(exchange_address, &exchange);

        let initial_authority_lamports = 2_000_000_000; // Sufficient for 0.001 SOL
        let initial_exchange_lamports = account.lamports;

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            sol(authority, initial_authority_lamports),
            (exchange_address, account),
            system_program(),
        ];

        let instruction = build_deposit_sol_ix(fee_payer, authority, exchange_address, amount);

        let expected_exchange = Exchange {
            balance_sol: exchange.balance_sol + amount,
            ..exchange
        };

        let env = test_env();
        env.process_instruction(
            &instruction,
            &accounts,
            &[
                Check::success(),
                Check::account(&exchange_address).data(
                    expected_exchange.pack().as_ref()
                ).build(),
                Check::account(&authority).lamports(
                    initial_authority_lamports - amount.as_u64()
                ).build(),
                Check::account(&exchange_address).lamports(
                    initial_exchange_lamports + amount.as_u64()
                ).build(),
            ]
        );
    }

    #[test]
    fn test_withdraw_tape() {
        let fee_payer = Pubkey::new_unique();
        let authority = Pubkey::new_unique();
        let authority_ata = ata_address(&authority);

        let amount = TAPE(500); // 0.0005 TAPE

        let (exchange_address, _) = exchange_pda(authority);
        let (exchange_ata, _) = exchange_ata(exchange_address);

        let exchange = create_exchange(authority, 1000, 0, 1, 1);
        let account = create_account(exchange_address, &exchange);
        let instruction = build_withdraw_tape_ix(fee_payer, authority, authority_ata, exchange_address, amount);

        let initial_authority_ata_balance = 500;
        let initial_exchange_ata_balance = 1000;

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            sol(authority, 0),
            token(authority_ata, authority, initial_authority_ata_balance),
            (exchange_address, account),
            token(exchange_ata, exchange_address, initial_exchange_ata_balance),
            token_program(),
        ];

        let expected_exchange = Exchange {
            balance_tape: exchange.balance_tape - amount,
            ..exchange
        };

        let env = test_env();
        env.process_instruction(
            &instruction,
            &accounts,
            &[
                Check::success(),
                Check::account(&exchange_address).data(
                    expected_exchange.pack().as_ref()
                ).build(),
                Check::account(&authority_ata).data(
                    token(authority_ata, authority, initial_authority_ata_balance + amount.as_u64()).1.data.as_ref()
                ).build(),
                Check::account(&exchange_ata).data(
                    token(exchange_ata, exchange_address, initial_exchange_ata_balance - amount.as_u64()).1.data.as_ref()
                ).build(),
            ]
        );
    }

    #[test]
    fn test_withdraw_sol() {
        let fee_payer = Pubkey::new_unique();
        let authority = Pubkey::new_unique();
        let amount = SOL(1_000_000); // 0.001 SOL

        let (exchange_address, _) = exchange_pda(authority);
        let exchange = create_exchange(authority, 0, 2_000_000, 1, 1);
        let account = create_account(exchange_address, &exchange);

        let initial_authority_lamports = 1_000_000_000;
        let initial_exchange_lamports = account.lamports;

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            sol(authority, initial_authority_lamports),
            (exchange_address, account),
            rent_sysvar(),
        ];

        let instruction = build_withdraw_sol_ix(fee_payer, authority, exchange_address, amount);

        let expected_exchange = Exchange {
            balance_sol: exchange.balance_sol - amount,
            ..exchange
        };

        let env = test_env();
        env.process_instruction(
            &instruction,
            &accounts,
            &[
                Check::success(),
                Check::account(&exchange_address).data(
                    expected_exchange.pack().as_ref()
                ).build(),
                Check::account(&authority).lamports(
                    initial_authority_lamports + amount.as_u64()
                ).build(),
                Check::account(&exchange_address).lamports(
                    initial_exchange_lamports - amount.as_u64()
                ).build(),
            ]
        );
    }

    #[test]
    fn test_swap_for_tape() {
        let fee_payer = Pubkey::new_unique();
        let authority = Pubkey::new_unique();
        let authority_ata = ata_address(&authority);

        let amount_sol = SOL(1_000_000); // 0.001 SOL
        let tape_rate = 100; // 100 TAPE per 1 SOL
        let sol_rate = 1000;

        let amount_out_tape = amount_sol.as_u64() * tape_rate / sol_rate; // 0.001 * 100 = 0.1 TAPE = 100_000

        let (exchange_address, _) = exchange_pda(authority);
        let (exchange_ata, _) = exchange_ata(exchange_address);

        let exchange = create_exchange(authority, 200_000, 500_000, tape_rate, sol_rate);
        let account = create_account(exchange_address, &exchange);
        let instruction = build_swap_for_tape_ix(fee_payer, authority, authority_ata, exchange_address, amount_sol);

        let initial_authority_lamports = 2_000_000_000;
        let initial_exchange_lamports = account.lamports;
        let initial_authority_ata_balance = 500;
        let initial_exchange_ata_balance = 200_000;

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            sol(authority, initial_authority_lamports),
            token(authority_ata, authority, initial_authority_ata_balance),
            (exchange_address, account),
            token(exchange_ata, exchange_address, initial_exchange_ata_balance),
            system_program(),
            token_program(),
        ];

        let expected_exchange = Exchange {
            balance_tape: exchange.balance_tape - TAPE(amount_out_tape),
            balance_sol: exchange.balance_sol + amount_sol,
            ..exchange
        };

        let env = test_env();
        env.process_instruction(
            &instruction,
            &accounts,
            &[
                Check::success(),
                Check::account(&exchange_address).data(
                    expected_exchange.pack().as_ref()
                ).build(),
                Check::account(&authority).lamports(
                    initial_authority_lamports - amount_sol.as_u64()
                ).build(),
                Check::account(&exchange_address).lamports(
                    initial_exchange_lamports + amount_sol.as_u64()
                ).build(),
                Check::account(&authority_ata).data(
                    token(authority_ata, authority, initial_authority_ata_balance + amount_out_tape).1.data.as_ref()
                ).build(),
                Check::account(&exchange_ata).data(
                    token(exchange_ata, exchange_address, initial_exchange_ata_balance - amount_out_tape).1.data.as_ref()
                ).build(),
            ]
        );
    }

    #[test]
    fn test_swap_for_sol() {
        let fee_payer = Pubkey::new_unique();
        let authority = Pubkey::new_unique();
        let authority_ata = ata_address(&authority);

        let amount_tape = TAPE(1000); // 0.001 TAPE
        let tape_rate = 100; // 100 TAPE per 1 SOL
        let sol_rate = 1000;

        let amount_out_sol = amount_tape.as_u64() * sol_rate / tape_rate; // 0.001 / 100 = 0.00001 SOL = 10_000
        let (exchange_address, _) = exchange_pda(authority);
        let (exchange_ata, _) = exchange_ata(exchange_address);
        let exchange = create_exchange(authority, 2000, 1_000_000, tape_rate, sol_rate);
        let account = create_account(exchange_address, &exchange);

        let instruction = build_swap_for_sol_ix(fee_payer, authority, authority_ata, exchange_address, amount_tape);

        let initial_authority_lamports = 1_000_000_000;
        let initial_exchange_lamports = account.lamports;
        let initial_authority_ata_balance = 1000;
        let initial_exchange_ata_balance = 2000;

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            sol(authority, initial_authority_lamports),
            token(authority_ata, authority, initial_authority_ata_balance),
            (exchange_address, account),
            token(exchange_ata, exchange_address, initial_exchange_ata_balance),
            token_program(),
            rent_sysvar(),
        ];

        let expected_exchange = Exchange {
            balance_tape: exchange.balance_tape + amount_tape,
            balance_sol: exchange.balance_sol - SOL(amount_out_sol),
            ..exchange
        };

        let env = test_env();
        env.process_instruction(
            &instruction,
            &accounts,
            &[
                Check::success(),
                Check::account(&exchange_address).data(
                    expected_exchange.pack().as_ref()
                ).build(),
                Check::account(&authority).lamports(
                    initial_authority_lamports + amount_out_sol
                ).build(),
                Check::account(&exchange_address).lamports(
                    initial_exchange_lamports - amount_out_sol
                ).build(),
                Check::account(&authority_ata).data(
                    token(authority_ata, authority, initial_authority_ata_balance - amount_tape.as_u64()).1.data.as_ref()
                ).build(),
                Check::account(&exchange_ata).data(
                    token(exchange_ata, exchange_address, initial_exchange_ata_balance + amount_tape.as_u64()).1.data.as_ref()
                ).build(),
            ]
        );
    }
}
