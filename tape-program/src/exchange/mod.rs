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

#[cfg(test)]
mod tests {
    use tape_api::prelude::*;
    use tape_test::*;
    use solana_sdk::account::Account;

    fn create_exchange_data(signer: Pubkey, balance_tape: u64, balance_sol: u64, tape_rate: u64, sol_rate: u64) -> Exchange {
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

    fn create_exchange_account(address: Pubkey, data: &Exchange) -> Account {
        let mut account = pda(address, data.pack()).1;
        account.lamports += data.balance_sol.as_u64();
        account
    }

    #[test]
    fn test_set_exchange_rate() {
        let signer = Pubkey::new_unique();
        let tape_rate = 100; // 100 TAPE
        let sol_rate = 1; // 1 SOL

        let (exchange_address, _) = exchange_pda(signer);
        let exchange = create_exchange_data(signer, 0, 0, 1, 1);
        let exchange_acc = create_exchange_account(exchange_address, &exchange);
        let instruction = build_set_exchange_rate_ix(signer, exchange_address, tape_rate, sol_rate);

        let accounts = vec![
            sol(signer, 1_000_000_000),
            (exchange_address, exchange_acc),
        ];

        let expected_exchange = Exchange {
            rate: ExchangeRate {
                tape: tape_rate,
                other: sol_rate,
            },
            ..exchange
        };

        let env = test_env("tape".to_string());
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
        let signer = Pubkey::new_unique();
        let amount = TAPE(1000); // 0.001 TAPE

        let (exchange_address, _) = exchange_pda(signer);
        let exchange = create_exchange_data(signer, 500, 0, 1, 1);
        let exchange_acc = create_exchange_account(exchange_address, &exchange);
        let (exchange_ata, _) = exchange_ata(exchange_address);
        let signer_ata = ata_address(&signer);
        let instruction = build_deposit_tape_ix(signer, signer_ata, exchange_address, amount);

        let initial_signer_balance = 2000; // Sufficient for 1000
        let initial_exchange_ata_balance = 500;

        let accounts = vec![
            sol(signer, 1_000_000_000),
            token(signer_ata, signer, initial_signer_balance),
            (exchange_address, exchange_acc),
            token(exchange_ata, exchange_address, initial_exchange_ata_balance),
            token_program(),
        ];

        let expected_exchange = Exchange {
            balance_tape: exchange.balance_tape + amount,
            ..exchange
        };

        let env = test_env("tape".to_string());
        env.process_instruction(
            &instruction,
            &accounts,
            &[
                Check::success(),
                Check::account(&exchange_address).data(
                    expected_exchange.pack().as_ref()
                ).build(),
                Check::account(&signer_ata).data(
                    token(signer_ata, signer, initial_signer_balance - amount.as_u64()).1.data.as_ref()
                ).build(),
                Check::account(&exchange_ata).data(
                    token(exchange_ata, exchange_address, initial_exchange_ata_balance + amount.as_u64()).1.data.as_ref()
                ).build(),
            ]
        );
    }

    #[test]
    fn test_deposit_sol() {
        let signer = Pubkey::new_unique();
        let amount = SOL(1_000_000); // 0.001 SOL

        let (exchange_address, _) = exchange_pda(signer);
        let exchange = create_exchange_data(signer, 0, 500_000, 1, 1);
        let exchange_acc = create_exchange_account(exchange_address, &exchange);
        let initial_signer_lamports = 2_000_000_000; // Sufficient for 0.001 SOL
        let initial_exchange_lamports = exchange_acc.lamports;

        let accounts = vec![
            sol(signer, initial_signer_lamports),
            (exchange_address, exchange_acc),
            system_program(),
        ];

        let instruction = build_deposit_sol_ix(signer, exchange_address, amount);

        let expected_exchange = Exchange {
            balance_sol: exchange.balance_sol + amount,
            ..exchange
        };

        let env = test_env("tape".to_string());
        env.process_instruction(
            &instruction,
            &accounts,
            &[
                Check::success(),
                Check::account(&exchange_address).data(
                    expected_exchange.pack().as_ref()
                ).build(),
                Check::account(&signer).lamports(
                    initial_signer_lamports - amount.as_u64()
                ).build(),
                Check::account(&exchange_address).lamports(
                    initial_exchange_lamports + amount.as_u64()
                ).build(),
            ]
        );
    }

    #[test]
    fn test_withdraw_tape() {
        let signer = Pubkey::new_unique();
        let amount = TAPE(500); // 0.0005 TAPE

        let (exchange_address, _) = exchange_pda(signer);
        let exchange = create_exchange_data(signer, 1000, 0, 1, 1);
        let exchange_acc = create_exchange_account(exchange_address, &exchange);
        let (exchange_ata, _) = exchange_ata(exchange_address);
        let signer_ata = ata_address(&signer);
        let instruction = build_withdraw_tape_ix(signer, signer_ata, exchange_address, amount);

        let initial_signer_ata_balance = 500;
        let initial_exchange_ata_balance = 1000;

        let accounts = vec![
            sol(signer, 1_000_000_000),
            token(signer_ata, signer, initial_signer_ata_balance),
            (exchange_address, exchange_acc),
            token(exchange_ata, exchange_address, initial_exchange_ata_balance),
            token_program(),
        ];

        let expected_exchange = Exchange {
            balance_tape: exchange.balance_tape - amount,
            ..exchange
        };

        let env = test_env("tape".to_string());
        env.process_instruction(
            &instruction,
            &accounts,
            &[
                Check::success(),
                Check::account(&exchange_address).data(
                    expected_exchange.pack().as_ref()
                ).build(),
                Check::account(&signer_ata).data(
                    token(signer_ata, signer, initial_signer_ata_balance + amount.as_u64()).1.data.as_ref()
                ).build(),
                Check::account(&exchange_ata).data(
                    token(exchange_ata, exchange_address, initial_exchange_ata_balance - amount.as_u64()).1.data.as_ref()
                ).build(),
            ]
        );
    }

    #[test]
    fn test_withdraw_sol() {
        let signer = Pubkey::new_unique();
        let amount = SOL(1_000_000); // 0.001 SOL

        let (exchange_address, _) = exchange_pda(signer);
        let exchange = create_exchange_data(signer, 0, 2_000_000, 1, 1);
        let exchange_acc = create_exchange_account(exchange_address, &exchange);
        let initial_signer_lamports = 1_000_000_000;
        let initial_exchange_lamports = exchange_acc.lamports;

        let accounts = vec![
            sol(signer, initial_signer_lamports),
            (exchange_address, exchange_acc)
        ];

        let instruction = build_withdraw_sol_ix(signer, exchange_address, amount);

        let expected_exchange = Exchange {
            balance_sol: exchange.balance_sol - amount,
            ..exchange
        };

        let env = test_env("tape".to_string());
        env.process_instruction(
            &instruction,
            &accounts,
            &[
                Check::success(),
                Check::account(&exchange_address).data(
                    expected_exchange.pack().as_ref()
                ).build(),
                Check::account(&signer).lamports(
                    initial_signer_lamports + amount.as_u64()
                ).build(),
                Check::account(&exchange_address).lamports(
                    initial_exchange_lamports - amount.as_u64()
                ).build(),
            ]
        );
    }

    #[test]
    fn test_swap_for_tape() {
        let signer = Pubkey::new_unique();
        let amount_sol = SOL(1_000_000); // 0.001 SOL
        let tape_rate = 100; // 100 TAPE per 1 SOL
        let sol_rate = 1000;

        let amount_out_tape = amount_sol.as_u64() * tape_rate / sol_rate; // 0.001 * 100 = 0.1 TAPE = 100_000
        let (exchange_address, _) = exchange_pda(signer);
        let exchange = create_exchange_data(signer, 200_000, 500_000, tape_rate, sol_rate);
        let exchange_acc = create_exchange_account(exchange_address, &exchange);
        let (exchange_ata, _) = exchange_ata(exchange_address);
        let signer_ata = ata_address(&signer);
        let instruction = build_swap_for_tape_ix(signer, signer_ata, exchange_address, amount_sol);

        let initial_signer_lamports = 2_000_000_000;
        let initial_exchange_lamports = exchange_acc.lamports;
        let initial_signer_ata_balance = 500;
        let initial_exchange_ata_balance = 200_000;

        let accounts = vec![
            sol(signer, initial_signer_lamports),
            token(signer_ata, signer, initial_signer_ata_balance),
            (exchange_address, exchange_acc),
            token(exchange_ata, exchange_address, initial_exchange_ata_balance),
            system_program(),
            token_program(),
        ];

        let expected_exchange = Exchange {
            balance_tape: exchange.balance_tape - TAPE(amount_out_tape),
            balance_sol: exchange.balance_sol + amount_sol,
            ..exchange
        };

        let env = test_env("tape".to_string());
        env.process_instruction(
            &instruction,
            &accounts,
            &[
                Check::success(),
                Check::account(&exchange_address).data(
                    expected_exchange.pack().as_ref()
                ).build(),
                Check::account(&signer).lamports(
                    initial_signer_lamports - amount_sol.as_u64()
                ).build(),
                Check::account(&exchange_address).lamports(
                    initial_exchange_lamports + amount_sol.as_u64()
                ).build(),
                Check::account(&signer_ata).data(
                    token(signer_ata, signer, initial_signer_ata_balance + amount_out_tape).1.data.as_ref()
                ).build(),
                Check::account(&exchange_ata).data(
                    token(exchange_ata, exchange_address, initial_exchange_ata_balance - amount_out_tape).1.data.as_ref()
                ).build(),
            ]
        );
    }

    #[test]
    fn test_swap_for_sol() {
        let signer = Pubkey::new_unique();
        let amount_tape = TAPE(1000); // 0.001 TAPE
        let tape_rate = 100; // 100 TAPE per 1 SOL
        let sol_rate = 1000;

        let amount_out_sol = amount_tape.as_u64() * sol_rate / tape_rate; // 0.001 / 100 = 0.00001 SOL = 10_000
        let (exchange_address, _) = exchange_pda(signer);
        let exchange = create_exchange_data(signer, 2000, 1_000_000, tape_rate, sol_rate);
        let exchange_acc = create_exchange_account(exchange_address, &exchange);
        let (exchange_ata, _) = exchange_ata(exchange_address);
        let signer_ata = ata_address(&signer);
        let instruction = build_swap_for_sol_ix(signer, signer_ata, exchange_address, amount_tape);

        let initial_signer_lamports = 1_000_000_000;
        let initial_exchange_lamports = exchange_acc.lamports;
        let initial_signer_ata_balance = 1000;
        let initial_exchange_ata_balance = 2000;

        let accounts = vec![
            sol(signer, initial_signer_lamports),
            token(signer_ata, signer, initial_signer_ata_balance),
            (exchange_address, exchange_acc),
            token(exchange_ata, exchange_address, initial_exchange_ata_balance),
            token_program(),
        ];

        let expected_exchange = Exchange {
            balance_tape: exchange.balance_tape + amount_tape,
            balance_sol: exchange.balance_sol - SOL(amount_out_sol),
            ..exchange
        };

        let env = test_env("tape".to_string());
        env.process_instruction(
            &instruction,
            &accounts,
            &[
                Check::success(),
                Check::account(&exchange_address).data(
                    expected_exchange.pack().as_ref()
                ).build(),
                Check::account(&signer).lamports(
                    initial_signer_lamports + amount_out_sol
                ).build(),
                Check::account(&exchange_address).lamports(
                    initial_exchange_lamports - amount_out_sol
                ).build(),
                Check::account(&signer_ata).data(
                    token(signer_ata, signer, initial_signer_ata_balance - amount_tape.as_u64()).1.data.as_ref()
                ).build(),
                Check::account(&exchange_ata).data(
                    token(exchange_ata, exchange_address, initial_exchange_ata_balance + amount_tape.as_u64()).1.data.as_ref()
                ).build(),
            ]
        );
    }
}
