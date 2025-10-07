use tape_api::prelude::*;
use steel::*;

pub fn process_register_exchange(accounts: &[AccountInfo<'_>], data: &[u8]) -> ProgramResult {
    let _args = RegisterExchange::try_from_bytes(data)?;
    let [
        signer_info, 
        exchange_info,
        exchange_ata_info, 
        mint_info, 
        system_program_info, 
        token_program_info, 
        associated_token_program_info, 
        rent_sysvar_info,
    ] = accounts else {
        return Err(ProgramError::NotEnoughAccountKeys);
    };

    mint_info
        .has_address(&MINT_ADDRESS)?;

    let (exchange_address, _) = exchange_pda(*signer_info.key);

    exchange_info
        .is_empty()?
        .is_writable()?
        .has_address(&exchange_address)?;

    exchange_ata_info
        .is_empty()?
        .is_writable()?;

    // Check programs and sysvars.
    system_program_info
        .is_program(&system_program::ID)?;
    token_program_info
        .is_program(&spl_token::ID)?;
    associated_token_program_info
        .is_program(&spl_associated_token_account::ID)?;
    rent_sysvar_info
        .is_sysvar(&sysvar::rent::ID)?;

    solana_program::log::msg!("Creating exchange...");

    // Initialize exchange.
    create_program_account::<Exchange>(
        exchange_info,
        system_program_info,
        signer_info,
        &tape_api::ID,
        &[EXCHANGE, signer_info.key.as_ref()],
    )?;

    solana_program::log::msg!("Initializing exchange...");
    let exchange = exchange_info.as_account_mut::<Exchange>(&tape_api::ID)?;

    exchange.authority = *signer_info.key;
    exchange.balance_sol = SOL::zero();
    exchange.balance_tape = TAPE::zero();
    exchange.rate = ExchangeRate::flat();

    solana_program::log::msg!("Creating exchange token account...");
    // Initialize exchange token account.
    create_associated_token_account(
        signer_info,
        exchange_info,
        exchange_ata_info,
        mint_info,
        system_program_info,
        token_program_info,
        associated_token_program_info,
    )?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    use spl_token::{
        state::{Account as TokenAccount, Mint},
    };
    use mollusk_svm::{
        Mollusk, 
        program::keyed_account_for_system_program,
        sysvar::Sysvars,
    };
    use mollusk_svm_programs_token::{associated_token, token};

    use solana_sdk::{
        account::{AccountSharedData, Account},
        pubkey::Pubkey,
        rent::Rent,
        program_pack::Pack,
    };

    fn funded(key: Pubkey, lamports: u64) -> (Pubkey, Account) {
        (key, Account {
            lamports,
            data: vec![],
            owner: system_program::ID,
            executable: false,
            rent_epoch: 0,
        })
    }

    fn mint(key: Pubkey) -> (Pubkey, Account) {
        let mint_data = Mint {
            mint_authority: Some(key).into(),
            supply: 1,
            decimals: 6,
            is_initialized: true,
            freeze_authority: None.into(),
        };

        let mut data = vec![0u8; Mint::LEN];
        Mint::pack(mint_data, &mut data).unwrap();

        (key, Account {
            lamports: Rent::default().minimum_balance(Mint::LEN),
            data,
            owner: token::ID,
            executable: false,
            rent_epoch: 0,
        })
    }

    #[test]
    fn test_register() {
        let sysvars = Sysvars::default();

        let signer = Pubkey::new_unique();
        let instruction = build_register_exchange_ix(signer);

        let (mint_address, _) = mint_pda();
        let (exchange_address, _) = exchange_pda(signer);
        let (exchange_ata, _) = exchange_ata(exchange_address);

        let accounts = vec![
            funded(signer, 1_000_000_000),

            (exchange_address, Account::default()),
            (exchange_ata, Account::default()),
            mint(mint_address),

            keyed_account_for_system_program(),
            token::keyed_account(),
            associated_token::keyed_account(),
            sysvars.keyed_account_for_rent_sysvar(),
        ];

        let mut mollusk = Mollusk::new(&tape_api::ID, "../target/deploy/tape");
        token::add_program(&mut mollusk);
        associated_token::add_program(&mut mollusk);

        mollusk.process_instruction(&instruction, &accounts );
    }
}
