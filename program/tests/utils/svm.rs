use std::path::PathBuf;
use std::str::FromStr;
use std::error::Error;
use solana_client::nonblocking::rpc_client::RpcClient;
use steel::Pubkey;
use tape_api::prelude::InstructionType;
use solana_sdk::{signature::Keypair, signer::Signer, transaction::Transaction};
use solana_sdk::bpf_loader_upgradeable::UpgradeableLoaderState;
use solana_compute_budget::compute_budget::ComputeBudget;
use litesvm::{types::{TransactionMetadata, TransactionResult}, LiteSVM};
use pretty_hex::*;
use bincode;

pub fn program_bytes() -> Vec<u8> {
    let mut so_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    so_path.push("../target/deploy/tape.so");
    std::fs::read(so_path).unwrap()
}

pub async fn metaplex_bytes() -> Result<Vec<u8>, Box<dyn Error>> {
    let data_account = Pubkey::from_str("PwDiXFxQsGra4sFFTT8r1QWRMd4vfumiWC1jfWNfdYT")?;
    let client = RpcClient::new("https://api.mainnet-beta.solana.com".to_string());

    let account_data = client.get_account_data(&data_account).await?;
    let state: UpgradeableLoaderState = bincode::deserialize(&account_data)?;

    let elf_data = match state {
        UpgradeableLoaderState::ProgramData { .. } => {
            let metadata_size = UpgradeableLoaderState::size_of_programdata_metadata();
            if account_data.len() < metadata_size {
                return Err("Account data too short".into());
            }
            account_data[metadata_size..].to_vec()
        }
        _ => return Err("Not a ProgramData account".into()),
    };

    Ok(elf_data)
}

pub async fn setup_svm() -> Result<LiteSVM, Box<dyn Error>> {
    let mut svm = LiteSVM::new().with_compute_budget(ComputeBudget {
        compute_unit_limit: 1_000_000,
        ..Default::default()
    });

    let metaplex_data = metaplex_bytes().await?;

    svm.add_program(mpl_token_metadata::ID, &metaplex_data);
    svm.add_program(tape_api::ID, &program_bytes());

    Ok(svm)
}

pub fn send_tx(svm: &mut LiteSVM, tx: Transaction) -> TransactionResult {
    let res = svm.send_transaction(tx.clone());

    let meta = match res.as_ref() {
        Ok(v) => v.clone(),
        Err(v) => v.meta.clone()
    };

    print_tx(meta, tx);

    if res.is_err() {
        println!("error:\t{:?}", res.as_ref().err().unwrap().err);
    }

    res.clone()
}

pub fn create_payer(svm: &mut LiteSVM) -> Keypair {
    let payer_kp = Keypair::new();
    let payer_pk = payer_kp.pubkey();
    svm.airdrop(&payer_pk, 1_000_000_000).unwrap();
    payer_kp
}

pub fn create_keypair() -> Keypair {
    Keypair::new()
}

pub fn get_tx_size(tx: &Transaction) -> usize {
    bincode::serialize(tx).unwrap().len()
}

pub fn print_tx(meta: TransactionMetadata, tx: Transaction) {
    let msg = tx.message().serialize();

    println!("\n");
    println!("--------------------------------------------------------------------------------");
    println!("sig:\t{:?}", meta.signature);
    println!("len:\t{:?}", msg.len());

    for i in 0..tx.message.instructions.len() {
        let ix = &tx.message.instructions[i];
        let ix_type = InstructionType::try_from(ix.data[0] as u8).unwrap();

        println!("\nix:\t{:?} ({})", ix_type, ix.data[0]);
        println!("accounts:");

        for key in &ix.accounts {
            println!("\t{}: {:?}", key, tx.message.account_keys[*key as usize]);
        }

        println!("\ndata:\n\t{:?}", ix.data);
        println!("\n\n{}\n", pretty_hex(&ix.data))
    }

    println!("");
    println!("size:\t{:?}", get_tx_size(&tx));
    println!("cu:\t{:?}", meta.compute_units_consumed);
    println!("logs:");
    for log in &meta.logs {
        println!("\t{:?}", log);
    }
    println!("");
}
