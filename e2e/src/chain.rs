use std::path::PathBuf;
use std::str::FromStr;

use anyhow::{Context, Result};
use rpc_client::RpcClient;
use rpc_litesvm::LiteSvmRpc;
use solana_sdk::pubkey::Pubkey as SolanaPubkey;
use solana_sdk::signature::{Keypair, Signer};
use tape_api::instruction::{
    build_create_system_ix, build_expand_system_ix, build_initialize_ix, build_initialize_mint_ix,
    build_reserve_snapshot_tape_ix,
};

fn ws_root() -> PathBuf {
    let manifest_dir = env!("CARGO_MANIFEST_DIR");
    PathBuf::from(manifest_dir).parent().unwrap().to_path_buf()
}

fn dep_path(name: &str) -> PathBuf {
    ws_root().join("target/deploy").join(format!("{name}.so"))
}

fn elf_path(name: &str) -> PathBuf {
    ws_root().join("test/elfs").join(format!("{name}.so"))
}

pub fn load_programs(rpc: &LiteSvmRpc) -> Result<()> {
    rpc.add_program_from_file(tape_api::program::tapedrive::ID, dep_path("tapedrive"))
        .context("load tapedrive program")?;
    rpc.add_program_from_file(tape_api::program::token::ID, dep_path("token"))
        .context("load token program")?;
    rpc.add_program_from_file(tape_api::program::exchange::ID, dep_path("exchange"))
        .context("load exchange program")?;
    rpc.add_program_from_file(tape_api::program::staking::ID, dep_path("staking"))
        .context("load staking program")?;

    let mpl_id = SolanaPubkey::from_str("metaqbxxUerdq28cj1RbAWkYQm3ybzjb6a8bt518x1s")
        .context("parse mpl id")?;
    rpc.add_program_from_file(mpl_id, elf_path("mpl_token_metadata"))
        .context("load mpl token metadata")?;

    Ok(())
}

pub async fn init_chain(cli: &RpcClient<LiteSvmRpc>, pay: &Keypair) -> Result<()> {
    cli.send_instructions(
        pay,
        vec![build_initialize_mint_ix(pay.pubkey(), pay.pubkey())],
    )
    .await
    .context("init mint")?;

    cli.send_instructions(pay, vec![build_create_system_ix(pay.pubkey(), pay.pubkey())])
        .await
        .context("create system")?;

    for _ in 0..10 {
        let res = cli
            .send_instructions(pay, vec![build_expand_system_ix(pay.pubkey(), pay.pubkey())])
            .await;
        if let Err(e) = res {
            let msg = format!("{e:?}");
            if msg.contains("AccountAlreadyInitialized")
                || msg.contains("already initialized")
                || msg.contains("uninitialized account")
            {
                break;
            }
            return Err(anyhow::anyhow!("expand system failed: {e}"));
        }
    }

    cli.send_instructions(pay, vec![build_initialize_ix(pay.pubkey(), pay.pubkey())])
        .await
        .context("init tape system")?;

    cli.send_instructions(pay, vec![build_reserve_snapshot_tape_ix(pay.pubkey())])
        .await
        .context("reserve snapshot tape")?;

    Ok(())
}
