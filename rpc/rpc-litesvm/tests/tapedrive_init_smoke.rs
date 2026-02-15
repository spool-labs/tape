use std::path::PathBuf;
use std::str::FromStr;

use rpc_client::RpcClient;
use rpc_litesvm::LiteSvmRpc;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::{Keypair, Signer};

use rpc_client::tape_api;

fn workspace_root() -> PathBuf {
    let manifest_dir = env!("CARGO_MANIFEST_DIR");
    PathBuf::from(manifest_dir)
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .to_path_buf()
}

fn deploy_path(name: &str) -> PathBuf {
    workspace_root().join("target/deploy").join(format!("{name}.so"))
}

fn external_program_path(name: &str) -> PathBuf {
    workspace_root().join("test/elfs").join(format!("{name}.so"))
}

#[tokio::test]
async fn initialize_system_with_tapedrive_programs() {
    let rpc = LiteSvmRpc::new();

    // Load on-chain programs expected by tapedrive instruction flows.
    rpc.add_program_from_file(tape_api::program::tapedrive::ID, deploy_path("tapedrive"))
        .expect("load tapedrive program");
    rpc.add_program_from_file(tape_api::program::token::ID, deploy_path("token"))
        .expect("load token program");
    rpc.add_program_from_file(tape_api::program::exchange::ID, deploy_path("exchange"))
        .expect("load exchange program");
    rpc.add_program_from_file(tape_api::program::staking::ID, deploy_path("staking"))
        .expect("load staking program");

    // Metaplex program used by token metadata interactions.
    let mpl_id = Pubkey::from_str("metaqbxxUerdq28cj1RbAWkYQm3ybzjb6a8bt518x1s")
        .expect("valid mpl program id");
    rpc.add_program_from_file(mpl_id, external_program_path("mpl_token_metadata"))
        .expect("load mpl token metadata program");

    let client = RpcClient::from_rpc(rpc.clone());
    let payer = Keypair::new();

    rpc.airdrop(&payer.pubkey(), 20_000_000_000)
        .expect("airdrop payer");

    use tape_api::instruction::{
        build_create_system_ix, build_expand_system_ix, build_initialize_ix, build_initialize_mint_ix,
    };

    let mint_ix = build_initialize_mint_ix(payer.pubkey(), payer.pubkey());
    client
        .send_instructions(&payer, vec![mint_ix])
        .await
        .expect("initialize mint");

    let create_system_ix = build_create_system_ix(payer.pubkey(), payer.pubkey());
    client
        .send_instructions(&payer, vec![create_system_ix])
        .await
        .expect("create system");

    // Expand System account to full size (~70KB).
    for _ in 0..10 {
        let expand_ix = build_expand_system_ix(payer.pubkey(), payer.pubkey());
        let result = client.send_instructions(&payer, vec![expand_ix]).await;

        match result {
            Ok(_) => {}
            Err(e) => {
                let err_str = format!("{e:?}");
                if err_str.contains("AccountAlreadyInitialized")
                    || err_str.contains("already initialized")
                    || err_str.contains("uninitialized account")
                {
                    break;
                }
                panic!("expand failed unexpectedly: {e}");
            }
        }
    }

    let init_ix = build_initialize_ix(payer.pubkey(), payer.pubkey());
    client
        .send_instructions(&payer, vec![init_ix])
        .await
        .expect("initialize epoch/archive");

    client.get_system().await.expect("fetch system");
    client.get_epoch().await.expect("fetch epoch");
    client.get_archive().await.expect("fetch archive");
}
