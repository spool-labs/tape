use std::path::PathBuf;
use std::str::FromStr;

use rpc_client::RpcClient;
use rpc_litesvm::LiteSvmRpc;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::{Keypair, Signer};
use tape_api::genesis::GenesisConfig;
use tape_api::instruction::{
    build_create_archive_ix, build_create_committee_ix, build_create_epoch_ix,
    build_create_peer_set_ix, build_create_system_ix, build_initialize_mint_ix,
};
use tape_api::prelude::EpochNumber;
use tape_crypto::address::Address;
use tape_crypto::ed25519::Keypair as TapeKeypair;

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

    // RpcClient signs with tape's own Signer; derive it from the funded payer.
    let signer = TapeKeypair::from_solana_keypair(&payer).expect("derive tape keypair");
    let admin: Address = payer.pubkey().into();
    let config = GenesisConfig::local();

    // Genesis singleton bringup, mirroring tape-admin `init_all`: mint, system,
    // peer set, archive, then the bootstrap/genesis/candidate epoch + committee
    // accounts. Node staging and StartNetwork are out of scope for this smoke test.
    client
        .send_instructions(&signer, vec![build_initialize_mint_ix(admin, admin)])
        .await
        .expect("initialize mint");

    client
        .send_instructions(&signer, vec![build_create_system_ix(admin, admin, &config)])
        .await
        .expect("create system");

    client
        .send_instructions(&signer, vec![build_create_peer_set_ix(admin)])
        .await
        .expect("create peer set");

    client
        .send_instructions(&signer, vec![build_create_archive_ix(admin, admin, &config)])
        .await
        .expect("create archive");

    for epoch in [EpochNumber(0), EpochNumber(1), EpochNumber(2)] {
        client
            .send_instructions(&signer, vec![build_create_epoch_ix(admin, epoch)])
            .await
            .expect("create epoch");
        client
            .send_instructions(&signer, vec![build_create_committee_ix(admin, epoch)])
            .await
            .expect("create committee");
    }

    client.get_system().await.expect("fetch system");
    client.get_epoch(EpochNumber(0)).await.expect("fetch epoch");
    client.get_archive().await.expect("fetch archive");
}
