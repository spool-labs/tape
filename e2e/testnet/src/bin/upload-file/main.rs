use std::path::PathBuf;
use std::time::Instant;

use anyhow::{Context, Result, ensure};
use clap::Parser;
use rpc_solana::RpcConfig;
use tape_core::types::StorageUnits;
use tape_crypto::ed25519::Keypair as CryptoKeypair;
use tape_sdk::keys::helpers::load_solana_keypair;
use tape_sdk::keys::tape_key::TapeKey;
use tape_sdk::stream::manifest::CHUNK_SIZE;
use tape_sdk::tapedrive::Tapedrive;
use tokio::io::AsyncReadExt;

const DEFAULT_FILE_SIZE_BYTES: usize = 1 << 30;
const RESERVE_HEADROOM_BYTES: u64 = 1 << 20;
const DEFAULT_UPLOAD_EPOCHS: u64 = 4;
const DEFAULT_FILL_BYTE: u8 = 0xA5;

#[derive(Parser, Debug)]
#[command(name = "upload-file", about = "Upload a large file against the running testnet")]
struct Cli {
    #[arg(long, default_value = "http://127.0.0.1:8899")]
    rpc_url: String,

    #[arg(long, default_value = "target/testnet/admin.json")]
    admin_keypair: PathBuf,

    #[arg(long, default_value_t = DEFAULT_FILE_SIZE_BYTES)]
    size_bytes: usize,

    #[arg(long, default_value_t = DEFAULT_UPLOAD_EPOCHS)]
    epochs: u64,

    #[arg(long, default_value_t = DEFAULT_FILL_BYTE)]
    fill_byte: u8,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    ensure!(cli.size_bytes > 0, "--size-bytes must be greater than zero");

    let admin = load_solana_keypair(&cli.admin_keypair)
        .with_context(|| format!("load admin keypair: {}", cli.admin_keypair.display()))?;
    let rpc = rpc_solana::SolanaRpc::new(RpcConfig {
        endpoints: vec![cli.rpc_url.clone()],
        ..Default::default()
    })
    .context("create rpc client")?;
    let admin = CryptoKeypair::from_solana_keypair(&admin)
        .context("convert admin keypair")?;
    let sdk = Tapedrive::new(rpc, admin);

    let tape_key = TapeKey::generate();
    let size = StorageUnits::from_bytes(cli.size_bytes as u64);
    let chunk_count = size.to_bytes().div_ceil(CHUNK_SIZE as u64);
    let reserve_capacity = size + StorageUnits::from_bytes(RESERVE_HEADROOM_BYTES);

    println!("preparing upload stream");
    println!("  size_bytes: {}", cli.size_bytes);
    println!("  chunk_count: {}", chunk_count);
    println!("  tape_address: {}", tape_key.address());

    println!("reserving tape");
    let reserve_start = Instant::now();
    sdk.reserve(&tape_key, reserve_capacity, cli.epochs)
        .await
        .context("reserve tape")?;
    println!("reserve completed in {:.2?}", reserve_start.elapsed());

    println!("uploading file");
    let upload_start = Instant::now();
    let data = tokio::io::repeat(cli.fill_byte).take(size.to_bytes());
    let receipt = sdk
        .write_stream(&tape_key, size, data)
        .await
        .context("write file")?;
    println!("upload completed in {:.2?}", upload_start.elapsed());

    println!("manifest address: {}", receipt.manifest);
    println!("manifest track number: {}", receipt.manifest_track_number.as_u64());
    println!("tape address: {}", receipt.tape);

    Ok(())
}
