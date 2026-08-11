//! Measure the window where a just-written track cannot be deleted.

use std::path::PathBuf;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use clap::Parser;
use rpc_solana::RpcConfig;
use tape_api::program::tapedrive::track_pda;
use tape_core::types::StorageUnits;
use tape_crypto::ed25519::Keypair as CryptoKeypair;
use tape_sdk::keys::helpers::load_solana_keypair;
use tape_sdk::keys::tape_key::TapeKey;
use tape_sdk::tapedrive::Tapedrive;

const RESERVE_HEADROOM_BYTES: u64 = 1 << 20;

#[derive(Parser, Debug)]
#[command(name = "delete-race", about = "Time the delete-after-write window on a running localnet")]
struct Cli {
    #[arg(long, default_value = "http://127.0.0.1:8899")]
    rpc_url: String,

    #[arg(long, default_value = "target/localnet/admin.json")]
    admin_keypair: PathBuf,

    /// Uploads to run, each on its own fresh tape.
    #[arg(long, default_value_t = 5)]
    rounds: usize,

    /// Payload size, small enough to stay on the inline path by default.
    #[arg(long, default_value_t = 4096)]
    size_bytes: usize,

    /// Epochs to reserve each tape.
    #[arg(long, default_value_t = 2)]
    epochs: u64,

    /// How long to keep retrying a refused delete.
    #[arg(long, default_value_t = 90)]
    give_up_secs: u64,

    /// Seconds to let the tape reservation finalize before writing.
    ///
    /// Discriminates the two candidate blockers: if the window collapses when
    /// only the tape has settled, the tape read is what refuses the delete.
    #[arg(long, default_value_t = 0)]
    settle_tape_secs: u64,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    let admin = load_solana_keypair(&cli.admin_keypair)
        .with_context(|| format!("load admin keypair: {}", cli.admin_keypair.display()))?;
    let admin = CryptoKeypair::from_solana_keypair(&admin).context("convert admin keypair")?;
    let rpc = rpc_solana::SolanaRpc::new(RpcConfig {
        endpoints: vec![cli.rpc_url.clone()],
        ..Default::default()
    })
    .context("create rpc client")?;
    let sdk = Tapedrive::new(rpc, admin);

    let size = StorageUnits::from_bytes(cli.size_bytes as u64);
    let give_up = Duration::from_secs(cli.give_up_secs);
    let mut refused_rounds = 0usize;

    for round in 1..=cli.rounds {
        let tape_key = TapeKey::generate();
        sdk.reserve(
            &tape_key,
            size + StorageUnits::from_bytes(RESERVE_HEADROOM_BYTES),
            cli.epochs,
        )
        .await
        .context("reserve tape")?;

        if cli.settle_tape_secs > 0 {
            tokio::time::sleep(Duration::from_secs(cli.settle_tape_secs)).await;
        }

        let payload: Vec<u8> = (0..cli.size_bytes).map(|i| (i % 251) as u8).collect();
        let written_track = sdk
            .write_track(&tape_key, &payload)
            .await
            .context("write track")?;
        let track = track_pda(written_track.tape, written_track.track_number).0;

        let written = Instant::now();
        let mut attempts = 0usize;
        let mut first_refusal = None;

        loop {
            attempts += 1;
            match sdk.delete(&tape_key, track).await {
                Ok(()) => {
                    let waited = written.elapsed();
                    if attempts == 1 {
                        println!("round {round}: deleted first try, {waited:.2?}");
                    } else {
                        refused_rounds += 1;
                        println!(
                            "round {round}: refused for {waited:.2?} over {attempts} attempts, \
                             first refusal: {}",
                            first_refusal.as_deref().unwrap_or("unknown")
                        );
                    }
                    break;
                }
                Err(error) => {
                    if first_refusal.is_none() {
                        first_refusal = Some(format!("{error}"));
                    }
                    if written.elapsed() > give_up {
                        refused_rounds += 1;
                        println!(
                            "round {round}: still refused after {give_up:?} and {attempts} \
                             attempts, last: {error}"
                        );
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(250)).await;
                }
            }
        }
    }

    println!("{refused_rounds} of {} rounds were refused at least once", cli.rounds);
    Ok(())
}
