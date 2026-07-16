use std::path::PathBuf;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail, ensure};
use clap::Parser;
use rand::RngCore;
use rpc_solana::RpcConfig;
use tape_api::compute::INVALIDATE_TRACK_CU;
use tape_api::instruction::build_invalidate_track_ix;
use tape_api::program::tapedrive::track_pda;
use tape_core::bls::{BlsPrivateKey, BlsPubkey, BlsSignature};
use tape_core::cert::track::TrackInvalidateMessage;
use tape_core::erasure::GROUP_SIZE;
use tape_core::track::types::CompressedTrackProof;
use tape_core::types::{SpoolBitmap, StorageUnits};
use tape_crypto::address::Address;
use tape_crypto::ed25519::Keypair as CryptoKeypair;
use tape_sdk::keys::helpers::{load_bls_keypair, load_solana_keypair};
use tape_sdk::keys::tape_key::TapeKey;
use tape_sdk::tapedrive::Tapedrive;

const DEFAULT_BLOB_SIZE_BYTES: usize = 1 << 20;
const DEFAULT_UPLOAD_EPOCHS: u64 = 4;
const PROOF_TIMEOUT: Duration = Duration::from_secs(120);

#[derive(Parser, Debug)]
#[command(
    name = "invalidate-track",
    about = "Write a track, invalidate it with the localnet node BLS keys, and verify the refund"
)]
struct Cli {
    #[arg(long, default_value = "http://127.0.0.1:8899")]
    rpc_url: String,

    #[arg(long, default_value = "target/localnet/admin.json")]
    admin_keypair: PathBuf,

    #[arg(long, default_value = "target/localnet")]
    data_dir: PathBuf,

    #[arg(long, default_value_t = DEFAULT_BLOB_SIZE_BYTES)]
    size_bytes: usize,

    #[arg(long, default_value_t = DEFAULT_UPLOAD_EPOCHS)]
    epochs: u64,
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

    let node_keys = load_node_keys(&cli.data_dir)?;
    println!("loaded {} node bls keys", node_keys.len());

    let tape_key = TapeKey::generate();
    let tape_address = tape_key.address();
    let reserve_capacity =
        StorageUnits::from_bytes(4 * cli.size_bytes as u64) + StorageUnits::mb(2);

    println!("reserving tape {tape_address}");
    sdk.reserve(&tape_key, reserve_capacity, cli.epochs)
        .await
        .context("reserve tape")?;

    let mut blob = vec![0u8; cli.size_bytes];
    rand::thread_rng().fill_bytes(&mut blob);

    println!("writing {} byte track", cli.size_bytes);
    let track = sdk
        .write_track(&tape_key, &blob)
        .await
        .context("write track")?;
    ensure!(track.is_certified(), "track should certify");

    let track_address = track_pda(track.tape, track.track_number).0;
    println!("track {track_address} certified in group {}", track.group.0);

    let proof = wait_track_proof(&sdk, &track_address, |_| true).await?;

    let tape_before = sdk.get_tape(&tape_address).await.context("tape before")?;
    ensure!(
        tape_before.used == track.size,
        "tape should charge the track size, used {} size {}",
        tape_before.used,
        track.size
    );

    let system = sdk.rpc().get_system().await.context("get system")?;
    let epoch = system.current_epoch;
    let group = sdk
        .rpc()
        .get_group(epoch, proof.state.group)
        .await
        .context("get group")?;

    let message = TrackInvalidateMessage::new(epoch, proof.state.get_hash()).to_bytes();

    // Derive each node pubkey once; matching per spool is then byte equality.
    let mut signer_keys: Vec<(BlsPubkey, &BlsPrivateKey)> = Vec::with_capacity(node_keys.len());
    for secret in &node_keys {
        let pubkey = secret
            .public_key()
            .map_err(|error| anyhow::anyhow!("derive bls pubkey: {error:?}"))?;
        signer_keys.push((pubkey, secret));
    }

    let mut indices = Vec::new();
    let mut partials = Vec::new();
    for (spool_index, spool) in group.spools.iter().enumerate() {
        let Some((_, secret)) = signer_keys
            .iter()
            .find(|(pubkey, _)| *pubkey == spool.bls_pubkey)
        else {
            continue;
        };

        partials.push(
            secret
                .sign(message)
                .map_err(|error| anyhow::anyhow!("sign invalidate: {error:?}"))?,
        );
        indices.push(spool_index);
    }
    ensure!(
        indices.len() == GROUP_SIZE,
        "expected every group spool key locally, found {}",
        indices.len()
    );

    let bitmap = SpoolBitmap::from_indices(&indices);
    let signature = BlsSignature::aggregate(&partials)
        .map_err(|error| anyhow::anyhow!("aggregate: {error:?}"))?;

    let payer = sdk.payer().context("payer")?;
    let ix = build_invalidate_track_ix(payer.pubkey().into(), proof, epoch, bitmap, signature);

    println!("submitting invalidate with {} signers", indices.len());
    sdk.rpc()
        .send_instructions_with_compute_unit_limit(payer, INVALIDATE_TRACK_CU, vec![ix])
        .await
        .context("submit invalidate")?;

    let tape_after = sdk.get_tape(&tape_address).await.context("tape after")?;
    ensure!(
        tape_after.used == StorageUnits(0),
        "invalidation should return capacity, used {}",
        tape_after.used
    );
    println!(
        "capacity returned: used {} -> {}",
        tape_before.used, tape_after.used
    );

    println!("waiting for nodes to serve the invalidated leaf");
    let start = Instant::now();
    wait_track_proof(&sdk, &track_address, |proof| proof.state.is_invalidated()).await?;
    println!("nodes converged in {:.2?}", start.elapsed());

    let mut second = vec![0u8; cli.size_bytes];
    rand::thread_rng().fill_bytes(&mut second);
    let track_b = sdk
        .write_track(&tape_key, &second)
        .await
        .context("write track after invalidate")?;
    ensure!(track_b.is_certified(), "second track should certify");

    let tape_final = sdk.get_tape(&tape_address).await.context("tape final")?;
    ensure!(
        tape_final.used == track_b.size,
        "returned capacity should be reusable, used {}",
        tape_final.used
    );

    println!("invalidate flow ok");
    Ok(())
}

async fn wait_track_proof<Blockchain, Cluster>(
    sdk: &Tapedrive<Blockchain, Cluster>,
    track: &Address,
    accept: impl Fn(&CompressedTrackProof) -> bool,
) -> Result<CompressedTrackProof>
where
    Blockchain: rpc::Rpc,
    Cluster: tape_protocol::Api,
{
    let start = Instant::now();
    loop {
        if let Ok(proof) = sdk.get_track_proof(track).await {
            if accept(&proof) {
                return Ok(proof);
            }
        }
        if start.elapsed() >= PROOF_TIMEOUT {
            bail!("timed out waiting for track proof");
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

fn load_node_keys(data_dir: &PathBuf) -> Result<Vec<BlsPrivateKey>> {
    let mut keys = Vec::new();
    for id in 0.. {
        let path = data_dir.join(format!("node-{id}")).join("bls.key");
        if !path.exists() {
            break;
        }
        let key = load_bls_keypair(&path)
            .with_context(|| format!("load bls key: {}", path.display()))?;
        keys.push(key);
    }
    ensure!(!keys.is_empty(), "no node keys under {}", data_dir.display());
    Ok(keys)
}
