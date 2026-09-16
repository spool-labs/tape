use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow, ensure};
use clap::Parser;
use ed25519_dalek::SigningKey;
use rand::RngCore;
use rpc_solana::RpcConfig;
use tape_core::track::types::TrackVisibility;
use tape_core::types::StorageUnits;
use tape_core::types::tls::NetworkTlsPubkey;
use tape_crypto::ed25519::Keypair as CryptoKeypair;
use tape_crypto::hash::hash;
use tape_privacy::envelope::binding::EnvelopeBinding;
use tape_privacy::key_source::KeySource;
use tape_privacy::recipient::Recipient;
use tape_privacy::sealed::{is_envelope, keyring_name, open, seal};
use tape_privacy::wrap::keyring::Keyring;
use tape_sdk::keys::helpers::load_solana_keypair;
use tape_sdk::keys::tape_key::TapeKey;
use tape_sdk::read_options::ReadOptions;
use tape_sdk::stream::manifest::MAX_TRACK_SIZE;
use tape_sdk::tapedrive::Tapedrive;
use tape_sdk::write_options::WriteOptions;
use tokio::io::AsyncReadExt;

const DEFAULT_FILE_SIZE_BYTES: usize = 1 << 30;
const DEFAULT_RESTRICTED_SIZE_BYTES: usize = 8 << 20;
const RESERVE_HEADROOM_BYTES: u64 = 1 << 20;
const DEFAULT_UPLOAD_EPOCHS: u64 = 4;
const DEFAULT_FILL_BYTE: u8 = 0xA5;
const RESTRICTED_OBJECT_NAME: &str = "restricted/upload.bin";
const ENCRYPTED_OBJECT_NAME: &str = "encrypted/upload.bin";
const OCTETS: &str = "application/octet-stream";
const GRANT_TTL: Duration = Duration::from_secs(3600);
const RESTRICTED_READ_TIMEOUT: Duration = Duration::from_secs(120);

type Client = Tapedrive<rpc_solana::SolanaRpc, peer_http::HttpApi>;

#[derive(Parser, Debug)]
#[command(name = "upload-file", about = "Upload a large file against the running localnet")]
struct Cli {
    #[arg(long, default_value = "http://127.0.0.1:8899")]
    rpc_url: String,

    #[arg(long, default_value = "target/localnet/admin.json")]
    admin_keypair: PathBuf,

    /// Defaults to 1 GiB for the public stream, 8 MiB for a restricted object.
    #[arg(long)]
    size_bytes: Option<usize>,

    #[arg(long, default_value_t = DEFAULT_UPLOAD_EPOCHS)]
    epochs: u64,

    #[arg(long, default_value_t = DEFAULT_FILL_BYTE)]
    fill_byte: u8,

    /// Upload a restricted object, then read it back through a granted reader,
    /// refuse an ungranted one, and revoke.
    #[arg(long)]
    restricted: bool,

    /// Seal the object client-side first: ciphertext restricted, keyring public,
    /// opened by a granted reader the keyring names.
    #[arg(long)]
    encrypted: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    // Off unless RUST_LOG asks: the SDK's own stage lines are what attributes an
    // upload's wall clock, and this binary is where the clock is read.
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_timer(tracing_subscriber::fmt::time::uptime())
        .init();

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

    if cli.encrypted {
        run_encrypted(&cli, sdk).await
    } else if cli.restricted {
        run_restricted(&cli, sdk).await
    } else {
        run_stream(&cli, sdk).await
    }
}

async fn run_stream(cli: &Cli, sdk: Client) -> Result<()> {
    let size_bytes = cli.size_bytes.unwrap_or(DEFAULT_FILE_SIZE_BYTES);
    ensure!(size_bytes > 0, "--size-bytes must be greater than zero");

    let tape_key = TapeKey::generate();
    let size = StorageUnits::from_bytes(size_bytes as u64);
    let chunk_count = size.to_bytes().div_ceil(MAX_TRACK_SIZE as u64);
    let reserve_capacity = size + StorageUnits::from_bytes(RESERVE_HEADROOM_BYTES);

    println!("preparing upload stream");
    println!("  size_bytes: {size_bytes}");
    println!("  chunk_count: {chunk_count}");
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

async fn run_restricted(cli: &Cli, sdk: Client) -> Result<()> {
    let size_bytes = cli.size_bytes.unwrap_or(DEFAULT_RESTRICTED_SIZE_BYTES);
    ensure!(size_bytes > 0, "--size-bytes must be greater than zero");
    ensure!(
        size_bytes <= MAX_TRACK_SIZE,
        "a restricted object here fits one track; max is {MAX_TRACK_SIZE} bytes"
    );

    let writer = sdk.with_write_options(WriteOptions {
        visibility: TrackVisibility::Restricted,
        ..WriteOptions::default()
    });
    let tape_key = TapeKey::generate();
    let size = StorageUnits::from_bytes(size_bytes as u64);
    let reserve_capacity = size + StorageUnits::from_bytes(RESERVE_HEADROOM_BYTES);

    let mut data = vec![0u8; size_bytes];
    rand::thread_rng().fill_bytes(&mut data);

    println!("preparing restricted upload");
    println!("  size_bytes: {size_bytes}");
    println!("  object_name: {RESTRICTED_OBJECT_NAME}");
    println!("  tape_address: {}", tape_key.address());

    println!("reserving tape");
    let reserve_start = Instant::now();
    writer
        .reserve(&tape_key, reserve_capacity, cli.epochs)
        .await
        .context("reserve tape")?;
    println!("reserve completed in {:.2?}", reserve_start.elapsed());

    println!("uploading restricted object");
    let upload_start = Instant::now();
    let address = writer
        .put_object(&tape_key, RESTRICTED_OBJECT_NAME, &data, None)
        .await
        .context("put restricted object")?;
    println!("upload completed in {:.2?}", upload_start.elapsed());
    println!("object address: {address}");

    let reader_identity = Arc::new(CryptoKeypair::new(&mut rand::thread_rng()));
    let reader_tls = NetworkTlsPubkey::new(reader_identity.pubkey().to_bytes());
    let key_hash = hash(RESTRICTED_OBJECT_NAME.as_bytes());
    let grant = writer
        .issue_read_grant(&tape_key, key_hash, reader_tls, GRANT_TTL)
        .await
        .context("issue read grant")?;

    let reader = Tapedrive::new_reader(rpc_for(cli)?, reader_identity)
        .map_err(|error| anyhow!("create reader client: {error}"))?
        .with_read_options(ReadOptions {
            grant: Some(grant),
            ..ReadOptions::default()
        });

    println!("reading restricted object back");
    let read_start = Instant::now();
    let opened = read_until_visible(&reader, &tape_key).await?;
    println!("read completed in {:.2?}", read_start.elapsed());
    ensure!(
        opened == data,
        "read bytes differ from the uploaded payload ({} vs {} bytes)",
        opened.len(),
        data.len()
    );
    println!("restricted object round-tripped: {} bytes match", opened.len());

    // An identity with no grant is refused at the node.
    let blind_identity = Arc::new(CryptoKeypair::new(&mut rand::thread_rng()));
    let blind = Tapedrive::new_reader(rpc_for(cli)?, blind_identity)
        .map_err(|error| anyhow!("create blind reader: {error}"))?;
    let refused = blind.get_object(&tape_key.address(), RESTRICTED_OBJECT_NAME).await;
    ensure!(
        refused.is_err(),
        "an ungranted reader should be refused, got {:?} bytes",
        refused.map(|bytes| bytes.len())
    );
    println!("  an ungranted reader is refused");

    // BumpGrantGeneration only ever ran under litesvm's native program in
    // simnet; this is the one place it executes as real BPF, where a compute
    // ceiling would show up.
    println!("revoking outstanding grants");
    let revoke_start = Instant::now();
    writer
        .bump_grant_generation(&tape_key)
        .await
        .context("bump grant generation")?;
    println!("bump landed in {:.2?}", revoke_start.elapsed());

    let revoked = wait_until_refused(&reader, &tape_key).await?;
    println!("  outstanding grant refused after the bump: {revoked}");

    Ok(())
}

async fn run_encrypted(cli: &Cli, sdk: Client) -> Result<()> {
    let size_bytes = cli.size_bytes.unwrap_or(DEFAULT_RESTRICTED_SIZE_BYTES);
    ensure!(size_bytes > 0, "--size-bytes must be greater than zero");

    let tape_key = TapeKey::generate();
    let size = StorageUnits::from_bytes(size_bytes as u64);
    let reserve_capacity = size + StorageUnits::from_bytes(RESERVE_HEADROOM_BYTES);
    let mut data = vec![0u8; size_bytes];
    rand::thread_rng().fill_bytes(&mut data);

    // The owner and the reader are both named in the keyring; the owner so it
    // can share later, the reader so this run can open what it fetches.
    let owner_wallet = wallet(tape_key.keypair())?;
    let reader_identity = Arc::new(CryptoKeypair::new(&mut rand::thread_rng()));
    let reader_wallet = wallet(&reader_identity)?;
    let binding = EnvelopeBinding::for_name(tape_key.address().to_bytes(), ENCRYPTED_OBJECT_NAME.as_bytes());
    let sealed = seal(
        &binding,
        &data,
        &[
            Recipient::Wallet(owner_wallet.verifying_key()),
            Recipient::Wallet(reader_wallet.verifying_key()),
        ],
    )
    .map_err(|error| anyhow!("seal: {error}"))?;
    ensure!(is_envelope(&sealed.ciphertext), "sealed bytes carry no envelope header");
    ensure!(sealed.ciphertext.len() > data.len(), "random input cannot shrink under a seal");
    let sidecar = keyring_name(ENCRYPTED_OBJECT_NAME.as_bytes());
    let sidecar_name = String::from_utf8(sidecar.clone()).context("keyring name is utf-8")?;

    println!("preparing encrypted upload");
    println!("  size_bytes: {size_bytes}");
    println!("  ciphertext_bytes: {}", sealed.ciphertext.len());
    println!("  object_name: {ENCRYPTED_OBJECT_NAME}");
    println!("  tape_address: {}", tape_key.address());

    println!("reserving tape");
    let reserve_start = Instant::now();
    sdk.reserve(&tape_key, reserve_capacity, cli.epochs)
        .await
        .context("reserve tape")?;
    println!("reserve completed in {:.2?}", reserve_start.elapsed());

    let admin = load_solana_keypair(&cli.admin_keypair)
        .with_context(|| format!("load admin keypair: {}", cli.admin_keypair.display()))?;
    let admin = CryptoKeypair::from_solana_keypair(&admin).context("convert admin keypair")?;
    let writer = Tapedrive::new(rpc_for(cli)?, admin).with_write_options(WriteOptions {
        visibility: TrackVisibility::Restricted,
        ..WriteOptions::default()
    });
    println!("uploading ciphertext restricted, keyring public");
    let upload_start = Instant::now();
    let address = writer
        .put_object(&tape_key, ENCRYPTED_OBJECT_NAME, &sealed.ciphertext, Some(OCTETS))
        .await
        .context("put restricted ciphertext")?;
    sdk.put_object(&tape_key, &sidecar_name, &sealed.keyring.pack(), Some(OCTETS))
        .await
        .context("put public keyring")?;
    println!("upload completed in {:.2?}", upload_start.elapsed());
    println!("object address: {address}");

    let reader_tls = NetworkTlsPubkey::new(reader_identity.pubkey().to_bytes());
    let key_hash = hash(ENCRYPTED_OBJECT_NAME.as_bytes());
    let grant = sdk
        .issue_read_grant(&tape_key, key_hash, reader_tls, GRANT_TTL)
        .await
        .context("issue read grant")?;
    let reader = Tapedrive::new_reader(rpc_for(cli)?, reader_identity)
        .map_err(|error| anyhow!("create reader client: {error}"))?
        .with_read_options(ReadOptions {
            grant: Some(grant),
            ..ReadOptions::default()
        });

    println!("reading the envelope and keyring back");
    let read_start = Instant::now();
    let fetched = read_named_until_visible(&reader, &tape_key, ENCRYPTED_OBJECT_NAME).await?;
    let keyring_bytes = read_named_until_visible(&reader, &tape_key, &sidecar_name).await?;
    println!("read completed in {:.2?}", read_start.elapsed());
    ensure!(fetched == sealed.ciphertext, "nodes changed the envelope");
    let keyring = Keyring::unpack(&keyring_bytes).map_err(|error| anyhow!("unpack keyring: {error}"))?;
    let opened = open(&binding, &fetched, &keyring, &KeySource::Wallet(reader_wallet))
        .map_err(|error| anyhow!("open: {error}"))?;
    ensure!(opened == data, "opened bytes differ from the plaintext");
    println!("encrypted object round-tripped: {} bytes match", opened.len());

    // A grant buys the fetch; a wallet the keyring does not name still reads noise.
    let blind_identity = Arc::new(CryptoKeypair::new(&mut rand::thread_rng()));
    let blind_wallet = wallet(&blind_identity)?;
    let blind_grant = sdk
        .issue_read_grant(
            &tape_key,
            key_hash,
            NetworkTlsPubkey::new(blind_identity.pubkey().to_bytes()),
            GRANT_TTL,
        )
        .await
        .context("issue blind grant")?;
    let blind = Tapedrive::new_reader(rpc_for(cli)?, blind_identity)
        .map_err(|error| anyhow!("create blind reader: {error}"))?
        .with_read_options(ReadOptions {
            grant: Some(blind_grant),
            ..ReadOptions::default()
        });
    let blind_bytes = blind
        .get_object(&tape_key.address(), ENCRYPTED_OBJECT_NAME)
        .await
        .context("blind reader fetches the envelope")?;
    ensure!(
        open(&binding, &blind_bytes, &keyring, &KeySource::Wallet(blind_wallet)).is_err(),
        "a wallet the keyring does not name opened the object"
    );
    println!("  a grant without a wrap fetches bytes it cannot open");

    println!("revoking outstanding grants");
    let revoke_start = Instant::now();
    sdk.bump_grant_generation(&tape_key)
        .await
        .context("bump grant generation")?;
    println!("bump landed in {:.2?}", revoke_start.elapsed());
    let revoked = wait_until_refused_named(&reader, &tape_key, ENCRYPTED_OBJECT_NAME).await?;
    println!("  outstanding grant refused after the bump: {revoked}");

    Ok(())
}

/// The same ed25519 key as a privacy-crate wallet
fn wallet(keypair: &CryptoKeypair) -> Result<SigningKey> {
    SigningKey::from_keypair_bytes(&keypair.to_keypair_bytes()).context("ed25519 keypair bytes")
}

fn rpc_for(cli: &Cli) -> Result<rpc_solana::SolanaRpc> {
    rpc_solana::SolanaRpc::new(RpcConfig {
        endpoints: vec![cli.rpc_url.clone()],
        ..Default::default()
    })
    .context("create rpc client")
}

/// Nodes enforce a generation bump once they ingest it, so the read keeps
/// working for a moment after the transaction confirms.
async fn wait_until_refused(reader: &Client, tape_key: &TapeKey) -> Result<String> {
    wait_until_refused_named(reader, tape_key, RESTRICTED_OBJECT_NAME).await
}

async fn wait_until_refused_named(reader: &Client, tape_key: &TapeKey, name: &str) -> Result<String> {
    let deadline = Instant::now() + RESTRICTED_READ_TIMEOUT;
    loop {
        match reader.get_object(&tape_key.address(), name).await {
            Err(error) => return Ok(error.to_string()),
            Ok(bytes) if Instant::now() < deadline => {
                println!("  bump not enforced yet, still served {} bytes", bytes.len());
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
            Ok(bytes) => {
                return Err(anyhow!(
                    "a revoked grant still opened {} bytes after {RESTRICTED_READ_TIMEOUT:?}",
                    bytes.len()
                ));
            }
        }
    }
}

/// A write returns once certification confirms, which can be before every node
/// has replayed the registration block; reads by name retry until a node
/// answers or the timeout passes.
async fn read_until_visible(reader: &Client, tape_key: &TapeKey) -> Result<Vec<u8>> {
    read_named_until_visible(reader, tape_key, RESTRICTED_OBJECT_NAME).await
}

async fn read_named_until_visible(reader: &Client, tape_key: &TapeKey, name: &str) -> Result<Vec<u8>> {
    let deadline = Instant::now() + RESTRICTED_READ_TIMEOUT;
    loop {
        match reader.get_object(&tape_key.address(), name).await {
            Ok(bytes) => return Ok(bytes),
            Err(error) if Instant::now() < deadline => {
                println!("  read not ready yet: {error}");
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
            Err(error) => return Err(anyhow!("read restricted object: {error}")),
        }
    }
}
