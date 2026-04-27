//! `tape write` — three-tier upload to an existing tape:
//!
//! - payload ≤ 825 bytes   → `write_raw`   (single inline on-chain write,
//!                                          no node uploads required)
//! - payload fits one blob → `write_track` (single blob, uploaded + certified)
//! - larger files          → `write_stream` (multi-chunk + manifest track)
//!
//! Files up to one stream chunk are written as direct tracks. Larger files are
//! streamed so a gigabyte-scale file never lands entirely in memory.

use std::path::{Path, PathBuf};

use peer_http::HttpApi;
use rand::RngCore;
use rpc_solana::SolanaRpc;
use serde::Serialize;
use tape_api::program::tapedrive::track_pda;
use tape_api::prelude::CompressedTrack;
use tape_core::types::StorageUnits;
use tape_crypto::hash::Hash;
use tape_sdk::keys::tape_key::TapeKey;
use tape_sdk::stream::manifest::CHUNK_SIZE;
use tape_sdk::tapedrive::Tapedrive;
use tape_sdk::track::write::SDK_INLINE_RAW_MAX_BYTES;
use tokio::fs::{metadata, read, File};
use tokio::io::{stdin, AsyncReadExt};

use crate::cassette;
use crate::context::Context;
use crate::error::{Error, Result};
use crate::output::CliOutput;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriteTier {
    Raw,
    Blob,
    Stream,
}

impl WriteTier {
    fn as_str(self) -> &'static str {
        match self {
            Self::Raw => "raw",
            Self::Blob => "blob",
            Self::Stream => "stream",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AddressKind {
    Track,
    StreamManifest,
}

impl AddressKind {
    fn as_str(self) -> &'static str {
        match self {
            Self::Track => "track",
            Self::StreamManifest => "stream-manifest",
        }
    }
}

#[derive(Serialize)]
pub struct WriteOutput {
    pub tape_keypair: PathBuf,
    pub tape_address: String,
    pub address: String,
    pub address_kind: &'static str,
    pub tier: &'static str,
    pub bytes_written: u64,
}

impl CliOutput for WriteOutput {
    fn print_text(&self) {
        println!("tape keypair:     {}", self.tape_keypair.display());
        println!("tape address:     {}", self.tape_address);
        println!("address:          {}", self.address);
        println!("address kind:     {}", self.address_kind);
        println!("tier:             {}", self.tier);
        println!("bytes written:    {}", self.bytes_written);
    }
}

pub struct Args<'a> {
    pub file: Option<&'a Path>,
    pub message: Option<&'a str>,
    pub stdin: bool,
    pub tape: Option<&'a Path>,
}

pub async fn run(ctx: &Context, args: Args<'_>) -> Result<WriteOutput> {
    let cassette_path = ctx.require_cassette(args.tape)?;
    let tape_key = cassette::load(&cassette_path)?;

    let sources = args.file.is_some() as u8 + args.message.is_some() as u8 + args.stdin as u8;
    if sources != 1 {
        return Err(Error::Invalid(
            "pass exactly one of <FILE>, --message <TEXT>, or --stdin".into(),
        ));
    }

    match (args.message, args.file, args.stdin) {
        (Some(msg), None, false) => run_message(ctx, &cassette_path, &tape_key, msg).await,
        (None, Some(path), false) => run_file(ctx, &cassette_path, &tape_key, path).await,
        (None, None, true) => run_stdin(ctx, &cassette_path, &tape_key).await,
        _ => unreachable!("source count already validated"),
    }
}

async fn run_message(
    ctx: &Context,
    cassette_path: &Path,
    tape_key: &TapeKey,
    message: &str,
) -> Result<WriteOutput> {
    run_direct_bytes(ctx, cassette_path, tape_key, message.as_bytes()).await
}

async fn run_stdin(
    ctx: &Context,
    cassette_path: &Path,
    tape_key: &TapeKey,
) -> Result<WriteOutput> {
    let mut data = Vec::new();
    stdin()
        .read_to_end(&mut data)
        .await
        .map_err(|source| Error::Io {
            path: "stdin".into(),
            source,
        })?;
    run_direct_bytes(ctx, cassette_path, tape_key, &data).await
}

async fn run_file(
    ctx: &Context,
    cassette_path: &Path,
    tape_key: &TapeKey,
    path: &Path,
) -> Result<WriteOutput> {
    let metadata = metadata(path).await.map_err(|source| Error::Io {
        path: path.display().to_string(),
        source,
    })?;
    let size_bytes = metadata.len();

    let sdk = ctx.sdk()?;
    require_reserved(&sdk, tape_key).await?;
    let key = random_hash();

    let (address, address_kind, tier) = if size_bytes <= CHUNK_SIZE as u64 {
        let data = read(path).await.map_err(|source| Error::Io {
            path: path.display().to_string(),
            source,
        })?;
        let track = sdk
            .write_track(tape_key, key, &data)
            .await
            .map_err(|e| Error::Sdk(format!("write_track: {e}")))?;
        let tier = tier_for_direct_len(data.len());
        (track_address(&track), AddressKind::Track, tier)
    } else {
        let file = File::open(path).await.map_err(|source| Error::Io {
            path: path.display().to_string(),
            source,
        })?;
        let receipt = sdk
            .write_stream(tape_key, key, StorageUnits::from_bytes(size_bytes), file)
            .await
            .map_err(|e| Error::Sdk(format!("write_stream: {e}")))?;
        (
            receipt.manifest.to_string(),
            AddressKind::StreamManifest,
            WriteTier::Stream,
        )
    };

    Ok(WriteOutput {
        tape_keypair: cassette_path.to_path_buf(),
        tape_address: tape_key.address().to_string(),
        address,
        address_kind: address_kind.as_str(),
        tier: tier.as_str(),
        bytes_written: size_bytes,
    })
}

async fn run_direct_bytes(
    ctx: &Context,
    cassette_path: &Path,
    tape_key: &TapeKey,
    data: &[u8],
) -> Result<WriteOutput> {
    let sdk = ctx.sdk()?;
    require_reserved(&sdk, tape_key).await?;
    let key = random_hash();
    let track = sdk
        .write_track(tape_key, key, data)
        .await
        .map_err(|e| Error::Sdk(format!("write_track: {e}")))?;

    Ok(WriteOutput {
        tape_keypair: cassette_path.to_path_buf(),
        tape_address: tape_key.address().to_string(),
        address: track_address(&track),
        address_kind: AddressKind::Track.as_str(),
        tier: tier_for_direct_len(data.len()).as_str(),
        bytes_written: data.len() as u64,
    })
}

async fn require_reserved(
    sdk: &Tapedrive<SolanaRpc, HttpApi>,
    tape_key: &TapeKey,
) -> Result<()> {
    sdk.get_tape(&tape_key.address()).await.map_err(|e| {
        Error::Sdk(format!(
            "get_tape: {e}; reserve a tape first with `tape create --capacity <SIZE> --epochs <N>`"
        ))
    })?;
    Ok(())
}

fn tier_for_direct_len(len: usize) -> WriteTier {
    if len <= SDK_INLINE_RAW_MAX_BYTES {
        WriteTier::Raw
    } else {
        WriteTier::Blob
    }
}

fn random_hash() -> Hash {
    let mut bytes = [0u8; 32];
    rand::thread_rng().fill_bytes(&mut bytes);
    Hash::from(bytes)
}

fn track_address(track: &CompressedTrack) -> String {
    track_pda(track.tape, track.track_number).0.to_string()
}
