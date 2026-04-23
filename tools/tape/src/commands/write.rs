//! `tape write` — three-tier upload:
//!
//! - payload ≤ 825 bytes   → `write_raw`   (single inline on-chain write,
//!                                          no node uploads required)
//! - payload fits one blob → `write_bytes` (single blob, uploaded + certified)
//! - larger                → `write_stream` (multi-chunk + index track)
//!
//! File inputs are streamed via `tokio::fs::File` so a gigabyte-scale file
//! never lands entirely in memory.

use std::path::{Path, PathBuf};

use rand::RngCore;
use serde::Serialize;
use tape_api::prelude::CompressedTrack;
use tape_core::types::StorageUnits;
use tape_crypto::hash::Hash;
use tape_sdk::stream::write::write_stream;
use tape_sdk::stream::write::write_bytes;
use tape_sdk::track::write::SDK_INLINE_RAW_MAX_BYTES;
use tokio::fs::File;

use crate::cassette;
use crate::context::Context;
use crate::error::{Error, Result};
use crate::output::CliOutput;

/// Headroom added to `capacity` when auto-reserving the tape: the tape's
/// indexing tracks and merkle commitments eat some extra bytes that aren't
/// accounted for in the payload size.
const AUTO_RESERVE_HEADROOM_BYTES: u64 = 1 << 20; // 1 MiB

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

#[derive(Serialize)]
pub struct WriteOutput {
    pub cassette: PathBuf,
    pub cassette_pubkey: String,
    pub track_address: String,
    pub tier: &'static str,
    pub bytes_written: u64,
    pub reserved_new_tape: bool,
}

impl CliOutput for WriteOutput {
    fn print_text(&self) {
        println!("cassette:         {}", self.cassette.display());
        println!("cassette pubkey:  {}", self.cassette_pubkey);
        println!("track address:    {}", self.track_address);
        println!("tier:             {}", self.tier);
        println!("bytes written:    {}", self.bytes_written);
        if self.reserved_new_tape {
            println!("(tape reserved by this write)");
        }
    }
}

pub struct Args<'a> {
    pub file: Option<&'a Path>,
    pub message: Option<&'a str>,
    pub cassette: Option<&'a Path>,
    pub epochs: u64,
}

pub async fn run(ctx: &Context, args: Args<'_>) -> Result<WriteOutput> {
    // Content source: one of message / file must be set.
    let cassette_path = ctx.require_cassette(args.cassette)?;
    let tape_key = cassette::load(&cassette_path)?;

    match (args.message, args.file) {
        (Some(msg), None) => run_message(ctx, &cassette_path, &tape_key, msg, args.epochs).await,
        (None, Some(path)) => run_file(ctx, &cassette_path, &tape_key, path, args.epochs).await,
        (Some(_), Some(_)) => Err(Error::Invalid(
            "pass either a file path or -m <message>, not both".into(),
        )),
        (None, None) => Err(Error::Invalid(
            "nothing to write — pass a file path or -m <message>".into(),
        )),
    }
}

async fn run_message(
    ctx: &Context,
    cassette_path: &Path,
    tape_key: &tape_sdk::keys::tape_key::TapeKey,
    message: &str,
    epochs: u64,
) -> Result<WriteOutput> {
    let data = message.as_bytes();
    let sdk = ctx.sdk()?;
    let key = random_hash();

    let size = StorageUnits::from_bytes(data.len() as u64);
    let capacity = size + StorageUnits::from_bytes(AUTO_RESERVE_HEADROOM_BYTES);
    let reserved = ensure_reserved(&sdk, tape_key, capacity, epochs).await?;

    let (track_address, tier) = if data.len() <= SDK_INLINE_RAW_MAX_BYTES {
        let track = sdk
            .write_raw(tape_key, key, data)
            .await
            .map_err(|e| Error::Sdk(format!("write_raw: {e}")))?;
        (track_address_of_raw(&track), WriteTier::Raw)
    } else {
        let receipt = write_bytes(&sdk, tape_key, key, data)
            .await
            .map_err(|e| Error::Sdk(format!("write_bytes: {e}")))?;
        (receipt.manifest.to_string(), WriteTier::Blob)
    };

    Ok(WriteOutput {
        cassette: cassette_path.to_path_buf(),
        cassette_pubkey: tape_key.address().to_string(),
        track_address,
        tier: tier.as_str(),
        bytes_written: data.len() as u64,
        reserved_new_tape: reserved,
    })
}

async fn run_file(
    ctx: &Context,
    cassette_path: &Path,
    tape_key: &tape_sdk::keys::tape_key::TapeKey,
    path: &Path,
    epochs: u64,
) -> Result<WriteOutput> {
    let metadata = tokio::fs::metadata(path).await.map_err(|source| Error::Io {
        path: path.display().to_string(),
        source,
    })?;
    let size_bytes = metadata.len();
    let size = StorageUnits::from_bytes(size_bytes);

    let sdk = ctx.sdk()?;
    let key = random_hash();
    let capacity = size + StorageUnits::from_bytes(AUTO_RESERVE_HEADROOM_BYTES);
    let reserved = ensure_reserved(&sdk, tape_key, capacity, epochs).await?;

    let (track_address, tier) = if size_bytes <= SDK_INLINE_RAW_MAX_BYTES as u64 {
        // Small files still use raw — load the whole thing (< 1 KiB).
        let data = tokio::fs::read(path).await.map_err(|source| Error::Io {
            path: path.display().to_string(),
            source,
        })?;
        let track = sdk
            .write_raw(tape_key, key, &data)
            .await
            .map_err(|e| Error::Sdk(format!("write_raw: {e}")))?;
        (track_address_of_raw(&track), WriteTier::Raw)
    } else {
        // All other files go through the streaming path. `write_stream`
        // chunks at the SDK's 64 MiB boundary, so a single-chunk write is
        // effectively a blob + a 1-entry index track, and multi-chunk is
        // the full split-and-index flow. Never buffers the file.
        let file = File::open(path).await.map_err(|source| Error::Io {
            path: path.display().to_string(),
            source,
        })?;
        let receipt = write_stream(&sdk, tape_key, key, size, file)
            .await
            .map_err(|e| Error::Sdk(format!("write_stream: {e}")))?;
        let tier = if size_bytes <= tape_sdk::stream::manifest::CHUNK_SIZE as u64 {
            WriteTier::Blob
        } else {
            WriteTier::Stream
        };
        (receipt.manifest.to_string(), tier)
    };

    Ok(WriteOutput {
        cassette: cassette_path.to_path_buf(),
        cassette_pubkey: tape_key.address().to_string(),
        track_address,
        tier: tier.as_str(),
        bytes_written: size_bytes,
        reserved_new_tape: reserved,
    })
}

/// Reserve the tape if the on-chain `Tape` account doesn't exist yet.
/// Returns whether we actually issued a reserve instruction.
async fn ensure_reserved(
    sdk: &tape_sdk::tapedrive::Tapedrive<rpc_solana::SolanaRpc, peer_http::HttpApi>,
    tape_key: &tape_sdk::keys::tape_key::TapeKey,
    capacity: StorageUnits,
    epochs: u64,
) -> Result<bool> {
    // Attempt a cheap probe via the SDK's fetch helpers; fall back to
    // issuing `reserve` and tolerating the "already reserved" case.
    match sdk.get_tape(&tape_key.address()).await {
        Ok(_) => Ok(false),
        Err(_) => {
            sdk.reserve(tape_key, capacity, epochs)
                .await
                .map_err(|e| Error::Sdk(format!("reserve: {e}")))?;
            Ok(true)
        }
    }
}

fn random_hash() -> Hash {
    let mut bytes = [0u8; 32];
    rand::thread_rng().fill_bytes(&mut bytes);
    Hash::from(bytes)
}

fn track_address_of_raw(track: &CompressedTrack) -> String {
    // A raw write's "track address" is the Track PDA derived from
    // (authority, track_number). CompressedTrack carries both; for now
    // we report the tape address (the cassette) as the user-facing
    // identifier — every raw write on a cassette is read back via the
    // same cassette address + track number. This keeps the single
    // string in the receipt useful for `tape read`.
    format!("{}", track.tape)
}
