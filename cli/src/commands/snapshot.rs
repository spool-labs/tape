use std::env;
use std::io::{self, Write};
use std::str::FromStr;

use anyhow::Result;
use chrono::Utc;
use num_enum::TryFromPrimitive;
use solana_sdk::pubkey::Pubkey;

use tape_client as tapedrive;
use tape_api::SEGMENT_SIZE;
use tape_network::archive::sync::sync_from_block;
use tape_network::snapshot::{create_snapshot, load_from_snapshot};
use tape_network::store::StoreError;
use tapedrive::{decode_tape, MimeType, TapeHeader};

use crate::cli::{Cli, Commands, Context, SnapshotCommands};
use crate::log;
use crate::utils::write_output;

pub async fn handle_snapshot_commands(cli: Cli, context: Context) -> Result<()> {
    if let Commands::Snapshot(snapshot) = cli.command {
        match snapshot {
            SnapshotCommands::Stats {} => {
                handle_stats(context)?
            }
            SnapshotCommands::Resync { tape } => {
                handle_resync(context, &tape).await?
            }
            SnapshotCommands::Create { output } => {
                handle_create(context, output)?
            }
            SnapshotCommands::Load { input } => {
                handle_load(&input)?
            }
            SnapshotCommands::GetTape { tape, output, raw } => {
                handle_get_tape(context, &tape, output, raw).await?
            }
            SnapshotCommands::GetSegment { tape, index } => {
                handle_get_segment(context, &tape, index).await?
            }
        }
    }

    Ok(())
}

fn handle_stats(context: Context) -> Result<()> {
    let store = context.open_read_only_store_conn()?;
    let stats = store.get_local_stats()?;
    log::print_section_header("Local Store Stats");
    log::print_message(&format!("Number of Tapes: {}", stats.tapes));
    log::print_message(&format!("Size: {} bytes", stats.size_bytes));
    Ok(())
}

async fn handle_resync(context: Context, tape: &str) -> Result<()> {
    let tape_pubkey: Pubkey = FromStr::from_str(tape)?;
    let (tape_account, _) = tapedrive::get_tape_account(context.rpc(), &tape_pubkey).await?;
    let starting_slot = tape_account.tail_slot;
    let store = context.open_primary_store_conn()?;
    log::print_message(&format!("Re-syncing tape: {tape}, please wait"));
    sync_from_block(&store, context.rpc(), &tape_pubkey, starting_slot).await?;
    log::print_message("Done");
    Ok(())
}

fn handle_create(context: Context, output: Option<String>) -> Result<()> {
    let snapshot_path =
        output.unwrap_or_else(|| format!("snapshot_{}.tar.gz", Utc::now().timestamp()));
    let store = context.open_read_only_store_conn()?;
    create_snapshot(&store.db, &snapshot_path)?;
    log::print_message(&format!("Snapshot created at: {snapshot_path}"));
    Ok(())
}

fn handle_load(input: &str) -> Result<()> {
    let primary_path = env::current_dir()?.join("db_tapestore");
    load_from_snapshot(input, &primary_path)?;
    log::print_message("Snapshot loaded into primary store");
    Ok(())
}

async fn handle_get_tape(
    context: Context,
    tape: &str,
    output: Option<String>,
    raw: bool,
) -> Result<()> {
    let tape_pubkey: Pubkey = FromStr::from_str(tape)?;
    let (tape_account, _) = tapedrive::get_tape_account(context.rpc(), &tape_pubkey).await?;
    let total_segments = tape_account.total_segments;
    let store = context.open_read_only_store_conn()?;
    let mut data: Vec<u8> = Vec::with_capacity((total_segments as usize) * SEGMENT_SIZE);
    let mut missing: Vec<u64> = Vec::new();
    for seg_idx in 0..total_segments {
        match store.get_segment(&tape_pubkey, seg_idx) {
            Ok(seg) => {
                data.extend_from_slice(&seg);
            }
            Err(StoreError::SegmentNotFoundForAddress(..)) => {
                data.extend_from_slice(&[0u8; SEGMENT_SIZE]);
                missing.push(seg_idx);
            }
            Err(e) => return Err(e.into()),
        }
    }

    if !missing.is_empty() {
        log::print_message(&format!("Missing segments: {missing:?}"));
    }

    let mime_type = if raw {
        MimeType::Unknown
    } else {
        let header = TapeHeader::try_from_bytes(&tape_account.header)?;
        MimeType::try_from_primitive(header.mime_type).unwrap_or(MimeType::Unknown)
    };

    let data_to_write = if raw {
        data
    } else {
        let header = TapeHeader::try_from_bytes(&tape_account.header)?;
        decode_tape(data, header)?
    };

    write_output(output, &data_to_write, mime_type)?;

    Ok(())
}

async fn handle_get_segment(context: Context, tape: &str, index: u32) -> Result<()> {
    let tape_pubkey: Pubkey = FromStr::from_str(tape)?;
    let (tape_account, _) = tapedrive::get_tape_account(context.rpc(), &tape_pubkey).await?;
    if (index as u64) >= tape_account.total_segments {
        anyhow::bail!(
            "Invalid segment index: {} (tape has {} segments)",
            index,
            tape_account.total_segments
        );
    }

    let store = context.open_read_only_store_conn()?;

    match store.get_segment(&tape_pubkey, index as u64) {
        Ok(data) => {
            let mut stdout = io::stdout();
            stdout.write_all(&data)?;
            stdout.flush()?;
        }
        Err(StoreError::SegmentNotFoundForAddress(..)) => {
            log::print_message("Segment not found in local store");
        }
        Err(e) => return Err(e.into()),
    }
    Ok(())
}
