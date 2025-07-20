use std::env;
use std::io::{self, Write};
use std::str::FromStr;

use anyhow::Result;
use chrono::Utc;
use num_enum::TryFromPrimitive;
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::pubkey::Pubkey;

use tape_client as tapedrive;
use tape_api::SEGMENT_SIZE;
use tape_api::utils::padded_array;
use tape_network::archive::sync_from_block;
use tape_network::snapshot::{create_snapshot, load_from_snapshot};
use tape_network::store::{
    primary as get_primary_store, 
    read_only as get_read_only_store, 
    StoreError,
    TapeStore,
};
use tapedrive::{decode_tape, MimeType, TapeHeader};

use crate::cli::{Cli, Commands, SnapshotCommands};
use crate::log;
use crate::utils::write_output;

pub async fn handle_snapshot_commands(cli: Cli, client: RpcClient) -> Result<()> {
    if let Commands::Snapshot(snapshot) = cli.command {
        match snapshot {
            SnapshotCommands::Stats {} => {
                handle_stats()?
            }
            SnapshotCommands::Resync { tape } => {
                handle_resync(&client, &tape).await?
            }
            SnapshotCommands::Create { output } => {
                handle_create(output)?
            }
            SnapshotCommands::Load { input } => {
                handle_load(&input)?
            }
            SnapshotCommands::GetTape { tape, output, raw } => {
                handle_get_tape(&client, &tape, output, raw).await?
            }
            SnapshotCommands::GetSegment { tape, index } => {
                handle_get_segment(&client, &tape, index).await?
            }
        }
    }

    Ok(())
}

fn handle_stats() -> Result<()> {
    let store: TapeStore = get_read_only_store()?;
    let stats = store.get_local_stats()?;
    log::print_section_header("Local Store Stats");
    log::print_message(&format!("Number of Tapes: {}", stats.tapes));
    log::print_message(&format!("Number of Segments: {}", stats.segments));
    log::print_message(&format!("Size: {} bytes", stats.size_bytes));
    Ok(())
}

async fn handle_resync(client: &RpcClient, tape: &str) -> Result<()> {
    let tape_pubkey: Pubkey = FromStr::from_str(tape)?;
    let (tape_account, _) = tapedrive::get_tape_account(client, &tape_pubkey).await?;
    let starting_slot = tape_account.tail_slot;
    let store: TapeStore = get_primary_store()?;
    log::print_message(&format!("Re-syncing tape: {}, please wait", tape));
    sync_from_block(&store, client, &tape_pubkey, starting_slot).await?;
    log::print_message("Done");
    Ok(())
}

fn handle_create(output: Option<String>) -> Result<()> {
    let snapshot_path =
        output.unwrap_or_else(|| format!("snapshot_{}.tar.gz", Utc::now().timestamp()));
    let store: TapeStore = get_read_only_store()?;
    create_snapshot(&store.db, &snapshot_path)?;
    log::print_message(&format!("Snapshot created at: {}", snapshot_path));
    Ok(())
}

fn handle_load(input: &str) -> Result<()> {
    let primary_path = env::current_dir()?.join("db_tapestore");
    load_from_snapshot(input, &primary_path)?;
    log::print_message("Snapshot loaded into primary store");
    Ok(())
}

async fn handle_get_tape(
    client: &RpcClient,
    tape: &str,
    output: Option<String>,
    raw: bool,
) -> Result<()> {
    let tape_pubkey: Pubkey = FromStr::from_str(tape)?;
    let (tape_account, _) = tapedrive::get_tape_account(client, &tape_pubkey).await?;
    let total_segments = tape_account.total_segments;
    let store: TapeStore = get_read_only_store()?;
    let mut data: Vec<u8> = Vec::with_capacity((total_segments as usize) * SEGMENT_SIZE);
    let mut missing: Vec<u64> = Vec::new();
    for seg_idx in 0..total_segments {
        match store.get_segment_by_address(&tape_pubkey, seg_idx) {
            Ok(seg) => {
                let canonical_seg = padded_array::<SEGMENT_SIZE>(&seg);
                data.extend_from_slice(&canonical_seg);
            }
            Err(e) if matches!(e, StoreError::SegmentNotFoundForAddress(..)) => {
                data.extend_from_slice(&vec![0u8; SEGMENT_SIZE]);
                missing.push(seg_idx);
            }
            Err(e) => return Err(e.into()),
        }
    }

    if !missing.is_empty() {
        log::print_message(&format!("Missing segments: {:?}", missing));
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
        decode_tape(data, &header)?
    };

    write_output(output, &data_to_write, mime_type)?;

    Ok(())
}

async fn handle_get_segment(client: &RpcClient, tape: &str, index: u32) -> Result<()> {
    let tape_pubkey: Pubkey = FromStr::from_str(tape)?;
    let (tape_account, _) = tapedrive::get_tape_account(client, &tape_pubkey).await?;
    if (index as u64) >= tape_account.total_segments {
        anyhow::bail!(
            "Invalid segment index: {} (tape has {} segments)",
            index,
            tape_account.total_segments
        );
    }

    let store: TapeStore = get_read_only_store()?;

    match store.get_segment_by_address(&tape_pubkey, index as u64) {
        Ok(data) => {
            let mut stdout = io::stdout();
            stdout.write_all(&data)?;
            stdout.flush()?;
        }
        Err(e) if matches!(e, StoreError::SegmentNotFoundForAddress(..)) => {
            log::print_message("Segment not found in local store");
        }
        Err(e) => return Err(e.into()),
    }
    Ok(())
}
