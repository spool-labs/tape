use anyhow::{bail, Result};
use chrono::Utc;
use dialoguer::{theme::ColorfulTheme, Confirm};
use indicatif::{ProgressBar, ProgressStyle};
use mime_guess::Mime;
use mime_guess::MimeGuess;
use reqwest;
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::{
    pubkey::Pubkey,
    signature::{Keypair, Signature},
};
use std::io::Read;
use std::sync::Arc;
use tokio::sync::Semaphore;
use tokio::{task, time::Duration};

use tape_api::prelude::*;
use tape_client::{
    create_tape, encode_tape, finalize_tape, write_to_tape, CompressionAlgo, EncryptionAlgo,
    TapeFlags, TapeHeader,
};

use crate::cli::{Cli, Commands};
use crate::log;
use crate::utils::{mime_to_type, default_octet};

const SEGMENTS_PER_TX: usize = 7; // 7 x 128 = 896 bytes
const SAFE_SIZE: usize = SEGMENT_SIZE * SEGMENTS_PER_TX;
const MAX_CONCURRENT: usize = 10;

pub async fn handle_write_command(cli: Cli, client: RpcClient, payer: Keypair) -> Result<()> {
    if let Commands::Write {
        ref filename,
        ref message,
        ref remote,
        ref tape_name,
    } = cli.command
    {
        let (data, source, mime) =
            process_input(filename.clone(), message.clone(), remote.clone()).await?;
        let mime_type = mime_to_type(&mime);

        let compression_algo = CompressionAlgo::Gzip;
        let encryption_algo = EncryptionAlgo::None; // No encryption for now
        let flags = TapeFlags::Prefixed;

        let mut header = TapeHeader::new(mime_type, compression_algo, encryption_algo, flags);

        let encoded = encode_tape(&data, &mut header)?;
        let chunks: Vec<_> = encoded.chunks(SAFE_SIZE).map(|c| c.to_vec()).collect();
        let chunks_len = chunks.len();

        let tape_name = tape_name
            .clone()
            .unwrap_or_else(|| Utc::now().timestamp().to_string());

        print_write_summary(
            &cli,
            &source,
            &tape_name,
            &mime,
            compression_algo,
            encryption_algo,
            flags,
            chunks_len,
        );

        if !confirm_proceed()? {
            log::print_error("Write operation cancelled");
            return Ok(());
        }

        let pb = setup_progress_bar(chunks_len as u64);

        let client = Arc::new(client);
        let payer = Arc::new(payer);

        pb.set_message("Creating new tape (please wait)...");
        let (tape_address, writer_address, _sig) =
            create_tape(&client, &payer, &tape_name, header).await?;

        write_chunks(&client, &payer, tape_address, writer_address, chunks, &pb).await?;

        pb.set_message("finalizing tape...");
        tokio::time::sleep(Duration::from_secs(32)).await;

        finalize_tape(&client, &payer, tape_address, writer_address, header).await?;

        pb.finish_with_message("");

        print_write_completion(&cli, tape_address, chunks_len);
    }
    Ok(())
}

fn print_write_summary(
    cli: &Cli,
    source: &str,
    tape_name: &str,
    mime: &Mime,
    compression_algo: CompressionAlgo,
    encryption_algo: EncryptionAlgo,
    flags: TapeFlags,
    chunk_count: usize,
) {
    if cli.verbose {
        log::print_section_header("Tape Write");
        log::print_message(&format!("Source: {}", source));
        log::print_message(&format!("Tape Name: {}", tape_name));
        log::print_message(&format!("MIME Type: {}", mime));
        log::print_message(&format!("Compression: {:?}", compression_algo));
        log::print_message(&format!("Encryption: {:?}", encryption_algo));
        log::print_message(&format!("Flags: {:?}", flags));
    }
    log::print_count(&format!("Total Chunks: {}", chunk_count));
    log::print_divider();
}

fn confirm_proceed() -> Result<bool> {
    Confirm::with_theme(&ColorfulTheme::default())
        .with_prompt("→ Begin writing to tape?")
        .default(false)
        .interact()
        .map_err(|e| anyhow::anyhow!("Failed to get user input: {}", e))
}

fn setup_progress_bar(total: u64) -> ProgressBar {
    let pb = ProgressBar::new(total);
    pb.set_style(
        ProgressStyle::default_bar()
            .template("{spinner:.green} {wide_msg}")
            .expect("Failed to set progress style"),
    );
    let pb_clone = pb.clone();
    task::spawn(async move {
        while !pb_clone.is_finished() {
            pb_clone.tick();
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    });
    pb
}

async fn write_chunks(
    client: &Arc<RpcClient>,
    payer: &Arc<Keypair>,
    tape_address: Pubkey,
    writer_address: Pubkey,
    chunks: Vec<Vec<u8>>,
    pb: &ProgressBar,
) -> Result<()> {
    pb.set_message("");
    pb.set_style(
        ProgressStyle::default_bar()
            .template("{spinner:.green} [{bar:40.white/gray}] {pos}/{len} {wide_msg}")
            .expect("Failed to set progress style"),
    );

    let semaphore = Arc::new(Semaphore::new(MAX_CONCURRENT));

    let mut handles = Vec::with_capacity(chunks.len());
    for chunk in chunks {
        let client_clone = client.clone();
        let payer_clone = payer.clone();
        let pb_clone = pb.clone();
        let semaphore_clone = semaphore.clone();
        let handle: task::JoinHandle<Result<Signature>> = task::spawn(async move {
            let _permit = semaphore_clone.acquire().await?;
            let sig = write_to_tape(
                &client_clone,
                &payer_clone,
                tape_address,
                writer_address,
                &chunk,
            )
            .await?;
            pb_clone.inc(1);
            Ok(sig)
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.await??;
    }

    Ok(())
}

fn print_write_completion(cli: &Cli, tape_address: Pubkey, chunk_count: usize) {
    log::print_divider();

    if cli.verbose {
        log::print_divider();
        log::print_section_header("Metadata");
        log::print_count(&format!("Tape Address: {}", tape_address));
        log::print_count(&format!("Total Chunks: {}", chunk_count));
    }

    log::print_divider();
    log::print_info("To read the tape, run:");
    log::print_title(&format!("tapedrive read {}", tape_address));
    log::print_divider();
}

/// Processes input from file, message, or remote URL.
/// Returns the data, source description, and MIME type.
async fn process_input(
    filename: Option<String>,
    message: Option<String>,
    remote: Option<String>,
) -> Result<(Vec<u8>, String, Mime)> {
    match (filename, message, remote) {
        (Some(path_str), None, None) => process_file_input(&path_str),
        (None, Some(m), None) => process_message_input(m),
        (None, None, Some(url)) => process_remote_input(&url).await,
        _ => bail!("Must provide exactly one of: <FILE>, -m <MSG>, or -r <URL>"),
    }
}

fn process_file_input(path_str: &str) -> Result<(Vec<u8>, String, Mime)> {
    let data = std::fs::read(path_str)?;
    let source = path_str.to_string();
    let mime = MimeGuess::from_path(std::path::Path::new(path_str)).first_or_octet_stream();
    Ok((data, source, mime))
}

fn process_message_input(m: String) -> Result<(Vec<u8>, String, Mime)> {
    if m == "-" {
        let stdin_data = read_from_stdin()?;
        if stdin_data.is_empty() {
            bail!("No data provided via piped input");
        }
        let source = "piped input".to_string();
        let mime = default_octet();
        Ok((stdin_data, source, mime))
    } else {
        let data = m.as_bytes().to_vec();
        let source = "command-line message".to_string();
        let mime: Mime = "text/plain".parse().unwrap();
        Ok((data, source, mime))
    }
}

async fn process_remote_input(url: &str) -> Result<(Vec<u8>, String, Mime)> {
    let response = reqwest::get(url).await?;
    if !response.status().is_success() {
        bail!("Failed to fetch remote file: HTTP {}", response.status());
    }

    let mime = response
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|ct| ct.to_str().ok())
        .and_then(|s| s.parse().ok())
        .unwrap_or_else(|| MimeGuess::from_path(std::path::Path::new(url)).first_or_octet_stream());

    let data = response.bytes().await?.to_vec();
    let source = url.to_string();
    Ok((data, source, mime))
}

/// Reads data from stdin into a vector of bytes.
fn read_from_stdin() -> std::io::Result<Vec<u8>> {
    let mut buffer = Vec::new();
    std::io::stdin().read_to_end(&mut buffer)?;
    Ok(buffer)
}

