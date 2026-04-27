//! `tape read <address>` — stream data back to stdout or `--out <file>`.

use std::path::Path;
use std::str::FromStr;

use clap::ValueEnum;
use peer_http::HttpApi;
use rpc_solana::SolanaRpc;
use serde::Serialize;
use tape_crypto::address::Address;
use tape_sdk::stream::manifest::ChunkManifest;
use tape_sdk::tapedrive::Tapedrive;
use tokio::fs::{metadata, try_exists, File};
use tokio::io::{stdout, AsyncWriteExt, BufWriter};

use crate::context::Context;
use crate::error::{Error, Result};
use crate::output::CliOutput;

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
#[clap(rename_all = "kebab-case")]
pub enum ReadMode {
    Auto,
    Track,
    Stream,
}

impl ReadMode {
    fn as_str(self) -> &'static str {
        match self {
            Self::Auto => "auto",
            Self::Track => "track",
            Self::Stream => "stream",
        }
    }
}

#[derive(Serialize)]
pub struct ReadOutput {
    pub address: String,
    pub bytes_written: u64,
    pub destination: String,
    pub mode: &'static str,
}

impl CliOutput for ReadOutput {
    fn print_text(&self) {
        // When streaming to stdout we deliberately write NO trailing text
        // so piping the output to a file works. The only text case here is
        // "--out <file>" where we report what we did.
        if self.destination != "stdout" {
            println!("wrote {} bytes to {}", self.bytes_written, self.destination);
            println!("address: {}", self.address);
            println!("mode: {}", self.mode);
        }
    }
}

pub async fn run(
    ctx: &Context,
    address: &str,
    out: Option<&Path>,
    overwrite: bool,
    mode: ReadMode,
) -> Result<ReadOutput> {
    let addr = Address::from_str(address)
        .map_err(|e| Error::Invalid(format!("invalid address: {e}")))?;
    let sdk = ctx.sdk()?;

    let actual_mode = match mode {
        ReadMode::Auto => {
            let direct = sdk
                .read(&addr)
                .await
                .map_err(|e| Error::Sdk(format!("read track: {e}")))?;
            if ChunkManifest::from_bytes(&direct).is_ok() {
                read_stream_to_destination(&sdk, &addr, out, overwrite).await?
            } else {
                write_bytes_to_destination(&direct, out, overwrite).await?
            }
        }
        ReadMode::Track => {
            let direct = sdk
                .read(&addr)
                .await
                .map_err(|e| Error::Sdk(format!("read track: {e}")))?;
            write_bytes_to_destination(&direct, out, overwrite).await?
        }
        ReadMode::Stream => read_stream_to_destination(&sdk, &addr, out, overwrite).await?,
    };

    Ok(ReadOutput {
        address: addr.to_string(),
        bytes_written: actual_mode.bytes_written,
        destination: actual_mode.destination,
        mode: actual_mode.mode.as_str(),
    })
}

struct ReadResult {
    bytes_written: u64,
    destination: String,
    mode: ReadMode,
}

async fn read_stream_to_destination(
    sdk: &Tapedrive<SolanaRpc, HttpApi>,
    addr: &Address,
    out: Option<&Path>,
    overwrite: bool,
) -> Result<ReadResult> {
    match out {
        Some(path) => {
            create_output_guard(path, overwrite).await?;
            let path_buf = path.to_path_buf();
            let file = File::create(path).await.map_err(|source| Error::Io {
                path: path.display().to_string(),
                source,
            })?;
            let mut writer = BufWriter::new(file);
            sdk.read_into(addr, &mut writer)
                .await
                .map_err(|e| Error::Sdk(format!("read stream: {e}")))?;
            writer.flush().await.map_err(|source| Error::Io {
                path: path_buf.display().to_string(),
                source,
            })?;
            let metadata = metadata(path).await.map_err(|source| Error::Io {
                path: path.display().to_string(),
                source,
            })?;
            Ok(ReadResult {
                bytes_written: metadata.len(),
                destination: path_buf.display().to_string(),
                mode: ReadMode::Stream,
            })
        }
        None => {
            let mut writer = BufWriter::new(stdout());
            sdk.read_into(addr, &mut writer)
                .await
                .map_err(|e| Error::Sdk(format!("read stream: {e}")))?;
            writer.flush().await.map_err(|source| Error::Io {
                path: "stdout".into(),
                source,
            })?;
            Ok(ReadResult {
                bytes_written: 0,
                destination: "stdout".into(),
                mode: ReadMode::Stream,
            })
        }
    }
}

async fn write_bytes_to_destination(
    bytes: &[u8],
    out: Option<&Path>,
    overwrite: bool,
) -> Result<ReadResult> {
    match out {
        Some(path) => {
            create_output_guard(path, overwrite).await?;
            let mut file = File::create(path).await.map_err(|source| Error::Io {
                path: path.display().to_string(),
                source,
            })?;
            file.write_all(bytes).await.map_err(|source| Error::Io {
                path: path.display().to_string(),
                source,
            })?;
            file.flush().await.map_err(|source| Error::Io {
                path: path.display().to_string(),
                source,
            })?;
            Ok(ReadResult {
                bytes_written: bytes.len() as u64,
                destination: path.display().to_string(),
                mode: ReadMode::Track,
            })
        }
        None => {
            let mut writer = BufWriter::new(stdout());
            writer.write_all(bytes).await.map_err(|source| Error::Io {
                path: "stdout".into(),
                source,
            })?;
            writer.flush().await.map_err(|source| Error::Io {
                path: "stdout".into(),
                source,
            })?;
            Ok(ReadResult {
                bytes_written: 0,
                destination: "stdout".into(),
                mode: ReadMode::Track,
            })
        }
    }
}

async fn create_output_guard(path: &Path, overwrite: bool) -> Result<()> {
    if !overwrite && try_exists(path).await.map_err(|source| Error::Io {
        path: path.display().to_string(),
        source,
    })? {
        return Err(Error::Invalid(format!(
            "{} already exists; pass --overwrite to replace it",
            path.display()
        )));
    }
    Ok(())
}
