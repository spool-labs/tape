//! `tape read <track-addr>` — stream data back to stdout or `--out <file>`.
//!
//! The SDK's `read_into` handles all three write tiers transparently: for a
//! multi-chunk stream it follows the index track to fetch and reassemble
//! chunks; for a single blob or raw write it returns the payload directly.

use std::path::{Path, PathBuf};
use std::str::FromStr;

use serde::Serialize;
use tape_crypto::address::Address;
use tape_sdk::stream::read::read_into;
use tokio::fs::File;
use tokio::io::{stdout, AsyncWriteExt, BufWriter};

use crate::context::Context;
use crate::error::{Error, Result};
use crate::output::CliOutput;

#[derive(Serialize)]
pub struct ReadOutput {
    pub track_address: String,
    pub bytes_written: u64,
    pub destination: String,
}

impl CliOutput for ReadOutput {
    fn print_text(&self) {
        // When streaming to stdout we deliberately write NO trailing text
        // so piping the output to a file works. The only text case here is
        // "--out <file>" where we report what we did.
        if self.destination != "stdout" {
            println!("wrote {} bytes to {}", self.bytes_written, self.destination);
            println!("track address: {}", self.track_address);
        }
    }
}

pub async fn run(
    ctx: &Context,
    track_address: &str,
    out: Option<&Path>,
) -> Result<ReadOutput> {
    let addr = Address::from_str(track_address)
        .map_err(|e| Error::Invalid(format!("invalid track address: {e}")))?;
    let sdk = ctx.sdk()?;

    let (bytes_written, destination) = match out {
        Some(path) => {
            let path_buf = path.to_path_buf();
            let file = File::create(path).await.map_err(|source| Error::Io {
                path: path.display().to_string(),
                source,
            })?;
            let mut writer = BufWriter::new(file);
            read_into(&sdk, &addr, &mut writer)
                .await
                .map_err(|e| Error::Sdk(format!("read_into: {e}")))?;
            writer
                .flush()
                .await
                .map_err(|source| Error::Io {
                    path: path_buf.display().to_string(),
                    source,
                })?;
            // Probe the resulting file size for reporting.
            let metadata = tokio::fs::metadata(path).await.map_err(|source| Error::Io {
                path: path.display().to_string(),
                source,
            })?;
            (metadata.len(), path_buf.display().to_string())
        }
        None => {
            let mut writer = BufWriter::new(stdout());
            read_into(&sdk, &addr, &mut writer)
                .await
                .map_err(|e| Error::Sdk(format!("read_into: {e}")))?;
            writer.flush().await.map_err(|source| Error::Io {
                path: "stdout".into(),
                source,
            })?;
            // Total byte count isn't surfaced by read_into; for stdout we
            // report 0 to avoid noise. Callers that need a count use --out.
            (0, "stdout".into())
        }
    };

    Ok(ReadOutput {
        track_address: addr.to_string(),
        bytes_written,
        destination: destination_as_string(destination),
    })
}

fn destination_as_string(s: String) -> String {
    s
}

// keep a plain PathBuf type in the public surface for JSON callers that
// might want a structured path field — currently we flatten to string.
#[allow(dead_code)]
fn _pathbuf_unused(_p: PathBuf) {}
