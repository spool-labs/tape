//! `tape resize` — grow a tape's reserved capacity.

use std::path::Path;

use serde::Serialize;
use tape_core::types::StorageUnits;

use crate::cassette;
use crate::commands::size::parse_size;
use crate::context::Context;
use crate::error::{Error, Result};
use crate::output::CliOutput;

#[derive(Serialize)]
pub struct ResizeOutput {
    pub address: String,
    pub added_bytes: u64,
    pub new_capacity_bytes: u64,
}

impl CliOutput for ResizeOutput {
    fn print_text(&self) {
        println!("tape address:      {}", self.address);
        println!("added capacity:    {} bytes", self.added_bytes);
        println!("new capacity:      {} bytes", self.new_capacity_bytes);
    }
}

pub async fn run(
    ctx: &Context,
    tape_flag: Option<&Path>,
    add: Option<&str>,
    to: Option<&str>,
) -> Result<ResizeOutput> {
    let tape_path = ctx.require_cassette(tape_flag)?;
    let tape_key = cassette::load(&tape_path)?;
    let sdk = ctx.sdk()?;

    let current = sdk
        .get_tape(&tape_key.address())
        .await
        .map_err(|e| Error::Sdk(format!("get_tape: {e}")))?;

    let extra_bytes = match (add, to) {
        (Some(size), None) => parse_size(size)?,
        (None, Some(size)) => {
            let target = parse_size(size)?;
            let current_capacity = current.capacity.to_bytes();
            if target <= current_capacity {
                return Err(Error::Invalid(format!(
                    "--to must be larger than current capacity ({current_capacity} bytes)"
                )));
            }
            target - current_capacity
        }
        (Some(_), Some(_)) => {
            return Err(Error::Invalid("pass either --add or --to, not both".into()));
        }
        (None, None) => {
            return Err(Error::Invalid("pass --add <SIZE> or --to <SIZE>".into()));
        }
    };

    if extra_bytes == 0 {
        return Err(Error::Invalid("capacity delta must be > 0".into()));
    }

    let tape = sdk
        .extend_capacity(&tape_key, StorageUnits::from_bytes(extra_bytes))
        .await
        .map_err(|e| Error::Sdk(format!("extend_capacity: {e}")))?;

    Ok(ResizeOutput {
        address: tape_key.address().to_string(),
        added_bytes: extra_bytes,
        new_capacity_bytes: tape.capacity.to_bytes(),
    })
}
