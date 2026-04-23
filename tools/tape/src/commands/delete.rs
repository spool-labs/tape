//! `tape delete <track>` — remove a single track from a cassette, freeing
//! its capacity. Tape-level destroy is not supported; the cassette stays.

use std::path::Path;
use std::str::FromStr;

use serde::Serialize;
use tape_crypto::address::Address;

use crate::cassette;
use crate::context::Context;
use crate::error::{Error, Result};
use crate::output::CliOutput;

#[derive(Serialize)]
pub struct DeleteOutput {
    pub cassette: String,
    pub track_address: String,
}

impl CliOutput for DeleteOutput {
    fn print_text(&self) {
        println!("deleted track:    {}", self.track_address);
        println!("from cassette:    {}", self.cassette);
    }
}

pub async fn run(
    ctx: &Context,
    cassette_flag: Option<&Path>,
    track_address: &str,
) -> Result<DeleteOutput> {
    let cassette_path = ctx.require_cassette(cassette_flag)?;
    let tape_key = cassette::load(&cassette_path)?;
    let addr = Address::from_str(track_address)
        .map_err(|e| Error::Invalid(format!("invalid track address: {e}")))?;

    let sdk = ctx.sdk()?;
    sdk.delete(&tape_key, addr)
        .await
        .map_err(|e| Error::Sdk(format!("delete: {e}")))?;

    Ok(DeleteOutput {
        cassette: tape_key.address().to_string(),
        track_address: addr.to_string(),
    })
}
