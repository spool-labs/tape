//! `tape extend` — add more epochs to a tape's reservation.

use std::path::Path;

use serde::Serialize;

use crate::cassette;
use crate::context::Context;
use crate::error::{Error, Result};
use crate::output::CliOutput;

#[derive(Serialize)]
pub struct ExtendOutput {
    pub address: String,
    pub added_epochs: u64,
    pub new_expiry_epoch: u64,
}

impl CliOutput for ExtendOutput {
    fn print_text(&self) {
        println!("tape address:     {}", self.address);
        println!("added epochs:     {}", self.added_epochs);
        println!("new expiry epoch: {}", self.new_expiry_epoch);
    }
}

pub async fn run(
    ctx: &Context,
    cassette_flag: Option<&Path>,
    epochs: u64,
) -> Result<ExtendOutput> {
    if epochs == 0 {
        return Err(Error::Invalid("--epochs must be > 0".into()));
    }
    let cassette_path = ctx.require_cassette(cassette_flag)?;
    let tape_key = cassette::load(&cassette_path)?;
    let sdk = ctx.sdk()?;

    let tape = sdk
        .extend_expiry(&tape_key, epochs)
        .await
        .map_err(|e| Error::Sdk(format!("extend_expiry: {e}")))?;

    Ok(ExtendOutput {
        address: tape_key.address().to_string(),
        added_epochs: epochs,
        new_expiry_epoch: tape.expiry_epoch.as_u64(),
    })
}
