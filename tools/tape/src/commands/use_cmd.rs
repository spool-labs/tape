//! `tape use <tape-keypair>` — mark a tape keypair as the current default
//! so subsequent commands don't need `--tape`. Persists in
//! `cli-config.yaml`.

use std::path::{Path, PathBuf};

use serde::Serialize;

use crate::cassette;
use crate::config;
use crate::context::Context;
use crate::error::Result;
use crate::output::CliOutput;

#[derive(Serialize)]
pub struct UseOutput {
    pub active_tape: PathBuf,
    pub tape_address: String,
}

impl CliOutput for UseOutput {
    fn print_text(&self) {
        println!("active tape: {}", self.active_tape.display());
        println!("address:     {}", self.tape_address);
    }
}

pub fn run(ctx: &mut Context, path: &Path) -> Result<UseOutput> {
    let expanded = config::expand(path);
    // Validate it's a real keypair before persisting so the user gets a
    // clear error now instead of on the next `tape write`.
    let key = cassette::load(&expanded)?;

    ctx.active_cassette = Some(expanded.clone());
    ctx.config.active_cassette = Some(expanded.clone());
    ctx.save_config()?;

    Ok(UseOutput {
        active_tape: expanded,
        tape_address: key.address().to_string(),
    })
}
