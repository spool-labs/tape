//! `tape use <cassette>` — mark a cassette keypair as the current default
//! so subsequent commands don't need `--cassette`. Persists in
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
    pub active_cassette: PathBuf,
    pub pubkey: String,
}

impl CliOutput for UseOutput {
    fn print_text(&self) {
        println!("active cassette: {}", self.active_cassette.display());
        println!("pubkey:          {}", self.pubkey);
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
        active_cassette: expanded,
        pubkey: key.address().to_string(),
    })
}
