//! `tape create` — generate a new cassette keypair and save it.
//!
//! Note: this does NOT send a `ReserveTape` instruction yet — reservation
//! happens on first `tape write`, when we know the required capacity and
//! epoch horizon. Keeping this command offline-only means users can
//! generate cassettes without a funded wallet.

use std::path::PathBuf;

use serde::Serialize;

use crate::cassette;
use crate::config;
use crate::context::Context;
use crate::error::{Error, Result};
use crate::output::CliOutput;

#[derive(Serialize)]
pub struct CreateOutput {
    pub cassette: PathBuf,
    pub pubkey: String,
    pub set_active: bool,
}

impl CliOutput for CreateOutput {
    fn print_text(&self) {
        println!("created cassette: {}", self.cassette.display());
        println!("pubkey:           {}", self.pubkey);
        if self.set_active {
            println!("(set as active cassette)");
        }
    }
}

pub fn run(
    ctx: &mut Context,
    out: Option<PathBuf>,
    set_active: bool,
    force: bool,
) -> Result<CreateOutput> {
    let key = cassette::generate();
    let path = match out {
        Some(p) => config::expand(&p),
        None => cassette::default_path(&key),
    };

    if path.exists() && !force {
        return Err(Error::Invalid(format!(
            "{} already exists — pass --force to overwrite",
            path.display()
        )));
    }

    cassette::save(&key, &path)?;

    if set_active {
        ctx.active_cassette = Some(path.clone());
        ctx.config.active_cassette = Some(path.clone());
        ctx.save_config()?;
    }

    Ok(CreateOutput {
        cassette: path,
        pubkey: key.address().to_string(),
        set_active,
    })
}
