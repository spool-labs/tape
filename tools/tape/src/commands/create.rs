//! `tape create` — reserve a new tape and save its local keypair.

use std::path::PathBuf;

use serde::Serialize;
use tape_core::types::StorageUnits;

use crate::cassette;
use crate::commands::size::parse_size;
use crate::config;
use crate::context::Context;
use crate::error::{Error, Result};
use crate::output::CliOutput;

#[derive(Serialize)]
pub struct CreateOutput {
    pub tape_keypair: PathBuf,
    pub tape_address: String,
    pub capacity_bytes: u64,
    pub active_epoch: u64,
    pub expiry_epoch: u64,
    pub set_active: bool,
}

impl CliOutput for CreateOutput {
    fn print_text(&self) {
        println!("tape keypair:     {}", self.tape_keypair.display());
        println!("tape address:     {}", self.tape_address);
        println!("capacity:         {} bytes", self.capacity_bytes);
        println!(
            "epochs:           active={} expiry={}",
            self.active_epoch, self.expiry_epoch
        );
        if self.set_active {
            println!("(set as active tape)");
        } else {
            println!(
                "active tape:      unchanged; run `tape use {}` or pass `--use` next time",
                self.tape_keypair.display()
            );
        }
    }
}

pub async fn run(
    ctx: &mut Context,
    out: Option<PathBuf>,
    capacity: &str,
    epochs: u64,
    set_active: bool,
    overwrite_key: bool,
) -> Result<CreateOutput> {
    let capacity_bytes = parse_size(capacity)?;
    if capacity_bytes == 0 {
        return Err(Error::Invalid("--capacity must be > 0".into()));
    }
    if epochs == 0 {
        return Err(Error::Invalid("--epochs must be > 0".into()));
    }

    let key = cassette::generate();
    let path = match out {
        Some(p) => config::expand(&p),
        None => cassette::default_path(&key),
    };

    if path.exists() && !overwrite_key {
        return Err(Error::Invalid(format!(
            "{} already exists; pass --overwrite-key to replace it",
            path.display()
        )));
    }

    cassette::save(&key, &path)?;

    let sdk = ctx.sdk()?;
    let tape = sdk
        .reserve(&key, StorageUnits::from_bytes(capacity_bytes), epochs)
        .await
        .map_err(|e| Error::Sdk(format!("reserve: {e}")))?;

    if set_active {
        ctx.active_cassette = Some(path.clone());
        ctx.config.active_cassette = Some(path.clone());
        ctx.save_config()?;
    }

    Ok(CreateOutput {
        tape_keypair: path,
        tape_address: key.address().to_string(),
        capacity_bytes: tape.capacity.to_bytes(),
        active_epoch: tape.active_epoch.as_u64(),
        expiry_epoch: tape.expiry_epoch.as_u64(),
        set_active,
    })
}
