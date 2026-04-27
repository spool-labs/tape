//! `tape info` — on-chain state of a tape.

use std::path::Path;

use serde::Serialize;

use crate::cassette;
use crate::context::Context;
use crate::error::{Error, Result};
use crate::output::CliOutput;

#[derive(Serialize)]
pub struct InfoOutput {
    pub address: String,
    pub id: u64,
    pub authority: String,
    pub capacity_bytes: u64,
    pub used_bytes: u64,
    pub free_bytes: u64,
    pub tracks: u64,
    pub next_track_number: u64,
    pub active_epoch: u64,
    pub expiry_epoch: u64,
    pub epochs_remaining: u64,
}

impl CliOutput for InfoOutput {
    fn print_text(&self) {
        println!("tape address:      {}", self.address);
        println!("id:                {}", self.id);
        println!("authority:         {}", self.authority);
        println!(
            "capacity:          {} bytes ({} used, {} free)",
            self.capacity_bytes, self.used_bytes, self.free_bytes
        );
        println!("tracks:            {}", self.tracks);
        println!("next track number: {}", self.next_track_number);
        println!(
            "epochs:            active={} expiry={} remaining={}",
            self.active_epoch, self.expiry_epoch, self.epochs_remaining
        );
    }
}

pub async fn run(ctx: &Context, cassette_flag: Option<&Path>) -> Result<InfoOutput> {
    let cassette_path = ctx.require_cassette(cassette_flag)?;
    let tape_key = cassette::load(&cassette_path)?;
    let sdk = ctx.sdk()?;

    let tape = sdk
        .get_tape(&tape_key.address())
        .await
        .map_err(|e| Error::Sdk(format!("get_tape: {e}")))?;

    let capacity = tape.capacity.to_bytes();
    let used = tape.used.to_bytes();
    let expiry = tape.expiry_epoch.as_u64();
    let active = tape.active_epoch.as_u64();

    Ok(InfoOutput {
        address: tape_key.address().to_string(),
        id: tape.id.as_u64(),
        authority: tape.authority.to_string(),
        capacity_bytes: capacity,
        used_bytes: used,
        free_bytes: capacity.saturating_sub(used),
        tracks: tape.tracks.num_tracks(),
        next_track_number: tape.tracks.next_number().as_u64(),
        active_epoch: active,
        expiry_epoch: expiry,
        epochs_remaining: expiry.saturating_sub(active),
    })
}
