//! `tape list` — every tape the current wallet owns on-chain.

use serde::Serialize;
use tape_crypto::address::Address;

use crate::context::Context;
use crate::error::{Error, Result};
use crate::output::CliOutput;

#[derive(Serialize)]
pub struct TapeSummary {
    pub address: String,
    pub id: u64,
    pub capacity_bytes: u64,
    pub used_bytes: u64,
    pub tracks: u64,
    pub active_epoch: u64,
    pub expiry_epoch: u64,
}

#[derive(Serialize)]
pub struct ListOutput {
    pub authority: String,
    pub count: usize,
    pub tapes: Vec<TapeSummary>,
}

impl CliOutput for ListOutput {
    fn print_text(&self) {
        if self.tapes.is_empty() {
            println!("no tapes owned by {}", self.authority);
            return;
        }
        println!(
            "{:>4}  {:<44}  {:>12}  {:>12}  {:>6}  {:>5}  {:>5}",
            "ID", "ADDRESS", "USED", "CAPACITY", "TRACKS", "ACTIV", "EXPIR"
        );
        for t in &self.tapes {
            println!(
                "{:>4}  {:<44}  {:>12}  {:>12}  {:>6}  {:>5}  {:>5}",
                t.id,
                t.address,
                t.used_bytes,
                t.capacity_bytes,
                t.tracks,
                t.active_epoch,
                t.expiry_epoch,
            );
        }
    }
}

pub async fn run(ctx: &Context) -> Result<ListOutput> {
    let rpc = ctx.rpc_client()?;
    let authority: Address = ctx.payer.pubkey().into();

    let all = rpc
        .get_all_tapes()
        .await
        .map_err(Error::Rpc)?;

    let mut tapes: Vec<TapeSummary> = all
        .into_iter()
        .filter(|(_, tape)| tape.authority == authority)
        .map(|(addr, tape)| TapeSummary {
            address: addr.to_string(),
            id: tape.id.as_u64(),
            capacity_bytes: tape.capacity.to_bytes(),
            used_bytes: tape.used.to_bytes(),
            tracks: tape.tracks.num_tracks(),
            active_epoch: tape.active_epoch.as_u64(),
            expiry_epoch: tape.expiry_epoch.as_u64(),
        })
        .collect();
    tapes.sort_by_key(|t| t.id);

    Ok(ListOutput {
        authority: authority.to_string(),
        count: tapes.len(),
        tapes,
    })
}
