//! Operator commands against the records and the chain

use std::collections::HashMap;
use std::sync::Arc;

use tape_blacklist::entry::{kind_name, Entry};
use tape_core::types::EpochNumber;
use tape_crypto::address::Address;

use crate::chain::error::ChainError;
use crate::chain::sdk::SdkChain;
use crate::chain::tape_chain::{TapeChain, TapeStatus};
use crate::config::settings::Config;
use crate::funder::upkeep::Funder;
use crate::operator::error::OperatorError;
use crate::publisher::flush::Publisher;
use crate::records::rows::{ListTape, ListTapes};
use crate::records::states::{TapeRole, TapeState};
use crate::records::store::Records;
use crate::types::target::Target;
use crate::types::timestamp::Timestamp;

/// What the chain says now about the list's tapes
struct LiveChain {
    epoch: EpochNumber,
    tapes: HashMap<Address, TapeStatus>,
}

/// Queue a remove or unlist for the running service to publish
pub async fn queue_decision(config: &Config, entry: Entry) -> Result<(), OperatorError> {
    let records = Records::open(&config.database)?;
    let id = records.queue_operator_decision(entry, Timestamp::now()).await?;
    let kind = kind_name(&entry.target).unwrap_or_default();
    println!(
        "decision {}: {} {kind} {}",
        id.0,
        entry.action.as_str(),
        entry.target.target
    );
    println!("the running service publishes it within the batch window");
    Ok(())
}

/// Undo this producer's decisions on a target, for the running service to take off its tapes
pub async fn restore(config: &Config, target: Target) -> Result<(), OperatorError> {
    let records = Records::open(&config.database)?;
    let undone = records.restore_target(target, Timestamp::now()).await?;
    if undone == 0 {
        println!("no live decision on {target}; recorded it as restored");
        return Ok(());
    }
    println!("undid {undone} decision(s) on {target}");
    println!("the running service writes the other entries of each track naming it into a new track, then deletes that track");
    Ok(())
}

/// Create the list's rack and first blacklist tape, and print the subscription consumers use
pub async fn create_rack(config: &Config) -> Result<(), OperatorError> {
    let records = Records::open(&config.database)?;
    let chain = Arc::new(SdkChain::try_new(&config.chain)?);
    let publisher = Publisher::new(records.clone(), Arc::clone(&chain), config.publisher.batch_window);
    let funder = Funder::new(records, chain, config.funder.clone(), config.tapes.clone());
    let created = funder.create_rack(&publisher, Timestamp::now()).await?;

    println!("rack: {}", created.rack.address);
    println!("first blacklist tape: {}", created.blacklist.address);
    println!("tape keys: {}", config.tapes.key_dir.display());
    println!();
    println!("consumers subscribe with:");
    println!("blacklists:");
    println!("  - name: tapedrive-moderation");
    println!("    tape: {}", created.rack.address);
    println!("    actions: [remove, unlist]");
    Ok(())
}

/// Print pending jobs, queued decisions, tracks to delete, and the rack and blacklist tapes with their expiry
pub async fn status(config: &Config) -> Result<(), OperatorError> {
    let records = Records::open(&config.database)?;
    let now = Timestamp::now();
    let summary = records.status(now).await?;

    println!(
        "jobs: {} pending ({} due), {} failed",
        summary.pending_jobs, summary.due_jobs, summary.failed_jobs
    );
    match summary.oldest_queued {
        Some(oldest) => println!(
            "decisions: {} queued, oldest decided {}s ago",
            summary.queued_decisions,
            now.since(oldest).as_secs()
        ),
        None => println!("decisions: none queued"),
    }
    println!("tracks waiting to be deleted: {}", summary.deleting_tracks);

    let live = match read_live_chain(config, &summary.tapes).await {
        Ok(live) => {
            println!("current epoch: {}", live.epoch.0);
            Some(live)
        }
        Err(error) => {
            println!("chain unreachable ({error}); showing recorded values");
            None
        }
    };
    match &summary.tapes.rack {
        Some(rack) => println!("rack: {}", describe(rack, &summary.tapes, live.as_ref())),
        None => println!("rack: none; run create-rack"),
    }
    println!("blacklist tapes:");
    for tape in &summary.tapes.blacklists {
        println!("  {}", describe(tape, &summary.tapes, live.as_ref()));
    }
    Ok(())
}

async fn read_live_chain(config: &Config, tapes: &ListTapes) -> Result<LiveChain, ChainError> {
    let chain = SdkChain::try_new(&config.chain)?;
    let epoch = chain.current_epoch().await?;
    let mut statuses = HashMap::new();
    for tape in tapes.all() {
        if let Some(status) = chain.tape_status(tape.address).await? {
            statuses.insert(tape.address, status);
        }
    }
    Ok(LiveChain {
        epoch,
        tapes: statuses,
    })
}

/// One line for a rack or blacklist tape: address, state, next track and expiry
fn describe(tape: &ListTape, tapes: &ListTapes, live: Option<&LiveChain>) -> String {
    let status = live.and_then(|live| live.tapes.get(&tape.address));
    let expiry = match (status, tape.expiry_epoch) {
        (Some(status), _) => format!("expires at epoch {}", status.expiry_epoch.0),
        (None, Some(recorded)) => format!("expiry epoch {} when last seen", recorded.0),
        (None, None) => "expiry unknown".to_string(),
    };
    let next_track = status.map(|status| status.next_track).unwrap_or(tape.next_track);
    let is_newest = tapes.newest().is_some_and(|newest| newest.address == tape.address);
    format!("{} {}, next track {}, {expiry}", tape.address, label(tape, is_newest), next_track.0)
}

/// Where a tape stands, in words
fn label(tape: &ListTape, is_newest: bool) -> &'static str {
    match tape.state {
        TapeState::Reserved => reserved_label(tape, is_newest),
        TapeState::Reserving | TapeState::Vacating | TapeState::Retired | TapeState::Expired => tape.state.as_str(),
    }
}

fn reserved_label(tape: &ListTape, is_newest: bool) -> &'static str {
    match (tape.is_declared, tape.is_named, is_newest, tape.role) {
        (false, _, _, _) => "reserved, header not written",
        (true, _, true, _) => "newest",
        (true, true, false, _) => "named",
        (true, false, false, TapeRole::Rack) => "declared",
        (true, false, false, TapeRole::Blacklist) => "declared, not in the rack yet",
    }
}
