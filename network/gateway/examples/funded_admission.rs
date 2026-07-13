//! Embed the gateway with a prepaid in-memory admission gate
//!
//! Shows the three pieces an operator wires together: an Admission
//! implementation (the ticket lifecycle of reserve, commit, refund, plus
//! orphan expiry), and the embedding seam that injects it into the stock
//! runtime. Not production: balances live in memory, are seeded with a fixed
//! allowance instead of real deposits, and vanish on restart. The blueprint
//! for a real funded-balance operator is in docs/gateway.md.
//!
//! Runs like the stock binary:
//!
//! ```text
//! cargo run --example funded_admission -- path/to/config.yaml
//! ```

use std::collections::HashMap;
use std::process::ExitCode;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use tape_crypto::address::Address;
use tape_gateway::admission::{Admission, AdmissionDeny, AdmissionRequest};
use tape_gateway::runtime::run_with_context;
use tape_node::config::node::{NodeConfig, default_config_path};
use tape_node::context::AppContext;
use tape_node::core::startup::build_context;
use tape_node::core::error::NodeError;
use tape_node::runtime::{build_runtime, init_tracing};

/// Allowance seeded for a principal on first sight; a real operator credits
/// balances from observed deposits instead.
const STARTING_BALANCE_BYTES: u64 = 64 * 1024 * 1024;

/// Holds older than this are presumed orphaned (a crash between reserve and
/// settle) and released, mirroring the gateway's own ledger sweep.
const HOLD_TTL: Duration = Duration::from_secs(300);

/// One outstanding reserve: whose balance it debits and how much it holds.
struct Hold {
    principal: Address,
    bytes: u64,
    created_at: Instant,
}

/// Balances and outstanding holds behind one lock.
#[derive(Default)]
struct Ledger {
    balances: HashMap<Address, u64>,
    holds: HashMap<u64, Hold>,
}

/// Prepaid byte balances held in memory.
#[derive(Default)]
struct PrepaidAdmission {
    ledger: Mutex<Ledger>,
}

impl PrepaidAdmission {
    fn lock_ledger(&self) -> std::sync::MutexGuard<'_, Ledger> {
        self.ledger.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Release holds that were never settled, crediting them back.
    fn expire_orphans(ledger: &mut Ledger) {
        let mut expired: Vec<u64> = Vec::new();
        for (ticket, hold) in &ledger.holds {
            if hold.created_at.elapsed() >= HOLD_TTL {
                expired.push(*ticket);
            }
        }
        for ticket in expired {
            if let Some(hold) = ledger.holds.remove(&ticket) {
                *ledger.balances.entry(hold.principal).or_default() += hold.bytes;
            }
        }
    }
}

#[async_trait]
impl Admission for PrepaidAdmission {
    async fn reserve(&self, request: AdmissionRequest) -> Result<(), AdmissionDeny> {
        let mut ledger = self.lock_ledger();
        Self::expire_orphans(&mut ledger);

        let balance = ledger
            .balances
            .entry(request.principal)
            .or_insert(STARTING_BALANCE_BYTES);
        if *balance < request.estimated_bytes {
            return Err(AdmissionDeny {
                reason: "prepaid balance is too low for this write".to_string(),
                retry_after_seconds: None,
            });
        }

        // Hold the estimate; commit settles the difference against the actual
        *balance -= request.estimated_bytes;
        ledger.holds.insert(
            request.ticket,
            Hold {
                principal: request.principal,
                bytes: request.estimated_bytes,
                created_at: Instant::now(),
            },
        );
        Ok(())
    }

    fn commit(&self, ticket: u64, actual_bytes: u64) {
        let mut ledger = self.lock_ledger();
        if let Some(hold) = ledger.holds.remove(&ticket) {
            let unused = hold.bytes.saturating_sub(actual_bytes);
            *ledger.balances.entry(hold.principal).or_default() += unused;
        }
    }

    fn refund(&self, ticket: u64) {
        let mut ledger = self.lock_ledger();
        if let Some(hold) = ledger.holds.remove(&ticket) {
            *ledger.balances.entry(hold.principal).or_default() += hold.bytes;
        }
    }
}

async fn run(config: NodeConfig) -> Result<(), NodeError> {
    let context: AppContext = build_context(&config).await?;
    run_with_context(context, config, Arc::new(PrepaidAdmission::default())).await
}

fn main() -> ExitCode {
    let config_path = std::env::args()
        .nth(1)
        .unwrap_or_else(|| default_config_path().to_string_lossy().into_owned());

    let config = match NodeConfig::from_yaml_file(&config_path) {
        Ok(config) => config,
        Err(error) => {
            eprintln!("configuration failed: {error}");
            return ExitCode::FAILURE;
        }
    };

    if let Err(error) = init_tracing(&config.logging) {
        eprintln!("tracing initialization failed: {error}");
        return ExitCode::FAILURE;
    }

    let runtime = match build_runtime() {
        Ok(runtime) => runtime,
        Err(error) => {
            eprintln!("runtime build failed: {error}");
            return ExitCode::FAILURE;
        }
    };

    match runtime.block_on(run(config)) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("application failed: {error}");
            ExitCode::FAILURE
        }
    }
}
