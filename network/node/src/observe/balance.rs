//! Samples the fee payer balance so the metrics collector and the observe
//! board can report it without an RPC call on the scrape path
//!
//! A node that cannot pay transaction fees stops advancing its pool, never
//! joins a committee, and is never assigned spools, all without surfacing an
//! error. The balance is the leading indicator for that failure.

use std::sync::Arc;
use std::time::Duration;

use tokio::select;
use tokio::time::{interval, MissedTickBehavior};
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

use rpc::Rpc;
use store::Store;
use tape_core::types::coin::SOL;
use tape_observe_api::LOW_BALANCE_LAMPORTS;
use tape_protocol::Api;
use tape_sdk::balance::sol_balance_of;

use crate::context::NodeContext;
use crate::core::error::NodeError;

/// How often the balance is resampled
const REFRESH_INTERVAL: Duration = Duration::from_secs(300);

/// Balance below which the node is at risk of not affording fees
///
/// A system account must retain the rent-exempt minimum after paying a fee, so
/// the spendable balance runs out well before the raw balance does. This floor
/// leaves headroom above that.
const LOW_BALANCE: SOL = SOL(LOW_BALANCE_LAMPORTS);

/// Background service that resamples the fee payer balance into the context
pub struct BalanceMonitor<Db: Store, Cluster: Api, Blockchain: Rpc> {
    context: Arc<NodeContext<Db, Cluster, Blockchain>>,
    cancel: CancellationToken,
}

impl<Db: Store, Cluster: Api, Blockchain: Rpc> BalanceMonitor<Db, Cluster, Blockchain> {
    /// Build a monitor bound to the node's shutdown token
    pub fn new(
        context: Arc<NodeContext<Db, Cluster, Blockchain>>,
        cancel: CancellationToken,
    ) -> Self {
        Self { context, cancel }
    }

    /// Sample until cancelled, keeping the last good reading when a sample fails
    pub async fn run(self) -> Result<(), NodeError> {
        let fee_payer = self.context.pubkey().into();

        // The first tick fires immediately: the gauge reads as unsampled until
        // it lands, and leaving it that way for a refresh interval would hide a
        // node that boots broke.
        let mut ticker = interval(REFRESH_INTERVAL);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);

        loop {
            select! {
                _ = self.cancel.cancelled() => return Ok(()),
                _ = ticker.tick() => {
                    match sol_balance_of(&self.context.rpc, &fee_payer).await {
                        Ok(balance) => {
                            self.context.set_fee_payer_balance(balance);

                            if balance <= LOW_BALANCE {
                                warn!(
                                    %fee_payer,
                                    lamports = balance.0,
                                    "balance: fee payer low, transactions may stop landing"
                                );
                            } else {
                                debug!(%fee_payer, lamports = balance.0, "balance: sampled fee payer");
                            }
                        }
                        Err(error) => {
                            // Keep the last sample rather than reporting a drop to zero
                            warn!(%error, %fee_payer, "balance: fee payer sample failed");
                        }
                    }
                }
            }
        }
    }
}
