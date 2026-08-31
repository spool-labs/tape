use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use rpc::Rpc;
use store::Store;
use tape_core::types::EpochNumber;
use tape_protocol::{Api, ProtocolState, fetch::fetch_state};
use tape_retry::{RetryConfig, retry_if};
use tokio_util::sync::CancellationToken;
use tracing::warn;

use crate::context::NodeContext;
use crate::core::error::NodeError;

/// What sent the node back to the chain for a fresh view.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RealignCause {
    /// Consecutive rounds in which nothing this node judged stood.
    Tripwire,
    /// The group agreed on a view of the epoch that was not this node's.
    Divergence,
}

impl RealignCause {
    pub fn label(self) -> &'static str {
        match self {
            RealignCause::Tripwire => "tripwire",
            RealignCause::Divergence => "divergence",
        }
    }
}

/// Re-reads protocol state from the chain and swaps it in.
///
/// Startup, epoch advance and the challenge tripwire all come through here, so
/// a realigned node holds what a freshly started one would.
pub async fn refetch_state<Db, Cluster, Blockchain>(
    context: &Arc<NodeContext<Db, Cluster, Blockchain>>,
    cancel: Option<&CancellationToken>,
    at_least: Option<EpochNumber>,
) -> Result<Arc<ProtocolState>, NodeError>
where
    Db: Store,
    Cluster: Api,
    Blockchain: Rpc,
{
    let source = context.clone();
    let state = retry_if(
        RetryConfig::infinite(),
        cancel,
        move || {
            let context = source.clone();
            async move {
                let state = fetch_state(&context.rpc).await.map_err(NodeError::from)?;
                match at_least {
                    Some(epoch) if state.epoch() < epoch => {
                        Err(NodeError::StateUnavailable { expected_epoch: epoch })
                    }
                    Some(_) | None => Ok(state),
                }
            }
        },
        |error| match error {
            NodeError::Rpc(error) => error.is_retriable() && !error.is_skipped_slot(),
            NodeError::StateUnavailable { expected_epoch } => Some(*expected_epoch) == at_least,
            _ => false,
        },
    )
    .await?;

    context.set_state(state)?;
    if let Err(error) = context.refresh_peers().await {
        warn!(%error, "peer refresh failed after realign");
    }

    Ok(context.state())
}

/// Re-reads state off the round path, then lets judging resume.
///
/// The tripwire holds judging suspended until this finishes, so the node stops
/// charging misses it derived from the view under suspicion.
pub fn spawn_realign<Db, Cluster, Blockchain>(
    context: &Arc<NodeContext<Db, Cluster, Blockchain>>,
    cancel: &CancellationToken,
    delay: Duration,
    cause: RealignCause,
) where
    Db: Store + 'static,
    Cluster: Api + 'static,
    Blockchain: Rpc + 'static,
{
    context
        .challenge_counters
        .realigns
        .fetch_add(1, Ordering::Relaxed);
    warn!(
        cause = cause.label(),
        delay_ms = delay.as_millis() as u64,
        "protocol state realign triggered"
    );

    let context = context.clone();
    let cancel = cancel.clone();

    tokio::spawn(async move {
        if !delay.is_zero() {
            tokio::select! {
                _ = cancel.cancelled() => return,
                _ = tokio::time::sleep(delay) => {}
            }
        }

        let before = context.state().epoch();
        match refetch_state(&context, Some(&cancel), None).await {
            Ok(state) => warn!(
                cause = cause.label(),
                from = before.0,
                to = state.epoch().0,
                "protocol state realigned"
            ),
            Err(error) => warn!(cause = cause.label(), %error, "protocol state realign failed"),
        }

        context.epoch_digest.invalidate();
        context.challenge_tripwire.settled();
    });
}
