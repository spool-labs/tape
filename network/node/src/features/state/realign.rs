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

/// How long a realign off the round path keeps asking before giving the node back.
///
/// Bounded, unlike startup and epoch advance. Those have nothing to do until the
/// chain answers; a realigning node is suspended while it waits, so an RPC
/// outage that never clears would leave it refusing its group indefinitely. It
/// resumes instead, and the next blank run or digest report trips again on the
/// tripwire's own backoff.
const REALIGN_RETRY: RetryConfig = RetryConfig {
    base_delay: Duration::from_millis(500),
    max_delay: Duration::from_secs(5),
    max_retries: Some(10),
};

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
/// a realigned node holds what a freshly started one would. `at_least` is the
/// floor the answer has to clear: without one, an RPC lagging behind the node
/// rolls the view back an epoch, and the advance that would correct it has
/// already been read off the block stream and will not come again.
pub async fn refetch_state<Db, Cluster, Blockchain>(
    context: &Arc<NodeContext<Db, Cluster, Blockchain>>,
    cancel: Option<&CancellationToken>,
    at_least: Option<EpochNumber>,
    retry: RetryConfig,
) -> Result<Arc<ProtocolState>, NodeError>
where
    Db: Store,
    Cluster: Api,
    Blockchain: Rpc,
{
    let source = context.clone();
    let state = retry_if(
        retry,
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
/// charging misses it derived from the view under suspicion. It answers for its
/// own spools throughout: a suspended node that went quiet would earn the misses
/// it suspended itself to avoid handing out.
pub fn spawn_realign<Db, Cluster, Blockchain>(
    context: &Arc<NodeContext<Db, Cluster, Blockchain>>,
    delay: Duration,
    cause: RealignCause,
) where
    Db: Store + 'static,
    Cluster: Api + 'static,
    Blockchain: Rpc + 'static,
{
    let before = context.state().epoch();
    context
        .challenge_counters
        .realigns
        .fetch_add(1, Ordering::Relaxed);
    warn!(
        cause = cause.label(),
        epoch = before.0,
        delay_ms = delay.as_millis() as u64,
        "protocol state realign triggered"
    );

    let context = context.clone();
    let cancel = context.shutdown.clone();

    tokio::spawn(async move {
        if !delay.is_zero() {
            tokio::select! {
                _ = cancel.cancelled() => return,
                _ = tokio::time::sleep(delay) => {}
            }
        }

        // The view it is replacing is the one under suspicion, but its epoch is
        // still a floor: the chain does not go backwards, so an answer that does
        // is a lagging endpoint, not a correction.
        match refetch_state(&context, Some(&cancel), Some(before), REALIGN_RETRY).await {
            Ok(state) => warn!(
                cause = cause.label(),
                from = before.0,
                to = state.epoch().0,
                "protocol state realigned"
            ),
            Err(error) => {
                context
                    .challenge_counters
                    .realign_failures
                    .fetch_add(1, Ordering::Relaxed);
                warn!(cause = cause.label(), %error, "protocol state realign failed");
            }
        }

        context.challenge_tripwire.settled();
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::harness::{NodeHarness, TestContext};

    async fn context() -> TestContext {
        NodeHarness::builder()
            .nodes(25)
            .no_prev_snapshot_tape()
            .build()
            .await
            .expect("build harness")
            .ctx_for(0)
    }

    // an endpoint behind the node answers with the epoch it has, and adopting
    // that answer rolls the view back to a committee that no longer runs. The
    // advance that would correct it has already gone past on the block stream.
    #[tokio::test]
    async fn a_lagging_answer_never_replaces_a_later_view() {
        let ctx = context().await;
        let ahead = ctx.state().epoch().next().next();

        let mut state = (*ctx.state()).clone();
        state.current.epoch.id = ahead;
        ctx.set_state(state).expect("publish");

        let refused = refetch_state(&ctx, None, Some(ahead), RetryConfig::none()).await;

        assert!(
            matches!(refused, Err(NodeError::StateUnavailable { expected_epoch }) if expected_epoch == ahead),
            "an answer below the floor was accepted"
        );
        assert_eq!(ctx.state().epoch(), ahead, "the view was rolled back");
    }

    // without the floor the same answer is adopted, which is the shape the
    // guard exists to refuse
    #[tokio::test]
    async fn no_floor_adopts_whatever_the_endpoint_has() {
        let ctx = context().await;
        let chain = ctx.state().epoch();
        let ahead = chain.next().next();

        let mut state = (*ctx.state()).clone();
        state.current.epoch.id = ahead;
        ctx.set_state(state).expect("publish");

        refetch_state(&ctx, None, None, RetryConfig::none())
            .await
            .expect("fetched");

        assert_eq!(ctx.state().epoch(), chain);
    }
}
