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
use crate::features::challenge::certify::certify_banked;
use crate::features::challenge::tripwire::Tripwire;

/// How long a realign off the round path keeps asking before giving the node back.
///
/// Bounded, unlike startup and epoch advance. Those have nothing to do until the
/// chain answers; a realigning node is suspended while it waits, so an RPC
/// outage that never clears would leave it refusing its group indefinitely. It
/// resumes instead, and the next blank run trips again on the tripwire's own
/// backoff.
const REALIGN_RETRY: RetryConfig = RetryConfig {
    base_delay: Duration::from_millis(500),
    max_delay: Duration::from_secs(5),
    max_retries: Some(10),
};

/// The epoch an answer has to reach before it is adopted.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EpochFloor {
    /// Whatever the endpoint has. Startup has nothing to compare against.
    Any,
    /// A named epoch, for an advance the node has already seen announced.
    Fixed(EpochNumber),
    /// Whatever the node holds when the answer lands.
    ///
    /// Read at fetch time, not when the realign was scheduled: a trip can sleep
    /// out a five minute backoff, and an epoch captured before that sleep is a
    /// floor the node has long since climbed past.
    Held,
}

/// What sent the node back to the chain for a fresh view.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RealignCause {
    /// Consecutive rounds in which nothing this node weighed stood.
    Tripwire,
}

impl RealignCause {
    pub fn label(self) -> &'static str {
        match self {
            RealignCause::Tripwire => "tripwire",
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
    floor: EpochFloor,
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
                match floor_epoch(&context, floor) {
                    Some(epoch) if state.epoch() < epoch => {
                        Err(NodeError::StateUnavailable { expected_epoch: epoch })
                    }
                    Some(_) | None => Ok(state),
                }
            }
        },
        |error| is_retriable(error, floor),
    )
    .await?;

    context.set_state(state)?;
    if let Err(error) = context.refresh_peers().await {
        warn!(%error, "peer refresh failed after realign");
    }

    Ok(context.state())
}

/// The floor to hold this answer to, read now rather than when it was scheduled.
fn floor_epoch<Db: Store, Cluster: Api, Blockchain: Rpc>(
    context: &NodeContext<Db, Cluster, Blockchain>,
    floor: EpochFloor,
) -> Option<EpochNumber> {
    match floor {
        EpochFloor::Any => None,
        EpochFloor::Fixed(epoch) => Some(epoch),
        EpochFloor::Held => Some(context.state().epoch()),
    }
}

/// Whether another attempt could do better than this one did.
fn is_retriable(error: &NodeError, floor: EpochFloor) -> bool {
    if let NodeError::Rpc(rpc) = error {
        return rpc.is_retriable() && !rpc.is_skipped_slot();
    }

    // A floor the answer fell short of is the endpoint lagging, which the next
    // attempt may not.
    matches!(error, NodeError::StateUnavailable { .. }) && floor != EpochFloor::Any
}

/// Releases the suspension however the realign ends, panic included.
///
/// A task that died holding it would leave the node refusing its group for the
/// rest of the process's life.
struct Suspension {
    tripwire: Arc<Tripwire>,
}

impl Drop for Suspension {
    fn drop(&mut self) {
        self.tripwire.settled();
    }
}

/// Re-reads state off the round path, then lets settlement resume.
///
/// The tripwire holds settlement suspended until this finishes, so the node stops
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
    let cancel = context.shutdown();

    tokio::spawn(async move {
        let _suspension = Suspension {
            tripwire: context.challenge_tripwire.clone(),
        };

        if !delay.is_zero() {
            tokio::select! {
                _ = cancel.cancelled() => return,
                _ = tokio::time::sleep(delay) => {}
            }
        }

        match refetch_state(&context, Some(&cancel), EpochFloor::Held, REALIGN_RETRY).await {
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

        // Certification is edge-triggered on the arriving attestation, so a
        // quorum that filled while settlement was off is claimed by nothing else.
        // Swept before the suspension lifts, so the round path finds the record
        // already written rather than settling a miss over it.
        certify_banked(&context);
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
    async fn lagging_answer_refused() {
        let ctx = context().await;
        let ahead = ctx.state().epoch().next().next();

        let mut state = (*ctx.state()).clone();
        state.current.epoch.id = ahead;
        ctx.set_state(state).expect("publish");

        let refused = refetch_state(&ctx, None, EpochFloor::Held, RetryConfig::none()).await;

        assert!(
            matches!(refused, Err(NodeError::StateUnavailable { expected_epoch }) if expected_epoch == ahead),
            "an answer below the floor was accepted"
        );
        assert_eq!(ctx.state().epoch(), ahead, "the view was rolled back");
    }

    // the held floor is whatever the node has when the answer lands, so an
    // epoch it climbed to during a backoff is still a floor
    #[tokio::test]
    async fn held_floor_moves_with_the_node() {
        let ctx = context().await;
        let chain = ctx.state().epoch();

        // At the chain's own epoch the floor is met, so the answer is adopted.
        refetch_state(&ctx, None, EpochFloor::Held, RetryConfig::none())
            .await
            .expect("fetched");
        assert_eq!(ctx.state().epoch(), chain);

        // Move the node on, and the same answer no longer clears it.
        let mut state = (*ctx.state()).clone();
        state.current.epoch.id = chain.next();
        ctx.set_state(state).expect("publish");
        assert!(
            refetch_state(&ctx, None, EpochFloor::Held, RetryConfig::none())
                .await
                .is_err()
        );
    }

    // without a floor the same answer is adopted, which is the shape the guard
    // exists to refuse
    #[tokio::test]
    async fn no_floor_takes_anything() {
        let ctx = context().await;
        let chain = ctx.state().epoch();
        let ahead = chain.next().next();

        let mut state = (*ctx.state()).clone();
        state.current.epoch.id = ahead;
        ctx.set_state(state).expect("publish");

        refetch_state(&ctx, None, EpochFloor::Any, RetryConfig::none())
            .await
            .expect("fetched");

        assert_eq!(ctx.state().epoch(), chain);
    }

    // a realign task that dies must not leave the node suspended for the rest
    // of the process
    #[tokio::test]
    async fn suspension_releases_on_panic() {
        let ctx = context().await;
        assert!(ctx.challenge_tripwire.trip().is_some());
        assert!(ctx.challenge_tripwire.is_realigning());

        let tripwire = ctx.challenge_tripwire.clone();
        let died = tokio::spawn(async move {
            let _suspension = Suspension { tripwire };
            panic!("realign died");
        });

        assert!(died.await.is_err(), "the task was meant to panic");
        assert!(!ctx.challenge_tripwire.is_realigning());
    }
}
