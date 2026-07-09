use std::future::Future;
use std::time::Duration;

use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use rpc::RpcError;
use rpc_client::parse_tape_error;
use tape_api::errors::{TapeError, is_account_state_pending_error};
use tape_crypto::tx::Txid;
use tape_retry::{Backoff, backoff_or_cancel};

use crate::core::ingest::IngestBus;

/// Per-rank delay before a committee member submits a contended any-member
/// transaction, so lower ranks submit first and higher ranks observe the result.
const STAGGER_STEP: Duration = Duration::from_millis(400);

/// Rank cap on the stagger, so a large committee cannot push high ranks past the
/// whole submission window.
const STAGGER_MAX_RANK: usize = 8;

/// Block until the next state update or cancellation, for retries whose
/// precondition only flips when a new block is ingested. Returns true when the
/// task should stop (cancelled, or the state channel closed).
pub async fn wait_for_state_change<State>(
    state_rx: &mut watch::Receiver<State>,
    cancel: &CancellationToken,
) -> bool {
    // Both branches are cancellation-safe: neither leaves partial state behind.
    tokio::select! {
        _ = cancel.cancelled() => true,
        changed = state_rx.changed() => changed.is_err(),
    }
}

/// Wait out this node's rank-ordered stagger before submitting a contended
/// transaction. Rank 0 returns immediately. Returns true when the task should
/// stop (cancelled).
pub async fn stagger_by_rank(rank: usize, cancel: &CancellationToken) -> bool {
    if rank == 0 {
        return false;
    }
    let delay = STAGGER_STEP * rank.min(STAGGER_MAX_RANK) as u32;
    // Both branches are cancellation-safe.
    tokio::select! {
        _ = cancel.cancelled() => true,
        _ = tokio::time::sleep(delay) => false,
    }
}

/// Spawn a detached consensus submit into the given slot, unless a submit of
/// that kind is still in flight. A finished handle is left in place and the next
/// call overwrites it, so a failed submit is naturally re-driven on the next
/// block or heartbeat. This keeps the stagger sleep inside the submit off the
/// manager event loop while still deduping the per-block and per-heartbeat re-fire.
pub fn spawn_guarded<F>(slot: &mut Option<JoinHandle<()>>, task: F)
where
    F: Future<Output = ()> + Send + 'static,
{
    if slot.as_ref().is_some_and(|handle| !handle.is_finished()) {
        return;
    }
    *slot = Some(tokio::spawn(task));
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TxRejectionKind {
    /// A transaction raced with local state propagation and should be retried
    /// after the node observes the newer on-chain state.
    KnownStaleState,
    /// Another node already created or finalized the same on-chain state.
    KnownContention,
    /// On-chain program returned a typed TapeError.
    Program(TapeError),
    /// Transaction execution failed, but the error was not recognized.
    UnknownExecution,
    /// RPC/transport failure before transaction execution completed.
    Transport,
}

/// Outcome of a Solana transaction submission attempt.
pub enum TxOutcome {
    /// Transaction confirmed on chain.
    Confirmed(Txid),
    /// Transaction was rejected or failed before confirmation.
    Rejected {
        kind: TxRejectionKind,
        err: RpcError,
    },
    /// Submit was refused because the local block ingestor is not caught up
    /// to the finalized dispatch edge. The submit future was never polled.
    SkippedStale,
}

/// Classify the result of `rpc.send_instructions()`.
///
/// Parses program errors from the RPC error string, and separates expected
/// contention/stale-state rejections from true transport failures.
pub fn classify_tx(result: Result<Txid, RpcError>) -> TxOutcome {
    match result {
        Ok(sig) => TxOutcome::Confirmed(sig),
        Err(err) => TxOutcome::Rejected {
            kind: classify_rejection(&err),
            err,
        },
    }
}

pub fn classify_rejection(err: &RpcError) -> TxRejectionKind {
    if let Some(tape_err) = parse_tape_error(err) {
        return TxRejectionKind::Program(tape_err);
    }

    let Some(msg) = transaction_error_message(err) else {
        return TxRejectionKind::Transport;
    };

    if is_known_contention_error(msg) {
        return TxRejectionKind::KnownContention;
    }

    if is_known_stale_state_error(msg) || is_account_state_pending_error(msg) {
        return TxRejectionKind::KnownStaleState;
    }

    TxRejectionKind::UnknownExecution
}

fn transaction_error_message(err: &RpcError) -> Option<&str> {
    match err {
        RpcError::Transaction(msg) => Some(msg),
        RpcError::Request(msg) if looks_like_transaction_error(msg) => Some(msg),
        _ => None,
    }
}

fn looks_like_transaction_error(msg: &str) -> bool {
    msg.contains("Error processing Instruction")
        || msg.contains("InstructionError")
        || msg.contains("custom program error")
}

fn is_known_contention_error(msg: &str) -> bool {
    let msg = msg.to_ascii_lowercase();
    msg.contains("accountalreadyinitialized")
        || msg.contains("already initialized")
        || msg.contains("requires an uninitialized account")
}

fn is_known_stale_state_error(msg: &str) -> bool {
    let msg = msg.to_ascii_lowercase();
    msg.contains("account has invalid address")
        || msg.contains("invalid account data")
        || msg.contains("invalid account owner")
        || msg.contains("accountnotinitialized")
        || msg.contains("could not find account")
}

/// Funnel every protocol-changing transaction through here. If the
/// ingestor is not caught up to the finalized dispatch edge, the submit
/// future is dropped without being polled and the outcome is skipped-stale.
/// Otherwise the future is awaited and its result classified. The action label
/// makes the burn observable per submitter and outcome.
pub async fn submit_if_at_tip<F>(ingest: &IngestBus, action: &'static str, submit: F) -> TxOutcome
where
    F: Future<Output = Result<Txid, RpcError>>,
{
    let outcome = if ingest.is_at_tip() {
        classify_tx(submit.await)
    } else {
        TxOutcome::SkippedStale
    };
    record_lifecycle_tx(action, outcome.metric_label());
    outcome
}

/// How a looping task should pace its next retry after a rejection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetryPace {
    /// The precondition flips on a specific new block; wait for the next state change.
    StateChange,
    /// A transient condition (stale view, contention, transport) that clears over
    /// several blocks with no single observable signal; back off rather than fire
    /// on every block.
    Backoff,
}

impl TxOutcome {
    /// Prometheus outcome label for this submission result.
    fn metric_label(&self) -> &'static str {
        match self {
            TxOutcome::Confirmed(_) => "confirmed",
            TxOutcome::SkippedStale => "skipped_stale",
            TxOutcome::Rejected { kind, .. } => match kind {
                TxRejectionKind::KnownContention => "contention",
                TxRejectionKind::KnownStaleState => "stale",
                TxRejectionKind::Program(_) => "program_error",
                TxRejectionKind::UnknownExecution => "unknown",
                TxRejectionKind::Transport => "transport",
            },
        }
    }

    /// Retry pacing for this outcome. A program error flips on a specific,
    /// observable state change, so it waits on state; every other rejection is a
    /// transient view/contention condition that clears over several blocks, so it
    /// backs off instead of resubmitting on every block.
    pub fn retry_pace(&self) -> RetryPace {
        match self {
            TxOutcome::Rejected {
                kind: TxRejectionKind::Program(_),
                ..
            } => RetryPace::StateChange,
            _ => RetryPace::Backoff,
        }
    }
}

/// Wait for the next retry according to the chosen pace, or cancellation.
/// Returns true when the task should stop (cancelled, or the state channel closed).
pub async fn wait_by_pace<State>(
    pace: RetryPace,
    backoff: &mut Backoff,
    state_rx: &mut watch::Receiver<State>,
    cancel: &CancellationToken,
) -> bool {
    match pace {
        RetryPace::StateChange => wait_for_state_change(state_rx, cancel).await,
        RetryPace::Backoff => backoff_or_cancel(backoff, cancel).await,
    }
}

/// Count one lifecycle or consensus transaction submission by action and outcome.
#[cfg(feature = "metrics")]
fn record_lifecycle_tx(action: &str, outcome: &str) {
    if let Some(counter) = lifecycle_tx_counter() {
        counter.with_label_values(&[action, outcome]).inc();
    }
}

#[cfg(not(feature = "metrics"))]
fn record_lifecycle_tx(_action: &str, _outcome: &str) {}

/// Lazily build and register the lifecycle transaction counter in the default
/// registry. None if a counter of the same name is already registered, in which
/// case that registration is authoritative.
#[cfg(feature = "metrics")]
fn lifecycle_tx_counter() -> Option<&'static tape_metrics::IntCounterVec> {
    use std::sync::OnceLock;
    static COUNTER: OnceLock<Option<tape_metrics::IntCounterVec>> = OnceLock::new();
    COUNTER
        .get_or_init(|| {
            let counter = tape_metrics::IntCounterVec::new(
                tape_metrics::prometheus::Opts::new(
                    "tape_node_lifecycle_tx_total",
                    "Lifecycle and consensus transaction submissions by action and outcome",
                ),
                &["action", "outcome"],
            )
            .ok()?;
            tape_metrics::prometheus::default_registry()
                .register(Box::new(counter.clone()))
                .ok()?;
            Some(counter)
        })
        .as_ref()
}


#[cfg(test)]
mod tests {
    use super::*;
    use solana_signature::Signature;
    use std::time::Duration;

    fn test_txid(byte: u8) -> Txid {
        Signature::from([byte; 64]).into()
    }

    #[test]
    fn confirmed() {
        let sig = test_txid(1);
        let outcome = classify_tx(Ok(sig));
        assert!(matches!(outcome, TxOutcome::Confirmed(_)));
    }

    #[test]
    fn program_error() {
        let err = RpcError::Transaction("custom program error: 0x51".to_string());
        let outcome = classify_tx(Err(err));
        assert!(matches!(
            outcome,
            TxOutcome::Rejected {
                kind: TxRejectionKind::Program(TapeError::AlreadySynced),
                ..
            }
        ));
    }

    #[test]
    fn pool_accounting_failed_program_error() {
        let err = RpcError::Transaction("custom program error: 0x67".to_string());
        let outcome = classify_tx(Err(err));
        assert!(matches!(
            outcome,
            TxOutcome::Rejected {
                kind: TxRejectionKind::Program(TapeError::PoolAccountingFailed),
                ..
            }
        ));
    }

    #[test]
    fn transport_error() {
        let err = RpcError::Timeout(Duration::from_secs(5));
        let outcome = classify_tx(Err(err));
        assert!(matches!(
            outcome,
            TxOutcome::Rejected {
                kind: TxRejectionKind::Transport,
                ..
            }
        ));
    }

    #[test]
    fn unparseable_tx_error() {
        let err = RpcError::Transaction("unknown error 999".to_string());
        let outcome = classify_tx(Err(err));
        assert!(matches!(
            outcome,
            TxOutcome::Rejected {
                kind: TxRejectionKind::UnknownExecution,
                ..
            }
        ));
    }

    #[test]
    fn account_already_initialized_is_contention() {
        let err = RpcError::Transaction(
            "Error processing Instruction 1: instruction requires an uninitialized account"
                .to_string(),
        );
        let outcome = classify_tx(Err(err));
        assert!(matches!(
            outcome,
            TxOutcome::Rejected {
                kind: TxRejectionKind::KnownContention,
                ..
            }
        ));
    }

    #[test]
    fn invalid_account_data_is_stale_state() {
        let err = RpcError::Transaction(
            "Error processing Instruction 1: invalid account data for instruction".to_string(),
        );
        let outcome = classify_tx(Err(err));
        assert!(matches!(
            outcome,
            TxOutcome::Rejected {
                kind: TxRejectionKind::KnownStaleState,
                ..
            }
        ));
    }

    #[tokio::test]
    async fn submit_if_at_tip_short_circuits_when_catching() {
        let bus = IngestBus::new();
        let polled = std::sync::atomic::AtomicBool::new(false);

        let outcome = submit_if_at_tip(&bus, "test", async {
            polled.store(true, std::sync::atomic::Ordering::Relaxed);
            Ok(test_txid(2))
        })
        .await;

        assert!(matches!(outcome, TxOutcome::SkippedStale));
        assert!(!polled.load(std::sync::atomic::Ordering::Relaxed));
    }

    #[tokio::test]
    async fn submit_if_at_tip_passes_through_at_tip() {
        let bus = IngestBus::new();
        bus.publish(crate::core::ingest::IngestState::AtTip);

        let sig = test_txid(3);
        let outcome = submit_if_at_tip(&bus, "test", async move { Ok(sig) }).await;

        assert!(matches!(outcome, TxOutcome::Confirmed(_)));
    }

    #[tokio::test]
    async fn submit_if_at_tip_classifies_program_error() {
        let bus = IngestBus::new();
        bus.publish(crate::core::ingest::IngestState::AtTip);

        let outcome = submit_if_at_tip(&bus, "test", async {
            Err(RpcError::Transaction("custom program error: 0x51".to_string()))
        })
        .await;

        assert!(matches!(
            outcome,
            TxOutcome::Rejected {
                kind: TxRejectionKind::Program(TapeError::AlreadySynced),
                ..
            }
        ));
    }
}
