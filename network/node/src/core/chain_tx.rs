use std::future::Future;

use rpc::RpcError;
use rpc_client::parse_tape_error;
use tape_api::errors::TapeError;
use tape_crypto::tx::Txid;

use crate::core::ingest::IngestBus;

/// Outcome of a Solana transaction submission attempt.
pub enum TxOutcome {
    /// Transaction confirmed on chain.
    Confirmed(Txid),
    /// On-chain program returned a typed TapeError.
    Program(TapeError),
    /// Transport/RPC error (timeout, connection, blockhash expired, etc.)
    /// These are always retriable.
    Transport(RpcError),
    /// Submit was refused because the local block ingestor is not at the
    /// live edge. The submit future was never polled.
    SkippedStale,
}

/// Classify the result of `rpc.send_instructions()`.
///
/// Parses program errors from the RPC error string. If no program error
/// is found, the error is treated as a transport-level issue.
pub fn classify_tx(result: Result<Txid, RpcError>) -> TxOutcome {
    match result {
        Ok(sig) => TxOutcome::Confirmed(sig),
        Err(err) => match parse_tape_error(&err) {
            Some(tape_err) => TxOutcome::Program(tape_err),
            None => TxOutcome::Transport(err),
        },
    }
}

/// Funnel every protocol-changing transaction through here. If the
/// ingestor is not at the live edge, the submit future is dropped without
/// being polled and `SkippedStale` is returned. Otherwise the future is
/// awaited and its result classified via `classify_tx`.
pub async fn submit_if_at_tip<F>(ingest: &IngestBus, submit: F) -> TxOutcome
where
    F: Future<Output = Result<Txid, RpcError>>,
{
    if !ingest.is_at_tip() {
        return TxOutcome::SkippedStale;
    }
    classify_tx(submit.await)
}


#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use solana_sdk::signature::Signature;

    #[test]
    fn confirmed() {
        let sig: Txid = Signature::new_unique().into();
        let outcome = classify_tx(Ok(sig));
        assert!(matches!(outcome, TxOutcome::Confirmed(_)));
    }

    #[test]
    fn program_error() {
        let err = RpcError::Transaction("custom program error: 0x51".to_string());
        let outcome = classify_tx(Err(err));
        assert!(matches!(outcome, TxOutcome::Program(TapeError::AlreadySynced)));
    }

    #[test]
    fn pool_accounting_failed_program_error() {
        let err = RpcError::Transaction("custom program error: 0x67".to_string());
        let outcome = classify_tx(Err(err));
        assert!(matches!(outcome, TxOutcome::Program(TapeError::PoolAccountingFailed)));
    }

    #[test]
    fn transport_error() {
        let err = RpcError::Timeout(Duration::from_secs(5));
        let outcome = classify_tx(Err(err));
        assert!(matches!(outcome, TxOutcome::Transport(_)));
    }

    #[test]
    fn unparseable_tx_error() {
        let err = RpcError::Transaction("unknown error 999".to_string());
        let outcome = classify_tx(Err(err));
        assert!(matches!(outcome, TxOutcome::Transport(_)));
    }

    #[tokio::test]
    async fn submit_if_at_tip_short_circuits_when_catching() {
        let bus = IngestBus::new();
        let polled = std::sync::atomic::AtomicBool::new(false);

        let outcome = submit_if_at_tip(&bus, async {
            polled.store(true, std::sync::atomic::Ordering::Relaxed);
            Ok(Signature::new_unique().into())
        })
        .await;

        assert!(matches!(outcome, TxOutcome::SkippedStale));
        assert!(!polled.load(std::sync::atomic::Ordering::Relaxed));
    }

    #[tokio::test]
    async fn submit_if_at_tip_passes_through_at_tip() {
        let bus = IngestBus::new();
        bus.publish(crate::core::ingest::IngestState::AtTip);

        let sig: Txid = Signature::new_unique().into();
        let outcome = submit_if_at_tip(&bus, async move { Ok(sig) }).await;

        assert!(matches!(outcome, TxOutcome::Confirmed(_)));
    }

    #[tokio::test]
    async fn submit_if_at_tip_classifies_program_error() {
        let bus = IngestBus::new();
        bus.publish(crate::core::ingest::IngestState::AtTip);

        let outcome = submit_if_at_tip(&bus, async {
            Err(RpcError::Transaction("custom program error: 0x51".to_string()))
        })
        .await;

        assert!(matches!(outcome, TxOutcome::Program(TapeError::AlreadySynced)));
    }
}
