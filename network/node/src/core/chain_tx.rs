use std::future::Future;

use rpc::{InstructionError, RpcError, TransactionError};
use rpc_client::parse_tape_error;
use tape_api::errors::{TapeError, is_account_state_pending_error};
use tape_crypto::tx::Txid;

use crate::core::ingest::IngestBus;

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

    if let Some(kind) = classify_instruction_error(err) {
        return kind;
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

/// Classify from the structured runtime error when the RPC reported one.
/// Message matching below stays as the fallback for proxies that flatten
/// errors into text, and for conditions only visible in program logs.
fn classify_instruction_error(err: &RpcError) -> Option<TxRejectionKind> {
    let TransactionError::InstructionError(_, ix_err) = err.transaction_error()? else {
        return None;
    };

    match ix_err {
        InstructionError::AccountAlreadyInitialized => Some(TxRejectionKind::KnownContention),
        InstructionError::UninitializedAccount
        | InstructionError::InvalidAccountData
        | InstructionError::InvalidAccountOwner => Some(TxRejectionKind::KnownStaleState),
        _ => None,
    }
}

fn transaction_error_message(err: &RpcError) -> Option<&str> {
    match err {
        RpcError::Transaction { message, .. } => Some(message),
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
/// future is dropped without being polled and `SkippedStale` is returned.
/// Otherwise the future is awaited and its result classified via
/// `classify_tx`.
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
    use solana_signature::Signature;
    use std::time::Duration;

    fn test_txid(byte: u8) -> Txid {
        Signature::from([byte; 64]).into()
    }

    fn text_error(message: &str) -> RpcError {
        RpcError::Transaction {
            err: None,
            message: message.to_string(),
        }
    }

    #[test]
    fn confirmed() {
        let sig = test_txid(1);
        let outcome = classify_tx(Ok(sig));
        assert!(matches!(outcome, TxOutcome::Confirmed(_)));
    }

    #[test]
    fn program_error() {
        let err = text_error("custom program error: 0x51");
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
        let err = text_error("custom program error: 0x67");
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
        let err = text_error("unknown error 999");
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
        let err = text_error(
            "Error processing Instruction 1: instruction requires an uninitialized account",
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
    fn structured_contention_and_stale_state() {
        let contention = RpcError::Transaction {
            err: Some(TransactionError::InstructionError(
                1,
                InstructionError::AccountAlreadyInitialized,
            )),
            message: "Error processing Instruction 1: instruction requires an uninitialized account"
                .to_string(),
        };
        assert!(matches!(
            classify_rejection(&contention),
            TxRejectionKind::KnownContention
        ));

        let stale = RpcError::Transaction {
            err: Some(TransactionError::InstructionError(
                1,
                InstructionError::InvalidAccountData,
            )),
            message: "Error processing Instruction 1: invalid account data for instruction"
                .to_string(),
        };
        assert!(matches!(
            classify_rejection(&stale),
            TxRejectionKind::KnownStaleState
        ));

        let budget = RpcError::Transaction {
            err: Some(TransactionError::InstructionError(
                1,
                InstructionError::ComputationalBudgetExceeded,
            )),
            message: "Error processing Instruction 1: Computational budget exceeded".to_string(),
        };
        assert!(matches!(
            classify_rejection(&budget),
            TxRejectionKind::UnknownExecution
        ));
    }

    #[test]
    fn invalid_account_data_is_stale_state() {
        let err = text_error(
            "Error processing Instruction 1: invalid account data for instruction",
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

        let outcome = submit_if_at_tip(&bus, async {
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
        let outcome = submit_if_at_tip(&bus, async move { Ok(sig) }).await;

        assert!(matches!(outcome, TxOutcome::Confirmed(_)));
    }

    #[tokio::test]
    async fn submit_if_at_tip_classifies_program_error() {
        let bus = IngestBus::new();
        bus.publish(crate::core::ingest::IngestState::AtTip);

        let outcome = submit_if_at_tip(&bus, async {
            Err(text_error("custom program error: 0x51"))
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
