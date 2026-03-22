use rpc::RpcError;
use rpc_client::parse_tape_error;
use solana_sdk::signature::Signature;
use tape_api::errors::TapeError;
use tape_retry::Backoff;
use tokio_util::sync::CancellationToken;

/// Outcome of a Solana transaction submission attempt.
///
/// Each epoch lifecycle task submits a transaction, then matches on this
/// to decide whether to retry, return success, or return rejected.
///
/// Transport errors are always retriable (timeout, connection, blockhash).
/// Program errors require per-task classification — some mean "already done"
/// (AlreadySynced, AlreadyAdvanced), some mean "retry later" (TooSoon),
/// and some mean "rejected" (NotInCommittee, BadSchedule).
pub enum TxOutcome {
    /// Transaction confirmed on chain.
    Confirmed(Signature),
    /// On-chain program returned a typed TapeError.
    Program(TapeError),
    /// Transport/RPC error (timeout, connection, blockhash expired, etc.)
    /// These are always retriable.
    Transport(RpcError),
}

/// Classify the result of `rpc.send_instructions()`.
///
/// Parses program errors from the RPC error string. If no program error
/// is found, the error is treated as a transport-level issue.
pub fn classify_tx(result: Result<Signature, RpcError>) -> TxOutcome {
    match result {
        Ok(sig) => TxOutcome::Confirmed(sig),
        Err(err) => match parse_tape_error(&err) {
            Some(tape_err) => TxOutcome::Program(tape_err),
            None => TxOutcome::Transport(err),
        },
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn confirmed() {
        let sig = Signature::new_unique();
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
}
