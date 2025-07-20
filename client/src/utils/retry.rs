use anyhow::{anyhow, Result};
use log::{debug, error};
use solana_client::{
    client_error::{ClientErrorKind, Result as ClientResult},
    rpc_response::RpcSimulateTransactionResult,
};

use solana_sdk::signature::Signature;
use tokio::time::Duration;

const MAX_RETRIES: u32 = 8;
const INITIAL_BACKOFF: u64 = 200;
const TIMEOUT: Duration = Duration::from_secs(8);

/// Generic retry function for asynchronous operations with exponential backoff.
pub async fn retry<F, Fut, T>(f: F) -> Result<T, anyhow::Error>
where
    F: Fn() -> Fut,
    Fut: std::future::Future<Output = Result<T, anyhow::Error>>,
{
    let mut backoff = Duration::from_millis(INITIAL_BACKOFF);

    for attempt in 0..MAX_RETRIES {
        match tokio::time::timeout(TIMEOUT, f()).await {
            Ok(Ok(result)) => {
                return Ok(result);
            }
            Ok(Err(e)) if attempt == MAX_RETRIES - 1 => {
                error!("Attempt {} failed with error: {:?}", attempt + 1, e);
                return Err(e);
            }
            Err(_) if attempt == MAX_RETRIES - 1 => {
                error!("Attempt {} timed out after {:?}", attempt + 1, TIMEOUT);
                return Err(anyhow::anyhow!("Retry failed"));
            }
            _ => {
                error!("Attempt {} failed, retrying after backoff", attempt + 1);
                debug!("Waiting for backoff: {:?}", backoff);

                tokio::time::sleep(backoff).await;
                backoff *= 2; // Exponential backoff
            }
        }
    }

    Err(anyhow::anyhow!("All retry attempts failed"))
}

/// Handles transaction simulation logs for failed transactions.
pub fn with_logs(res: ClientResult<Signature>) -> Result<Signature> {
    match res {
        Ok(signature) => Ok(signature),
        Err(e) => {
            if let ClientErrorKind::RpcError(
                solana_client::rpc_request::RpcError::RpcResponseError { data, .. },
            ) = e.kind()
            {
                if let solana_client::rpc_request::RpcResponseErrorData::SendTransactionPreflightFailure(
                    RpcSimulateTransactionResult { logs: Some(logs), .. }
                ) = data {
                    eprintln!("Transaction simulation failed:");
                    for log in logs {
                        eprintln!("  {}", log);
                    }
                }
            }
            Err(anyhow!("Transaction failed: {}", e))
        }
    }
}
