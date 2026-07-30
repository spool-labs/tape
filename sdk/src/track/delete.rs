use std::time::Duration;

use rpc::Rpc;
use rpc_client::parse_tape_error;
use tape_api::errors::TapeError;
use tape_api::instruction::build_delete_track_ix;
use tape_crypto::address::Address;
use tape_protocol::Api;
use tape_retry::{retry_if, RetryConfig, Retryable};

use crate::error::TapedriveError;
use crate::keys::operator::TapeOperator;
use crate::keys::tape_key::TapeKey;
use crate::tapedrive::Tapedrive;
use crate::track::query;

impl<Blockchain: Rpc, Cluster: Api> Tapedrive<Blockchain, Cluster> {
    /// Delete a concrete track version and free its capacity on the tape.
    pub async fn delete(&self, tape_key: &TapeKey, track: Address) -> Result<(), TapedriveError> {
        self.delete_as(tape_key, track).await
    }

    /// Delete a concrete track version as an arbitrary TapeOperator
    ///
    /// A delete that follows a recent mutation of the same tape sees stale
    /// proofs until peers catch up with the chain, so the whole fetch-proof
    /// and submit attempt retries until visibility recovers.
    pub async fn delete_as(
        &self,
        operator: &impl TapeOperator,
        track: Address,
    ) -> Result<(), TapedriveError> {
        retry_if(
            delete_retry_config(),
            None,
            || async {
                let payer = self.payer()?;
                let tape_signer = operator.keypair();
                let proof = query::query_track_proof(self, &track).await?;
                let ix =
                    build_delete_track_ix(payer.pubkey().into(), operator.pubkey().into(), proof);

                self.rpc()
                    .send_instructions_with_signers(payer, vec![ix], &[tape_signer])
                    .await?;

                Ok(())
            },
            should_retry_delete,
        )
        .await
    }
}

/// A delete retries while proof visibility lags the chain or the transaction
/// loses a proof race with another mutation of the same tape
fn should_retry_delete(err: &TapedriveError) -> bool {
    match err {
        TapedriveError::Peer(err) => err.is_retryable(),
        TapedriveError::RateLimited { .. } => true,
        TapedriveError::Rpc(rpc) => {
            matches!(parse_tape_error(rpc), Some(TapeError::BadProof)) || rpc.is_retriable()
        }
        _ => false,
    }
}

fn delete_retry_config() -> RetryConfig {
    RetryConfig {
        base_delay: Duration::from_millis(250),
        max_delay: Duration::from_secs(2),
        max_retries: Some(20),
    }
}

#[cfg(test)]
mod tests {
    use tape_protocol::api::ApiError;

    use super::should_retry_delete;
    use crate::error::TapedriveError;

    // stale proof visibility retries; a genuinely missing track does not
    #[test]
    fn retry_predicate() {
        assert!(should_retry_delete(&TapedriveError::Peer(
            ApiError::StaleTrackProof
        )));
        assert!(!should_retry_delete(&TapedriveError::NotFound));
    }
}
