use std::time::Duration;

use tape_rpc::Rpc;
use tape_rpc_client::parse_tape_error;
use tape_api::errors::TapeError;
use tape_api::instruction::build_delete_track_ix;
use tape_api::program::tapedrive::TrackAddress;
use tape_core::types::TrackNumber;
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
        self.delete_as(tape_key, track, None).await
    }

    /// Delete a track as an arbitrary TapeOperator
    ///
    /// Tracks written with the same content share an address. A track number
    /// picks one of them. Without it every track under the address goes, one
    /// transaction each, since a delete proves one leaf.
    ///
    /// A delete that follows a recent mutation of the same tape sees stale
    /// proofs until peers catch up with the chain, so each fetch-proof and
    /// submit attempt retries until visibility recovers.
    pub async fn delete_as(
        &self,
        operator: &impl TapeOperator,
        track: Address,
        track_number: Option<TrackNumber>,
    ) -> Result<(), TapedriveError> {
        let numbers = match track_number {
            Some(track_number) => vec![track_number],
            None => {
                let tracks = query::query_tracks(self, &track).await?;
                tracks.iter().map(|info| info.track_number).collect()
            }
        };

        for track_number in numbers {
            retry_if(
                delete_retry_config(),
                None,
                || self.delete_once(operator, track, track_number),
                should_retry_delete,
            )
            .await?;
        }
        Ok(())
    }

    async fn delete_once(
        &self,
        operator: &impl TapeOperator,
        track: Address,
        track_number: TrackNumber,
    ) -> Result<(), TapedriveError> {
        let payer = self.payer()?;
        let tape_signer = operator.keypair();
        let proof = query::query_track_proof(self, &operator.address(), track_number).await?;
        // The number says where to look, and the address says what must be there.
        if proof.state.address() != track {
            return Err(TapedriveError::NotFound);
        }
        let ix = build_delete_track_ix(payer.pubkey().into(), operator.pubkey().into(), proof);

        self.rpc()
            .send_instructions_with_signers(payer, vec![ix], &[tape_signer])
            .await?;

        Ok(())
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
