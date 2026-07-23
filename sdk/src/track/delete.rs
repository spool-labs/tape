use std::time::Duration;

use rpc::Rpc;
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

    /// Delete a concrete track version as an arbitrary TapeOperator.
    ///
    /// The proof is fetched from peers and verified against the current tape
    /// root, which can be transiently stale right after another write to the
    /// same tape (an overwrite reclaim), so a stale proof is refetched and
    /// retried before giving up.
    pub async fn delete_as(
        &self,
        operator: &impl TapeOperator,
        track: Address,
    ) -> Result<(), TapedriveError> {
        retry_if(
            RetryConfig {
                base_delay: Duration::from_millis(300),
                max_delay: Duration::from_secs(2),
                max_retries: Some(5),
            },
            None,
            || self.delete_once(operator, track),
            should_retry_delete,
        )
        .await
    }

    async fn delete_once(
        &self,
        operator: &impl TapeOperator,
        track: Address,
    ) -> Result<(), TapedriveError> {
        let payer = self.payer()?;
        let tape_signer = operator.keypair();
        let proof = query::query_track_proof(self, &track).await?;
        let ix = build_delete_track_ix(payer.pubkey().into(), operator.pubkey().into(), proof);

        self.rpc()
            .send_instructions_with_signers(payer, vec![ix], &[tape_signer])
            .await?;

        Ok(())
    }
}

/// A missing track is final and surfaces to the caller; a stale proof or a
/// transient peer/RPC error is retried.
fn should_retry_delete(error: &TapedriveError) -> bool {
    match error {
        TapedriveError::NotFound => false,
        TapedriveError::Peer(api) => api.is_retryable(),
        TapedriveError::Rpc(rpc) => rpc.is_retriable(),
        TapedriveError::Network(_) => true,
        _ => false,
    }
}
