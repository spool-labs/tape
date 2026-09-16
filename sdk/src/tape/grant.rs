use std::time::{Duration, SystemTime, UNIX_EPOCH};

use rpc::Rpc;
use tape_api::instruction::build_bump_grant_generation_ix;
use tape_core::grant::{ReadGrant, WILDCARD_TRACK_KEY};
use tape_core::types::tls::NetworkTlsPubkey;
use tape_crypto::hash::Hash;
use tape_protocol::Api;

use crate::error::TapedriveError;
use crate::keys::operator::TapeOperator;
use crate::tapedrive::Tapedrive;

impl<Blockchain: Rpc, Cluster: Api> Tapedrive<Blockchain, Cluster> {
    /// Sign a read grant for one reader on one track key.
    ///
    /// The tape's current generation is read on-chain so the grant is valid
    /// until it expires or the owner revokes.
    pub async fn issue_read_grant(
        &self,
        operator: &impl TapeOperator,
        key_hash: Hash,
        reader: NetworkTlsPubkey,
        ttl: Duration,
    ) -> Result<ReadGrant, TapedriveError> {
        let tape = self.get_tape(&operator.address()).await?;
        let expiry = now_secs().saturating_add(ttl.as_secs() as i64);

        Ok(operator.issue_read_grant(key_hash, reader, tape.grant_generation, expiry))
    }

    /// Sign a read grant for one reader on every track the tape holds.
    ///
    /// The wildcard key is what widens it; everything else a node checks is
    /// unchanged, revocation included.
    pub async fn issue_tape_read_grant(
        &self,
        operator: &impl TapeOperator,
        reader: NetworkTlsPubkey,
        ttl: Duration,
    ) -> Result<ReadGrant, TapedriveError> {
        self.issue_read_grant(operator, WILDCARD_TRACK_KEY, reader, ttl)
            .await
    }

    /// Invalidate every grant outstanding on a tape.
    pub async fn bump_grant_generation(
        &self,
        operator: &impl TapeOperator,
    ) -> Result<(), TapedriveError> {
        let payer = self.payer()?;
        let signer = operator.keypair();
        let ix = build_bump_grant_generation_ix(
            payer.pubkey().into(),
            operator.pubkey().into(),
            operator.address(),
        );

        self.rpc()
            .send_instructions_with_signers(payer, vec![ix], &[signer])
            .await?;

        Ok(())
    }
}

fn now_secs() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_secs() as i64)
        .unwrap_or_default()
}
