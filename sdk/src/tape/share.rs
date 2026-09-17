use std::time::Duration;

use tape_rpc::Rpc;
use tape_api::instruction::build_advance_share_nonce_ix;
use tape_core::share::{unix_now, ShareToken};
use tape_core::types::tls::NetworkTlsPubkey;
use tape_crypto::hash::Hash;
use tape_protocol::Api;

use crate::error::TapedriveError;
use crate::keys::operator::TapeOperator;
use crate::tapedrive::Tapedrive;

impl<Blockchain: Rpc, Cluster: Api> Tapedrive<Blockchain, Cluster> {
    /// Sign a share token letting one reader fetch one track key, or every track with `ALL_TRACKS`
    pub async fn share(
        &self,
        operator: &impl TapeOperator,
        track_key: Hash,
        reader: NetworkTlsPubkey,
        ttl: Duration,
    ) -> Result<ShareToken, TapedriveError> {
        let tape = self.get_tape(&operator.address()).await?;
        let expiry = unix_now().saturating_add(ttl.as_secs() as i64);

        Ok(ShareToken::issue(
            operator.keypair(),
            operator.address(),
            track_key,
            reader,
            tape.share_nonce,
            expiry,
        ))
    }

    /// Revoke every shared token on a tape by advancing its share nonce
    pub async fn revoke_shared_tokens(
        &self,
        operator: &impl TapeOperator,
    ) -> Result<(), TapedriveError> {
        let payer = self.payer()?;
        let ix = build_advance_share_nonce_ix(
            payer.pubkey().into(),
            operator.pubkey().into(),
            operator.address(),
        );

        self.rpc()
            .send_instructions_with_signers(payer, vec![ix], &[operator.keypair()])
            .await?;

        Ok(())
    }
}
