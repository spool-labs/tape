use rpc::Rpc;
use tape_api::instruction::{build_revoke_tape_delegate_ix, build_set_tape_delegate_ix};
use tape_crypto::Address;
use tape_protocol::Api;

use crate::error::TapedriveError;
use crate::keys::tape_key::TapeKey;
use crate::tapedrive::Tapedrive;

impl<Blockchain: Rpc, Cluster: Api> Tapedrive<Blockchain, Cluster> {
    /// Set the delegate allowed to write, certify, and delete tracks on a tape.
    pub async fn set_tape_delegate(
        &self,
        tape_key: &TapeKey,
        delegate: Address,
    ) -> Result<(), TapedriveError> {
        let payer = self.payer()?;
        let tape_signer = tape_key.keypair();
        let ix = build_set_tape_delegate_ix(
            payer.pubkey().into(),
            tape_key.pubkey().into(),
            tape_key.address(),
            delegate,
        );

        self.rpc()
            .send_instructions_with_signers(payer, vec![ix], &[tape_signer])
            .await?;

        Ok(())
    }

    /// Revoke the current delegate for a tape.
    pub async fn revoke_tape_delegate(&self, tape_key: &TapeKey) -> Result<(), TapedriveError> {
        let payer = self.payer()?;
        let tape_signer = tape_key.keypair();
        let ix = build_revoke_tape_delegate_ix(
            payer.pubkey().into(),
            tape_key.pubkey().into(),
            tape_key.address(),
        );

        self.rpc()
            .send_instructions_with_signers(payer, vec![ix], &[tape_signer])
            .await?;

        Ok(())
    }
}
