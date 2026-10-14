use tape_api::instruction::build_close_tape_ix;
use tape_protocol::Api;
use tape_rpc::Rpc;

use crate::error::TapedriveError;
use crate::keys::tape_key::TapeKey;
use crate::tapedrive::Tapedrive;

impl<Blockchain: Rpc, Cluster: Api> Tapedrive<Blockchain, Cluster> {
    /// Close a tape. Every track on it is gone and its tape number is never
    /// given out again. An immutable tape can be closed only after it expires.
    ///
    /// The tape account's rent comes back to the payer.
    pub async fn close_tape(&self, tape_key: &TapeKey) -> Result<(), TapedriveError> {
        let payer = self.payer()?;
        let tape_signer = tape_key.keypair();
        let ix = build_close_tape_ix(
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
