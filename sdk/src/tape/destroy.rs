use rpc::Rpc;
use tape_api::instruction::build_destroy_tape_ix;
use tape_protocol::Api;

use crate::error::TapedriveError;
use crate::keys::tape_key::TapeKey;
use crate::tapedrive::Tapedrive;

impl<Blockchain: Rpc, Cluster: Api> Tapedrive<Blockchain, Cluster> {
    /// Destroy an empty, expired tape.
    pub async fn destroy(&self, tape_key: &TapeKey) -> Result<(), TapedriveError> {
        let payer = self.payer()?;
        let tape_signer = tape_key.keypair();
        let ix = build_destroy_tape_ix(payer.pubkey().into(), tape_key.pubkey().into());

        self.rpc()
            .send_instructions_with_signers(payer, vec![ix], &[tape_signer])
            .await?;

        Ok(())
    }
}
