use rpc::Rpc;
use tape_crypto::address::Address;
use tape_protocol::Api;

use crate::error::TapedriveError;
use crate::keys::tape_key::TapeKey;
use crate::tapedrive::Tapedrive;
use tape_api::instruction::build_delete_track_ix;
use crate::track::queries;

impl<Blockchain: Rpc, Cluster: Api> Tapedrive<Blockchain, Cluster> {
    /// Delete a concrete track version and free its capacity on the tape.
    pub async fn delete(
        &self,
        tape_key: &TapeKey,
        track: Address,
    ) -> Result<(), TapedriveError> {
        let payer = self.payer()?;
        let tape_signer = tape_key.keypair();
        let proof = queries::query_track_proof(self, &track).await?;
        let ix = build_delete_track_ix(
            payer.pubkey().into(),
            tape_key.address(),
            proof,
        );

        self.rpc()
            .send_instructions_with_signers(
                payer,
                vec![ix],
                &[tape_signer],
            )
            .await?;

        Ok(())
    }
}
