use rpc::Rpc;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::Signer;
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
        track: Pubkey,
    ) -> Result<(), TapedriveError> {
        let payer = self.payer()?;
        let proof = queries::query_track_proof(self, &track).await?;
        let ix = build_delete_track_ix(
            payer.pubkey(),
            tape_key.pubkey(),
            proof,
        );

        self.rpc()
            .send_instructions_with_signers(
                payer,
                vec![ix],
                &[tape_key.as_keypair()],
            )
            .await?;

        Ok(())
    }
}
