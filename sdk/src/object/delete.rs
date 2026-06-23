use rpc::Rpc;
use tape_api::compute::TRACK_WRITE_CU;
use tape_api::instruction::build_delete_track_ix;
use tape_protocol::Api;

use crate::error::TapedriveError;
use crate::keys::tape_key::TapeKey;
use crate::tapedrive::Tapedrive;

impl<Blockchain: Rpc, Cluster: Api> Tapedrive<Blockchain, Cluster> {
    /// Delete a named object from a bucket.
    pub async fn delete_object(&self, bucket: &TapeKey, name: &str) -> Result<(), TapedriveError> {
        let address = self.resolve_object(&bucket.address(), name).await?;
        let proof = self.get_track_proof(&address).await?;

        let payer = self.payer()?;
        let tape_signer = bucket.keypair();
        let ix = build_delete_track_ix(payer.pubkey().into(), bucket.pubkey().into(), proof);

        self.rpc()
            .send_instructions_with_signers_and_compute_unit_limit(
                payer,
                TRACK_WRITE_CU,
                vec![ix],
                &[tape_signer],
            )
            .await?;

        Ok(())
    }
}
