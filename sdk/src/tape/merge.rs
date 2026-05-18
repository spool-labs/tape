use rpc::Rpc;
use tape_api::instruction::build_merge_tape_ix;
use tape_api::state::Tape;
use tape_protocol::Api;

use crate::error::TapedriveError;
use crate::keys::tape_key::TapeKey;
use crate::tapedrive::Tapedrive;

impl<Blockchain: Rpc, Cluster: Api> Tapedrive<Blockchain, Cluster> {
    /// Merge source tape into destination.
    pub async fn merge(
        &self,
        source: &TapeKey,
        destination: &TapeKey,
    ) -> Result<Tape, TapedriveError> {
        let payer = self.payer()?;
        let source_signer = source.keypair();
        let destination_signer = destination.keypair();
        let ix = build_merge_tape_ix(
            payer.pubkey().into(),
            source.pubkey().into(),
            destination.pubkey().into(),
        );

        self.rpc()
            .send_instructions_with_signers(payer, vec![ix], &[source_signer, destination_signer])
            .await?;

        self.get_tape(&destination.address()).await
    }
}
