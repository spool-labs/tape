use tape_api::instruction::build_rewind_tape_ix;
use tape_api::state::Tape;
use tape_protocol::Api;
use tape_rpc::Rpc;

use crate::error::TapedriveError;
use crate::keys::tape_key::TapeKey;
use crate::tapedrive::Tapedrive;

impl<Blockchain: Rpc, Cluster: Api> Tapedrive<Blockchain, Cluster> {
    /// Remove every track from a tape and revoke every shared token. The
    /// reservation and the tape number stay. An immutable tape can't be
    /// rewound, and an expired tape is renewed instead.
    pub async fn rewind_tape(&self, tape_key: &TapeKey) -> Result<Tape, TapedriveError> {
        let payer = self.payer()?;
        let tape_signer = tape_key.keypair();
        let ix = build_rewind_tape_ix(
            payer.pubkey().into(),
            tape_key.pubkey().into(),
            tape_key.address(),
        );

        self.rpc()
            .send_instructions_with_signers(payer, vec![ix], &[tape_signer])
            .await?;

        self.get_tape(&tape_key.address()).await
    }
}
