use rpc::Rpc;
use tape_api::instruction::{build_split_tape_by_epoch_ix, build_split_tape_by_size_ix};
use tape_api::state::Tape;
use tape_core::types::{EpochNumber, StorageUnits};
use tape_protocol::Api;

use crate::error::TapedriveError;
use crate::keys::tape_key::TapeKey;
use crate::tapedrive::Tapedrive;

impl<Blockchain: Rpc, Cluster: Api> Tapedrive<Blockchain, Cluster> {
    /// Split a tape at an epoch boundary.
    pub async fn split_by_time(
        &self,
        source: &TapeKey,
        destination: &TapeKey,
        at_epoch: EpochNumber,
    ) -> Result<(Tape, Tape), TapedriveError> {
        let payer = self.payer()?;
        let source_signer = source.keypair();
        let destination_signer = destination.keypair();
        let ix = build_split_tape_by_epoch_ix(
            payer.pubkey().into(),
            source.pubkey().into(),
            destination.pubkey().into(),
            at_epoch,
        );

        self.rpc()
            .send_instructions_with_signers(payer, vec![ix], &[source_signer, destination_signer])
            .await?;

        let src = self.get_tape(&source.address()).await?;
        let dst = self.get_tape(&destination.address()).await?;

        Ok((src, dst))
    }

    /// Split a tape by capacity.
    pub async fn split_by_capacity(
        &self,
        source: &TapeKey,
        destination: &TapeKey,
        keep: StorageUnits,
    ) -> Result<(Tape, Tape), TapedriveError> {
        let payer = self.payer()?;
        let source_signer = source.keypair();
        let destination_signer = destination.keypair();
        let ix = build_split_tape_by_size_ix(
            payer.pubkey().into(),
            source.pubkey().into(),
            destination.pubkey().into(),
            keep,
        );

        self.rpc()
            .send_instructions_with_signers(payer, vec![ix], &[source_signer, destination_signer])
            .await?;

        let src = self.get_tape(&source.address()).await?;
        let dst = self.get_tape(&destination.address()).await?;

        Ok((src, dst))
    }
}
