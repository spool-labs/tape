use rpc::Rpc;
use tape_api::helpers::build_authority_with_tokens_ix;
use tape_api::instruction::{build_merge_tape_ix, build_reserve_tape_ix};
use tape_api::state::Tape;
use tape_core::types::{EpochNumber, StorageUnits};
use tape_protocol::Api;

use crate::error::TapedriveError;
use crate::keys::tape_key::TapeKey;
use crate::tape::price::{remaining_epochs, reservation_cost};
use crate::tapedrive::Tapedrive;

impl<Blockchain: Rpc, Cluster: Api> Tapedrive<Blockchain, Cluster> {
    /// Add time to a tape's expiry.
    pub async fn extend_expiry(
        &self,
        tape_key: &TapeKey,
        extra_epochs: u64,
    ) -> Result<Tape, TapedriveError> {
        let payer = self.payer()?;
        let temp = TapeKey::generate();
        let temp_signer = temp.keypair();
        let tape_signer = tape_key.keypair();
        let tape = self.get_tape(&tape_key.address()).await?;
        let archive = self.rpc().get_archive().await?;

        let new_expiry = tape.expiry_epoch + EpochNumber(extra_epochs);
        let cost = reservation_cost(archive.storage_price, tape.capacity, extra_epochs)?;

        let mut ixs = build_authority_with_tokens_ix(
            payer.pubkey().into(),
            temp.pubkey().into(),
            cost,
        )
        .map_err(|error| TapedriveError::InvalidArgument(error.to_string()))?;

        ixs.push(build_reserve_tape_ix(
            payer.pubkey().into(),
            temp.pubkey().into(),
            tape.capacity,
            tape.expiry_epoch,
            new_expiry,
        ));

        ixs.push(build_merge_tape_ix(
            payer.pubkey().into(),
            temp.pubkey().into(),
            tape_key.pubkey().into(),
        ));

        self.rpc()
            .send_instructions_with_signers(payer, ixs, &[temp_signer, tape_signer])
            .await?;

        self.get_tape(&tape_key.address()).await
    }

    /// Add storage capacity to a tape.
    pub async fn extend_capacity(
        &self,
        tape_key: &TapeKey,
        extra: StorageUnits,
    ) -> Result<Tape, TapedriveError> {
        let payer = self.payer()?;
        let temp = TapeKey::generate();
        let temp_signer = temp.keypair();
        let tape_signer = tape_key.keypair();
        let tape = self.get_tape(&tape_key.address()).await?;
        let archive = self.rpc().get_archive().await?;
        let system = self.rpc().get_system().await?;
        let epoch = self.rpc().get_epoch(system.current_epoch).await?;

        let activation = epoch.id.max(tape.active_epoch);
        let duration = remaining_epochs(epoch.id, tape.active_epoch, tape.expiry_epoch)?;
        let cost = reservation_cost(archive.storage_price, extra, duration)?;

        let mut ixs = build_authority_with_tokens_ix(
            payer.pubkey().into(),
            temp.pubkey().into(),
            cost,
        )
        .map_err(|error| TapedriveError::InvalidArgument(error.to_string()))?;

        ixs.push(build_reserve_tape_ix(
            payer.pubkey().into(),
            temp.pubkey().into(),
            extra,
            activation,
            tape.expiry_epoch,
        ));
        ixs.push(build_merge_tape_ix(
            payer.pubkey().into(),
            temp.pubkey().into(),
            tape_key.pubkey().into(),
        ));

        self.rpc()
            .send_instructions_with_signers(payer, ixs, &[temp_signer, tape_signer])
            .await?;

        self.get_tape(&tape_key.address()).await
    }
}
