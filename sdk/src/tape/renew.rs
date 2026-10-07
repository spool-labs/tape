use tape_api::helpers::build_authority_with_tokens_ix;
use tape_api::instruction::build_renew_tape_ix;
use tape_api::state::Tape;
use tape_core::types::{EpochNumber, StorageUnits};
use tape_protocol::Api;
use tape_rpc::Rpc;

use crate::error::TapedriveError;
use crate::keys::tape_key::TapeKey;
use crate::tape::price::reservation_cost;
use crate::tapedrive::Tapedrive;

impl<Blockchain: Rpc, Cluster: Api> Tapedrive<Blockchain, Cluster> {
    /// Reserve an expired tape again under its tape number. It comes back
    /// empty. The expiry is rounded as in `reserve_with_flags`, and the cost
    /// is what `estimate_cost` gives for the same capacity and epochs.
    pub async fn renew_tape(
        &self,
        tape_key: &TapeKey,
        capacity: StorageUnits,
        epochs: u64,
    ) -> Result<Tape, TapedriveError> {
        let payer = self.payer()?;
        let tape_signer = tape_key.keypair();
        let system = self.rpc().get_system().await?;
        let archive = self.rpc().get_archive().await?;

        let activation = system.current_epoch;
        let requested = activation.saturating_add(EpochNumber(epochs));
        let expiry = archive
            .expiry_for(requested)
            .map_err(|error| TapedriveError::InvalidArgument(format!("archive schedule: {error:?}")))?;
        let reserved_epochs = expiry.saturating_sub(activation).as_u64();
        let cost = reservation_cost(archive.storage_price, capacity, reserved_epochs)?;

        let mut ixs = build_authority_with_tokens_ix(
            payer.pubkey().into(),
            tape_key.pubkey().into(),
            cost,
        )
        .map_err(|error| TapedriveError::InvalidArgument(error.to_string()))?;

        ixs.push(build_renew_tape_ix(
            payer.pubkey().into(),
            tape_key.pubkey().into(),
            tape_key.address(),
            capacity,
            expiry,
        ));

        self.rpc()
            .send_instructions_with_signers(payer, ixs, &[tape_signer])
            .await?;

        self.get_tape(&tape_key.address()).await
    }
}
