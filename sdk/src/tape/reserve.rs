use rpc::Rpc;
use tape_api::helpers::build_authority_with_tokens_ix;
use tape_api::instruction::build_reserve_tape_ix;
use tape_api::state::Tape;
use tape_core::types::{EpochNumber, StorageUnits};
use tape_protocol::Api;

use crate::error::TapedriveError;
use crate::keys::tape_key::TapeKey;
use crate::tape::price::reservation_cost;
use crate::tapedrive::Tapedrive;

impl<Blockchain: Rpc, Cluster: Api> Tapedrive<Blockchain, Cluster> {
    /// Reserve a new tape (storage allocation).
    pub async fn reserve(
        &self,
        tape_key: &TapeKey,
        capacity: StorageUnits,
        epochs: u64,
    ) -> Result<Tape, TapedriveError> {
        let payer = self.payer()?;
        let tape_signer = tape_key.keypair();
        let system = self.rpc().get_system().await?;
        let epoch = self.rpc().get_epoch(system.current_epoch).await?;
        let archive = self.rpc().get_archive().await?;

        let activation = epoch.id;
        let expiry = EpochNumber(epoch.id.as_u64() + epochs);
        let cost = reservation_cost(archive.storage_price, capacity, epochs)?;

        let mut ixs = build_authority_with_tokens_ix(
            payer.pubkey().into(),
            tape_key.pubkey().into(),
            cost,
        )
        .map_err(|error| TapedriveError::InvalidArgument(error.to_string()))?;

        ixs.push(build_reserve_tape_ix(
            payer.pubkey().into(),
            tape_key.pubkey().into(),
            capacity,
            activation,
            expiry,
        ));

        self.rpc()
            .send_instructions_with_signers(payer, ixs, &[tape_signer])
            .await?;

        self.get_tape(&tape_key.address()).await
    }
}
