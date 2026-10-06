use tape_rpc::Rpc;
use tape_api::helpers::build_authority_with_tokens_ix;
use tape_api::instruction::build_reserve_tape_ix;
use tape_api::state::Tape;
use tape_core::tape::TapeFlags;
use tape_core::types::{EpochNumber, StorageUnits};
use tape_protocol::Api;

use crate::error::TapedriveError;
use crate::keys::tape_key::TapeKey;
use crate::tape::price::reservation_cost;
use crate::tapedrive::Tapedrive;

impl<Blockchain: Rpc, Cluster: Api> Tapedrive<Blockchain, Cluster> {
    /// Reserve a new tape (storage allocation). The expiry is rounded as in
    /// `reserve_with_flags`.
    pub async fn reserve(
        &self,
        tape_key: &TapeKey,
        capacity: StorageUnits,
        epochs: u64,
    ) -> Result<Tape, TapedriveError> {
        self.reserve_with_flags(tape_key, capacity, epochs, TapeFlags::NONE)
            .await
    }

    /// Reserve a new tape with `TapeFlags` bits. A hidden or immutable tape
    /// makes every track written to it the same, and the flags can't change later.
    ///
    /// The expiry is rounded up to the next row of the archive schedule, which
    /// far ahead is up to 512 epochs wide, and stops at the network's longest
    /// reservation. The tape pays for the epochs it gets.
    pub async fn reserve_with_flags(
        &self,
        tape_key: &TapeKey,
        capacity: StorageUnits,
        epochs: u64,
        flags: u64,
    ) -> Result<Tape, TapedriveError> {
        if !TapeFlags::is_reservable(flags) {
            return Err(TapedriveError::InvalidArgument(
                "a tape can be reserved hidden or immutable only".into(),
            ));
        }

        let payer = self.payer()?;
        let tape_signer = tape_key.keypair();
        let system = self.rpc().get_system().await?;
        let epoch = self.rpc().get_epoch(system.current_epoch).await?;
        let archive = self.rpc().get_archive().await?;

        let activation = epoch.id;
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

        ixs.push(build_reserve_tape_ix(
            payer.pubkey().into(),
            tape_key.pubkey().into(),
            capacity,
            activation,
            expiry,
            flags,
        ));

        self.rpc()
            .send_instructions_with_signers(payer, ixs, &[tape_signer])
            .await?;

        self.get_tape(&tape_key.address()).await
    }
}
