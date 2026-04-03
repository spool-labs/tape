use rpc::Rpc;
use tape_api::helpers::build_authority_with_tokens_ix;
use tape_api::instruction::{
    build_destroy_tape_ix, build_merge_tape_ix, build_reserve_tape_ix,
    build_split_tape_by_epoch_ix, build_split_tape_by_size_ix,
};
use tape_api::state::Tape;
use tape_core::types::{EpochNumber, StorageUnits};
use tape_protocol::Api;

use crate::error::TapedriveError;
use crate::keys::tape_key::TapeKey;
use crate::tape::billing::{remaining_epochs, reservation_cost};
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
        let epoch = self.rpc().get_epoch().await?;
        let archive = self.rpc().get_archive().await?;

        let activation = epoch.id;
        let expiry = EpochNumber(epoch.id.as_u64() + epochs);
        let cost = reservation_cost(archive.storage_price, capacity, epochs)?;

        let mut ixs = build_authority_with_tokens_ix(
            payer.pubkey().into(),
            tape_key.address(),
            cost,
        )
        .map_err(|error| TapedriveError::InvalidArgument(error.to_string()))?;

        ixs.push(build_reserve_tape_ix(
            payer.pubkey().into(),
            tape_key.address(),
            capacity,
            activation,
            expiry,
        ));

        self.rpc()
            .send_instructions_with_signers(payer, ixs, &[tape_signer])
            .await?;

        self.rpc()
            .get_tape(&tape_key.address())
            .await
            .map_err(TapedriveError::Rpc)
    }

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
        let tape = self.rpc().get_tape(&tape_key.address()).await?;
        let archive = self.rpc().get_archive().await?;

        let new_expiry = tape.expiry_epoch + EpochNumber(extra_epochs);
        let cost = reservation_cost(archive.storage_price, tape.capacity, extra_epochs)?;

        let mut ixs = build_authority_with_tokens_ix(
            payer.pubkey().into(),
            temp.address(),
            cost,
        )
        .map_err(|error| TapedriveError::InvalidArgument(error.to_string()))?;

        ixs.push(build_reserve_tape_ix(
            payer.pubkey().into(),
            temp.address(),
            tape.capacity,
            tape.expiry_epoch,
            new_expiry,
        ));

        ixs.push(build_merge_tape_ix(
            payer.pubkey().into(),
            temp.address(),
            tape_key.address(),
        ));

        self.rpc()
            .send_instructions_with_signers(
                payer,
                ixs,
                &[temp_signer, tape_signer],
            )
            .await?;

        self.rpc()
            .get_tape(&tape_key.address())
            .await
            .map_err(TapedriveError::Rpc)
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
        let tape = self.rpc().get_tape(&tape_key.address()).await?;
        let archive = self.rpc().get_archive().await?;
        let epoch = self.rpc().get_epoch().await?;

        let activation = epoch.id.max(tape.active_epoch);
        let duration = remaining_epochs(epoch.id, tape.active_epoch, tape.expiry_epoch)?;
        let cost = reservation_cost(archive.storage_price, extra, duration)?;

        let mut ixs = build_authority_with_tokens_ix(
            payer.pubkey().into(),
            temp.address(),
            cost,
        )
        .map_err(|error| TapedriveError::InvalidArgument(error.to_string()))?;

        ixs.push(build_reserve_tape_ix(
            payer.pubkey().into(),
            temp.address(),
            extra,
            activation,
            tape.expiry_epoch,
        ));
        ixs.push(build_merge_tape_ix(
            payer.pubkey().into(),
            temp.address(),
            tape_key.address(),
        ));

        self.rpc()
            .send_instructions_with_signers(
                payer,
                ixs,
                &[temp_signer, tape_signer],
            )
            .await?;

        self.rpc()
            .get_tape(&tape_key.address())
            .await
            .map_err(TapedriveError::Rpc)
    }

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
            source.address(),
            destination.address(),
            at_epoch,
        );

        self.rpc()
            .send_instructions_with_signers(
                payer,
                vec![ix],
                &[source_signer, destination_signer],
            )
            .await?;

        let src = self.rpc().get_tape(&source.address()).await?;
        let dst = self.rpc().get_tape(&destination.address()).await?;

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
            source.address(),
            destination.address(),
            keep,
        );

        self.rpc()
            .send_instructions_with_signers(
                payer,
                vec![ix],
                &[source_signer, destination_signer],
            )
            .await?;

        let src = self.rpc().get_tape(&source.address()).await?;
        let dst = self.rpc().get_tape(&destination.address()).await?;

        Ok((src, dst))
    }

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
            source.address(),
            destination.address(),
        );

        self.rpc()
            .send_instructions_with_signers(
                payer,
                vec![ix],
                &[source_signer, destination_signer],
            )
            .await?;

        self.rpc()
            .get_tape(&destination.address())
            .await
            .map_err(TapedriveError::Rpc)
    }

    /// Destroy an empty, expired tape.
    pub async fn destroy(&self, tape_key: &TapeKey) -> Result<(), TapedriveError> {
        let payer = self.payer()?;
        let tape_signer = tape_key.keypair();
        let ix = build_destroy_tape_ix(payer.pubkey().into(), tape_key.address());

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
