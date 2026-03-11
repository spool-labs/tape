use rpc::Rpc;
use solana_sdk::signature::Signer;
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
        let epoch = self.rpc().get_epoch().await?;
        let archive = self.rpc().get_archive().await?;

        let activation = epoch.id;
        let expiry = EpochNumber(epoch.id.as_u64() + epochs);
        let cost = reservation_cost(archive.storage_price, capacity, epochs)?;

        let mut ixs =
            build_authority_with_tokens_ix(self.payer.pubkey(), tape_key.pubkey(), cost);

        ixs.push(build_reserve_tape_ix(
            self.payer.pubkey(),
            tape_key.pubkey(),
            capacity,
            activation,
            expiry,
        ));

        self.rpc()
            .send_instructions_with_signers(&self.payer, ixs, &[tape_key.as_keypair()])
            .await?;

        self.rpc()
            .get_tape(&tape_key.pubkey())
            .await
            .map_err(TapedriveError::Rpc)
    }

    /// Add time to a tape's expiry.
    pub async fn extend_expiry(
        &self,
        tape_key: &TapeKey,
        extra_epochs: u64,
    ) -> Result<Tape, TapedriveError> {
        let temp = TapeKey::generate();
        let tape = self.rpc().get_tape(&tape_key.pubkey()).await?;
        let archive = self.rpc().get_archive().await?;

        let new_expiry = tape.expiry_epoch + EpochNumber(extra_epochs);
        let cost = reservation_cost(archive.storage_price, tape.capacity, extra_epochs)?;

        let mut ixs =
            build_authority_with_tokens_ix(self.payer.pubkey(), temp.pubkey(), cost);

        ixs.push(build_reserve_tape_ix(
            self.payer.pubkey(),
            temp.pubkey(),
            tape.capacity,
            tape.expiry_epoch,
            new_expiry,
        ));

        ixs.push(build_merge_tape_ix(
            self.payer.pubkey(),
            temp.pubkey(),
            tape_key.pubkey(),
        ));

        self.rpc()
            .send_instructions_with_signers(
                &self.payer,
                ixs,
                &[temp.as_keypair(), tape_key.as_keypair()],
            )
            .await?;

        self.rpc()
            .get_tape(&tape_key.pubkey())
            .await
            .map_err(TapedriveError::Rpc)
    }

    /// Add storage capacity to a tape.
    pub async fn extend_capacity(
        &self,
        tape_key: &TapeKey,
        extra: StorageUnits,
    ) -> Result<Tape, TapedriveError> {
        let temp = TapeKey::generate();
        let tape = self.rpc().get_tape(&tape_key.pubkey()).await?;
        let archive = self.rpc().get_archive().await?;
        let epoch = self.rpc().get_epoch().await?;

        let activation = epoch.id.max(tape.active_epoch);
        let duration = remaining_epochs(epoch.id, tape.active_epoch, tape.expiry_epoch)?;
        let cost = reservation_cost(archive.storage_price, extra, duration)?;

        let mut ixs =
            build_authority_with_tokens_ix(self.payer.pubkey(), temp.pubkey(), cost);

        ixs.push(build_reserve_tape_ix(
            self.payer.pubkey(),
            temp.pubkey(),
            extra,
            activation,
            tape.expiry_epoch,
        ));
        ixs.push(build_merge_tape_ix(
            self.payer.pubkey(),
            temp.pubkey(),
            tape_key.pubkey(),
        ));

        self.rpc()
            .send_instructions_with_signers(
                &self.payer,
                ixs,
                &[temp.as_keypair(), tape_key.as_keypair()],
            )
            .await?;

        self.rpc()
            .get_tape(&tape_key.pubkey())
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
        let ix = build_split_tape_by_epoch_ix(
            self.payer.pubkey(),
            source.pubkey(),
            destination.pubkey(),
            at_epoch,
        );

        self.rpc()
            .send_instructions_with_signers(
                &self.payer,
                vec![ix],
                &[source.as_keypair(), destination.as_keypair()],
            )
            .await?;

        let src = self.rpc().get_tape(&source.pubkey()).await?;
        let dst = self.rpc().get_tape(&destination.pubkey()).await?;

        Ok((src, dst))
    }

    /// Split a tape by capacity.
    pub async fn split_by_capacity(
        &self,
        source: &TapeKey,
        destination: &TapeKey,
        keep: StorageUnits,
    ) -> Result<(Tape, Tape), TapedriveError> {
        let ix = build_split_tape_by_size_ix(
            self.payer.pubkey(),
            source.pubkey(),
            destination.pubkey(),
            keep,
        );

        self.rpc()
            .send_instructions_with_signers(
                &self.payer,
                vec![ix],
                &[source.as_keypair(), destination.as_keypair()],
            )
            .await?;

        let src = self.rpc().get_tape(&source.pubkey()).await?;
        let dst = self.rpc().get_tape(&destination.pubkey()).await?;

        Ok((src, dst))
    }

    /// Merge source tape into destination.
    pub async fn merge(
        &self,
        source: &TapeKey,
        destination: &TapeKey,
    ) -> Result<Tape, TapedriveError> {
        let ix = build_merge_tape_ix(
            self.payer.pubkey(),
            source.pubkey(),
            destination.pubkey(),
        );

        self.rpc()
            .send_instructions_with_signers(
                &self.payer,
                vec![ix],
                &[source.as_keypair(), destination.as_keypair()],
            )
            .await?;

        self.rpc()
            .get_tape(&destination.pubkey())
            .await
            .map_err(TapedriveError::Rpc)
    }

    /// Destroy an empty, expired tape.
    pub async fn destroy(&self, tape_key: &TapeKey) -> Result<(), TapedriveError> {
        let ix = build_destroy_tape_ix(self.payer.pubkey(), tape_key.pubkey());

        self.rpc()
            .send_instructions_with_signers(
                &self.payer,
                vec![ix],
                &[tape_key.as_keypair()],
            )
            .await?;

        Ok(())
    }
}
