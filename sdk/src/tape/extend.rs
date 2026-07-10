use rpc::Rpc;
use tape_api::instruction::{build_extend_tape_capacity_ix, build_extend_tape_expiry_ix};
use tape_api::state::Tape;
use tape_core::types::{EpochNumber, StorageUnits};
use tape_crypto::address::Address;
use tape_protocol::Api;

use crate::error::TapedriveError;
use crate::tapedrive::Tapedrive;

impl<Blockchain: Rpc, Cluster: Api> Tapedrive<Blockchain, Cluster> {
    /// Add time to a tape's expiry. Anyone can pay; no tape signature needed.
    pub async fn extend_expiry(
        &self,
        tape_address: &Address,
        extra_epochs: u64,
    ) -> Result<Tape, TapedriveError> {
        let payer = self.payer()?;
        let tape = self.get_tape(tape_address).await?;

        let new_expiry = tape.expiry_epoch + EpochNumber(extra_epochs);
        let ix = build_extend_tape_expiry_ix(
            payer.pubkey().into(),
            payer.pubkey().into(),
            *tape_address,
            new_expiry,
        );

        self.rpc().send_instructions(payer, vec![ix]).await?;

        self.get_tape(tape_address).await
    }

    /// Add storage capacity to a tape. Anyone can pay; no tape signature needed.
    pub async fn extend_capacity(
        &self,
        tape_address: &Address,
        extra: StorageUnits,
    ) -> Result<Tape, TapedriveError> {
        let payer = self.payer()?;

        let ix = build_extend_tape_capacity_ix(
            payer.pubkey().into(),
            payer.pubkey().into(),
            *tape_address,
            extra,
        );

        self.rpc().send_instructions(payer, vec![ix]).await?;

        self.get_tape(tape_address).await
    }
}
