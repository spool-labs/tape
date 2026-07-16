use crate::client::RpcClient;
use rpc::{Rpc, RpcError};
use solana_commitment_config::CommitmentLevel;
use tape_api::program::tapedrive::snapshot_tape_pda;
use tape_api::state::Tape;
use tape_core::types::EpochNumber;

impl<R: Rpc> RpcClient<R> {
    /// Fetch the canonical snapshot tape account for a specific epoch.
    pub async fn get_snapshot_tape(&self, epoch: EpochNumber) -> Result<Tape, RpcError> {
        self.get_snapshot_tape_with_commitment(epoch, self.rpc().commitment())
            .await
    }

    /// Fetch the canonical snapshot tape account for a specific epoch at an explicit commitment.
    pub async fn get_snapshot_tape_with_commitment(
        &self,
        epoch: EpochNumber,
        commitment: CommitmentLevel,
    ) -> Result<Tape, RpcError> {
        let (address, _) = snapshot_tape_pda(epoch);
        let account = self
            .rpc()
            .get_account_with_commitment(&address, commitment)
            .await?;

        if account.data.len() < Tape::get_size() {
            return Err(RpcError::Deserialization(format!(
                "Snapshot tape account too small: {} bytes (expected {})",
                account.data.len(),
                Tape::get_size()
            )));
        }

        let tape = Tape::unpack_with_discriminator(&account.data)
            .map(|tape| *tape)
            .map_err(|error| RpcError::Deserialization(error.to_string()))?;

        if !tape.is_snapshot_tape(epoch) {
            return Err(RpcError::Deserialization(format!(
                "snapshot tape account does not match epoch {epoch}"
            )));
        }

        Ok(tape)
    }
}

#[cfg(test)]
mod tests {
    use crate::RpcClient;
    use rpc::RpcError;
    use rpc_litesvm::LiteSvmRpc;
    use tape_api::program::tapedrive::{self, snapshot_tape_pda};
    use tape_api::state::Tape;
    use tape_core::types::EpochNumber;

    fn client() -> RpcClient<LiteSvmRpc> {
        RpcClient::from_rpc(LiteSvmRpc::new())
    }

    fn sample_snapshot_tape(epoch: EpochNumber) -> Tape {
        Tape::snapshot(epoch)
    }

    #[tokio::test]
    async fn snapshot_tape_roundtrip() {
        let client = client();
        let epoch = EpochNumber(22);
        let tape = sample_snapshot_tape(epoch);
        let (address, _) = snapshot_tape_pda(epoch);

        client
            .rpc()
            .set_account_data(address, tapedrive::ID, &tape.pack())
            .expect("store snapshot tape");

        let decoded = client
            .get_snapshot_tape(epoch)
            .await
            .expect("read snapshot tape");
        assert_eq!(decoded, tape);
    }

    #[tokio::test]
    async fn malformed_snapshot_tape_rejected() {
        let client = client();
        let epoch = EpochNumber(22);
        let mut tape = sample_snapshot_tape(epoch);
        tape.active_epoch = EpochNumber(21);
        let (address, _) = snapshot_tape_pda(epoch);

        client
            .rpc()
            .set_account_data(address, tapedrive::ID, &tape.pack())
            .expect("store malformed snapshot tape");

        let err = client
            .get_snapshot_tape(epoch)
            .await
            .expect_err("snapshot tape should be rejected");
        assert!(matches!(err, RpcError::Deserialization(_)));
    }

    #[tokio::test]
    async fn missing_snapshot_tape_propagates_account_not_found() {
        let client = client();
        let err = client
            .get_snapshot_tape(EpochNumber(99))
            .await
            .expect_err("snapshot tape should be missing");
        assert!(matches!(err, RpcError::AccountNotFound(_)));
    }
}
