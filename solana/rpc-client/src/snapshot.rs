use crate::client::RpcClient;
use core::mem::size_of;
use rpc::{Rpc, RpcError};
use tape_api::program::tapedrive::snapshot_pda;
use tape_api::state::Snapshot;
use tape_core::types::EpochNumber;

impl<R: Rpc> RpcClient<R> {
    /// Fetch the snapshot manifest account for a specific epoch.
    pub async fn get_snapshot(&self, epoch: EpochNumber) -> Result<Snapshot, RpcError> {
        let (address, _) = snapshot_pda(epoch);
        let account = self.rpc().get_account(&address).await?;
        let expected_size = size_of::<Snapshot>() + 8;
        if account.data.len() < expected_size {
            return Err(RpcError::Deserialization(format!(
                "Snapshot account too small: {} bytes (expected {})",
                account.data.len(),
                expected_size
            )));
        }
        match Snapshot::unpack_with_discriminator(&account.data) {
            Ok(snapshot) => Ok(*snapshot),
            Err(error) => Err(RpcError::Deserialization(error.to_string())),
        }
    }
}

#[cfg(test)]
mod tests {
    use bytemuck::Zeroable;
    use rpc::RpcError;
    use crate::RpcClient;
    use rpc_litesvm::LiteSvmRpc;
    use tape_api::program::tapedrive::{self, snapshot_pda};
    use tape_api::state::Snapshot;
    use tape_core::snapshot::types::SnapshotState;
    use tape_core::types::{EpochNumber, GroupBitmap};

    fn client() -> RpcClient<LiteSvmRpc> {
        RpcClient::from_rpc(LiteSvmRpc::new())
    }

    fn sample_snapshot(epoch: EpochNumber) -> Snapshot {
        Snapshot {
            epoch,
            state: SnapshotState::Registered as u64,
            group_bitmap: GroupBitmap::zeroed(),
        }
    }

    #[tokio::test]
    async fn snapshot_roundtrip() {
        let client = client();
        let epoch = EpochNumber(22);
        let snapshot = sample_snapshot(epoch);
        let (address, _) = snapshot_pda(epoch);

        client
            .rpc()
            .set_account_data(address, tapedrive::ID, &snapshot.pack())
            .expect("store snapshot");

        let decoded = client
            .get_snapshot(epoch)
            .await
            .expect("read snapshot");
        assert_eq!(decoded, snapshot);
    }

    #[tokio::test]
    async fn missing_snapshot_propagates_account_not_found() {
        let client = client();
        let err = client
            .get_snapshot(EpochNumber(99))
            .await
            .expect_err("snapshot should be missing");
        assert!(matches!(err, RpcError::AccountNotFound(_)));
    }
}
