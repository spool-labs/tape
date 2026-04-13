use crate::client::RpcClient;
use core::mem::size_of;
use rpc::{Rpc, RpcError};
use tape_api::program::tapedrive::snapshot_manifest_pda;
use tape_api::state::SnapshotManifest;
use tape_core::types::EpochNumber;

impl<R: Rpc> RpcClient<R> {
    /// Fetch a snapshot manifest for a specific epoch.
    pub async fn get_snapshot_manifest(
        &self,
        epoch: EpochNumber,
    ) -> Result<SnapshotManifest, RpcError> {
        let (address, _) = snapshot_manifest_pda(epoch);
        let account = self.rpc().get_account(&address).await?;
        let expected_size = size_of::<SnapshotManifest>() + 8;
        if account.data.len() < expected_size {
            return Err(RpcError::Deserialization(format!(
                "SnapshotManifest account too small: {} bytes (expected {})",
                account.data.len(),
                expected_size
            )));
        }
        SnapshotManifest::unpack_with_discriminator(&account.data)
            .map(|manifest| *manifest)
            .map_err(|error| RpcError::Deserialization(error.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use bytemuck::Zeroable;
    use rpc::RpcError;
    use crate::RpcClient;
    use rpc_litesvm::LiteSvmRpc;
    use tape_api::program::tapedrive::{self, snapshot_manifest_pda};
    use tape_api::state::{SnapshotChunkRecord, SnapshotManifest};
    use tape_core::erasure::SPOOL_GROUP_COUNT;
    use tape_core::types::{EpochNumber, SnapshotGroupBitmap, StorageUnits, TrackNumber};
    use tape_crypto::Hash;

    fn client() -> RpcClient<LiteSvmRpc> {
        RpcClient::from_rpc(LiteSvmRpc::new())
    }

    fn snapshot_manifest(epoch: EpochNumber) -> SnapshotManifest {
        let mut groups = [SnapshotChunkRecord::zeroed(); SPOOL_GROUP_COUNT];
        groups[7] = SnapshotChunkRecord {
            value_hash: Hash::from([0x33; 32]),
            commitment: Hash::from([0x44; 32]),
            track_number: TrackNumber(3),
        };

        SnapshotManifest {
            epoch,
            group_bitmap: {
                let mut bitmap = SnapshotGroupBitmap::zeroed();
                bitmap.set(7);
                bitmap
            },
            chunk_size: StorageUnits::from_bytes(1_537),
            groups,
        }
    }

    #[tokio::test]
    async fn snapshot_manifest_roundtrip() {
        let client = client();
        let epoch = EpochNumber(22);
        let manifest = snapshot_manifest(epoch);
        let (address, _) = snapshot_manifest_pda(epoch);

        client
            .rpc()
            .set_account_data(address, tapedrive::ID, &manifest.pack())
            .expect("store snapshot manifest");

        let decoded = client
            .get_snapshot_manifest(epoch)
            .await
            .expect("read manifest");
        assert_eq!(decoded, manifest);
    }

    #[tokio::test]
    async fn missing_manifest_propagates_account_not_found() {
        let client = client();
        let err = client
            .get_snapshot_manifest(EpochNumber(99))
            .await
            .expect_err("manifest should be missing");
        assert!(matches!(err, RpcError::AccountNotFound(_)));
    }
}
