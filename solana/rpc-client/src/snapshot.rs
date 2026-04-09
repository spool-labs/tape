use crate::client::RpcClient;
use core::mem::size_of;
use rpc::{Rpc, RpcError};
use tape_api::program::tapedrive::{
    snapshot_manifest_pda, SNAPSHOT_STATE_ADDRESS,
};
use tape_api::state::{SnapshotManifest, SnapshotState};
use tape_core::types::EpochNumber;

impl<R: Rpc> RpcClient<R> {
    /// Fetch the SnapshotState singleton account.
    pub async fn get_snapshot_state(&self) -> Result<SnapshotState, RpcError> {
        let account = self.rpc().get_account(&SNAPSHOT_STATE_ADDRESS).await?;
        let expected_size = size_of::<SnapshotState>() + 8;
        if account.data.len() < expected_size {
            return Err(RpcError::Deserialization(format!(
                "SnapshotState account too small: {} bytes (expected {})",
                account.data.len(),
                expected_size
            )));
        }
        SnapshotState::unpack_with_discriminator(&account.data)
            .map(|state| *state)
            .map_err(|error| RpcError::Deserialization(error.to_string()))
    }

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

    /// Fetch the manifest for the current canonical snapshot tail.
    pub async fn get_snapshot_tail_manifest(&self) -> Result<SnapshotManifest, RpcError> {
        let state = self.get_snapshot_state().await?;
        self.get_snapshot_manifest(state.tail_epoch).await
    }
}

#[cfg(test)]
mod tests {
    use bytemuck::Zeroable;
    use rpc::RpcError;
    use crate::RpcClient;
    use rpc_litesvm::LiteSvmRpc;
    use tape_api::program::tapedrive::{self, snapshot_manifest_pda, snapshot_state_pda};
    use tape_api::state::{SnapshotChunkRecord, SnapshotManifest, SnapshotState};
    use tape_core::encoding::EncodingProfile;
    use tape_core::erasure::SPOOL_GROUP_COUNT;
    use tape_core::types::{
        EpochNumber, SnapshotGroupBitmap, StorageUnits, StripeCount, TrackNumber,
    };
    use tape_crypto::Hash;

    fn client() -> RpcClient<LiteSvmRpc> {
        RpcClient::from_rpc(LiteSvmRpc::new())
    }

    fn snapshot_manifest(epoch: EpochNumber) -> SnapshotManifest {
        let mut groups = [SnapshotChunkRecord::zeroed(); SPOOL_GROUP_COUNT];
        groups[7] = SnapshotChunkRecord {
            size: StorageUnits::from_bytes(1_537),
            value_hash: Hash::from([0x33; 32]),
            commitment: Hash::from([0x44; 32]),
            track_number: TrackNumber(3),
            profile: EncodingProfile::basic_default(),
            stripe_size: StorageUnits::from_bytes(512),
            stripe_count: StripeCount(4),
        };

        SnapshotManifest {
            parent_epoch: epoch - EpochNumber(1),
            group_bitmap: {
                let mut bitmap = SnapshotGroupBitmap::zeroed();
                bitmap.set(7);
                bitmap
            },
            groups,
        }
    }

    #[tokio::test]
    async fn snapshot_state_roundtrip() {
        let client = client();
        let state = SnapshotState {
            tail_epoch: EpochNumber(21),
        };
        let (address, _) = snapshot_state_pda();

        client
            .rpc()
            .set_account_data(address, tapedrive::ID, &state.pack())
            .expect("store snapshot state");

        let decoded = client.get_snapshot_state().await.expect("read state");
        assert_eq!(decoded, state);
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
    async fn tail_manifest_uses_tail_epoch() {
        let client = client();
        let state = SnapshotState {
            tail_epoch: EpochNumber(23),
        };
        let manifest = snapshot_manifest(state.tail_epoch);
        let (state_address, _) = snapshot_state_pda();
        let (manifest_address, _) = snapshot_manifest_pda(state.tail_epoch);

        client
            .rpc()
            .set_account_data(state_address, tapedrive::ID, &state.pack())
            .expect("store snapshot state");
        client
            .rpc()
            .set_account_data(manifest_address, tapedrive::ID, &manifest.pack())
            .expect("store snapshot manifest");

        let decoded = client
            .get_snapshot_tail_manifest()
            .await
            .expect("read tail manifest");
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

    #[tokio::test]
    async fn invalid_state_data_is_rejected() {
        let client = client();
        let (address, _) = snapshot_state_pda();

        client
            .rpc()
            .set_account_data(address, tapedrive::ID, &[1, 2, 3])
            .expect("store invalid snapshot state");

        let err = client
            .get_snapshot_state()
            .await
            .expect_err("invalid state should fail");
        assert!(matches!(err, RpcError::Deserialization(_)));
    }
}
