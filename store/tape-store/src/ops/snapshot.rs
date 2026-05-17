//! Snapshot build artifact operations.
//!
//! Vote signatures are stored by the generic vote operations. Snapshot
//! artifacts are local build products retained until the canonical snapshot
//! tape can be finalized.

use store::{Column, Store};
use tape_core::spooler::SpoolGroup;
use tape_core::types::{ChunkNumber, EpochNumber};

use crate::columns::SnapshotArtifactCol;
use crate::error::{Result, TapeStoreError};
use crate::types::{SnapshotArtifact, SnapshotArtifactKey};
use crate::TapeStore;

pub trait SnapshotOps {
    fn put_snapshot_artifact(
        &self,
        epoch: EpochNumber,
        group: SpoolGroup,
        chunk: ChunkNumber,
        artifact: &SnapshotArtifact,
    ) -> Result<()>;

    fn get_snapshot_artifact(
        &self,
        epoch: EpochNumber,
        group: SpoolGroup,
        chunk: ChunkNumber,
    ) -> Result<Option<SnapshotArtifact>>;

    /// Local staged artifacts for a group, ordered by ascending chunk number.
    fn iter_snapshot_artifacts(
        &self,
        epoch: EpochNumber,
        group: SpoolGroup,
    ) -> Result<Vec<(ChunkNumber, SnapshotArtifact)>>;

    fn delete_snapshot_artifact(
        &self,
        epoch: EpochNumber,
        group: SpoolGroup,
        chunk: ChunkNumber,
    ) -> Result<()>;

    /// Delete every snapshot artifact row for a single epoch.
    fn delete_snapshot_epoch(&self, epoch: EpochNumber) -> Result<()>;

    /// Delete every snapshot artifact row whose epoch is not `keep`.
    fn delete_snapshot_epochs_except(&self, keep: EpochNumber) -> Result<()>;
}

impl<S: Store> SnapshotOps for TapeStore<S> {
    fn put_snapshot_artifact(
        &self,
        epoch: EpochNumber,
        group: SpoolGroup,
        chunk: ChunkNumber,
        artifact: &SnapshotArtifact,
    ) -> Result<()> {
        let key = SnapshotArtifactKey::new(epoch.0, group.0, chunk.0);
        self.put::<SnapshotArtifactCol>(&key, artifact)?;
        Ok(())
    }

    fn get_snapshot_artifact(
        &self,
        epoch: EpochNumber,
        group: SpoolGroup,
        chunk: ChunkNumber,
    ) -> Result<Option<SnapshotArtifact>> {
        let key = SnapshotArtifactKey::new(epoch.0, group.0, chunk.0);
        Ok(self.get::<SnapshotArtifactCol>(&key)?)
    }

    fn iter_snapshot_artifacts(
        &self,
        epoch: EpochNumber,
        group: SpoolGroup,
    ) -> Result<Vec<(ChunkNumber, SnapshotArtifact)>> {
        let prefix = SnapshotArtifactKey::group_prefix(epoch.0, group.0);
        let iter = self
            .inner()
            .inner()
            .iter_prefix(SnapshotArtifactCol::CF_NAME, &prefix)?;

        let mut out = Vec::new();
        for (key_bytes, value_bytes) in iter {
            let key: SnapshotArtifactKey = wincode::deserialize(&key_bytes).map_err(|e| {
                TapeStoreError::Serialization(format!("snapshot artifact key: {e}"))
            })?;
            let artifact: SnapshotArtifact = wincode::deserialize(&value_bytes).map_err(|e| {
                TapeStoreError::Serialization(format!("snapshot artifact value: {e}"))
            })?;
            out.push((ChunkNumber(key.chunk), artifact));
        }
        Ok(out)
    }

    fn delete_snapshot_artifact(
        &self,
        epoch: EpochNumber,
        group: SpoolGroup,
        chunk: ChunkNumber,
    ) -> Result<()> {
        let key = SnapshotArtifactKey::new(epoch.0, group.0, chunk.0);
        self.delete::<SnapshotArtifactCol>(&key)?;
        Ok(())
    }

    fn delete_snapshot_epoch(&self, epoch: EpochNumber) -> Result<()> {
        delete_prefix(
            self,
            SnapshotArtifactCol::CF_NAME,
            &SnapshotArtifactKey::epoch_prefix(epoch.0),
        )?;
        Ok(())
    }

    fn delete_snapshot_epochs_except(&self, keep: EpochNumber) -> Result<()> {
        delete_except_epoch(self, SnapshotArtifactCol::CF_NAME, keep.0)?;
        Ok(())
    }
}

fn delete_prefix<S: Store>(store: &TapeStore<S>, cf: &str, prefix: &[u8]) -> Result<()> {
    let raw = store.inner().inner();
    let keys: Vec<Vec<u8>> = raw.iter_prefix(cf, prefix)?.map(|(k, _)| k).collect();
    for key in keys {
        raw.delete(cf, &key)?;
    }
    Ok(())
}

fn delete_except_epoch<S: Store>(store: &TapeStore<S>, cf: &str, keep: u64) -> Result<()> {
    let raw = store.inner().inner();
    let keep_prefix = keep.to_be_bytes();

    let keys: Vec<Vec<u8>> = raw
        .iter(cf)?
        .filter_map(|(k, _)| {
            if k.len() >= 8 && k[..8] == keep_prefix {
                None
            } else {
                Some(k)
            }
        })
        .collect();

    for key in keys {
        raw.delete(cf, &key)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::SnapshotArtifact;
    use store_memory::MemoryStore;
    use tape_core::encoding::EncodingProfile;
    use tape_core::erasure::GROUP_SIZE;
    use tape_core::track::blob::BlobInfo;
    use tape_core::types::{SpoolIndex, StorageUnits, StripeCount};
    use tape_crypto::Hash;

    fn test_store() -> TapeStore<MemoryStore> {
        TapeStore::new(MemoryStore::new())
    }

    fn artifact(tag: u8) -> SnapshotArtifact {
        SnapshotArtifact {
            blob: BlobInfo {
                size: StorageUnits::from_bytes(64),
                commitment: Hash::from([tag; 32]),
                profile: EncodingProfile::default(),
                stripe_size: StorageUnits::from_bytes(64),
                stripe_count: StripeCount(1),
                leaves: [Hash::from([tag; 32]); GROUP_SIZE],
            },
            spool_index: SpoolIndex(tag as u64),
            slice: vec![tag; 32],
        }
    }

    #[test]
    fn artifact_roundtrip() {
        let store = test_store();
        let epoch = EpochNumber(9);
        let group = SpoolGroup(2);
        let chunk = ChunkNumber(4);

        assert!(
            store
                .get_snapshot_artifact(epoch, group, chunk)
                .unwrap()
                .is_none()
        );

        let a = artifact(0xAA);
        store.put_snapshot_artifact(epoch, group, chunk, &a).unwrap();
        assert_eq!(
            store.get_snapshot_artifact(epoch, group, chunk).unwrap().unwrap(),
            a
        );

        store.delete_snapshot_artifact(epoch, group, chunk).unwrap();
        assert!(
            store
                .get_snapshot_artifact(epoch, group, chunk)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn artifacts_iter_by_chunk() {
        let store = test_store();
        let epoch = EpochNumber(9);
        let group = SpoolGroup(2);
        let other_group = SpoolGroup(3);

        store
            .put_snapshot_artifact(epoch, group, ChunkNumber(2), &artifact(0x22))
            .unwrap();
        store
            .put_snapshot_artifact(epoch, group, ChunkNumber(0), &artifact(0x00))
            .unwrap();
        store
            .put_snapshot_artifact(epoch, group, ChunkNumber(1), &artifact(0x11))
            .unwrap();
        store
            .put_snapshot_artifact(epoch, other_group, ChunkNumber(0), &artifact(0x33))
            .unwrap();

        let rows = store.iter_snapshot_artifacts(epoch, group).unwrap();
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0], (ChunkNumber(0), artifact(0x00)));
        assert_eq!(rows[1], (ChunkNumber(1), artifact(0x11)));
        assert_eq!(rows[2], (ChunkNumber(2), artifact(0x22)));
    }

    #[test]
    fn delete_snapshot_epoch_clears_artifacts() {
        let store = test_store();
        let epoch = EpochNumber(10);
        let other = EpochNumber(11);
        let group = SpoolGroup(0);
        let chunk = ChunkNumber(0);

        store
            .put_snapshot_artifact(epoch, group, chunk, &artifact(0x33))
            .unwrap();
        store
            .put_snapshot_artifact(other, group, chunk, &artifact(0x44))
            .unwrap();

        store.delete_snapshot_epoch(epoch).unwrap();

        assert!(
            store
                .get_snapshot_artifact(epoch, group, chunk)
                .unwrap()
                .is_none()
        );
        assert!(
            store
                .get_snapshot_artifact(other, group, chunk)
                .unwrap()
                .is_some()
        );
    }

    #[test]
    fn delete_snapshot_epochs_except_keeps_one() {
        let store = test_store();
        let keep = EpochNumber(20);
        let group = SpoolGroup(0);
        let chunk = ChunkNumber(0);

        for e in [18u64, 19, 20, 21] {
            let epoch = EpochNumber(e);
            store
                .put_snapshot_artifact(epoch, group, chunk, &artifact(e as u8))
                .unwrap();
        }

        store.delete_snapshot_epochs_except(keep).unwrap();

        for e in [18u64, 19, 21] {
            let epoch = EpochNumber(e);
            assert!(
                store
                    .get_snapshot_artifact(epoch, group, chunk)
                    .unwrap()
                    .is_none()
            );
        }

        assert!(
            store
                .get_snapshot_artifact(keep, group, chunk)
                .unwrap()
                .is_some()
        );
    }
}
