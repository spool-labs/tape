//! Snapshot coordination operations.

use tape_core::bls::BlsSignature;
use tape_core::spooler::SpoolGroup;
use tape_core::types::{ChunkNumber, EpochNumber};
use tape_crypto::address::Address;
use store::{Column, Store};

use crate::columns::{SnapshotArtifactCol, SnapshotFinalizeSigCol, SnapshotWriteSigCol};
use crate::error::{Result, TapeStoreError};
use crate::types::{SnapshotArtifact, SnapshotArtifactKey, SnapshotFinalizeSigKey, SnapshotWriteSigKey};
use crate::TapeStore;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SnapshotGroupProgress {
    pub built: usize,
    pub written: usize,
}

impl SnapshotGroupProgress {
    pub fn is_empty(self) -> bool {
        self.built == 0
    }

    pub fn is_complete(self) -> bool {
        self.built > 0 && self.built == self.written
    }
}

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

    fn mark_snapshot_artifact_written(
        &self,
        epoch: EpochNumber,
        group: SpoolGroup,
        chunk: ChunkNumber,
        track: Address,
    ) -> Result<Option<SnapshotArtifact>>;

    fn delete_snapshot_artifact(
        &self,
        epoch: EpochNumber,
        group: SpoolGroup,
        chunk: ChunkNumber,
    ) -> Result<()>;

    fn iter_snapshot_artifacts(
        &self,
        epoch: EpochNumber,
        group: SpoolGroup,
    ) -> Result<Vec<(ChunkNumber, SnapshotArtifact)>>;

    fn snapshot_group_progress(
        &self,
        epoch: EpochNumber,
        group: SpoolGroup,
    ) -> Result<SnapshotGroupProgress>;

    fn put_snapshot_write_sig(
        &self,
        epoch: EpochNumber,
        group: SpoolGroup,
        chunk: ChunkNumber,
        bitmap_index: u16,
        signature: &BlsSignature,
    ) -> Result<()>;

    fn iter_snapshot_write_sigs(
        &self,
        epoch: EpochNumber,
        group: SpoolGroup,
        chunk: ChunkNumber,
    ) -> Result<Vec<(u16, BlsSignature)>>;

    fn count_snapshot_write_sigs(
        &self,
        epoch: EpochNumber,
        group: SpoolGroup,
        chunk: ChunkNumber,
    ) -> Result<usize>;

    fn put_snapshot_finalize_sig(
        &self,
        epoch: EpochNumber,
        group: SpoolGroup,
        bitmap_index: u16,
        signature: &BlsSignature,
    ) -> Result<()>;

    fn iter_snapshot_finalize_sigs(
        &self,
        epoch: EpochNumber,
        group: SpoolGroup,
    ) -> Result<Vec<(u16, BlsSignature)>>;

    fn count_snapshot_finalize_sigs(
        &self,
        epoch: EpochNumber,
        group: SpoolGroup,
    ) -> Result<usize>;

    fn delete_snapshot_epoch(&self, epoch: EpochNumber) -> Result<()>;

    fn delete_snapshot_epochs_except(&self, keep_epoch: EpochNumber) -> Result<()>;

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

    fn mark_snapshot_artifact_written(
        &self,
        epoch: EpochNumber,
        group: SpoolGroup,
        chunk: ChunkNumber,
        track: Address,
    ) -> Result<Option<SnapshotArtifact>> {
        let key = SnapshotArtifactKey::new(epoch.0, group.0, chunk.0);
        let Some(mut artifact) = self.get::<SnapshotArtifactCol>(&key)? else {
            return Ok(None);
        };

        if artifact.written_track.is_some() {
            return Ok(Some(artifact));
        }

        let staged = artifact.clone();
        artifact.written_track = Some(track);
        artifact.local_slice.clear();
        self.put::<SnapshotArtifactCol>(&key, &artifact)?;

        Ok(Some(staged))
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
            let key: SnapshotArtifactKey = wincode::deserialize(&key_bytes)
                .map_err(|e| TapeStoreError::Serialization(format!("snapshot artifact key: {e}")))?;
            let artifact: SnapshotArtifact = wincode::deserialize(&value_bytes)
                .map_err(|e| TapeStoreError::Serialization(format!("snapshot artifact value: {e}")))?;
            out.push((ChunkNumber(key.chunk), artifact));
        }
        Ok(out)
    }

    fn snapshot_group_progress(
        &self,
        epoch: EpochNumber,
        group: SpoolGroup,
    ) -> Result<SnapshotGroupProgress> {
        let mut progress = SnapshotGroupProgress::default();
        for (_, artifact) in self.iter_snapshot_artifacts(epoch, group)? {
            progress.built += 1;
            if artifact.is_written() {
                progress.written += 1;
            }
        }
        Ok(progress)
    }

    fn put_snapshot_write_sig(
        &self,
        epoch: EpochNumber,
        group: SpoolGroup,
        chunk: ChunkNumber,
        bitmap_index: u16,
        signature: &BlsSignature,
    ) -> Result<()> {
        let key = SnapshotWriteSigKey::new(epoch.0, group.0, chunk.0, bitmap_index);
        self.put::<SnapshotWriteSigCol>(&key, signature)?;
        Ok(())
    }

    fn iter_snapshot_write_sigs(
        &self,
        epoch: EpochNumber,
        group: SpoolGroup,
        chunk: ChunkNumber,
    ) -> Result<Vec<(u16, BlsSignature)>> {
        let prefix = SnapshotWriteSigKey::chunk_prefix(epoch.0, group.0, chunk.0);
        let iter = self
            .inner()
            .inner()
            .iter_prefix(SnapshotWriteSigCol::CF_NAME, &prefix)?;

        let mut out = Vec::new();
        for (key_bytes, value_bytes) in iter {
            let key: SnapshotWriteSigKey = wincode::deserialize(&key_bytes)
                .map_err(|e| TapeStoreError::Serialization(format!("snapshot write key: {e}")))?;
            let signature: BlsSignature = wincode::deserialize(&value_bytes)
                .map_err(|e| TapeStoreError::Serialization(format!("snapshot write value: {e}")))?;
            out.push((key.bitmap_index, signature));
        }
        Ok(out)
    }

    fn count_snapshot_write_sigs(
        &self,
        epoch: EpochNumber,
        group: SpoolGroup,
        chunk: ChunkNumber,
    ) -> Result<usize> {
        let prefix = SnapshotWriteSigKey::chunk_prefix(epoch.0, group.0, chunk.0);
        Ok(self
            .inner()
            .inner()
            .iter_prefix(SnapshotWriteSigCol::CF_NAME, &prefix)?
            .count())
    }

    fn put_snapshot_finalize_sig(
        &self,
        epoch: EpochNumber,
        group: SpoolGroup,
        bitmap_index: u16,
        signature: &BlsSignature,
    ) -> Result<()> {
        let key = SnapshotFinalizeSigKey::new(epoch.0, group.0, bitmap_index);
        self.put::<SnapshotFinalizeSigCol>(&key, signature)?;
        Ok(())
    }

    fn iter_snapshot_finalize_sigs(
        &self,
        epoch: EpochNumber,
        group: SpoolGroup,
    ) -> Result<Vec<(u16, BlsSignature)>> {
        let prefix = SnapshotFinalizeSigKey::group_prefix(epoch.0, group.0);
        let iter = self
            .inner()
            .inner()
            .iter_prefix(SnapshotFinalizeSigCol::CF_NAME, &prefix)?;

        let mut out = Vec::new();
        for (key_bytes, value_bytes) in iter {
            let key: SnapshotFinalizeSigKey = wincode::deserialize(&key_bytes)
                .map_err(|e| TapeStoreError::Serialization(format!("snapshot finalize key: {e}")))?;
            let signature: BlsSignature = wincode::deserialize(&value_bytes)
                .map_err(|e| TapeStoreError::Serialization(format!("snapshot finalize value: {e}")))?;
            out.push((key.bitmap_index, signature));
        }
        Ok(out)
    }

    fn count_snapshot_finalize_sigs(
        &self,
        epoch: EpochNumber,
        group: SpoolGroup,
    ) -> Result<usize> {
        let prefix = SnapshotFinalizeSigKey::group_prefix(epoch.0, group.0);
        Ok(self
            .inner()
            .inner()
            .iter_prefix(SnapshotFinalizeSigCol::CF_NAME, &prefix)?
            .count())
    }

    fn delete_snapshot_epoch(&self, epoch: EpochNumber) -> Result<()> {
        delete_prefix(self, SnapshotWriteSigCol::CF_NAME, &SnapshotWriteSigKey::epoch_prefix(epoch.0))?;
        delete_prefix(
            self,
            SnapshotFinalizeSigCol::CF_NAME,
            &SnapshotFinalizeSigKey::epoch_prefix(epoch.0),
        )?;
        delete_prefix(
            self,
            SnapshotArtifactCol::CF_NAME,
            &SnapshotArtifactKey::epoch_prefix(epoch.0),
        )?;
        Ok(())
    }

    fn delete_snapshot_epochs_except(&self, keep_epoch: EpochNumber) -> Result<()> {
        delete_all_except_epoch::<S, SnapshotWriteSigKey>(
            self,
            SnapshotWriteSigCol::CF_NAME,
            keep_epoch,
        )?;
        delete_all_except_epoch::<S, SnapshotFinalizeSigKey>(
            self,
            SnapshotFinalizeSigCol::CF_NAME,
            keep_epoch,
        )?;
        delete_all_except_epoch::<S, SnapshotArtifactKey>(
            self,
            SnapshotArtifactCol::CF_NAME,
            keep_epoch,
        )?;
        Ok(())
    }
}

fn delete_prefix<S: Store>(store: &TapeStore<S>, cf_name: &str, prefix: &[u8]) -> Result<()> {
    let raw = store.inner().inner();
    let keys: Vec<Vec<u8>> = raw.iter_prefix(cf_name, prefix)?.map(|(key, _)| key).collect();
    for key in keys {
        raw.delete(cf_name, &key)?;
    }
    Ok(())
}

fn delete_all_except_epoch<S: Store, K>(
    store: &TapeStore<S>,
    cf_name: &str,
    keep_epoch: EpochNumber,
) -> Result<()>
where
    K: for<'de> wincode::SchemaRead<'de, Dst = K> + wincode::SchemaWrite<Src = K> + SnapshotEpochKey,
{
    let raw = store.inner().inner();
    let keys: Vec<Vec<u8>> = raw
        .iter(cf_name)?
        .filter_map(|(key, _)| {
            let decoded: K = wincode::deserialize(&key).ok()?;
            (decoded.epoch() != keep_epoch.0).then_some(key)
        })
        .collect();

    for key in keys {
        raw.delete(cf_name, &key)?;
    }
    Ok(())
}

trait SnapshotEpochKey {
    fn epoch(&self) -> u64;
}

impl SnapshotEpochKey for SnapshotWriteSigKey {
    fn epoch(&self) -> u64 {
        self.epoch
    }
}

impl SnapshotEpochKey for SnapshotFinalizeSigKey {
    fn epoch(&self) -> u64 {
        self.epoch
    }
}

impl SnapshotEpochKey for SnapshotArtifactKey {
    fn epoch(&self) -> u64 {
        self.epoch
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use store_memory::MemoryStore;
    use tape_core::bls::BlsPrivateKey;
    use tape_core::encoding::EncodingProfile;
    use tape_core::erasure::SPOOL_GROUP_SIZE;
    use tape_core::types::{StorageUnits, StripeCount};
    use tape_crypto::Hash;

    fn test_store() -> TapeStore<MemoryStore> {
        TapeStore::new(MemoryStore::new())
    }

    fn sample_artifact() -> SnapshotArtifact {
        SnapshotArtifact {
            blob: tape_core::track::blob::BlobInfo {
                size: StorageUnits::from_bytes(2_048),
                commitment: Hash::from([0xAB; 32]),
                profile: EncodingProfile::basic_default(),
                stripe_size: StorageUnits::from_bytes(512),
                stripe_count: StripeCount(4),
                leaves: [Hash::from([0x44; 32]); SPOOL_GROUP_SIZE],
            },
            local_slice: vec![7u8; 32],
            written_track: None,
        }
    }

    fn sample_sig(message: &[u8]) -> BlsSignature {
        BlsPrivateKey::from_random().sign(message).unwrap()
    }

    #[test]
    fn artifacts_roundtrip_and_progress() {
        let store = test_store();
        let epoch = EpochNumber(9);
        let group = SpoolGroup(3);
        let chunk = ChunkNumber(1);

        store
            .put_snapshot_artifact(epoch, group, chunk, &sample_artifact())
            .unwrap();

        let progress = store.snapshot_group_progress(epoch, group).unwrap();
        assert_eq!(progress, SnapshotGroupProgress { built: 1, written: 0 });

        let written = store
            .mark_snapshot_artifact_written(epoch, group, chunk, Address::from([0x11; 32]))
            .unwrap()
            .unwrap();
        assert_eq!(written.local_slice, vec![7u8; 32]);
        assert_eq!(written.written_track, None);

        let persisted = store
            .get_snapshot_artifact(epoch, group, chunk)
            .unwrap()
            .unwrap();
        assert_eq!(persisted.written_track, Some(Address::from([0x11; 32])));
        assert!(persisted.local_slice.is_empty());

        let progress = store.snapshot_group_progress(epoch, group).unwrap();
        assert!(progress.is_complete());
    }

    #[test]
    fn signature_iters_are_prefix_scoped() {
        let store = test_store();
        let epoch = EpochNumber(5);
        let group = SpoolGroup(4);
        let chunk = ChunkNumber(2);

        store
            .put_snapshot_write_sig(epoch, group, chunk, 1, &sample_sig(b"write-1"))
            .unwrap();
        store
            .put_snapshot_write_sig(epoch, group, chunk, 7, &sample_sig(b"write-7"))
            .unwrap();
        store
            .put_snapshot_finalize_sig(epoch, group, 3, &sample_sig(b"finalize-3"))
            .unwrap();

        assert_eq!(store.count_snapshot_write_sigs(epoch, group, chunk).unwrap(), 2);
        assert_eq!(store.count_snapshot_finalize_sigs(epoch, group).unwrap(), 1);
    }
}
