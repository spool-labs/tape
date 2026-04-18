//! Snapshot coordination operations (partial signatures + build artifacts).
//!
//! These columns are push-driven: peers deliver partial BLS signatures over
//! HTTP, they land here keyed by `(epoch, group, chunk?, bitmap_index)`, and
//! the snapshot manager scans them for quorum. Build artifacts are staged in a
//! separate column until the `WriteSnapshot` event is observed on-chain.

use store::{Column, Store};
use tape_core::spooler::SpoolGroup;
use tape_core::types::{ChunkNumber, EpochNumber};

use crate::columns::{SnapshotArtifactCol, SnapshotFinalizeSigCol, SnapshotWriteSigCol};
use crate::error::{Result, TapeStoreError};
use crate::types::{
    SnapshotArtifact, SnapshotArtifactKey, SnapshotFinalizeSigKey, SnapshotFinalizeVote,
    SnapshotWriteSigKey, SnapshotWriteVote,
};
use crate::TapeStore;

/// All partial write signatures collected for a single chunk, ordered by
/// bitmap index.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ChunkWriteSigs {
    pub chunk: ChunkNumber,
    pub votes: Vec<(u16, SnapshotWriteVote)>,
}

pub trait SnapshotOps {
    fn put_snapshot_write_sig(
        &self,
        epoch: EpochNumber,
        group: SpoolGroup,
        chunk: ChunkNumber,
        bitmap_index: u16,
        vote: &SnapshotWriteVote,
    ) -> Result<()>;

    fn put_snapshot_finalize_sig(
        &self,
        epoch: EpochNumber,
        group: SpoolGroup,
        bitmap_index: u16,
        vote: &SnapshotFinalizeVote,
    ) -> Result<()>;

    /// Write partials for a group, pre-grouped by chunk. Chunks appear in
    /// ascending chunk-number order; votes inside each chunk are in ascending
    /// bitmap-index order.
    fn iter_snapshot_write_sigs(
        &self,
        epoch: EpochNumber,
        group: SpoolGroup,
    ) -> Result<Vec<ChunkWriteSigs>>;

    /// Finalize partials for a group, ordered by bitmap index.
    fn iter_snapshot_finalize_sigs(
        &self,
        epoch: EpochNumber,
        group: SpoolGroup,
    ) -> Result<Vec<(u16, SnapshotFinalizeVote)>>;

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

    fn delete_snapshot_artifact(
        &self,
        epoch: EpochNumber,
        group: SpoolGroup,
        chunk: ChunkNumber,
    ) -> Result<()>;

    /// Delete every signature and artifact row for a single epoch.
    fn delete_snapshot_epoch(&self, epoch: EpochNumber) -> Result<()>;

    /// Delete every signature and artifact row whose epoch is not `keep`.
    fn delete_snapshot_epochs_except(&self, keep: EpochNumber) -> Result<()>;
}

impl<S: Store> SnapshotOps for TapeStore<S> {
    fn put_snapshot_write_sig(
        &self,
        epoch: EpochNumber,
        group: SpoolGroup,
        chunk: ChunkNumber,
        bitmap_index: u16,
        vote: &SnapshotWriteVote,
    ) -> Result<()> {
        let key = SnapshotWriteSigKey::new(epoch.0, group.0, chunk.0, bitmap_index);
        self.put::<SnapshotWriteSigCol>(&key, vote)?;
        Ok(())
    }

    fn put_snapshot_finalize_sig(
        &self,
        epoch: EpochNumber,
        group: SpoolGroup,
        bitmap_index: u16,
        vote: &SnapshotFinalizeVote,
    ) -> Result<()> {
        let key = SnapshotFinalizeSigKey::new(epoch.0, group.0, bitmap_index);
        self.put::<SnapshotFinalizeSigCol>(&key, vote)?;
        Ok(())
    }

    fn iter_snapshot_write_sigs(
        &self,
        epoch: EpochNumber,
        group: SpoolGroup,
    ) -> Result<Vec<ChunkWriteSigs>> {
        let prefix = SnapshotWriteSigKey::group_prefix(epoch.0, group.0);
        let iter = self
            .inner()
            .inner()
            .iter_prefix(SnapshotWriteSigCol::CF_NAME, &prefix)?;

        // Rocksdb key order is BE, so rows arrive sorted by (chunk, bitmap_index).
        // Walk the iterator with a cursor on the chunk field, emit one group per
        // chunk transition.
        let mut out: Vec<ChunkWriteSigs> = Vec::new();
        for (key_bytes, value_bytes) in iter {
            let key: SnapshotWriteSigKey = wincode::deserialize(&key_bytes).map_err(|e| {
                TapeStoreError::Serialization(format!("snapshot write sig key: {e}"))
            })?;
            let vote: SnapshotWriteVote = wincode::deserialize(&value_bytes).map_err(|e| {
                TapeStoreError::Serialization(format!("snapshot write sig value: {e}"))
            })?;

            match out.last_mut() {
                Some(tail) if tail.chunk.0 == key.chunk => tail.votes.push((key.bitmap_index, vote)),
                _ => out.push(ChunkWriteSigs {
                    chunk: ChunkNumber(key.chunk),
                    votes: vec![(key.bitmap_index, vote)],
                }),
            }
        }
        Ok(out)
    }

    fn iter_snapshot_finalize_sigs(
        &self,
        epoch: EpochNumber,
        group: SpoolGroup,
    ) -> Result<Vec<(u16, SnapshotFinalizeVote)>> {
        let prefix = SnapshotFinalizeSigKey::group_prefix(epoch.0, group.0);
        let iter = self
            .inner()
            .inner()
            .iter_prefix(SnapshotFinalizeSigCol::CF_NAME, &prefix)?;

        let mut out = Vec::new();
        for (key_bytes, value_bytes) in iter {
            let key: SnapshotFinalizeSigKey = wincode::deserialize(&key_bytes).map_err(|e| {
                TapeStoreError::Serialization(format!("snapshot finalize sig key: {e}"))
            })?;
            let vote: SnapshotFinalizeVote = wincode::deserialize(&value_bytes).map_err(|e| {
                TapeStoreError::Serialization(format!("snapshot finalize sig value: {e}"))
            })?;
            out.push((key.bitmap_index, vote));
        }
        Ok(out)
    }

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
        delete_prefix(self, SnapshotWriteSigCol::CF_NAME, &SnapshotWriteSigKey::epoch_prefix(epoch.0))?;
        delete_prefix(self, SnapshotFinalizeSigCol::CF_NAME, &SnapshotFinalizeSigKey::epoch_prefix(epoch.0))?;
        delete_prefix(self, SnapshotArtifactCol::CF_NAME, &SnapshotArtifactKey::epoch_prefix(epoch.0))?;
        Ok(())
    }

    fn delete_snapshot_epochs_except(&self, keep: EpochNumber) -> Result<()> {
        delete_except_epoch(self, SnapshotWriteSigCol::CF_NAME, keep.0)?;
        delete_except_epoch(self, SnapshotFinalizeSigCol::CF_NAME, keep.0)?;
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
    use store_memory::MemoryStore;
    use tape_core::bls::BlsSignature;
    use tape_core::cert::{SNAPSHOT_SIGN_MESSAGE_SIZE, SNAPSHOT_WRITE_MESSAGE_SIZE};
    use tape_core::encoding::EncodingProfile;
    use tape_core::erasure::SPOOL_GROUP_SIZE;
    use tape_core::track::blob::BlobInfo;
    use tape_core::types::{StorageUnits, StripeCount};
    use tape_crypto::bls12254::min_sig::G1CompressedPoint;
    use tape_crypto::Hash;

    fn test_store() -> TapeStore<MemoryStore> {
        TapeStore::new(MemoryStore::new())
    }

    fn write_vote(tag: u8) -> SnapshotWriteVote {
        SnapshotWriteVote {
            message: [tag; SNAPSHOT_WRITE_MESSAGE_SIZE],
            signature: BlsSignature(G1CompressedPoint([tag; 32])),
        }
    }

    fn finalize_vote(tag: u8) -> SnapshotFinalizeVote {
        SnapshotFinalizeVote {
            message: [tag; SNAPSHOT_SIGN_MESSAGE_SIZE],
            signature: BlsSignature(G1CompressedPoint([tag; 32])),
        }
    }

    fn artifact(tag: u8) -> SnapshotArtifact {
        SnapshotArtifact {
            blob: BlobInfo {
                size: StorageUnits::from_bytes(64),
                commitment: Hash::from([tag; 32]),
                profile: EncodingProfile::default(),
                stripe_size: StorageUnits::from_bytes(64),
                stripe_count: StripeCount(1),
                leaves: [Hash::from([tag; 32]); SPOOL_GROUP_SIZE],
            },
            local_slice: vec![tag; 32],
        }
    }

    #[test]
    fn write_sigs_group_by_chunk() {
        let store = test_store();
        let epoch = EpochNumber(7);
        let group = SpoolGroup(3);

        // Insert out of order to make sure the iter sorts correctly.
        for (chunk, idx) in [(1u64, 2u16), (0, 1), (1, 0), (0, 0), (1, 1)] {
            store
                .put_snapshot_write_sig(
                    epoch,
                    group,
                    ChunkNumber(chunk),
                    idx,
                    &write_vote((chunk as u8) << 4 | idx as u8),
                )
                .unwrap();
        }

        let out = store.iter_snapshot_write_sigs(epoch, group).unwrap();
        assert_eq!(out.len(), 2);

        assert_eq!(out[0].chunk, ChunkNumber(0));
        assert_eq!(out[0].votes.len(), 2);
        assert_eq!(out[0].votes[0].0, 0);
        assert_eq!(out[0].votes[1].0, 1);

        assert_eq!(out[1].chunk, ChunkNumber(1));
        assert_eq!(out[1].votes.len(), 3);
        assert_eq!(out[1].votes[0].0, 0);
        assert_eq!(out[1].votes[1].0, 1);
        assert_eq!(out[1].votes[2].0, 2);
    }

    #[test]
    fn finalize_sigs_roundtrip() {
        let store = test_store();
        let epoch = EpochNumber(7);
        let group = SpoolGroup(3);

        for i in 0u16..3 {
            store
                .put_snapshot_finalize_sig(epoch, group, i, &finalize_vote(i as u8))
                .unwrap();
        }

        let rows = store.iter_snapshot_finalize_sigs(epoch, group).unwrap();
        assert_eq!(rows.len(), 3);
        for (i, (idx, _)) in rows.iter().enumerate() {
            assert_eq!(*idx, i as u16);
        }
    }

    #[test]
    fn artifact_roundtrip() {
        let store = test_store();
        let epoch = EpochNumber(9);
        let group = SpoolGroup(2);
        let chunk = ChunkNumber(4);

        assert!(store
            .get_snapshot_artifact(epoch, group, chunk)
            .unwrap()
            .is_none());

        let a = artifact(0xAA);
        store.put_snapshot_artifact(epoch, group, chunk, &a).unwrap();
        assert_eq!(
            store.get_snapshot_artifact(epoch, group, chunk).unwrap().unwrap(),
            a
        );

        store.delete_snapshot_artifact(epoch, group, chunk).unwrap();
        assert!(store
            .get_snapshot_artifact(epoch, group, chunk)
            .unwrap()
            .is_none());
    }

    #[test]
    fn delete_snapshot_epoch_clears_all_cfs() {
        let store = test_store();
        let epoch = EpochNumber(10);
        let other = EpochNumber(11);
        let group = SpoolGroup(0);
        let chunk = ChunkNumber(0);

        store
            .put_snapshot_write_sig(epoch, group, chunk, 0, &write_vote(0x11))
            .unwrap();
        store
            .put_snapshot_finalize_sig(epoch, group, 0, &finalize_vote(0x22))
            .unwrap();
        store
            .put_snapshot_artifact(epoch, group, chunk, &artifact(0x33))
            .unwrap();

        store
            .put_snapshot_write_sig(other, group, chunk, 0, &write_vote(0x44))
            .unwrap();

        store.delete_snapshot_epoch(epoch).unwrap();

        assert!(store.iter_snapshot_write_sigs(epoch, group).unwrap().is_empty());
        assert!(store.iter_snapshot_finalize_sigs(epoch, group).unwrap().is_empty());
        assert!(store
            .get_snapshot_artifact(epoch, group, chunk)
            .unwrap()
            .is_none());
        assert_eq!(
            store.iter_snapshot_write_sigs(other, group).unwrap().len(),
            1
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
                .put_snapshot_write_sig(epoch, group, chunk, 0, &write_vote(e as u8))
                .unwrap();
            store
                .put_snapshot_finalize_sig(epoch, group, 0, &finalize_vote(e as u8))
                .unwrap();
            store
                .put_snapshot_artifact(epoch, group, chunk, &artifact(e as u8))
                .unwrap();
        }

        store.delete_snapshot_epochs_except(keep).unwrap();

        for e in [18u64, 19, 21] {
            let epoch = EpochNumber(e);
            assert!(store.iter_snapshot_write_sigs(epoch, group).unwrap().is_empty());
            assert!(store.iter_snapshot_finalize_sigs(epoch, group).unwrap().is_empty());
            assert!(store
                .get_snapshot_artifact(epoch, group, chunk)
                .unwrap()
                .is_none());
        }

        assert_eq!(store.iter_snapshot_write_sigs(keep, group).unwrap().len(), 1);
        assert_eq!(store.iter_snapshot_finalize_sigs(keep, group).unwrap().len(), 1);
        assert!(store
            .get_snapshot_artifact(keep, group, chunk)
            .unwrap()
            .is_some());
    }
}
