//! Generic vote signature operations.

use store::{Column, Store};
use tape_core::bls::BlsSignature;
use tape_core::spooler::SpoolGroup;
use tape_core::system::VoteCandidate;
use tape_core::types::EpochNumber;
use tape_crypto::Address;

use crate::columns::VoteSigCol;
use crate::error::{Result, TapeStoreError};
use crate::types::VoteSigKey;
use crate::TapeStore;

pub trait VoteOps {
    fn put_vote_sig(
        &self,
        candidate: VoteCandidate,
        group: SpoolGroup,
        signer: Address,
        signature: &BlsSignature,
    ) -> Result<()>;

    /// Vote signatures for one candidate and group, ordered by signer address.
    fn iter_vote_sigs(
        &self,
        candidate: VoteCandidate,
        group: SpoolGroup,
    ) -> Result<Vec<(Address, BlsSignature)>>;

    fn delete_vote_epoch(&self, voting_epoch: EpochNumber) -> Result<()>;

    fn delete_vote_epochs_except(&self, keep: EpochNumber) -> Result<()>;
}

impl<S: Store> VoteOps for TapeStore<S> {
    fn put_vote_sig(
        &self,
        candidate: VoteCandidate,
        group: SpoolGroup,
        signer: Address,
        signature: &BlsSignature,
    ) -> Result<()> {
        let key = VoteSigKey::new(candidate, group, signer);
        self.put::<VoteSigCol>(&key, signature)?;
        Ok(())
    }

    fn iter_vote_sigs(
        &self,
        candidate: VoteCandidate,
        group: SpoolGroup,
    ) -> Result<Vec<(Address, BlsSignature)>> {
        let prefix = VoteSigKey::group_prefix(candidate, group);
        let iter = self
            .inner()
            .inner()
            .iter_prefix(VoteSigCol::CF_NAME, &prefix)?;

        let mut out = Vec::new();
        for (key_bytes, value_bytes) in iter {
            let key: VoteSigKey = wincode::deserialize(&key_bytes)
                .map_err(|e| TapeStoreError::Serialization(format!("vote sig key: {e}")))?;
            let signature: BlsSignature = wincode::deserialize(&value_bytes)
                .map_err(|e| TapeStoreError::Serialization(format!("vote sig value: {e}")))?;
            out.push((key.signer, signature));
        }
        Ok(out)
    }

    fn delete_vote_epoch(&self, voting_epoch: EpochNumber) -> Result<()> {
        delete_prefix(
            self,
            VoteSigCol::CF_NAME,
            &VoteSigKey::epoch_prefix(voting_epoch),
        )?;
        Ok(())
    }

    fn delete_vote_epochs_except(&self, keep: EpochNumber) -> Result<()> {
        delete_except_epoch(self, VoteSigCol::CF_NAME, keep)?;
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

fn delete_except_epoch<S: Store>(
    store: &TapeStore<S>,
    cf: &str,
    keep: EpochNumber,
) -> Result<()> {
    let raw = store.inner().inner();
    let keep_prefix = keep.0.to_be_bytes();

    let keys: Vec<Vec<u8>> = raw
        .iter(cf)?
        .filter_map(|(k, _)| {
            if k.len() >= VoteSigKey::EPOCH_PREFIX_SIZE
                && k[..VoteSigKey::EPOCH_PREFIX_SIZE] == keep_prefix
            {
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
    use tape_core::system::VoteKind;
    use tape_crypto::bls12254::min_sig::G1CompressedPoint;
    use tape_crypto::Hash;

    fn test_store() -> TapeStore<MemoryStore> {
        TapeStore::new(MemoryStore::new())
    }

    fn address(byte: u8) -> Address {
        Address::from([byte; 32])
    }

    fn signature(byte: u8) -> BlsSignature {
        BlsSignature(G1CompressedPoint([byte; 32]))
    }

    fn candidate(kind: VoteKind, voting_epoch: u64, target_epoch: u64, hash: u8) -> VoteCandidate {
        VoteCandidate {
            kind,
            voting_epoch: EpochNumber(voting_epoch),
            target_epoch: EpochNumber(target_epoch),
            hash: Hash::from([hash; 32]),
        }
    }

    #[test]
    fn vote_sigs_roundtrip_ordered_by_signer() {
        let store = test_store();
        let candidate = candidate(VoteKind::Snapshot, 11, 10, 0xAA);
        let group = SpoolGroup(4);

        for byte in [3u8, 1, 2] {
            store
                .put_vote_sig(candidate, group, address(byte), &signature(byte))
                .unwrap();
        }

        let rows = store.iter_vote_sigs(candidate, group).unwrap();
        assert_eq!(
            rows,
            vec![
                (address(1), signature(1)),
                (address(2), signature(2)),
                (address(3), signature(3)),
            ]
        );
    }

    #[test]
    fn vote_sigs_are_scoped_by_candidate_and_group() {
        let store = test_store();
        let candidate_a = candidate(VoteKind::Snapshot, 11, 10, 0xAA);
        let candidate_b = candidate(VoteKind::Assignment, 11, 12, 0xBB);

        store
            .put_vote_sig(candidate_a, SpoolGroup(4), address(1), &signature(1))
            .unwrap();
        store
            .put_vote_sig(candidate_a, SpoolGroup(5), address(2), &signature(2))
            .unwrap();
        store
            .put_vote_sig(candidate_b, SpoolGroup(4), address(3), &signature(3))
            .unwrap();

        assert_eq!(
            store.iter_vote_sigs(candidate_a, SpoolGroup(4)).unwrap(),
            vec![(address(1), signature(1))]
        );
        assert_eq!(
            store.iter_vote_sigs(candidate_a, SpoolGroup(5)).unwrap(),
            vec![(address(2), signature(2))]
        );
        assert_eq!(
            store.iter_vote_sigs(candidate_b, SpoolGroup(4)).unwrap(),
            vec![(address(3), signature(3))]
        );
    }

    #[test]
    fn put_vote_sig_overwrites_existing_signer() {
        let store = test_store();
        let candidate = candidate(VoteKind::Snapshot, 11, 10, 0xAA);
        let group = SpoolGroup(4);
        let signer = address(1);

        store
            .put_vote_sig(candidate, group, signer, &signature(1))
            .unwrap();
        store
            .put_vote_sig(candidate, group, signer, &signature(2))
            .unwrap();

        assert_eq!(
            store.iter_vote_sigs(candidate, group).unwrap(),
            vec![(signer, signature(2))]
        );
    }

    #[test]
    fn delete_vote_epoch_clears_only_that_epoch() {
        let store = test_store();
        let old = candidate(VoteKind::Snapshot, 11, 10, 0xAA);
        let keep = candidate(VoteKind::Snapshot, 12, 11, 0xAA);
        let group = SpoolGroup(4);

        store
            .put_vote_sig(old, group, address(1), &signature(1))
            .unwrap();
        store
            .put_vote_sig(keep, group, address(2), &signature(2))
            .unwrap();

        store.delete_vote_epoch(old.voting_epoch).unwrap();

        assert!(store.iter_vote_sigs(old, group).unwrap().is_empty());
        assert_eq!(
            store.iter_vote_sigs(keep, group).unwrap(),
            vec![(address(2), signature(2))]
        );
    }

    #[test]
    fn delete_vote_epochs_except_keeps_one() {
        let store = test_store();
        let keep_epoch = EpochNumber(12);
        let keep = candidate(VoteKind::Snapshot, keep_epoch.0, 11, 0xAA);
        let group = SpoolGroup(4);

        for epoch in [10u64, 11, 12, 13] {
            let candidate = candidate(VoteKind::Snapshot, epoch, epoch - 1, 0xAA);
            store
                .put_vote_sig(candidate, group, address(epoch as u8), &signature(epoch as u8))
                .unwrap();
        }

        store.delete_vote_epochs_except(keep_epoch).unwrap();

        for epoch in [10u64, 11, 13] {
            let candidate = candidate(VoteKind::Snapshot, epoch, epoch - 1, 0xAA);
            assert!(store.iter_vote_sigs(candidate, group).unwrap().is_empty());
        }
        assert_eq!(
            store.iter_vote_sigs(keep, group).unwrap(),
            vec![(address(12), signature(12))]
        );
    }
}
