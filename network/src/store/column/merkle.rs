use solana_sdk::pubkey::Pubkey;
use crate::store::*;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MerkleCacheKey {
    ZeroValues { address: Pubkey },                     // Merkle tree zero hashes
    UnpackedTapeLayer  { address: Pubkey, layer: u8 },  // Unpacked tape layer nodes
    PackedTapeLayer { address: Pubkey, layer: u8 },     // Packed tape layer nodes
    Raw36([u8; 36]),
}

impl MerkleCacheKey {
    #[inline(always)]
    fn to_vec(&self) -> Vec<u8> {
        match *self {
            MerkleCacheKey::ZeroValues { address } =>
                build_key(&address, MERKLE_ZEROS, 0),

            MerkleCacheKey::UnpackedTapeLayer { address, layer } =>
                build_key(&address, TAPE_LAYER, layer),

            MerkleCacheKey::PackedTapeLayer { address, layer } =>
                build_key(&address, MINER_LAYER, layer),

            MerkleCacheKey::Raw36(bytes) => bytes.to_vec(),
        }
    }
}

pub trait MerkleOps {
    fn get_merkle_cache(&self, key: &MerkleCacheKey) -> Result<Vec<[u8; 32]>, StoreError>;
    fn put_merkle_cache(&self, key: &MerkleCacheKey, values: &[[u8; 32]]) -> Result<(), StoreError>;

    /// Optional: compile-time length check, returns a plain array.
    fn get_merkle_cache_fixed<const N: usize>(&self, key: &MerkleCacheKey)
        -> Result<[[u8; 32]; N], StoreError>
    {
        let v = self.get_merkle_cache(key)?;
        if v.len() != N {
            return Err(StoreError::InvalidHashSize(v.len() * 32));
        }

        let arr: [[u8; 32]; N] = v
            .try_into()
            .map_err(|vv: Vec<[u8; 32]>| StoreError::InvalidHashSize(vv.len() * 32))?;
        Ok(arr)
    }
}

impl MerkleOps for TapeStore {
    fn get_merkle_cache(&self, key: &MerkleCacheKey) -> Result<Vec<[u8; 32]>, StoreError> {
        let cf = self.get_cf_handle(ColumnFamily::MerkleHashes)?;
        let k = key.to_vec();
        let data = self.db.get_cf(&cf, &k)?.ok_or(StoreError::HashNotFound)?;
        if data.len() % 32 != 0 {
            return Err(StoreError::InvalidHashSize(data.len()));
        }
        let mut out = Vec::with_capacity(data.len() / 32);
        for chunk in data.chunks_exact(32) {
            let mut arr = [0u8; 32];
            arr.copy_from_slice(chunk);
            out.push(arr);
        }
        Ok(out)
    }

    fn put_merkle_cache(&self, key: &MerkleCacheKey, values: &[[u8; 32]]) -> Result<(), StoreError> {
        let cf = self.get_cf_handle(ColumnFamily::MerkleHashes)?;
        let k = key.to_vec();
        let data: Vec<u8> = values.iter().flatten().copied().collect();
        self.db.put_cf(&cf, &k, &data)?;
        Ok(())
    }
}

#[inline(always)]
fn build_key(address: &Pubkey, layer_type: u8, layer_id: u8) -> Vec<u8> {
    let mut key = Vec::with_capacity(36);
    key.extend_from_slice(&address.to_bytes()); // 32
    key.push(layer_id);                         // 1
    key.extend_from_slice(&[layer_type, 0, 0]); // 3 (type + padding)
    key
}


#[cfg(test)]
mod tests {
    use super::*;
    use tempdir::TempDir;

    fn setup_store() -> Result<(TapeStore, TempDir), StoreError> {
        let tmp = TempDir::new("rocksdb_test").map_err(StoreError::IoError)?;
        let store = TapeStore::new(tmp.path())?;
        Ok((store, tmp))
    }

    fn h(i: u8) -> [u8; 32] {
        [i; 32]
    }

    #[test]
    fn merkle_zero_values_roundtrip_fixed() -> Result<(), StoreError> {
        let (store, _tmp) = setup_store()?;
        let addr = Pubkey::new_unique();

        // Prepare exactly SECTOR_TREE_HEIGHT hashes
        let mut vals = Vec::<[u8; 32]>::with_capacity(SECTOR_TREE_HEIGHT);
        for i in 0..SECTOR_TREE_HEIGHT {
            vals.push(h(i as u8));
        }

        // Put
        store.put_merkle_cache(
            &MerkleCacheKey::ZeroValues { address: addr },
            &vals,
        )?;

        // Get
        let got: [[u8; 32]; SECTOR_TREE_HEIGHT] = store.get_merkle_cache_fixed(
            &MerkleCacheKey::ZeroValues { address: addr },
        )?;

        assert_eq!(got.as_slice(), vals.as_slice());
        Ok(())
    }

    #[test]
    fn merkle_unpacked_layer_roundtrip_vec() -> Result<(), StoreError> {
        let (store, _tmp) = setup_store()?;
        let addr = Pubkey::new_unique();
        let layer = SECTOR_TREE_HEIGHT as u8;

        // Arbitrary set of hashes for the layer
        let vals = vec![h(1), h(2), h(3), h(4)];

        store.put_merkle_cache(
            &MerkleCacheKey::UnpackedTapeLayer { address: addr, layer },
            &vals,
        )?;

        let got = store.get_merkle_cache(
            &MerkleCacheKey::UnpackedTapeLayer { address: addr, layer },
        )?;

        assert_eq!(got, vals);
        Ok(())
    }

    #[test]
    fn merkle_packed_layer_roundtrip_vec() -> Result<(), StoreError> {
        let (store, _tmp) = setup_store()?;
        let addr = Pubkey::new_unique();
        let layer = (SECTOR_TREE_HEIGHT as u8).saturating_add(1);

        let vals = vec![h(9), h(8)];

        store.put_merkle_cache(
            &MerkleCacheKey::PackedTapeLayer { address: addr, layer },
            &vals,
        )?;

        let got = store.get_merkle_cache(
            &MerkleCacheKey::PackedTapeLayer { address: addr, layer },
        )?;

        assert_eq!(got, vals);
        Ok(())
    }

    #[test]
    fn merkle_raw36_key_roundtrip() -> Result<(), StoreError> {
        let (store, _tmp) = setup_store()?;
        let addr = Pubkey::new_unique();
        let raw_key: [u8; 36] = {
            let v = build_key(&addr, TAPE_LAYER, 7);
            let mut arr = [0u8; 36];
            arr.copy_from_slice(&v);
            arr
        };

        let vals = vec![h(42), h(43), h(44)];

        store.put_merkle_cache(&MerkleCacheKey::Raw36(raw_key), &vals)?;
        let got = store.get_merkle_cache(&MerkleCacheKey::Raw36(raw_key))?;

        assert_eq!(got, vals);
        Ok(())
    }

    #[test]
    fn merkle_not_found_returns_error() -> Result<(), StoreError> {
        let (store, _tmp) = setup_store()?;
        let addr = Pubkey::new_unique();

        let res = store.get_merkle_cache(
            &MerkleCacheKey::UnpackedTapeLayer { address: addr, layer: 0 },
        );

        assert!(matches!(res, Err(StoreError::HashNotFound)));
        Ok(())
    }

    #[test]
    fn merkle_invalid_stored_length_is_rejected() -> Result<(), StoreError> {
        let (store, _tmp) = setup_store()?;
        let addr = Pubkey::new_unique();
        let key_vec = build_key(&addr, TAPE_LAYER, 3);

        // Manually write an invalid-length value (not a multiple of 32)
        let cf = store.get_cf_handle(ColumnFamily::MerkleHashes)?;
        store.db.put_cf(&cf, &key_vec, &[0u8; 5])?;

        let res = store.get_merkle_cache(
            &MerkleCacheKey::UnpackedTapeLayer { address: addr, layer: 3 },
        );

        match res {
            Err(StoreError::InvalidHashSize(len)) => {
                assert_eq!(len, 5);
            }
            other => panic!("expected InvalidHashSize, got {:?}", other),
        }
        Ok(())
    }

    #[test]
    fn merkle_fixed_len_mismatch_is_rejected() -> Result<(), StoreError> {
        let (store, _tmp) = setup_store()?;
        let addr = Pubkey::new_unique();

        // Store 3 hashes
        let vals = vec![h(1), h(2), h(3)];
        store.put_merkle_cache(
            &MerkleCacheKey::PackedTapeLayer { address: addr, layer: 1 },
            &vals,
        )?;

        // Ask for a different fixed size (e.g., 4) => should error
        let res = store.get_merkle_cache_fixed::<4>(
            &MerkleCacheKey::PackedTapeLayer { address: addr, layer: 1 },
        );

        assert!(matches!(res, Err(StoreError::InvalidHashSize(96))));
        Ok(())
    }
}
