use solana_sdk::pubkey::Pubkey;
use crate::store::*;

pub trait MerkleOps {
    fn get_hash_values(&self, key: Vec<u8>) -> Result<Vec<[u8; 32]>, StoreError>;
    fn put_hash_values(&self, key: Vec<u8>, values: &[[u8; 32]]) -> Result<(), StoreError>;

    fn get_zero_values(&self, address: &Pubkey) -> Result<Vec<[u8; 32]>, StoreError>;
    fn put_zero_values(&self, address: &Pubkey, values: &[[u8; 32]]) -> Result<(), StoreError>;

    fn get_layer(&self, tape_address: &Pubkey, layer:u8) -> Result<Vec<[u8; 32]>, StoreError>;
    fn put_layer(&self, tape_address: &Pubkey, layer:u8, values: &[[u8; 32]]) -> Result<(), StoreError>;
}

impl MerkleOps for TapeStore {
    fn get_hash_values(&self, key: Vec<u8>) -> Result<Vec<[u8; 32]>, StoreError> {
        let cf = self.get_cf_handle(ColumnFamily::MerkleHashes)?;
        let data = self
            .db
            .get_cf(&cf, &key)?
            .ok_or_else(|| StoreError::HashNotFound)?;

        let mut result = vec![];
        for chunk in data.chunks_exact(32) {
            let mut arr = [0u8; 32];
            arr.copy_from_slice(chunk);
            result.push(arr);
        }
        Ok(result)
    }

    fn put_hash_values(&self, key: Vec<u8>, values: &[[u8; 32]]) -> Result<(), StoreError> {
        let cf = self.get_cf_handle(ColumnFamily::MerkleHashes)?;

        let data = values.iter().flatten().copied().collect::<Vec<u8>>();
        self.db.put_cf(&cf, &key, &data)?;
        Ok(())
    }

    fn get_zero_values(&self, address: &Pubkey) -> Result<Vec<[u8; 32]>, StoreError> {
        let key = build_key(address, MERKLE_ZEROS, 0);
        self.get_hash_values(key)
    }

    fn put_zero_values(&self, address: &Pubkey, values: &[[u8; 32]]) -> Result<(), StoreError> {
        let key = build_key(address, MERKLE_ZEROS, 0);
        self.put_hash_values(key, values)
    }

    fn get_layer(&self, address: &Pubkey, layer: u8) -> Result<Vec<[u8; 32]>, StoreError> {
        let key = build_key(address, TAPE_LAYER, layer);
        self.get_hash_values(key)
    }

    fn put_layer(&self, address: &Pubkey, layer: u8, values: &[[u8; 32]]) -> Result<(), StoreError> {
        let key = build_key(address, TAPE_LAYER, layer);
        self.put_hash_values(key, values)
    }
}

#[inline(always)]
fn build_key(address: &Pubkey, layer_type: u8, layer_id: u8) -> Vec<u8> {
    let mut key = Vec::with_capacity(36);
    key.extend_from_slice(&address.to_bytes());
    key.push(layer_id);
    key.extend_from_slice(&[layer_type, 0, 0]);
    key
}
