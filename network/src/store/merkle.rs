use solana_sdk::pubkey::Pubkey;
use super::{consts::*, TapeStore, error::StoreError, layout::ColumnFamily};

pub trait MerkleOps {
    fn build_key(&self, address: &Pubkey, layer_type: u8, layer_id: u8) -> Vec<u8>;
    fn get_hashes(&self, address: &Pubkey, layer_type: u8, layer_id: u8) -> Result<Vec<[u8; 32]>, StoreError>;
    fn put_hashes(&self, address: &Pubkey, hashes: &[[u8; 32]], layer_type: u8, layer_id: u8) -> Result<(), StoreError>;
    fn get_zeros(&self, address: &Pubkey) -> Result<Vec<[u8; 32]>, StoreError>;
    fn put_zeros(&self, address: &Pubkey, seeds: &[[u8; 32]]) -> Result<(), StoreError>;
    fn get_l13m(&self, tape_address: &Pubkey) -> Result<Vec<[u8; 32]>, StoreError>;
    fn put_l13m(&self, tape_address: &Pubkey, l13: &[[u8; 32]]) -> Result<(), StoreError>;
    fn get_l13t(&self, tape_address: &Pubkey) -> Result<Vec<[u8; 32]>, StoreError>;
    fn put_l13t(&self, tape_address: &Pubkey, l13: &[[u8; 32]]) -> Result<(), StoreError>;
}

impl MerkleOps for TapeStore {
    fn build_key(&self, address: &Pubkey, layer_type: u8, layer_id: u8) -> Vec<u8> {
        let mut key = Vec::with_capacity(36);
        key.extend_from_slice(&address.to_bytes());
        key.push(layer_id); // Note: layer_id comes before layer_type in key structure
        key.extend_from_slice(&[layer_type, 0, 0]);
        key
    }

    fn get_hashes(&self, address: &Pubkey, layer_type: u8, layer_id: u8) -> Result<Vec<[u8; 32]>, StoreError> {
        let cf = self.get_cf_handle(ColumnFamily::MerkleHashes)?;
        let key = self.build_key(address, layer_type, layer_id);

        let data = self
            .db
            .get_cf(&cf, &key)?
            .ok_or_else(|| StoreError::ValueNotFoundForAddress(address.to_string()))?;

        let mut result = vec![];
        for chunk in data.chunks_exact(32) {
            let mut arr = [0u8; 32];
            arr.copy_from_slice(chunk);
            result.push(arr);
        }
        Ok(result)
    }

    fn put_hashes(&self, address: &Pubkey, hashes: &[[u8; 32]], layer_type: u8, layer_id: u8) -> Result<(), StoreError> {
        let cf = self.get_cf_handle(ColumnFamily::MerkleHashes)?;
        let key = self.build_key(address, layer_type, layer_id);

        let data = hashes.iter().flatten().copied().collect::<Vec<u8>>();
        self.db.put_cf(&cf, &key, &data)?;
        Ok(())
    }

    fn get_zeros(&self, address: &Pubkey) -> Result<Vec<[u8; 32]>, StoreError> {
        self.get_hashes(address, MERKLE_ZEROS, 0)
    }

    fn put_zeros(&self, address: &Pubkey, seeds: &[[u8; 32]]) -> Result<(), StoreError> {
        self.put_hashes(address, seeds, MERKLE_ZEROS, 0)
    }

    fn get_l13m(&self, tape_address: &Pubkey) -> Result<Vec<[u8; 32]>, StoreError> {
        self.get_hashes(tape_address, MINER_LAYER, 13)
    }

    fn put_l13m(&self, tape_address: &Pubkey, l13: &[[u8; 32]]) -> Result<(), StoreError> {
        self.put_hashes(tape_address, l13, MINER_LAYER, 13)
    }

    fn get_l13t(&self, tape_address: &Pubkey) -> Result<Vec<[u8; 32]>, StoreError> {
        self.get_hashes(tape_address, TAPE_LAYER, 13)
    }

    fn put_l13t(&self, tape_address: &Pubkey, l13: &[[u8; 32]]) -> Result<(), StoreError> {
        self.put_hashes(tape_address, l13, TAPE_LAYER, 13)
    }
}
