use solana_sdk::pubkey::Pubkey;
use rocksdb::WriteBatch;
use bytemuck::bytes_of;
use tape_api::consts::PACKED_SEGMENT_SIZE;
use crate::store::*;
use crate::metrics::inc_total_segments_written;

pub trait SegmentOps {
    fn get_segment(&self, tape_address: &Pubkey, global_seg_idx: u64) -> Result<Vec<u8>, StoreError>;
    fn put_segment(&self, tape_address: &Pubkey, global_seg_idx: u64, seg: Vec<u8>) -> Result<(), StoreError>;
    fn get_tape_segments(&self, tape_address: &Pubkey) -> Result<Vec<(u64, Vec<u8>)>, StoreError>;
    fn get_segment_count(&self, tape: &Pubkey) -> Result<u64, StoreError>;
}

impl SegmentOps for TapeStore {
    fn get_segment(&self, tape_address: &Pubkey, global_seg_idx: u64) -> Result<Vec<u8>, StoreError> {
        let sector_number = global_seg_idx / SECTOR_LEAVES as u64;
        let local_seg_idx = (global_seg_idx % SECTOR_LEAVES as u64) as usize;
        
        let sector = self.get_sector(tape_address, sector_number)?;
        
        // Check bitmap
        let bitmap_idx = local_seg_idx / 8;
        let bit_pos = local_seg_idx % 8;
        if (sector.0[bitmap_idx] & (1 << bit_pos)) == 0 {
            return Err(StoreError::SegmentNotFoundForAddress(tape_address.to_string(), global_seg_idx));
        }
        
        let seg_start = SECTOR_HEADER_BYTES + local_seg_idx * PACKED_SEGMENT_SIZE;
        Ok(sector.0[seg_start..seg_start + PACKED_SEGMENT_SIZE].to_vec())
    }

    fn put_segment(&self, tape_address: &Pubkey, global_seg_idx: u64, seg: Vec<u8>) -> Result<(), StoreError> {
        if seg.len() != PACKED_SEGMENT_SIZE {
            return Err(StoreError::InvalidSegmentSize(seg.len()));
        }
        
        let sector_number = global_seg_idx / SECTOR_LEAVES as u64;
        let local_seg_idx = (global_seg_idx % SECTOR_LEAVES as u64) as usize;
        
        let cf_sectors = self.get_cf_handle(ColumnFamily::Sectors)?;
        let cf_tape_segments = self.get_cf_handle(ColumnFamily::TapeSegments)?;
        
        let mut sector = self.get_sector(tape_address, sector_number).unwrap_or_else(|_| Sector::new());
        let is_new_segment = sector.set_segment(local_seg_idx, &seg);
        
        let mut batch = WriteBatch::default();
        let mut key = Vec::with_capacity(TAPE_STORE_SLOTS_KEY_SIZE);
        key.extend_from_slice(&tape_address.to_bytes());
        key.extend_from_slice(&sector_number.to_be_bytes());
        batch.put_cf(&cf_sectors, &key, bytes_of(&sector));
        
        if is_new_segment {
            let current_count = self.get_segment_count(tape_address).unwrap_or(0);
            batch.put_cf(&cf_tape_segments, tape_address.to_bytes(), (current_count + 1).to_be_bytes());
        }
        
        self.db.write(batch)?;
        inc_total_segments_written();
        Ok(())
    }

    fn get_tape_segments(&self, tape_address: &Pubkey) -> Result<Vec<(u64, Vec<u8>)>, StoreError> {
        let cf = self.get_cf_handle(ColumnFamily::Sectors)?;
        let prefix = tape_address.to_bytes().to_vec();
        let iter = self.db.prefix_iterator_cf(&cf, &prefix);
        let mut segments = Vec::new();

        for item in iter {
            let (key, data) = item?;
            if key.len() < TAPE_STORE_SLOTS_KEY_SIZE {
                continue;
            }
            let sector_number = u64::from_be_bytes(key[key.len() - 8..].try_into().unwrap());
            
            let sector: Sector = *bytemuck::try_from_bytes(&data)
                .map_err(|_| StoreError::InvalidSectorSize(data.len()))?;
            
            for local_idx in 0..SECTOR_LEAVES {
                if let Some(segment_data) = sector.get_segment(local_idx) {
                    let global_index = sector_number * SECTOR_LEAVES as u64 + local_idx as u64;
                    segments.push((global_index, segment_data.to_vec()));
                }
            }
        }

        segments.sort_by_key(|(idx, _)| *idx);
        Ok(segments)
    }

    fn get_segment_count(&self, tape: &Pubkey) -> Result<u64, StoreError> {
        let cf = self.get_cf_handle(ColumnFamily::TapeSegments)?;
        let count_bytes = self
            .db
            .get_cf(&cf, tape.to_bytes())?
            .unwrap_or_else(|| vec![0; 8]);
        Ok(u64::from_be_bytes(count_bytes[..].try_into().unwrap()))
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use solana_sdk::pubkey::Pubkey;
    use tempdir::TempDir;

    fn setup_store() -> Result<(TapeStore, TempDir), StoreError> {
        let temp_dir = TempDir::new("rocksdb_test").map_err(StoreError::IoError)?;
        let store = TapeStore::new(temp_dir.path())?;
        Ok((store, temp_dir))
    }

    fn make_data(marker: u8) -> Vec<u8> {
        vec![marker; PACKED_SEGMENT_SIZE]
    }

    #[test]
    fn test_put_segment() -> Result<(), StoreError> {
        let (store, _temp_dir) = setup_store()?;
        let address = Pubkey::new_unique();
        let global_seg_idx = 0;
        let data = make_data(42);

        store.put_segment(&address, global_seg_idx, data.clone())?;
        let retrieved_data = store.get_segment(&address, global_seg_idx)?;
        assert_eq!(retrieved_data, data);
        Ok(())
    }

    #[test]
    fn test_put_segment_count() -> Result<(), StoreError> {
        let (store, _temp_dir) = setup_store()?;
        let address = Pubkey::new_unique();
        let data = make_data(42);

        // Write two new segments
        store.put_segment(&address, 0, data.clone())?;
        store.put_segment(&address, 1, data.clone())?;
        assert_eq!(store.get_segment_count(&address)?, 2);

        // Overwrite existing segment (should not increment count)
        store.put_segment(&address, 0, data.clone())?;
        assert_eq!(store.get_segment_count(&address)?, 2);

        // Write new segment
        store.put_segment(&address, 2, data)?;
        assert_eq!(store.get_segment_count(&address)?, 3);
        Ok(())
    }

    #[test]
    fn test_get_tape_segments() -> Result<(), StoreError> {
        let (store, _tmp) = setup_store()?;
        let address = Pubkey::new_unique();

        // Pick a few scattered indices across 3 sectors
        let idx_sector0_a = 0u64;
        let idx_sector0_b = 5u64;
        let idx_sector1_a = SECTOR_LEAVES as u64; // first in sector 1
        let idx_sector1_b = SECTOR_LEAVES as u64 + 10;
        let idx_sector2_a = SECTOR_LEAVES as u64 * 2; // first in sector 2

        // Write them
        store.put_segment(&address, idx_sector0_a, make_data(10))?;
        store.put_segment(&address, idx_sector0_b, make_data(20))?;
        store.put_segment(&address, idx_sector1_a, make_data(30))?;
        store.put_segment(&address, idx_sector1_b, make_data(40))?;
        store.put_segment(&address, idx_sector2_a, make_data(50))?;

        // Read back
        let segments = store.get_tape_segments(&address)?;

        // We expect exactly 5 entries in ascending global index order
        let expected_indices = [idx_sector0_a,
            idx_sector0_b,
            idx_sector1_a,
            idx_sector1_b,
            idx_sector2_a];
        assert_eq!(segments.len(), expected_indices.len());
        for (i, (idx, data)) in segments.iter().enumerate() {
            assert_eq!(*idx, expected_indices[i], "segment index mismatch");
            assert_eq!(data[0], (i as u8 + 1) * 10, "segment data mismatch at index {}", idx);
            assert_eq!(data.len(), PACKED_SEGMENT_SIZE);
        }

        // Check that sector count is 3
        assert_eq!(store.get_sector_count(&address)?, 3);

        Ok(())
    }
}
