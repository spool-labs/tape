use solana_sdk::pubkey::Pubkey;
use tape_api::consts::PACKED_SEGMENT_SIZE;
use crate::store::*;
use crate::metrics::inc_total_segments_written;

pub trait SegmentOps {
    fn put_segment(&self, tape_address: &Pubkey, global_seg_idx: u64, seg: Vec<u8>) -> Result<(), StoreError>;
    fn get_segment(&self, tape_address: &Pubkey, global_seg_idx: u64) -> Result<Vec<u8>, StoreError>;
    fn get_segment_range(&self, tape_address: &Pubkey, start: u64, end: u64) -> Result<Vec<(u64, Vec<u8>)>, StoreError>;
    fn get_tape_segments(&self, tape_address: &Pubkey) -> Result<Vec<(u64, Vec<u8>)>, StoreError>;
    fn get_segment_count(&self, tape_address: &Pubkey) -> Result<usize, StoreError>;
}

impl SegmentOps for TapeStore {
    fn get_segment(&self, tape_address: &Pubkey, segment_number: u64) -> Result<Vec<u8>, StoreError> {
        let cf = self.get_cf_handle(ColumnFamily::Segments)?;
        let mut key = Vec::with_capacity(40);
        key.extend_from_slice(&tape_address.to_bytes());
        key.extend_from_slice(&segment_number.to_be_bytes());
        let segment_data = self
            .db
            .get_cf(&cf, &key)?
            .ok_or(StoreError::SegmentNotFound(tape_address.to_string(), segment_number))?;
        Ok(segment_data)
    }

    fn put_segment(&self, tape_address: &Pubkey, segment_number: u64, data: Vec<u8>) -> Result<(), StoreError> {
        if data.len() > PACKED_SEGMENT_SIZE {
            return Err(StoreError::InvalidSegmentSize(PACKED_SEGMENT_SIZE));
        }
        let cf = self.get_cf_handle(ColumnFamily::Segments)?;
        let mut key = Vec::with_capacity(40);
        key.extend_from_slice(&tape_address.to_bytes());
        key.extend_from_slice(&segment_number.to_be_bytes());
        self.db.put_cf(&cf, &key, &data)?;
        inc_total_segments_written();

        Ok(())
    }

    fn get_segment_range(&self, tape_address: &Pubkey, start: u64, end: u64) -> Result<Vec<(u64, Vec<u8>)>, StoreError> {
        if start >= end {
            return Ok(Vec::new());
        }
        let cf = self.get_cf_handle(ColumnFamily::Segments)?;
        let prefix = tape_address.to_bytes().to_vec();

        let mut segments = Vec::new();
        let iter = self.db.prefix_iterator_cf(&cf, &prefix);
        for item in iter {
            let (key, value) = item?;
            if key.len() != 40 {
                continue;
            }
            if !key.starts_with(&prefix) {
                continue;
            }
            let segment_number = u64::from_be_bytes(
                key[32..TAPE_STORE_SLOTS_KEY_SIZE]
                    .try_into()
                    .map_err(|_| StoreError::InvalidSegmentKey)?,
            );
            if segment_number >= start && segment_number < end {
                segments.push((segment_number, value.to_vec()));
            }
        }

        // Since the iterator is sorted by key (and thus by segment_number), the segments are already in order
        Ok(segments)
    }

    fn get_tape_segments(&self, tape_address: &Pubkey) -> Result<Vec<(u64, Vec<u8>)>, StoreError> {
        let cf = self.get_cf_handle(ColumnFamily::Segments)?;
        let prefix = tape_address.to_bytes().to_vec();

        let mut segments = Vec::new();
        let iter = self.db.prefix_iterator_cf(&cf, &prefix);
        for item in iter {
            let (key, value) = item?;
            if key.len() != 40 {
                continue;
            }
            if !key.starts_with(&prefix) {
                continue;
            }
            let segment_number = u64::from_be_bytes(
                key[32..TAPE_STORE_SLOTS_KEY_SIZE]
                    .try_into()
                    .map_err(|_| StoreError::InvalidSegmentKey)?,
            );
            segments.push((segment_number, value.to_vec()));
        }

        // Since the iterator is sorted by key (and thus by segment_number), the segments are already in order
        Ok(segments)
    }

    fn get_segment_count(
        &self,
        tape_address: &Pubkey,
    ) -> Result<usize, StoreError> {
        let cf = self.get_cf_handle(ColumnFamily::Segments)?;
        let prefix = tape_address.to_bytes().to_vec();
        let iter = self.db.prefix_iterator_cf(&cf, &prefix);
        let count = iter.count();
        Ok(count)
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
    fn test_get_segment_not_found() -> Result<(), StoreError> {
        let (store, _temp_dir) = setup_store()?;
        let address = Pubkey::new_unique();
        let global_seg_idx = 0;
        let result = store.get_segment(&address, global_seg_idx);
        assert!(matches!(result, Err(StoreError::SegmentNotFound(_, _))));
        Ok(())
    }

    #[test]
    fn test_put_segment_too_large() -> Result<(), StoreError> {
        let (store, _temp_dir) = setup_store()?;
        let address = Pubkey::new_unique();
        let global_seg_idx = 0;
        let oversized_data = vec![42u8; (PACKED_SEGMENT_SIZE as usize) + 1];
        let result = store.put_segment(&address, global_seg_idx, oversized_data);
        assert!(matches!(result, Err(StoreError::InvalidSegmentSize(_))));
        Ok(())
    }

    #[test]
    fn test_get_tape_segments() -> Result<(), StoreError> {
        let (store, _temp_dir) = setup_store()?;
        let address = Pubkey::new_unique();
        let data0 = make_data(0);
        let data1 = make_data(1);
        let data2 = make_data(2);

        store.put_segment(&address, 0, data0.clone())?;
        store.put_segment(&address, 1, data1.clone())?;
        store.put_segment(&address, 2, data2.clone())?;

        let segments = store.get_tape_segments(&address)?;
        assert_eq!(segments.len(), 3);

        let mut retrieved_segments = segments.clone();
        retrieved_segments.sort_by_key(|(idx, _)| *idx);

        assert_eq!(retrieved_segments[0].0, 0);
        assert_eq!(retrieved_segments[0].1, data0);
        assert_eq!(retrieved_segments[1].0, 1);
        assert_eq!(retrieved_segments[1].1, data1);
        assert_eq!(retrieved_segments[2].0, 2);
        assert_eq!(retrieved_segments[2].1, data2);

        Ok(())
    }

    #[test]
    fn test_get_segment_count() -> Result<(), StoreError> {
        let (store, _temp_dir) = setup_store()?;
        let address = Pubkey::new_unique();

        // Initially empty
        assert_eq!(store.get_segment_count(&address)?, 0);

        let data0 = make_data(0);
        store.put_segment(&address, 0, data0.clone())?;
        assert_eq!(store.get_segment_count(&address)?, 1);

        let data1 = make_data(1);
        store.put_segment(&address, 1, data1.clone())?;
        assert_eq!(store.get_segment_count(&address)?, 2);

        let data2 = make_data(2);
        store.put_segment(&address, 2, data2.clone())?;
        assert_eq!(store.get_segment_count(&address)?, 3);

        Ok(())
    }

    #[test]
    fn test_get_segment_range() -> Result<(), StoreError> {
        let (store, _temp_dir) = setup_store()?;
        let address = Pubkey::new_unique();
        let data0 = make_data(0);
        let data1 = make_data(1);
        let data2 = make_data(2);
        let data3 = make_data(3);

        store.put_segment(&address, 0, data0.clone())?;
        store.put_segment(&address, 1, data1.clone())?;
        store.put_segment(&address, 2, data2.clone())?;
        store.put_segment(&address, 3, data3.clone())?;

        // Test full range equivalent to get_tape_segments
        let full_range = store.get_segment_range(&address, 0, 4)?;
        assert_eq!(full_range.len(), 4);
        let mut full_retrieved = full_range.clone();
        full_retrieved.sort_by_key(|(idx, _)| *idx);
        assert_eq!(full_retrieved[0].0, 0);
        assert_eq!(full_retrieved[0].1, data0);
        assert_eq!(full_retrieved[1].0, 1);
        assert_eq!(full_retrieved[1].1, data1);
        assert_eq!(full_retrieved[2].0, 2);
        assert_eq!(full_retrieved[2].1, data2);
        assert_eq!(full_retrieved[3].0, 3);
        assert_eq!(full_retrieved[3].1, data3);

        // Test partial range 1 to 3
        let partial_range = store.get_segment_range(&address, 1, 3)?;
        assert_eq!(partial_range.len(), 2);
        let mut partial_retrieved = partial_range.clone();
        partial_retrieved.sort_by_key(|(idx, _)| *idx);
        assert_eq!(partial_retrieved[0].0, 1);
        assert_eq!(partial_retrieved[0].1, data1);
        assert_eq!(partial_retrieved[1].0, 2);
        assert_eq!(partial_retrieved[1].1, data2);

        // Test empty range start >= end
        let empty_range = store.get_segment_range(&address, 2, 2)?;
        assert_eq!(empty_range.len(), 0);

        // Test range beyond existing segments
        let beyond_range = store.get_segment_range(&address, 4, 5)?;
        assert_eq!(beyond_range.len(), 0);

        Ok(())
    }
}
