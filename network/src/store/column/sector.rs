use bytemuck::{Pod, Zeroable, try_from_bytes, bytes_of};
use solana_sdk::pubkey::Pubkey;
use tape_api::consts::*;
use crate::store::*;

pub trait SectorOps {
    fn get_sector(&self, tape_address: &Pubkey, sector_number: u64) -> Result<Sector, StoreError>;
    fn put_sector(&self, tape_address: &Pubkey, sector_number: u64, sector: &Sector) -> Result<(), StoreError>;
    fn get_sector_count(&self, tape_address: &Pubkey) -> Result<usize, StoreError>;
}

impl SectorOps for TapeStore {
    fn get_sector(&self, tape_address: &Pubkey, sector_number: u64) -> Result<Sector, StoreError> {
        let cf = self.get_cf_handle(ColumnFamily::Sectors)?;
        let mut key = Vec::with_capacity(TAPE_STORE_SLOTS_KEY_SIZE);
        key.extend_from_slice(&tape_address.to_bytes());
        key.extend_from_slice(&sector_number.to_be_bytes());
        
        let data = self
            .db
            .get_cf(&cf, &key)?
            .ok_or_else(|| StoreError::SegmentNotFoundForAddress(tape_address.to_string(), sector_number))?;
        
        if data.len() != SECTOR_HEADER_BYTES + SECTOR_LEAVES * PACKED_SEGMENT_SIZE {
            return Err(StoreError::InvalidSectorSize(data.len()));
        }
        
        Ok(*try_from_bytes(&data).map_err(|_| StoreError::InvalidSectorSize(data.len()))?)
    }

    fn put_sector(&self, tape_address: &Pubkey, sector_number: u64, sector: &Sector) -> Result<(), StoreError> {
        let cf = self.get_cf_handle(ColumnFamily::Sectors)?;
        let mut key = Vec::with_capacity(TAPE_STORE_SLOTS_KEY_SIZE);
        key.extend_from_slice(&tape_address.to_bytes());
        key.extend_from_slice(&sector_number.to_be_bytes());
        
        self.db.put_cf(&cf, &key, bytes_of(sector))?;
        Ok(())
    }

    fn get_sector_count(&self, tape_address: &Pubkey) -> Result<usize, StoreError> {
        let cf = self.get_cf_handle(ColumnFamily::Sectors)?;
        let prefix = tape_address.to_bytes().to_vec();
        let iter = self.db.prefix_iterator_cf(&cf, &prefix);
        Ok(iter.count())
    }
}

#[repr(transparent)]
#[derive(Clone, Copy, Debug)]
pub struct Sector(pub [u8; 
    SECTOR_HEADER_BYTES + SECTOR_LEAVES * PACKED_SEGMENT_SIZE]);

unsafe impl Zeroable for Sector {}
unsafe impl Pod for Sector {}

impl Default for Sector {
    fn default() -> Self {
        Self::new()
    }
}

impl Sector {
    pub fn new() -> Self {
        Self::zeroed()
    }

    pub fn set_segment(&mut self, local_seg_idx: usize, data: &[u8]) -> bool {
        if local_seg_idx >= SECTOR_LEAVES || data.len() != PACKED_SEGMENT_SIZE {
            return false;
        }

        let bitmap_idx = local_seg_idx / 8;
        let bit_pos = local_seg_idx % 8;
        let is_new_segment = (self.0[bitmap_idx] & (1 << bit_pos)) == 0;

        self.0[bitmap_idx] |= 1 << bit_pos;
        let seg_start = SECTOR_HEADER_BYTES + local_seg_idx * PACKED_SEGMENT_SIZE;
        self.0[seg_start..seg_start + PACKED_SEGMENT_SIZE].copy_from_slice(data);
        is_new_segment
    }

    pub fn get_segment(&self, local_seg_idx: usize) -> Option<&[u8]> {
        if local_seg_idx >= SECTOR_LEAVES {
            return None;
        }

        let bitmap_idx = local_seg_idx / 8;
        let bit_pos = local_seg_idx % 8;

        if (self.0[bitmap_idx] & (1 << bit_pos)) == 0 {
            return None;
        }

        let seg_start = SECTOR_HEADER_BYTES + local_seg_idx * PACKED_SEGMENT_SIZE;
        Some(&self.0[seg_start..seg_start + PACKED_SEGMENT_SIZE])
    }

    pub fn count_segments(&self) -> usize {
        let bitmap_len = SECTOR_LEAVES / 8;
        self.0[..bitmap_len].iter().map(|byte| byte.count_ones() as usize).sum()
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
    fn test_sector_set_segment_success() {
        let mut sector = Sector::new();
        let data = vec![1u8; PACKED_SEGMENT_SIZE];
        let local_seg_idx = 0;

        let is_new = sector.set_segment(local_seg_idx, &data);
        assert!(is_new);

        let retrieved = sector.get_segment(local_seg_idx).expect("Failed to get segment");
        assert_eq!(retrieved, data.as_slice());
        assert_eq!(sector.count_segments(), 1);
    }

    #[test]
    fn test_sector_set_segment_invalid_index() {
        let mut sector = Sector::new();
        let data = vec![1u8; PACKED_SEGMENT_SIZE];
        let invalid_idx = SECTOR_LEAVES;

        let is_new = sector.set_segment(invalid_idx, &data);
        assert!(!is_new);
        assert_eq!(sector.count_segments(), 0);
    }

    #[test]
    fn test_sector_set_segment_invalid_data_size() {
        let mut sector = Sector::new();
        let data = vec![1u8; PACKED_SEGMENT_SIZE + 1]; // Invalid size
        let local_seg_idx = 0;

        let is_new = sector.set_segment(local_seg_idx, &data);
        assert!(!is_new);
        assert_eq!(sector.count_segments(), 0);
    }

    #[test]
    fn test_sector_get_segment_not_set() {
        let sector = Sector::new();
        let local_seg_idx = 0;

        let result = sector.get_segment(local_seg_idx);
        assert!(result.is_none());
    }

    #[test]
    fn test_sector_get_segment_invalid_index() {
        let sector = Sector::new();
        let invalid_idx = SECTOR_LEAVES;

        let result = sector.get_segment(invalid_idx);
        assert!(result.is_none());
    }

    #[test]
    fn test_sector_count_segments() {
        let mut sector = Sector::new();
        let data = vec![1u8; PACKED_SEGMENT_SIZE];

        // Set multiple segments
        sector.set_segment(0, &data);
        sector.set_segment(1, &data);
        sector.set_segment(8, &data); // Different bitmap byte

        assert_eq!(sector.count_segments(), 3);
    }

    #[test]
    fn test_sector_default_and_new() {
        let default_sector = Sector::default();
        let new_sector = Sector::new();

        assert_eq!(default_sector.0, new_sector.0);
        assert_eq!(default_sector.count_segments(), 0);
        assert_eq!(new_sector.count_segments(), 0);
    }

    #[test]
    fn test_get_sector_success() -> Result<(), StoreError> {
        let (tape_store, _temp_dir) = setup_store()?;
        let tape_address = Pubkey::new_unique();
        let sector_number = 0u64;
        let mut sector = Sector::new();
        
        // Set a segment to ensure the sector has valid data
        let data = vec![1u8; PACKED_SEGMENT_SIZE];
        sector.set_segment(0, &data);
        
        // Store the sector
        tape_store
            .put_sector(&tape_address, sector_number, &sector)
            .expect("Failed to put sector");

        // Retrieve and verify the sector
        let retrieved_sector = tape_store
            .get_sector(&tape_address, sector_number)
            .expect("Failed to get sector");

        assert_eq!(retrieved_sector.0, sector.0);

        Ok(())
    }

    #[test]
    fn test_get_sector_not_found() -> Result<(), StoreError> {
        let (tape_store, _temp_dir) = setup_store()?;
        let tape_address = Pubkey::new_unique();
        let sector_number = 0u64;

        let result = tape_store.get_sector(&tape_address, sector_number);
        assert!(matches!(
            result,
            Err(StoreError::SegmentNotFoundForAddress(_, n)) if n == sector_number
        ));

        Ok(())
    }

    #[test]
    fn test_get_sector_invalid_size() -> Result<(), StoreError> {
        let (tape_store, _temp_dir) = setup_store()?;
        let tape_address = Pubkey::new_unique();
        let sector_number = 0u64;

        // Manually insert invalid data (wrong size)
        let cf = tape_store
            .get_cf_handle(ColumnFamily::Sectors)
            .expect("Failed to get column family");
        let mut key = Vec::with_capacity(TAPE_STORE_SLOTS_KEY_SIZE);
        key.extend_from_slice(&tape_address.to_bytes());
        key.extend_from_slice(&sector_number.to_be_bytes());
        tape_store
            .db
            .put_cf(&cf, &key, vec![0u8; 10]) // Invalid size
            .expect("Failed to put invalid data");

        let result = tape_store.get_sector(&tape_address, sector_number);
        assert!(matches!(result, Err(StoreError::InvalidSectorSize(10))));

        Ok(())
    }

    #[test]
    fn test_put_sector_success() -> Result<(), StoreError> {
        let (tape_store, _temp_dir) = setup_store()?;
        let tape_address = Pubkey::new_unique();
        let sector_number = 0u64;
        let sector = Sector::new();

        let result = tape_store.put_sector(&tape_address, sector_number, &sector);
        assert!(result.is_ok());

        // Verify the sector was stored
        let retrieved_sector = tape_store
            .get_sector(&tape_address, sector_number)
            .expect("Failed to get sector");
        assert_eq!(retrieved_sector.0, sector.0);

        Ok(())
    }

    #[test]
    fn test_fill_sector() -> Result<(), StoreError> {
        let (store, _tmp) = setup_store()?;
        let address = Pubkey::new_unique();

        // Fill exactly one full sector: indices [0, SECTOR_LEAVES)
        for i in 0..SECTOR_LEAVES as u64 {
            let data = make_data(i as u8);
            store.put_segment(&address, i, data)?;
        }

        // Sector count should be 1
        assert_eq!(store.get_sector_count(&address)?, 1);

        // Segment count should equal SECTOR_LEAVES
        assert_eq!(store.get_segment_count(&address)?, SECTOR_LEAVES as u64);

        // Verify bitmap is all 1s for the first sector
        let sector0 = store.get_sector(&address, 0)?;
        let bitmap_len = SECTOR_LEAVES / 8;
        for byte in &sector0.0[..bitmap_len] {
            assert_eq!(*byte, 0xFF);
        }

        // Spot-check a few reads across the sector
        for &idx in &[0u64, (SECTOR_LEAVES as u64) / 2, (SECTOR_LEAVES as u64) - 1] {
            let got = store.get_segment(&address, idx)?;
            assert_eq!(got, make_data(idx as u8));
        }

        Ok(())
    }

    #[test]
    fn test_sector_boundary() -> Result<(), StoreError> {
        let (store, _tmp) = setup_store()?;
        let address = Pubkey::new_unique();

        let last_in_s0 = (SECTOR_LEAVES as u64) - 1;
        let first_in_s1 = SECTOR_LEAVES as u64;

        store.put_segment(&address, last_in_s0, make_data(1))?;
        store.put_segment(&address, first_in_s1, make_data(2))?;

        // Two sectors should exist because we touched indices in both
        assert_eq!(store.get_sector_count(&address)?, 2);

        // Segment count is 2 (distinct indices)
        assert_eq!(store.get_segment_count(&address)?, 2);

        // Reads back
        assert_eq!(store.get_segment(&address, last_in_s0)?, make_data(1));
        assert_eq!(store.get_segment(&address, first_in_s1)?, make_data(2));

        // Bitmap spot check
        let s0 = store.get_sector(&address, 0)?;
        let s1 = store.get_sector(&address, 1)?;

        // sector 0: last_in_s0 => byte = (SECTOR_LEAVES-1)/8, bit = 7
        let byte_idx0 = (SECTOR_LEAVES - 1) / 8;
        assert_eq!(s0.0[byte_idx0] & (1 << 7), 1 << 7);

        // sector 1: first_in_s1 local idx = 0 => byte 0 bit 0
        assert_eq!(s1.0[0] & 0x01, 0x01);

        Ok(())
    }

    #[test]
    fn test_two_full_sectors() -> Result<(), StoreError> {
        let (store, _tmp) = setup_store()?;
        let address = Pubkey::new_unique();

        let total = (SECTOR_LEAVES as u64) * 2;
        for i in 0..total {
            store.put_segment(&address, i, make_data(i as u8))?;
        }

        // Should have 2 sectors and full segment count
        assert_eq!(store.get_sector_count(&address)?, 2);
        assert_eq!(store.get_segment_count(&address)?, total);

        // Verify both sector bitmaps are all 1s
        let s0 = store.get_sector(&address, 0)?;
        let s1 = store.get_sector(&address, 1)?;
        let bitmap_len = SECTOR_LEAVES / 8;
        for b in s0.0[..bitmap_len].iter() { assert_eq!(*b, 0xFF); }
        for b in s1.0[..bitmap_len].iter() { assert_eq!(*b, 0xFF); }

        // Spot-check edges
        assert_eq!(store.get_segment(&address, 0)?, make_data(0));
        assert_eq!(store.get_segment(&address, (SECTOR_LEAVES as u64) - 1)?, make_data(((SECTOR_LEAVES as u64) - 1) as u8));
        assert_eq!(store.get_segment(&address, SECTOR_LEAVES as u64)?, make_data((SECTOR_LEAVES as u64) as u8));
        assert_eq!(store.get_segment(&address, total - 1)?, make_data((total - 1) as u8));

        Ok(())
    }

    #[test]
    fn test_many_sectors() -> Result<(), StoreError> {
        let (store, _tmp) = setup_store()?;
        let address = Pubkey::new_unique();

        // Touch 5 sectors sparsely: indices spaced by SECTOR_LEAVES / 3 within each sector
        let sectors = 5u64;
        let stride = (SECTOR_LEAVES / 3).max(1) as u64;

        let mut written = 0u64;
        for s in 0..sectors {
            let base = s * (SECTOR_LEAVES as u64);
            for k in 0..3u64 {
                let idx = base + k * stride;
                store.put_segment(&address, idx, make_data(idx as u8))?;
                written += 1;
            }
        }

        assert_eq!(store.get_sector_count(&address)?, sectors as usize);
        assert_eq!(store.get_segment_count(&address)?, written);

        // Verify a couple of bit positions within random sectors
        for s in 0..sectors {
            let sector = store.get_sector(&address, s)?;
            for k in 0..3usize {
                let li = k * (stride as usize);
                let b = sector.0[li / 8] & (1 << (li % 8));
                assert!(b != 0, "bitmap not set for sector {}, local {}", s, li);
            }
        }

        Ok(())
    }
}
