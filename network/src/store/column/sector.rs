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

    pub fn get_last_index(&self) -> usize {
        let bitmap_len = SECTOR_LEAVES / 8;

        // Iterate over bitmap bytes from right to left
        for byte_idx in (0..bitmap_len).rev() {
            let byte = self.0[byte_idx];
            if byte != 0 {
                // Find the highest set bit in this byte
                for bit_pos in (0..8).rev() {
                    if (byte & (1 << bit_pos)) != 0 {
                        let segment_idx = byte_idx * 8 + bit_pos;
                        if segment_idx < SECTOR_LEAVES {
                            return segment_idx;
                        }
                    }
                }
            }
        }
        0
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

    fn setup_sector(indices: &[usize]) -> Sector {
        let mut sector = Sector::new();
        let data = make_data(1);
        for &idx in indices {
            sector.set_segment(idx, &data);
        }
        sector
    }

    #[test]
    fn sector_set_segment() {
        let mut sector = Sector::new();
        let data = make_data(1);
        assert!(sector.set_segment(0, &data));
        assert_eq!(sector.get_segment(0), Some(data.as_slice()));
        assert_eq!(sector.count_segments(), 1);

        // Invalid index
        assert!(!sector.set_segment(SECTOR_LEAVES, &data));
        assert_eq!(sector.count_segments(), 1);

        // Invalid data size
        let invalid_data = vec![1; PACKED_SEGMENT_SIZE + 1];
        assert!(!sector.set_segment(1, &invalid_data));
        assert_eq!(sector.count_segments(), 1);
    }

    #[test]
    fn sector_get_segment() {
        let sector = setup_sector(&[0, 2]);
        assert_eq!(sector.get_segment(0), Some(make_data(1).as_slice()));
        assert_eq!(sector.get_segment(1), None); // Not set
        assert_eq!(sector.get_segment(SECTOR_LEAVES), None); // Invalid index
    }

    #[test]
    fn sector_count_segments() {
        let sector = setup_sector(&[0, 1, 8]);
        assert_eq!(sector.count_segments(), 3);
        let empty_sector = Sector::new();
        assert_eq!(empty_sector.count_segments(), 0);
    }

    #[test]
    fn sector_rightmost_index() {
        let empty_sector = Sector::new();
        assert_eq!(empty_sector.get_last_index(), 0);

        let single = setup_sector(&[5]);
        assert_eq!(single.get_last_index(), 5);

        let multiple = setup_sector(&[0, 2, 5, 8]);
        assert_eq!(multiple.get_last_index(), 8);

        let last = setup_sector(&[SECTOR_LEAVES - 1]);
        assert_eq!(last.get_last_index(), SECTOR_LEAVES - 1);

        let sparse = setup_sector(&[0, SECTOR_LEAVES / 2, SECTOR_LEAVES - 2]);
        assert_eq!(sparse.get_last_index(), SECTOR_LEAVES - 2);
    }

    #[test]
    fn sector_default() {
        let sector = Sector::default();
        assert_eq!(sector.0, Sector::new().0);
        assert_eq!(sector.count_segments(), 0);
        assert_eq!(sector.get_last_index(), 0);
    }

    #[test]
    fn get_sector() -> Result<(), StoreError> {
        let (store, _temp_dir) = setup_store()?;
        let address = Pubkey::new_unique();
        let sector_num = 0u64;
        let sector = setup_sector(&[0]);

        store.put_sector(&address, sector_num, &sector)?;
        let retrieved = store.get_sector(&address, sector_num)?;
        assert_eq!(retrieved.0, sector.0);

        let result = store.get_sector(&address, 1);
        assert!(matches!(result, Err(StoreError::SegmentNotFoundForAddress(_, 1))));

        Ok(())
    }

    #[test]
    fn get_sector_invalid_size() -> Result<(), StoreError> {
        let (store, _temp_dir) = setup_store()?;
        let address = Pubkey::new_unique();
        let sector_num = 0u64;
        let cf = store.get_cf_handle(ColumnFamily::Sectors)?;
        let mut key = Vec::with_capacity(TAPE_STORE_SLOTS_KEY_SIZE);
        key.extend_from_slice(&address.to_bytes());
        key.extend_from_slice(&sector_num.to_be_bytes());
        store.db.put_cf(&cf, &key, vec![0; 10])?;

        let result = store.get_sector(&address, sector_num);
        assert!(matches!(result, Err(StoreError::InvalidSectorSize(10))));

        Ok(())
    }

    #[test]
    fn put_sector() -> Result<(), StoreError> {
        let (store, _temp_dir) = setup_store()?;
        let address = Pubkey::new_unique();
        let sector_num = 0u64;
        let sector = setup_sector(&[0]);

        store.put_sector(&address, sector_num, &sector)?;
        let retrieved = store.get_sector(&address, sector_num)?;
        assert_eq!(retrieved.0, sector.0);

        Ok(())
    }

    #[test]
    fn fill_sector() -> Result<(), StoreError> {
        let (store, _temp_dir) = setup_store()?;
        let address = Pubkey::new_unique();

        for i in 0..SECTOR_LEAVES as u64 {
            store.put_segment(&address, i, make_data(i as u8))?;
        }

        assert_eq!(store.get_sector_count(&address)?, 1);
        assert_eq!(store.get_segment_count(&address)?, SECTOR_LEAVES as u64);

        let sector = store.get_sector(&address, 0)?;
        let bitmap_len = SECTOR_LEAVES / 8;
        assert!(sector.0[..bitmap_len].iter().all(|&b| b == 0xFF));
        assert_eq!(sector.get_segment(0).unwrap(), make_data(0));
        assert_eq!(sector.get_segment(SECTOR_LEAVES - 1).unwrap(), make_data((SECTOR_LEAVES - 1) as u8));

        Ok(())
    }

    #[test]
    fn sector_boundary() -> Result<(), StoreError> {
        let (store, _temp_dir) = setup_store()?;
        let address = Pubkey::new_unique();
        let last_s0 = (SECTOR_LEAVES - 1) as u64;
        let first_s1 = SECTOR_LEAVES as u64;

        store.put_segment(&address, last_s0, make_data(1))?;
        store.put_segment(&address, first_s1, make_data(2))?;

        assert_eq!(store.get_sector_count(&address)?, 2);
        assert_eq!(store.get_segment_count(&address)?, 2);
        assert_eq!(store.get_segment(&address, last_s0)?, make_data(1));
        assert_eq!(store.get_segment(&address, first_s1)?, make_data(2));

        let s0 = store.get_sector(&address, 0)?;
        let s1 = store.get_sector(&address, 1)?;
        assert_eq!(s0.0[(SECTOR_LEAVES - 1) / 8] & (1 << 7), 1 << 7);
        assert_eq!(s1.0[0] & 0x01, 0x01);

        Ok(())
    }

    #[test]
    fn multi_sectors() -> Result<(), StoreError> {
        let (store, _temp_dir) = setup_store()?;
        let address = Pubkey::new_unique();
        let sectors = 100u64;
        let stride = (SECTOR_LEAVES / 3).max(1) as u64;
        let mut written = 0u64;

        for s in 0..sectors {
            let base = s * SECTOR_LEAVES as u64;
            for k in 0..3 {
                let idx = base + k * stride;
                store.put_segment(&address, idx, make_data(idx as u8))?;
                written += 1;
            }
        }

        assert_eq!(store.get_sector_count(&address)?, sectors as usize);
        assert_eq!(store.get_segment_count(&address)?, written);

        for s in 0..sectors {
            let sector = store.get_sector(&address, s)?;
            for k in 0..3 {
                let li = (k * stride) as usize;
                assert_ne!(sector.0[li / 8] & (1 << (li % 8)), 0);
            }
        }

        Ok(())
    }
}
