use std::path::Path;
use std::sync::Arc;
use rocksdb::{BoundColumnFamily, DB, Options};
use super::{
    consts::*,
    layout::{ColumnFamily, create_cf_descriptors},
    error::StoreError,
};

pub struct TapeStore {
    pub db: DB,
}

impl TapeStore {
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self, StoreError> {
        let path = path.as_ref();
        let cfs = create_cf_descriptors();
        let mut db_opts = Options::default();
        db_opts.create_if_missing(true);
        db_opts.create_missing_column_families(true);
        db_opts.set_write_buffer_size(TAPE_STORE_MAX_WRITE_BUFFER_SIZE);
        db_opts.set_max_write_buffer_number(TAPE_STORE_MAX_WRITE_BUFFERS as i32);
        db_opts.increase_parallelism(num_cpus::get() as i32);
        let db = DB::open_cf_descriptors(&db_opts, path, cfs)?;
        Ok(Self { db })
    }

    pub fn try_init_store() -> Result<(), StoreError> {
        if let Ok(_store) = super::helpers::primary() {
            log::debug!("Primary store initialized successfully");
        }
        Ok(())
    }

    pub fn get_cf_handle(&self, column_family: ColumnFamily) -> Result<Arc<BoundColumnFamily<'_>>, StoreError> {
        self.db
            .cf_handle(column_family.as_str())
            .ok_or(StoreError::from(&column_family))
    }

    pub fn new_read_only<P: AsRef<Path>>(path: P) -> Result<Self, StoreError> {
        let path = path.as_ref();
        let cfs = create_cf_descriptors();
        let db_opts = Options::default();
        let db = DB::open_cf_descriptors_read_only(&db_opts, path, cfs, false)?;
        Ok(Self { db })
    }

    pub fn new_secondary<P: AsRef<Path>>(
        primary_path: P,
        secondary_path: P,
    ) -> Result<Self, StoreError> {
        let primary_path = primary_path.as_ref();
        let secondary_path = secondary_path.as_ref();
        let cfs = create_cf_descriptors();
        let mut db_opts = Options::default();
        db_opts.create_if_missing(true);
        db_opts.create_missing_column_families(true);
        db_opts.set_write_buffer_size(TAPE_STORE_MAX_WRITE_BUFFER_SIZE);
        db_opts.set_max_write_buffer_number(TAPE_STORE_MAX_WRITE_BUFFERS as i32);
        db_opts.increase_parallelism(num_cpus::get() as i32);
        let db = DB::open_cf_descriptors_as_secondary(&db_opts, primary_path, secondary_path, cfs)?;
        Ok(Self { db })
    }

    pub fn catch_up_with_primary(&self) -> Result<(), StoreError> {
        self.db.try_catch_up_with_primary()?;
        Ok(())
    }
}

impl Drop for TapeStore {
    fn drop(&mut self) {
        // RocksDB handles cleanup automatically
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use solana_sdk::pubkey::Pubkey;
    use tempdir::TempDir;
    use tape_api::consts::*;
    use crate::store::{
        tape::TapeOps,
        segment::SegmentOps,
        sector::SectorOps,
        merkle::MerkleOps,
        stats::StatsOps,
    };

    fn setup_store() -> Result<(TapeStore, TempDir), StoreError> {
        let temp_dir = TempDir::new("rocksdb_test").map_err(StoreError::IoError)?;
        let store = TapeStore::new(temp_dir.path())?;
        Ok((store, temp_dir))
    }

    fn make_data(marker: u8) -> Vec<u8> {
        vec![marker; PACKED_SEGMENT_SIZE]
    }

    #[test]
    fn test_put_tape_address() -> Result<(), StoreError> {
        let (store, _temp_dir) = setup_store()?;
        let tape_number = 1;
        let address = Pubkey::new_unique();

        store.put_tape_address(tape_number, &address)?;
        let retrieved_number = store.get_tape_number(&address)?;
        assert_eq!(retrieved_number, tape_number);
        let retrieved_address = store.get_tape_address(tape_number)?;
        assert_eq!(retrieved_address, address);
        Ok(())
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
    fn test_put_l13() -> Result<(), StoreError> {
        let (store, _temp_dir) = setup_store()?;
        let address = Pubkey::new_unique();
        let t13_data = vec![[1u8; 32]; L13_NODES_PER_TAPE];

        store.put_l13t(&address, &t13_data)?;
        let retrieved_data = store.get_l13t(&address)?;
        assert_eq!(retrieved_data, t13_data);

        // Test that m13 is not found
        let retrieved_m13 = store.get_l13m(&address);
        assert!(retrieved_m13.is_err());

        // Test that m13 doesn't overwrite t13
        let m13_data = vec![[2u8; 32]; L13_NODES_PER_TAPE];
        store.put_l13m(&address, &m13_data)?;

        let retrieved_m13 = store.get_l13m(&address)?;
        assert_eq!(retrieved_m13, m13_data);
        let retrieved_t13 = store.get_l13t(&address)?;
        assert_eq!(retrieved_t13, t13_data);
        Ok(())
    }

    #[test]
    fn test_get_local_stats() -> Result<(), StoreError> {
        let (store, _temp_dir) = setup_store()?;
        let stats = store.get_local_stats()?;
        assert_eq!(stats.tapes, 0);
        assert_eq!(stats.sectors, 0);

        let tape_number = 1;
        let address = Pubkey::new_unique();
        store.put_tape_address(tape_number, &address)?;
        store.put_segment(&address, 0, make_data(42))?;

        let stats = store.get_local_stats()?;
        assert_eq!(stats.tapes, 1);
        assert_eq!(stats.sectors, 1);
        assert!(stats.size_bytes > 0);
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
        let expected_indices = vec![
            idx_sector0_a,
            idx_sector0_b,
            idx_sector1_a,
            idx_sector1_b,
            idx_sector2_a,
        ];
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
