use std::fs;
use std::path::Path;
use std::sync::Arc;
use rocksdb::{BoundColumnFamily, DB, Options, WriteBatch};
use solana_sdk::pubkey::Pubkey;

use tape_api::PACKED_SEGMENT_SIZE;
use crate::metrics::{inc_total_segments_written, inc_total_tapes_written};
use super::{
    column_family::ColumnFamily,
    consts::*,
    error::StoreError,
};

pub enum StoreStaticKeys {
    LastProcessedSlot,
    Drift,
}

impl StoreStaticKeys {
    fn as_bytes(&self) -> &'static [u8] {
        match self {
            StoreStaticKeys::LastProcessedSlot => b"last_processed_slot",
            StoreStaticKeys::Drift => b"drift",
        }
    }
}

pub struct TapeStore {
    pub db: DB,
}

#[derive(Debug)]
pub struct LocalStats {
    pub tapes: usize,
    pub sectors: usize,
    pub size_bytes: u64,
}

impl TapeStore {
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self, StoreError> {
        let path = path.as_ref();
        let cfs = super::cf_layout::create_cf_descriptors();
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
        let cfs = super::cf_layout::create_cf_descriptors();
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
        let cfs = super::cf_layout::create_cf_descriptors();
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

    pub fn update_health(&self, last_processed_slot: u64, drift: u64) -> Result<(), StoreError> {
        let cf = self.get_cf_handle(ColumnFamily::Health)?;
        let mut batch = WriteBatch::default();
        batch.put_cf(&cf, StoreStaticKeys::LastProcessedSlot.as_bytes(), last_processed_slot.to_be_bytes());
        batch.put_cf(&cf, StoreStaticKeys::Drift.as_bytes(), drift.to_be_bytes());
        self.db.write(batch)?;
        Ok(())
    }

    pub fn get_health(&self) -> Result<(u64, u64), StoreError> {
        let cf = self.get_cf_handle(ColumnFamily::Health)?;
        let bh = self
            .db
            .get_cf(&cf, StoreStaticKeys::LastProcessedSlot.as_bytes())?
            .ok_or(StoreError::HealthCfNotFound)?;
        let dr = self
            .db
            .get_cf(&cf, StoreStaticKeys::Drift.as_bytes())?
            .ok_or(StoreError::HealthCfNotFound)?;
        let height = u64::from_be_bytes(bh[..].try_into().unwrap());
        let drift = u64::from_be_bytes(dr[..].try_into().unwrap());
        Ok((height, drift))
    }

    pub fn put_tape(&self, tape_number: u64, address: &Pubkey) -> Result<(), StoreError> {
        let cf_tape_by_number = self.get_cf_handle(ColumnFamily::TapeByNumber)?;
        let cf_tape_by_address = self.get_cf_handle(ColumnFamily::TapeByAddress)?;
        let tape_number_key = tape_number.to_be_bytes().to_vec();
        let address_key = address.to_bytes().to_vec();
        let mut batch = WriteBatch::default();
        batch.put_cf(&cf_tape_by_number, &tape_number_key, address.to_bytes());
        batch.put_cf(&cf_tape_by_address, &address_key, tape_number.to_be_bytes());
        self.db.write(batch)?;
        inc_total_tapes_written();
        Ok(())
    }

    pub fn put_segment(&self, tape: &Pubkey, global_seg_idx: u64, seg: Vec<u8>) -> Result<(), StoreError> {
        if seg.len() != PACKED_SEGMENT_SIZE {
            return Err(StoreError::InvalidSegmentSize(seg.len()));
        }
        
        let sector_number = global_seg_idx / SECTOR_LEAVES as u64;
        let local_seg_idx = (global_seg_idx % SECTOR_LEAVES as u64) as usize;
        
        let cf_sectors = self.get_cf_handle(ColumnFamily::Sectors)?;
        let cf_tape_stats = self.get_cf_handle(ColumnFamily::TapeStats)?;
        
        let mut sector = self.get_sector(tape, sector_number).unwrap_or_else(|_| {
            vec![0u8; SECTOR_HEADER_BYTES + SECTOR_LEAVES * PACKED_SEGMENT_SIZE]
        });
        
        // Check if segment bit is already set
        let bitmap_idx = local_seg_idx / 8;
        let bit_pos = local_seg_idx % 8;
        let is_new_segment = (sector[bitmap_idx] & (1 << bit_pos)) == 0;
        
        // Set bitmap bit
        sector[bitmap_idx] |= 1 << bit_pos;
        
        // Write segment data
        let seg_start = SECTOR_HEADER_BYTES + local_seg_idx * PACKED_SEGMENT_SIZE;
        sector[seg_start..seg_start + PACKED_SEGMENT_SIZE].copy_from_slice(&seg);
        
        // Update segment count if new segment
        let mut batch = WriteBatch::default();
        let mut key = Vec::with_capacity(40);
        key.extend_from_slice(&tape.to_bytes());
        key.extend_from_slice(&sector_number.to_be_bytes());
        batch.put_cf(&cf_sectors, &key, &sector);
        
        if is_new_segment {
            let current_count = self.get_segment_count(tape).unwrap_or(0);
            batch.put_cf(&cf_tape_stats, tape.to_bytes(), (current_count + 1).to_be_bytes());
        }
        
        self.db.write(batch)?;
        inc_total_segments_written();

        Ok(())
    }

    pub fn get_segment(&self, tape: &Pubkey, global_seg_idx: u64) -> Result<Vec<u8>, StoreError> {
        let sector_number = global_seg_idx / SECTOR_LEAVES as u64;
        let local_seg_idx = (global_seg_idx % SECTOR_LEAVES as u64) as usize;
        
        let sector = self.get_sector(tape, sector_number)?;
        
        // Check bitmap
        let bitmap_idx = local_seg_idx / 8;
        let bit_pos = local_seg_idx % 8;
        if (sector[bitmap_idx] & (1 << bit_pos)) == 0 {
            return Err(StoreError::SegmentNotFoundForAddress(tape.to_string(), global_seg_idx));
        }
        
        let seg_start = SECTOR_HEADER_BYTES + local_seg_idx * PACKED_SEGMENT_SIZE;
        Ok(sector[seg_start..seg_start + PACKED_SEGMENT_SIZE].to_vec())
    }

    pub fn get_sector(&self, tape: &Pubkey, sector_number: u64) -> Result<Vec<u8>, StoreError> {
        let cf = self.get_cf_handle(ColumnFamily::Sectors)?;
        let mut key = Vec::with_capacity(40);
        key.extend_from_slice(&tape.to_bytes());
        key.extend_from_slice(&sector_number.to_be_bytes());
        
        self.db
            .get_cf(&cf, &key)?
            .ok_or_else(|| StoreError::SegmentNotFoundForAddress(tape.to_string(), sector_number))
    }

    pub fn put_sector(&self, tape: &Pubkey, sector: &[u8]) -> Result<(), StoreError> {
        let cf = self.get_cf_handle(ColumnFamily::Sectors)?;
        let mut key = Vec::with_capacity(40);
        key.extend_from_slice(&tape.to_bytes());
        key.extend_from_slice(&[0; 8]); // Assuming sector number is part of the sector data
        self.db.put_cf(&cf, &key, sector)?;
        Ok(())
    }

    pub fn get_m13(&self, tape: &Pubkey) -> Result<Vec<u8>, StoreError> {
        let cf = self.get_cf_handle(ColumnFamily::MerkleLayers)?;
        let mut key = Vec::with_capacity(36);
        key.extend_from_slice(&tape.to_bytes());
        key.push(13); // layer_id
        key.extend_from_slice(&[L13_MINER_LAYER, 0, 0]); // ID and padding
        
        self.db
            .get_cf(&cf, &key)?
            .ok_or_else(|| StoreError::TapeNotFoundForAddress(tape.to_string()))
    }

    pub fn put_m13(&self, tape: &Pubkey, l13: &[u8]) -> Result<(), StoreError> {
        let cf = self.get_cf_handle(ColumnFamily::MerkleLayers)?;
        let mut key = Vec::with_capacity(36);
        key.extend_from_slice(&tape.to_bytes());
        key.push(13); // layer_id
        key.extend_from_slice(&[L13_MINER_LAYER, 0, 0]); // ID and padding
        self.db.put_cf(&cf, &key, l13)?;
        Ok(())
    }

    pub fn get_t13(&self, tape: &Pubkey) -> Result<Vec<u8>, StoreError> {
        let cf = self.get_cf_handle(ColumnFamily::MerkleLayers)?;
        let mut key = Vec::with_capacity(36);
        key.extend_from_slice(&tape.to_bytes());
        key.push(13); // layer_id
        key.extend_from_slice(&[L13_TAPE_LAYER, 0, 0]); // ID and padding
        
        self.db
            .get_cf(&cf, &key)?
            .ok_or_else(|| StoreError::TapeNotFoundForAddress(tape.to_string()))
    }

    pub fn put_t13(&self, tape: &Pubkey, l13: &[u8]) -> Result<(), StoreError> {
        let cf = self.get_cf_handle(ColumnFamily::MerkleLayers)?;
        let mut key = Vec::with_capacity(36);
        key.extend_from_slice(&tape.to_bytes());
        key.push(13); // layer_id
        key.extend_from_slice(&[L13_TAPE_LAYER, 0, 0]); // ID and padding
        self.db.put_cf(&cf, &key, l13)?;
        Ok(())
    }

    pub fn get_tape_number(&self, address: &Pubkey) -> Result<u64, StoreError> {
        let cf = self.get_cf_handle(ColumnFamily::TapeByAddress)?;
        let key = address.to_bytes().to_vec();
        let tape_number_bytes = self
            .db
            .get_cf(&cf, &key)?
            .ok_or_else(|| StoreError::TapeNotFoundForAddress(address.to_string()))?;
        Ok(u64::from_be_bytes(
            tape_number_bytes
                .try_into()
                .map_err(|_| StoreError::InvalidSegmentKey)?,
        ))
    }

    pub fn get_tape_address(&self, tape_number: u64) -> Result<Pubkey, StoreError> {
        let cf = self.get_cf_handle(ColumnFamily::TapeByNumber)?;
        let key = tape_number.to_be_bytes().to_vec();
        let address_bytes = self
            .db
            .get_cf(&cf, &key)?
            .ok_or(StoreError::TapeNotFound(tape_number))?;
        Pubkey::try_from(address_bytes.as_slice())
            .map_err(|e| StoreError::InvalidPubkey(e.to_string()))
    }

    /// Get all tape segments for a given tape address. Returns a vector of segments, where each
    /// segment is a vector of bytes.
    ///
    /// @deprecated You should not be fetching a whole tape; it could be large.
    pub fn get_tape_segments(
        &self,
        tape_address: &Pubkey,
    ) -> Result<Vec<(u64, Vec<u8>)>, StoreError> {
        let cf = self.get_cf_handle(ColumnFamily::Sectors)?;
        let prefix = tape_address.to_bytes().to_vec();
        let iter = self.db.prefix_iterator_cf(&cf, &prefix);

        let bitmap_len = SECTOR_LEAVES / 8;
        let mut segments: Vec<(u64, Vec<u8>)> = Vec::new();

        for item in iter {
            let (key, sector) = item?;
            if key.len() < 40 {
                continue;
            }
            let sector_number = u64::from_be_bytes(key[key.len() - 8..].try_into().unwrap());

            if sector.len() < SECTOR_HEADER_BYTES + SECTOR_LEAVES * PACKED_SEGMENT_SIZE {
                continue;
            }

            let bitmap = &sector[..bitmap_len];
            let payload = &sector[SECTOR_HEADER_BYTES..];

            for li in 0..SECTOR_LEAVES {
                let bit_set = bitmap[li / 8] & (1 << (li % 8)) != 0;
                if bit_set {
                    let start = li * PACKED_SEGMENT_SIZE;
                    let end = start + PACKED_SEGMENT_SIZE;
                    let global_index = sector_number * SECTOR_LEAVES as u64 + li as u64;
                    segments.push((global_index, payload[start..end].to_vec()));
                }
            }
        }

        // Sort by global index so caller always gets ordered segments
        segments.sort_by_key(|(idx, _)| *idx);
        Ok(segments)
    }


    pub fn get_segment_count(&self, tape: &Pubkey) -> Result<u64, StoreError> {
        let cf = self.get_cf_handle(ColumnFamily::TapeStats)?;
        let count_bytes = self
            .db
            .get_cf(&cf, tape.to_bytes())?
            .unwrap_or_else(|| vec![0; 8]);
        Ok(u64::from_be_bytes(count_bytes[..].try_into().unwrap()))
    }

    pub fn get_sector_count(&self, tape_address: &Pubkey) -> Result<usize, StoreError> {
        let cf = self.get_cf_handle(ColumnFamily::Sectors)?;
        let prefix = tape_address.to_bytes().to_vec();
        let iter = self.db.prefix_iterator_cf(&cf, &prefix);
        Ok(iter.count())
    }

    pub fn get_local_stats(&self) -> Result<LocalStats, StoreError> {
        let tapes = self.count_tapes()?;
        let sectors = self.count_sectors()?;
        let size_bytes = self.db_size()?;
        Ok(LocalStats { tapes, sectors, size_bytes })
    }

    fn count_tapes(&self) -> Result<usize, StoreError> {
        let cf = self.get_cf_handle(ColumnFamily::TapeByNumber)?;
        let iter = self.db.iterator_cf(&cf, rocksdb::IteratorMode::Start);
        Ok(iter.count())
    }

    fn count_sectors(&self) -> Result<usize, StoreError> {
        let cf = self.get_cf_handle(ColumnFamily::Sectors)?;
        let iter = self.db.iterator_cf(&cf, rocksdb::IteratorMode::Start);
        Ok(iter.count())
    }

    fn db_size(&self) -> Result<u64, StoreError> {
        let mut size = 0u64;
        for entry in fs::read_dir(self.db.path())? {
            let entry = entry?;
            if entry.file_type()?.is_file() {
                size += entry.metadata()?.len();
            }
        }
        Ok(size)
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

    fn setup_store() -> Result<(TapeStore, TempDir), StoreError> {
        let temp_dir = TempDir::new("rocksdb_test").map_err(StoreError::IoError)?;
        let store = TapeStore::new(temp_dir.path())?;
        Ok((store, temp_dir))
    }

    // Helper function to generate a segment with a specific pattern. It creates a deterministic
    // test segment where the first 8 bytes store the big-endian segment index so it’s easy to
    // identify in debugging, and the remaining bytes are filled with a repeatable rolling pattern
    // based on that index.
    #[inline]
    fn seg_with_pattern(i: u64) -> Vec<u8> {
        let mut v = vec![0u8; PACKED_SEGMENT_SIZE];
        v[..8].copy_from_slice(&i.to_be_bytes());
        for (j, b) in v[8..].iter_mut().enumerate() {
            *b = ((i as usize + j) & 0xFF) as u8;
        }
        v
    }

    #[test]
    fn test_put_tape() -> Result<(), StoreError> {
        let (store, _temp_dir) = setup_store()?;
        let tape_number = 1;
        let address = Pubkey::new_unique();

        store.put_tape(tape_number, &address)?;
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
        let data = vec![0u8; tape_api::PACKED_SEGMENT_SIZE];

        store.put_segment(&address, global_seg_idx, data.clone())?;
        let retrieved_data = store.get_segment(&address, global_seg_idx)?;
        assert_eq!(retrieved_data, data);
        Ok(())
    }

    #[test]
    fn test_put_segment_count() -> Result<(), StoreError> {
        let (store, _temp_dir) = setup_store()?;
        let address = Pubkey::new_unique();
        let data = vec![0u8; tape_api::PACKED_SEGMENT_SIZE];

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
        let t13_data = vec![1u8; L13_NODES_PER_TAPE * 32];

        store.put_t13(&address, &t13_data)?;
        let retrieved_data = store.get_t13(&address)?;
        assert_eq!(retrieved_data, t13_data);

        // Test that m13 is not found
        let retrieved_m13 = store.get_m13(&address);
        assert!(retrieved_m13.is_err());

        // Test that m13 doesn't overwrite t13
        let m13_data = vec![2u8; L13_NODES_PER_TAPE * 32];
        store.put_m13(&address, &m13_data)?;

        let retrieved_m13 = store.get_m13(&address)?;
        assert_eq!(retrieved_m13, m13_data);
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
        store.put_tape(tape_number, &address)?;
        store.put_segment(&address, 0, vec![0u8; tape_api::PACKED_SEGMENT_SIZE])?;

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
            store.put_segment(&address, i, seg_with_pattern(i))?;
        }

        // Sector count should be 1
        assert_eq!(store.get_sector_count(&address)?, 1);

        // Segment count should equal SECTOR_LEAVES
        assert_eq!(store.get_segment_count(&address)?, SECTOR_LEAVES as u64);

        // Verify bitmap is all 1s for the first sector
        let sector0 = store.get_sector(&address, 0)?;
        let bitmap_len = SECTOR_LEAVES / 8;
        assert_eq!(sector0.len(), SECTOR_HEADER_BYTES + SECTOR_LEAVES * PACKED_SEGMENT_SIZE);
        for byte in &sector0[..bitmap_len] {
            assert_eq!(*byte, 0xFF);
        }

        // Spot-check a few reads across the sector
        for &idx in &[0u64, (SECTOR_LEAVES as u64) / 2, (SECTOR_LEAVES as u64) - 1] {
            let got = store.get_segment(&address, idx)?;
            assert_eq!(got, seg_with_pattern(idx));
        }

        Ok(())
    }

    #[test]
    fn test_sector_boundary() -> Result<(), StoreError> {
        let (store, _tmp) = setup_store()?;
        let address = Pubkey::new_unique();

        let last_in_s0 = (SECTOR_LEAVES as u64) - 1;
        let first_in_s1 = SECTOR_LEAVES as u64;

        store.put_segment(&address, last_in_s0, seg_with_pattern(last_in_s0))?;
        store.put_segment(&address, first_in_s1, seg_with_pattern(first_in_s1))?;

        // Two sectors should exist because we touched indices in both
        assert_eq!(store.get_sector_count(&address)?, 2);

        // Segment count is 2 (distinct indices)
        assert_eq!(store.get_segment_count(&address)?, 2);

        // Reads back
        assert_eq!(store.get_segment(&address, last_in_s0)?, seg_with_pattern(last_in_s0));
        assert_eq!(store.get_segment(&address, first_in_s1)?, seg_with_pattern(first_in_s1));

        // Bitmap spot check: last bit in sector 0 should be set, first bit in sector 1 set
        let s0 = store.get_sector(&address, 0)?;
        let s1 = store.get_sector(&address, 1)?;
        let bm_len = SECTOR_LEAVES / 8;

        // sector 0: last_in_s0 => byte = (SECTOR_LEAVES-1)/8, bit = 7
        let byte_idx0 = (SECTOR_LEAVES - 1) / 8;
        assert_eq!(s0[byte_idx0] & (1 << 7), 1 << 7);

        // sector 1: first_in_s1 local idx = 0 => byte 0 bit 0
        assert_eq!(s1[0] & 0x01, 0x01);

        // sanity: bitmap buffers exist
        assert!(s0.len() >= bm_len && s1.len() >= bm_len);

        Ok(())
    }

    #[test]
    fn test_two_full_sectors() -> Result<(), StoreError> {
        let (store, _tmp) = setup_store()?;
        let address = Pubkey::new_unique();

        let total = (SECTOR_LEAVES as u64) * 2;
        for i in 0..total {
            store.put_segment(&address, i, seg_with_pattern(i))?;
        }

        // Should have 2 sectors and full segment count
        assert_eq!(store.get_sector_count(&address)?, 2);
        assert_eq!(store.get_segment_count(&address)?, total);

        // Verify both sector bitmaps are all 1s
        let bm_len = SECTOR_LEAVES / 8;
        let s0 = store.get_sector(&address, 0)?;
        let s1 = store.get_sector(&address, 1)?;
        for b in &s0[..bm_len] { assert_eq!(*b, 0xFF); }
        for b in &s1[..bm_len] { assert_eq!(*b, 0xFF); }

        // Spot-check edges
        assert_eq!(store.get_segment(&address, 0)?, seg_with_pattern(0));
        assert_eq!(store.get_segment(&address, (SECTOR_LEAVES as u64) - 1)?, seg_with_pattern((SECTOR_LEAVES as u64) - 1));
        assert_eq!(store.get_segment(&address, SECTOR_LEAVES as u64)?, seg_with_pattern(SECTOR_LEAVES as u64));
        assert_eq!(store.get_segment(&address, total - 1)?, seg_with_pattern(total - 1));

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
                store.put_segment(&address, idx, seg_with_pattern(idx))?;
                written += 1;
            }
        }

        assert_eq!(store.get_sector_count(&address)?, sectors as usize);
        assert_eq!(store.get_segment_count(&address)?, written);

        // Verify a couple of bit positions within random sectors
        for s in 0..sectors {
            let sector = store.get_sector(&address, s)?;
            // Verify one of the three written bits per sector
            for k in 0..3usize {
                let li = k * (stride as usize);
                let b = sector[li / 8] & (1 << (li % 8));
                assert!(b != 0, "bitmap not set for sector {}, local {}", s, li);
            }
        }

        Ok(())
    }

    #[test]
    fn test_get_tape_segments() -> Result<(), StoreError> {
        let (store, _tmp) = setup_store()?;
        let address = Pubkey::new_unique();

        // Helper to create segment data with unique first byte
        let make_data = |marker: u8| vec![marker; PACKED_SEGMENT_SIZE];

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
            // First byte should match the "marker" we wrote
            assert_eq!(data[0], (i as u8 + 1) * 10, "segment data mismatch at index {}", idx);
            assert_eq!(data.len(), PACKED_SEGMENT_SIZE);
        }

        // Check that sector count is 3
        assert_eq!(store.get_sector_count(&address)?, 3);

        Ok(())
    }
}
