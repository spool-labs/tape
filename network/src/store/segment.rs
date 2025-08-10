use solana_sdk::pubkey::Pubkey;
use rocksdb::WriteBatch;
use bytemuck::bytes_of;
use tape_api::consts::PACKED_SEGMENT_SIZE;
use super::{
    consts::*, 
    sector::*,
    TapeStore,
    error::StoreError, 
    layout::ColumnFamily, 
};
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
