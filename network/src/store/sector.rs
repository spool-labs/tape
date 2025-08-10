use solana_sdk::pubkey::Pubkey;
use bytemuck::{Pod, Zeroable, try_from_bytes, bytes_of};
use tape_api::consts::*;
use super::{consts::*, TapeStore, error::StoreError, layout::ColumnFamily};

#[repr(transparent)]
#[derive(Clone, Copy, Debug)]
pub struct Sector(pub [u8; SECTOR_HEADER_BYTES + SECTOR_LEAVES * PACKED_SEGMENT_SIZE]);

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

pub trait SectorOps {
    fn get_sector(&self, tape_address: &Pubkey, sector_number: u64) -> Result<Sector, StoreError>;
    fn put_sector(&self, tape_address: &Pubkey, sector_number: u64, sector: &Sector) -> Result<(), StoreError>;
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
}
