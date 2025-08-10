use bytemuck::{Pod, Zeroable};
use tape_api::consts::*;
use super::consts::*;

#[repr(transparent)]
#[derive(Clone, Copy, Debug)]
pub struct Sector(pub [u8; SECTOR_HEADER_BYTES + SECTOR_LEAVES * PACKED_SEGMENT_SIZE]);

// Safe: transparent wrapper over a byte array
unsafe impl Zeroable for Sector {}
unsafe impl Pod for Sector {}

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
