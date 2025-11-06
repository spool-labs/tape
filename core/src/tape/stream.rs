use bytemuck::{ Pod, Zeroable };
use num_enum::{ IntoPrimitive, TryFromPrimitive };
use tape_crypto::merkle::MerkleTree;
use crate::types::SlotNumber;

#[repr(u64)]
#[derive(Clone, Copy, Debug, Eq, PartialEq, IntoPrimitive, TryFromPrimitive)]
pub enum StreamState {
    Unknown = 0,
    Writing,   // Appends / Updates allowed
    Finalized, // No more writes allowed
}

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct StreamData<const H: usize> {
    /// The state of this stream.
    pub state: u64,

    /// The slot when this stream was registered.
    pub registered_slot: SlotNumber,

    /// The slot when this stream last written to.
    pub last_slot: SlotNumber,

    /// The total number of segments in this stream.
    pub num_segments: u64,

    /// The merkle tree of the stream data.
    pub commitment_tree: MerkleTree<H>,
}

unsafe impl<const H: usize> Zeroable for StreamData<H> {}
unsafe impl<const H: usize> Pod for StreamData<H> {}

impl<const H: usize> StreamData<H> {
    #[inline]
    pub const fn size() -> usize {
        core::mem::size_of::<StreamData::<{H}>>()
    }

    #[inline]
    pub fn get_state(&self) -> StreamState {
        StreamState::try_from(self.state).unwrap_or(StreamState::Unknown)
    }
}
