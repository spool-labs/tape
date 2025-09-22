use bytemuck::{Pod, Zeroable};
use crate::types::NodeId;

/// A fixed-size set of (NodeId, stake) pairs, sorted by NodeId (ascending).
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct StakingSet<const N: usize> {
    pub len: u16,               // number of valid entries (<= N and <= u16::MAX)
    pub node_ids: [NodeId; N],  // sorted by NodeId (ascending)
    pub stakes: [u64; N],       // stakes[i] belongs to node_ids[i]
}

unsafe impl<const N: usize> Zeroable for StakingSet<N> {}
unsafe impl<const N: usize> Pod for StakingSet<N> {}

impl<const N: usize> StakingSet<N> {
    pub fn size_bytes() -> usize {
        std::mem::size_of::<Self>()
    }

    pub fn new(mut items: Vec<(NodeId, u64)>) -> Self {
        items.sort_by_key(|(id, _)| *id);
        assert!(items.len() <= N, "Too many items for N");

        let mut out: Self = Zeroable::zeroed();
        out.len = items.len() as u16;
        for (i, (id, st)) in items.into_iter().enumerate() {
            out.node_ids[i] = id;
            out.stakes[i] = st;
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn size_bytes_smoke() {
        // just make sure it compiles/works and is Pod/Zeroable
        let _ = StakingSet::<8>::size_bytes();
        let _: StakingSet<4> = Zeroable::zeroed();
    }
}
