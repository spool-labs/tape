use tape_solana::*;
use tape_core::system::Peer;
use tape_core::types::Tail;
use super::AccountType;
use crate::dynamic::DynamicState;

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Pod, Zeroable)]
pub struct PeerSet {
    /// Current, previous, and next committee member peers.
    pub peers: Tail<Peer>,
}

tape_solana::state!(AccountType, PeerSet);

impl DynamicState for PeerSet {
    type Entry = Peer;
    fn tail(&self)     -> &Tail<Peer>     { &self.peers }
    fn tail_mut(&mut self) -> &mut Tail<Peer> { &mut self.peers }
}

