//! Peer management: node directory, health tracking, and routing.

mod manager;
mod node;

pub use manager::{PeerManager, PeerManagerError, PeerStatus};
pub use node::{PeerNode, TrustedPeers};
