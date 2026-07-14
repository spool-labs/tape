use std::sync::Arc;

use solana_keypair::Keypair;
use solana_pubkey::Pubkey;
use tape_core::bls::BlsPrivateKey;
use tape_core::types::NodeId;

#[derive(Debug)]
pub struct HarnessNode {
    pub index: usize,
    pub node_id: NodeId,
    pub authority: Pubkey,
    pub node_address: Pubkey,
    pub member_index: Option<usize>,
    pub prev_member_index: Option<usize>,
    pub next_member_index: Option<usize>,
    keypair: Arc<Keypair>,
    bls_keypair: Arc<BlsPrivateKey>,
}

impl HarnessNode {
    pub(crate) fn new(
        index: usize,
        node_id: NodeId,
        authority: Pubkey,
        node_address: Pubkey,
        member_index: Option<usize>,
        prev_member_index: Option<usize>,
        next_member_index: Option<usize>,
        keypair: Arc<Keypair>,
        bls_keypair: Arc<BlsPrivateKey>,
    ) -> Self {
        Self {
            index,
            node_id,
            authority,
            node_address,
            member_index,
            prev_member_index,
            next_member_index,
            keypair,
            bls_keypair,
        }
    }

    pub fn keypair(&self) -> &Keypair {
        self.keypair.as_ref()
    }

    pub fn bls_keypair(&self) -> &BlsPrivateKey {
        self.bls_keypair.as_ref()
    }
}
