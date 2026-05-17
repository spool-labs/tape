use bytemuck::{Pod, Zeroable};
use serde::{Deserialize, Serialize};
#[cfg(feature = "wincode")]
use wincode_derive::{SchemaRead, SchemaWrite};

use tape_crypto::address::Address;
use crate::bls::BlsPubkey;

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Pod, Zeroable, Serialize, Deserialize)]
#[cfg_attr(feature = "wincode", derive(SchemaRead, SchemaWrite))]
pub struct Spool {
    pub node: Address,
    pub bls_pubkey: BlsPubkey,
}

impl Spool {
    pub fn new(node: Address, bls_pubkey: BlsPubkey) -> Self {
        Spool { node, bls_pubkey }
    }
}
