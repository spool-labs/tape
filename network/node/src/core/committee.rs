use std::collections::HashSet;

use solana_sdk::pubkey::Pubkey as SolanaPubkey;
use tape_api::program::tapedrive::node_pda;
use tape_core::erasure::group_for_spool;
use tape_core::spooler::SpoolGroup;
use tape_store::types::{NodeInfo, Pubkey};

pub fn our_member<'a>(
    committee: &'a [NodeInfo],
    authority: SolanaPubkey,
) -> Result<&'a NodeInfo, &'static str> {
    let (our_node_address, _) = node_pda(authority);
    let our_node_address = Pubkey::new(our_node_address.to_bytes());
    committee
        .iter()
        .find(|member| member.node_address == our_node_address)
        .ok_or("our node not present in committee")
}

pub fn our_member_index(
    committee: &[NodeInfo],
    authority: SolanaPubkey,
) -> Result<usize, &'static str> {
    let (our_node_address, _) = node_pda(authority);
    let our_node_address = Pubkey::new(our_node_address.to_bytes());
    committee
        .iter()
        .position(|member| member.node_address == our_node_address)
        .ok_or("our node not present in committee")
}

pub fn our_snapshot_groups(
    committee: &[NodeInfo],
    authority: SolanaPubkey,
) -> Result<HashSet<SpoolGroup>, &'static str> {
    let our_member = our_member(committee, authority)?;

    Ok(our_member
        .spools
        .iter()
        .copied()
        .map(group_for_spool)
        .collect())
}
