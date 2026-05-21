use bytemuck::{Pod, Zeroable};
use serde::{Deserialize, Serialize};
#[cfg(feature = "wincode")]
use wincode_derive::{SchemaRead, SchemaWrite};
use tape_crypto::address::Address;

use crate::types::*;

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Pod, Zeroable, Serialize, Deserialize)]
#[cfg_attr(feature = "wincode", derive(SchemaRead, SchemaWrite))]
pub struct Member {
    pub node: Address,
    pub stake: Coin<TAPE>,
    pub assigned: StorageUnits,
    pub refused: StorageUnits,
    pub spools: u64,
}

impl Member {
    pub fn new(node: Address, stake: Coin<TAPE>) -> Self {
        Member {
            node,
            stake,
            ..Member::zeroed()
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CommitteeError {
    AlreadyPresent { idx: usize },
    Full,
    NotFull,
    NotFound,
    NotBetter { min_idx: usize, min_stake: Coin<TAPE> },
    ZeroStake,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum CommitteeJoin {
    AlreadyPresent,
    Inserted,
    Replaced { removed: Member },
}

pub fn apply_member_join(
    members: &mut Vec<Member>,
    capacity: usize,
    member: Member,
) -> Result<CommitteeJoin, CommitteeError> {
    if member.stake == TAPE::zero() {
        return Err(CommitteeError::ZeroStake);
    }

    if members.iter().any(|m| m.node == member.node) {
        return Ok(CommitteeJoin::AlreadyPresent);
    }

    let result = if members.len() < capacity {
        members.push(member);
        CommitteeJoin::Inserted
    } else {
        let (min_idx, min_stake) = min_stake_member(members)
            .ok_or(CommitteeError::NotFull)?;

        if member.stake <= min_stake {
            return Err(CommitteeError::NotBetter { min_idx, min_stake });
        }

        let removed = members[min_idx];
        members[min_idx] = member;
        CommitteeJoin::Replaced { removed }
    };

    sort_members_for_committee(members);
    Ok(result)
}

pub fn apply_member_join_slice(
    members: &mut [Member],
    count: &mut u64,
    capacity: u64,
    member: Member,
) -> Result<CommitteeJoin, CommitteeError> {
    if member.stake == TAPE::zero() {
        return Err(CommitteeError::ZeroStake);
    }

    let count_usize = *count as usize;
    let capacity_usize = capacity as usize;
    if count_usize > members.len() || capacity_usize > members.len() {
        return Err(CommitteeError::Full);
    }

    let active = &mut members[..count_usize];
    if active.iter().any(|m| m.node == member.node) {
        return Ok(CommitteeJoin::AlreadyPresent);
    }

    let result = if count_usize < capacity_usize {
        members[count_usize] = member;
        *count = (*count).saturating_add(1);
        CommitteeJoin::Inserted
    } else {
        let (min_idx, min_stake) = min_stake_member(active)
            .ok_or(CommitteeError::NotFull)?;

        if member.stake <= min_stake {
            return Err(CommitteeError::NotBetter { min_idx, min_stake });
        }

        let removed = members[min_idx];
        members[min_idx] = member;
        CommitteeJoin::Replaced { removed }
    };

    sort_members_for_committee(&mut members[..(*count as usize)]);
    Ok(result)
}

pub fn sort_members_for_committee(members: &mut [Member]) {
    members.sort_by(|a, b| {
        b.stake
            .cmp(&a.stake)
            .then(a.node.as_bytes().cmp(b.node.as_bytes()))
    });
}

fn min_stake_member(members: &[Member]) -> Option<(usize, Coin<TAPE>)> {
    members
        .iter()
        .enumerate()
        .min_by_key(|(_, m)| m.stake)
        .map(|(idx, member)| (idx, member.stake))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn address(byte: u8) -> Address {
        let mut bytes = [0u8; 32];
        bytes[0] = byte;
        Address::new(bytes)
    }

    fn member(byte: u8, stake: u64) -> Member {
        Member {
            node: address(byte),
            stake: TAPE(stake),
            ..Member::zeroed()
        }
    }

    #[test]
    fn member_join_sorts_by_stake_desc_then_address() {
        let mut members = vec![member(2, 10), member(1, 10)];

        apply_member_join(&mut members, 3, member(3, 20)).expect("join");

        assert_eq!(members[0].node, address(3));
        assert_eq!(members[1].node, address(1));
        assert_eq!(members[2].node, address(2));
    }

    #[test]
    fn member_join_replaces_lowest_stake_when_full() {
        let mut members = vec![member(1, 10), member(2, 20)];

        let result = apply_member_join(&mut members, 2, member(3, 30)).expect("join");

        assert!(matches!(result, CommitteeJoin::Replaced { removed } if removed.node == address(1)));
        assert_eq!(members[0].node, address(3));
        assert_eq!(members[1].node, address(2));
    }
}
