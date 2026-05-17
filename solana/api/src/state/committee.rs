use tape_solana::*;
use tape_core::system::Member;
use tape_core::types::{EpochNumber, Tail};
use super::AccountType;
use crate::dynamic::DynamicState;

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Pod, Zeroable)]
pub struct Committee {
    /// The epoch this committee belongs to.
    pub epoch: EpochNumber,

    /// Active members.
    pub members: Tail<Member>,
}

tape_solana::state!(AccountType, Committee);

impl DynamicState for Committee {
    type Entry = Member;

    fn tail(&self) -> &Tail<Member> { &self.members }
    fn tail_mut(&mut self) -> &mut Tail<Member> { &mut self.members }
}

