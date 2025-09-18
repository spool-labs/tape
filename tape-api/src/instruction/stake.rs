use steel::*;
use crate::pda::*;

#[repr(u8)]
#[derive(Clone, Copy, Debug, Eq, PartialEq, TryFromPrimitive)]
pub enum StakeInstruction {
    Stake = 0x50,
    Unstake,
    Claim,
    Split,
    Merge,
}

instruction!(StakeInstruction, Stake);
instruction!(StakeInstruction, Unstake);
instruction!(StakeInstruction, Claim);
instruction!(StakeInstruction, Split);
instruction!(StakeInstruction, Merge);

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct Stake {
    pub amount: [u8; 8],
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct Unstake {}

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct Claim {}

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct Split {
    pub amount: [u8; 8],
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct Merge {}


