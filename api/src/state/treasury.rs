use tape_solana::*;
use super::AccountType;
use crate::state;

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Pod, Zeroable)]
pub struct Treasury {}

tape_solana::state!(AccountType, Treasury);
