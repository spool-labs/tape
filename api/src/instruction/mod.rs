use steel::*;

pub mod program;
pub mod staking;
pub mod data;

pub use program::*;
pub use staking::*;
pub use data::*;

#[repr(u8)]
#[derive(Clone, Copy, Debug, Eq, PartialEq, TryFromPrimitive)]
pub enum TapeInstruction {
    Unknown = 0,

    Initialize,
    Airdrop,
    AdvanceEpoch,

    RegisterNode,
    UnregisterNode,

    SetAuthority,
    SetNetworkAddress,
    SetNetworkTls,
    SetName,

    SetCommissionRate,
    ClaimCommission,

    Stake,
    Unstake,
    Claim,
    Split,
    Merge,
}

instruction!(TapeInstruction, Initialize);
instruction!(TapeInstruction, AdvanceEpoch);
instruction!(TapeInstruction, Airdrop);

instruction!(TapeInstruction, RegisterNode);
//instruction!(TapeInstruction, UnregisterNode);
instruction!(TapeInstruction, SetAuthority);
instruction!(TapeInstruction, SetNetworkAddress);
instruction!(TapeInstruction, SetNetworkTls);
instruction!(TapeInstruction, SetName);
instruction!(TapeInstruction, SetCommissionRate);
instruction!(TapeInstruction, ClaimCommission);

instruction!(TapeInstruction, Stake);
instruction!(TapeInstruction, Unstake);
instruction!(TapeInstruction, Claim);
instruction!(TapeInstruction, Split);
instruction!(TapeInstruction, Merge);

