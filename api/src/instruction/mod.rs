use steel::*;

pub mod data;
pub mod exchange;
pub mod program;
pub mod staking;

pub use data::*;
pub use exchange::*;
pub use program::*;
pub use staking::*;

#[repr(u8)]
#[derive(Clone, Copy, Debug, Eq, PartialEq, TryFromPrimitive)]
pub enum TapeInstruction {
    Unknown = 0,

    // Core
    Initialize,
    Airdrop,
    AdvanceEpoch,

    // Exchange
    RegisterExchange,
    SetExchangeRate,
    SetExchangeAuthority,
    DepositTape,
    DepositSol,
    WithdrawTape,
    WithdrawSol,
    SwapForTape,
    SwapForSol,

    // Staking
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

instruction!(TapeInstruction, RegisterExchange);
instruction!(TapeInstruction, SetExchangeRate);
instruction!(TapeInstruction, SetExchangeAuthority);
instruction!(TapeInstruction, DepositTape);
instruction!(TapeInstruction, DepositSol);
instruction!(TapeInstruction, WithdrawTape);
instruction!(TapeInstruction, WithdrawSol);
instruction!(TapeInstruction, SwapForTape);
instruction!(TapeInstruction, SwapForSol);

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

