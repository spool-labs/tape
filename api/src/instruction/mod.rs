use steel::*;

mod system;
mod exchange;
mod operator;
mod staking;
mod storage;
mod blob;

pub use system::*;
pub use exchange::*;
pub use operator::*;
pub use staking::*;
pub use storage::*;
pub use blob::*;

#[repr(u8)]
#[derive(Clone, Copy, Debug, Eq, PartialEq, TryFromPrimitive)]
pub enum TapeInstruction {
    Unknown = 0,

    // System
    Initialize,
    AdvanceEpoch,
    RegisterFeature,
    CertifyFeature,

    // Exchange
    RegisterExchange,
    SetExchangeRate,
    DepositTape,
    DepositSol,
    WithdrawTape,
    WithdrawSol,
    SwapForTape,
    SwapForSol,

    // Operator
    RegisterNode,
    JoinNetwork,
    SetAuthority,
    SetNetworkAddress,
    SetNetworkTls,
    SetName,
    SetCommissionRate,
    ClaimCommission,
    AddToBlacklist,
    RemoveFromBlacklist,
    VoteOnStoragePrice,
    VoteOnWritePrice,
    VoteOnShardSize,
    VoteOnFeature,

    // Staking
    StakeWithNode,
    UnstakeFromNode,
    ClaimStake,
    SplitStake,
    MergeStake,

    // Storage
    ReserveTape,
    BurnTape,
    SplitTapeByDuration,
    SplitTapeBySize,
    MergeTape,

    // Blob
    RegisterBlob,
    DeleteBlob,
    CertifyBlob,
    InvalidateBlob,
}

instruction!(TapeInstruction, Initialize);
instruction!(TapeInstruction, AdvanceEpoch);
instruction!(TapeInstruction, RegisterFeature);
instruction!(TapeInstruction, CertifyFeature);

instruction!(TapeInstruction, RegisterExchange);
instruction!(TapeInstruction, SetExchangeRate);
instruction!(TapeInstruction, DepositTape);
instruction!(TapeInstruction, DepositSol);
instruction!(TapeInstruction, WithdrawTape);
instruction!(TapeInstruction, WithdrawSol);
instruction!(TapeInstruction, SwapForTape);
instruction!(TapeInstruction, SwapForSol);

instruction!(TapeInstruction, RegisterNode);
instruction!(TapeInstruction, JoinNetwork);
instruction!(TapeInstruction, SetAuthority);
instruction!(TapeInstruction, SetNetworkAddress);
instruction!(TapeInstruction, SetNetworkTls);
instruction!(TapeInstruction, SetName);
instruction!(TapeInstruction, SetCommissionRate);
instruction!(TapeInstruction, ClaimCommission);
instruction!(TapeInstruction, AddToBlacklist);
instruction!(TapeInstruction, RemoveFromBlacklist);
instruction!(TapeInstruction, VoteOnStoragePrice);
instruction!(TapeInstruction, VoteOnWritePrice);
instruction!(TapeInstruction, VoteOnShardSize);
instruction!(TapeInstruction, VoteOnFeature);

instruction!(TapeInstruction, StakeWithNode);
instruction!(TapeInstruction, UnstakeFromNode);
instruction!(TapeInstruction, ClaimStake);
instruction!(TapeInstruction, SplitStake);
instruction!(TapeInstruction, MergeStake);

instruction!(TapeInstruction, ReserveTape);
instruction!(TapeInstruction, BurnTape);
instruction!(TapeInstruction, SplitTapeByDuration);
instruction!(TapeInstruction, SplitTapeBySize);
instruction!(TapeInstruction, MergeTape);

instruction!(TapeInstruction, RegisterBlob);
instruction!(TapeInstruction, DeleteBlob);
instruction!(TapeInstruction, CertifyBlob);
instruction!(TapeInstruction, InvalidateBlob);

