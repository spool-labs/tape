use steel::*;

mod blob;
mod committee;
mod epoch;
mod exchange;
mod feature;
mod node;
mod stake;
mod system;
mod tape;

pub use blob::*;
pub use committee::*;
pub use epoch::*;
pub use exchange::*;
pub use feature::*;
pub use node::*;
pub use stake::*;
pub use system::*;
pub use tape::*;

#[repr(u8)]
#[derive(Clone, Copy, Debug, Eq, PartialEq, TryFromPrimitive)]
pub enum TapeInstruction {
    Unknown = 0,

    // System
    Initialize,

    CreateEpoch,
    ExpandEpoch,
    AdvanceEpoch,

    CreateCommittee,
    ExpandCommittee,

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
    NominateNode,
    SetAuthority,
    SetNetworkAddress,
    SetNetworkTls,
    SetName,
    SetCommissionRate,
    ClaimCommission,
    AddToBlacklist,
    RemoveFromBlacklist,
    VoteOnStoragePrice,
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
instruction!(TapeInstruction, CreateEpoch);
instruction!(TapeInstruction, ExpandEpoch);
instruction!(TapeInstruction, AdvanceEpoch);
instruction!(TapeInstruction, CreateCommittee);
instruction!(TapeInstruction, ExpandCommittee);
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
instruction!(TapeInstruction, NominateNode);
instruction!(TapeInstruction, SetAuthority);
instruction!(TapeInstruction, SetNetworkAddress);
instruction!(TapeInstruction, SetNetworkTls);
instruction!(TapeInstruction, SetName);
instruction!(TapeInstruction, SetCommissionRate);
instruction!(TapeInstruction, ClaimCommission);
instruction!(TapeInstruction, AddToBlacklist);
instruction!(TapeInstruction, RemoveFromBlacklist);
instruction!(TapeInstruction, VoteOnStoragePrice);
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

